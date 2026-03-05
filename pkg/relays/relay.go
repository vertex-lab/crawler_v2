package relays

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	ws "github.com/gorilla/websocket"
	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/relays/auth"
)

var (
	ErrInvalidID        = errors.New("invalid event id")
	ErrInvalidSignature = errors.New("invalid event signature")

	ErrDisconnected       = errors.New("relay has been disconnected")
	ErrSendFailed         = errors.New("failed to send message, channel is full")
	ErrSubscriptionClosed = errors.New("subscription was closed")
)

// Relay is a read-only representation of a Nostr relay.
// Create one with New, interact with Query or Subscribe, then call Close to close it.
type Relay struct {
	url      string
	conn     *ws.Conn
	requests chan Request
	settings settings

	subs *subRouter
	auth *auth.Handler
	log  *slog.Logger

	isClosing atomic.Bool
	done      chan struct{}
}

// New returns a connected Relay.
// The context is only used to establish the connection; it does not control the lifetime of the relay.
// Call relay.Close to close the connection and free resources.
func New(ctx context.Context, url string, opts ...Option) (*Relay, error) {
	if err := ValidateURL(url); err != nil {
		return nil, err
	}

	r := &Relay{
		url:      url,
		requests: make(chan Request, 100),
		settings: newSettings(),
		subs:     newRouter(true),
		log:      slog.Default(),
		done:     make(chan struct{}),
	}

	for _, opt := range opts {
		if err := opt(r); err != nil {
			return nil, err
		}
	}

	conn, _, err := r.settings.WS.dialer.DialContext(ctx, r.url, nil)
	if err != nil {
		return nil, err
	}
	r.conn = conn

	go r.read()
	go r.write()
	return r, nil
}

// URL returns the URL of the relay.
func (r *Relay) URL() string {
	return r.url
}

// Close disconnects the relay, closing all subscriptions and stopping the read and write goroutines.
// Multiple calls to Close are a no-op.
func (r *Relay) Close() {
	if r.isClosing.CompareAndSwap(false, true) {
		close(r.done)
		r.subs.Clear(ErrDisconnected)
	}
}

// Done returns a channel that is closed when the relay is disconnected,
// useful for reacting to relay closures.
func (r *Relay) Done() <-chan struct{} {
	return r.done
}

// Subscribe sends a REQ to the relay with the given id and filters, returning the underlying subscription.
// Callers can read messages using the [Subscription.Events] channel.
// Callers are responsible for calling [Subscription.Close] when done.
func (r *Relay) Subscribe(id string, filters nostr.Filters) (*Subscription, error) {
	if r.isClosing.Load() {
		return nil, fmt.Errorf("failed to subscribe: %w", ErrDisconnected)
	}

	s := &Subscription{
		id:      id,
		filters: filters,
		relay:   r,
		events:  make(chan *nostr.Event, 1000),
		eose:    make(chan struct{}),
		done:    make(chan struct{}),
	}

	if err := r.subs.Add(s); err != nil {
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	if err := r.send(Req{ID: id, Filters: filters}); err != nil {
		r.subs.Remove(id)
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}
	return s, nil
}

// Query sends a REQ to the relay with the given id and filters.
// When it receives an EOSE, it returns all previous events and closes the subscription.
// When it receives a CLOSED, it returns the events collected thus far and the closed reason as an error.
//
// It is always recommended to use this method with a context timeout (e.g. 10s),
// to avoid bad relays that never send an EOSE (or CLOSED) from blocking indefinitely.
func (r *Relay) Query(ctx context.Context, id string, filters nostr.Filters) ([]nostr.Event, error) {
	s, err := r.Subscribe(id, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}
	defer s.Close()

	var events []nostr.Event
	for {
		select {
		case <-ctx.Done():
			return events, fmt.Errorf("failed to query: %w", ctx.Err())

		case <-s.Done():
			return events, fmt.Errorf("failed to query: %w", s.Err())

		case <-s.EOSE():
			// drain buffered events at the time the EOSE was received.
			// Subsequent events will likely be new events from the subscription
			n := len(s.events)
			for range n {
				event := <-s.events
				events = append(events, *event)
			}
			return events, nil

		case event := <-s.Events():
			events = append(events, *event)
		}
	}
}

// Send enqueues a request to be sent to the relay.
// Returns an error if the relay is disconnected or the requests channel is full.
func (r *Relay) send(request Request) error {
	if r.isClosing.Load() {
		return ErrDisconnected
	}

	select {
	case r.requests <- request:
		return nil

	case <-r.done:
		return ErrDisconnected

	default:
		return ErrSendFailed
	}
}

// read consumes incoming messages from the websocket connection.
func (r *Relay) read() {
	defer r.Close()

	r.conn.SetReadLimit(r.settings.WS.maxMessageSize)

	for {
		if r.isClosing.Load() {
			return
		}

		msgType, reader, err := r.conn.NextReader()
		if err != nil {
			if isUnexpectedClose(err) {
				r.log.Error("unexpected close error from relay", "relay", r.url, "error", err)
			}
			return
		}

		if msgType != ws.TextMessage {
			r.log.Warn("received binary message", "relay", r.url)
			continue
		}

		decoder := json.NewDecoder(reader)
		label, err := parseLabel(decoder)
		if err != nil {
			r.log.Error("failed to parse label", "relay", r.url, "error", err)
			continue
		}

		switch label {
		case "EVENT":
			msg, err := parseEvent(decoder)
			if err != nil {
				r.log.Error("failed to parse event", "relay", r.url, "error", err)
				continue
			}

			if err := verify(&msg.Event); err != nil {
				r.log.Error("failed to verify event", "relay", r.url, "error", err)
				continue
			}

			if err := r.subs.Route(msg.SubID, &msg.Event); err != nil {
				r.log.Error("failed to route event", "relay", r.url, "error", err)
			}

		case "CLOSED":
			closed, err := parseClosed(decoder)
			if err != nil {
				r.log.Error("failed to parse closed", "relay", r.url, "error", err)
				continue
			}
			r.subs.SignalClosed(closed.ID, closed.Message)

		case "EOSE":
			eose, err := parseEOSE(decoder)
			if err != nil {
				r.log.Error("failed to parse eose", "relay", r.url, "error", err)
				continue
			}
			r.subs.SignalEOSE(eose.ID)

		case "AUTH":
			if r.auth == nil {
				// auth handler not configured, skip
				continue
			}

			auth, err := parseAuth(decoder)
			if err != nil {
				r.log.Error("failed to parse auth", "relay", r.url, "error", err)
				continue
			}

			// after receiving the challenge we immediately auth
			r.auth.SetChallenge(auth.Challenge)
			response, err := r.auth.Response()
			if err != nil {
				r.log.Error("failed to generate auth response", "relay", r.url, "error", err)
				continue
			}

			err = r.send(Auth{Event: response})
			if err != nil && !errors.Is(err, ErrDisconnected) {
				r.log.Warn("failed to send auth response", "relay", r.url, "error", err)
			}

		case "OK":
			// this is in response to our AUTH message.
			// We don't need to do anything with it yet.

		case "NOTICE":
			notice, err := parseNotice(decoder)
			if err != nil {
				r.log.Error("failed to parse notice", "relay", r.url, "error", err)
				continue
			}
			r.log.Info("received notice message", "relay", r.url, "message", notice.Message)

		default:
			r.log.Debug("received unknown message", "relay", r.url, "label", label)
		}
	}
}

// write reads from the requests channel and forwards each message to the websocket connection.
// When done is closed it sends a clean close frame and shuts down the connection.
func (r *Relay) write() {
	ticker := time.NewTicker(r.settings.WS.pingPeriod)
	defer func() {
		ticker.Stop()
		r.Close()
		r.conn.Close()
	}()

	for {
		select {
		case <-r.done:
			r.writeClose()
			return

		case request := <-r.requests:
			bytes, err := request.MarshalJSON()
			if err != nil {
				r.log.Error("failed to marshal request", "error", err)
				return
			}

			if err := r.writeMessage(bytes); err != nil {
				if isUnexpectedClose(err) {
					r.log.Error("unexpected error when attemping to write", "error", err)
				}
				return
			}

		case <-ticker.C:
			if err := r.writePing(); err != nil {
				if isUnexpectedClose(err) {
					r.log.Error("unexpected error when attemping to ping", "error", err)
				}
				return
			}
		}
	}
}

// Compare compares two Relay URLs for sorting.
func Compare(r1, r2 *Relay) int {
	return cmp.Compare(r1.url, r2.url)
}

// ValidateURL validates a Relay URL.
func ValidateURL(u string) error {
	if u == "" {
		return errors.New("empty url")
	}

	parsed, err := url.Parse(u)
	if err != nil {
		return err
	}
	if parsed.Scheme != "wss" && parsed.Scheme != "ws" {
		return fmt.Errorf("invalid url scheme: %s", parsed.Scheme)
	}
	return nil
}

func verify(e *nostr.Event) error {
	if !e.CheckID() {
		return ErrInvalidID
	}
	match, err := e.CheckSignature()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSignature, err)
	}
	if !match {
		return ErrInvalidSignature
	}
	return nil
}

func (r *Relay) writeMessage(b []byte) error {
	r.conn.SetWriteDeadline(time.Now().Add(r.settings.WS.writeWait))
	return r.conn.WriteMessage(ws.TextMessage, b)
}

func (r *Relay) writeClose() error {
	return r.conn.WriteControl(
		ws.CloseMessage,
		ws.FormatCloseMessage(ws.CloseNormalClosure, ""),
		time.Now().Add(r.settings.WS.writeWait),
	)
}

func (r *Relay) writePing() error {
	return r.conn.WriteControl(
		ws.PingMessage,
		nil,
		time.Now().Add(r.settings.WS.writeWait),
	)
}

func isUnexpectedClose(err error) bool {
	return ws.IsUnexpectedCloseError(err,
		ws.CloseNormalClosure,
		ws.CloseNoStatusReceived,
		ws.CloseAbnormalClosure)
}
