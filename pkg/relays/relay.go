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
	"github.com/vertex-lab/crawler_v2/pkg/relays/subscription"
	"github.com/vertex-lab/crawler_v2/pkg/relays/watchdog"
)

var (
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

	subs *subscription.Manager
	auth *auth.Handler
	ping *watchdog.T

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
		requests: make(chan Request, 1000),
		settings: newSettings(),
		subs:     subscription.NewManager(1000),
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

// Close disconnects the relay, signalling the read and write goroutines to stop.
// Multiple calls to Close are a no-op.
func (r *Relay) Close() {
	if r.isClosing.CompareAndSwap(false, true) {
		close(r.done)
		r.subs.CloseAll()
	}
}

// Query sends a REQ to the relay with the given id and filters.
// When it receives an EOSE, it returns all previous events and closes the subscription.
// When it receives a CLOSED, it returns the events collected thus far and the closed reason as an error.
//
// It is always recommended to use this method with a context timeout (e.g. 10s),
// to avoid bad relays that never send an EOSE (or CLOSED) from blocking indefinitely.
func (r *Relay) Query(ctx context.Context, id string, filters nostr.Filters) ([]nostr.Event, error) {
	if r.isClosing.Load() {
		return nil, ErrDisconnected
	}

	req := Req{
		ID:      id,
		Filters: filters,
	}

	sub, err := r.subs.New(req.ID)
	if err != nil {
		return nil, err
	}

	if err := r.send(req); err != nil {
		r.subs.Close(req.ID)
		return nil, err
	}

	var events []nostr.Event
	for {
		select {
		case <-r.done:
			return events, ErrDisconnected

		case <-ctx.Done():
			r.send(Close{ID: req.ID})
			r.subs.Close(req.ID)
			return events, ctx.Err()

		case msg, ok := <-sub.Messages():
			if !ok {
				// The channel is closed without a Closed message because it was full.
				return events, ErrSubscriptionClosed
			}

			switch {
			case msg.Event != nil:
				events = append(events, *msg.Event)

			case msg.EOSE:
				r.send(Close{ID: req.ID})
				r.subs.Close(req.ID)
				return events, nil

			case msg.Err != nil:
				// ForceClose already removed the subscription from the manager.
				return events, fmt.Errorf("%w: %w", ErrSubscriptionClosed, msg.Err)
			}
		}
	}
}

// Subscribe sends a REQ to the relay with the given id and filters, returning the underlying subscription.
// Callers can read messages using the Subscription.Messages() channel.
// Callers must close the subscription when done, by calling the returned cancel function.
func (r *Relay) Subscribe(id string, filters nostr.Filters) (sub *subscription.T, cancel func(), err error) {
	if r.isClosing.Load() {
		return nil, nil, ErrDisconnected
	}

	req := Req{
		ID:      id,
		Filters: filters,
	}

	sub, err = r.subs.New(req.ID)
	if err != nil {
		return nil, nil, err
	}

	if err := r.send(req); err != nil {
		r.subs.Close(req.ID)
		return nil, nil, err
	}

	cancel = func() {
		r.send(Close{ID: req.ID})
		r.subs.Close(req.ID)
	}
	return sub, cancel, nil
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
				slog.Error("failed to marshal request", "error", err)
				return
			}

			if err := r.writeMessage(bytes); err != nil {
				if isUnexpectedClose(err) {
					slog.Error("unexpected error when attemping to write", "error", err)
				}
				return
			}

		case <-ticker.C:
			if err := r.writePing(); err != nil {
				if isUnexpectedClose(err) {
					slog.Error("unexpected error when attemping to ping", "error", err)
				}
				return
			}
			// r.ping.Arm()
		}
	}
}

// read consumes incoming messages from the websocket connection.
func (r *Relay) read() {
	defer r.Close()

	r.conn.SetReadLimit(r.settings.WS.maxMessageSize)
	// r.conn.SetPongHandler(func(_ string) error { r.ping.Disarm(); return nil })

	for {
		select {
		case <-r.done:
			return

		default:
			// proceed
		}

		msgType, reader, err := r.conn.NextReader()
		if err != nil {
			if isUnexpectedClose(err) {
				slog.Error("unexpected close error from relay", "relay", r.url, "error", err)
			}
			return
		}

		if msgType != ws.TextMessage {
			slog.Warn("received binary message", "relay", r.url)
			continue
		}

		decoder := json.NewDecoder(reader)
		label, err := parseLabel(decoder)
		if err != nil {
			slog.Error("failed to parse label", "relay", r.url, "error", err)
			continue
		}

		switch label {
		case "EVENT":
			msg, err := parseEvent(decoder)
			if err != nil {
				slog.Error("failed to parse event", "relay", r.url, "error", err)
				continue
			}
			r.subs.Route(msg.SubID, &msg.Event)

		case "CLOSED":
			closed, err := parseClosed(decoder)
			if err != nil {
				slog.Error("failed to parse closed", "relay", r.url, "error", err)
				continue
			}
			r.subs.ForceClose(closed.ID, closed.Message)

		case "EOSE":
			eose, err := parseEOSE(decoder)
			if err != nil {
				slog.Error("failed to parse eose", "relay", r.url, "error", err)
				continue
			}
			r.subs.EOSE(eose.ID)

		case "AUTH":
			if r.auth == nil {
				// auth handler not configured, skip
				continue
			}

			auth, err := parseAuth(decoder)
			if err != nil {
				slog.Error("failed to parse auth", "relay", r.url, "error", err)
				continue
			}

			// after receiving the challenge we immediately auth
			r.auth.SetChallenge(auth.Challenge)
			response, err := r.auth.Response()
			if err != nil {
				slog.Error("failed to generate auth response", "relay", r.url, "error", err)
				continue
			}
			r.send(Auth{Event: response})

		case "OK":
			// this is in response to our AUTH message.
			// We don't need to do anything with it yet.

		case "NOTICE":
			notice, err := parseNotice(decoder)
			if err != nil {
				slog.Error("failed to parse notice", "relay", r.url, "error", err)
				continue
			}
			slog.Info("received notice message", "relay", r.url, "message", notice.Message)

		default:
			slog.Debug("received unknown message", "relay", r.url, "label", label)
		}
		// messages are intentionally discarded for now
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

func logNoPong(url string) func() {
	return func() {
		slog.Warn("the relay did not respond to a PING", "relay", url)
	}
}
