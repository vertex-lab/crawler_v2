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

	subs *subscription.Router
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
		subs:     subscription.NewRouter(true),
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
		r.subs.Clear()
	}
}

// Done returns a channel that is closed when the relay is closed.
// Done is useful for reacting to relay closures.
func (r *Relay) Done() <-chan struct{} {
	return r.done
}

type Subscription struct {
	ID      string
	Filters nostr.Filters
	C       <-chan subscription.Message
	close   func()
}

// Close closes the subscription, releasing resources.
func (s *Subscription) Close() {
	s.close()
}

// Query sends a REQ to the relay with the given id and filters.
// When it receives an EOSE, it returns all previous events and closes the subscription.
// When it receives a CLOSED, it returns the events collected thus far and the closed reason as an error.
//
// It is always recommended to use this method with a context timeout (e.g. 10s),
// to avoid bad relays that never send an EOSE (or CLOSED) from blocking indefinitely.
func (r *Relay) Query(ctx context.Context, id string, filters nostr.Filters) ([]nostr.Event, error) {
	if r.isClosing.Load() {
		return nil, fmt.Errorf("failed to query: %w", ErrDisconnected)
	}

	sub := make(chan subscription.Message, 1000)
	if err := r.subscribe(id, filters, sub); err != nil {
		return nil, err
	}

	var events []nostr.Event
	for {
		select {
		case <-r.done:
			return events, fmt.Errorf("failed to query: %w", ErrDisconnected)

		case <-ctx.Done():
			r.send(Close{ID: id})
			r.subs.Remove(id)
			return events, fmt.Errorf("failed to query: %w", ctx.Err())

		case msg := <-sub:
			switch {
			case msg.Event != nil:
				events = append(events, *msg.Event)

			case msg.EOSE:
				r.send(Close{ID: id})
				r.subs.Remove(id)
				return events, nil

			case msg.Err != nil:
				return events, fmt.Errorf("failed to query: %w: %w", ErrSubscriptionClosed, msg.Err)
			}
		}
	}
}

// Subscribe sends a REQ to the relay with the given id and filters, returning the underlying subscription.
// Callers can read messages using the [Subscription.C] channel.
// Callers are responsible for calling [Subscription.Close] when done.
func (r *Relay) Subscribe(id string, filters nostr.Filters) (*Subscription, error) {
	if r.isClosing.Load() {
		return nil, fmt.Errorf("failed to subscribe: %w", ErrDisconnected)
	}

	c := make(chan subscription.Message, 1000)
	if err := r.subscribe(id, filters, c); err != nil {
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	sub := &Subscription{
		ID:      id,
		Filters: filters,
		C:       c,
		close: func() {
			r.send(Close{ID: id})
			r.subs.Remove(id)
		},
	}
	return sub, nil
}

// subscribe sends a REQ to the relay with the given id and filters,
// and associates the given channel with the subscription in the relay's router.
func (r *Relay) subscribe(id string, filters nostr.Filters, ch chan subscription.Message) error {
	if err := r.subs.Add(id, filters, ch); err != nil {
		return err
	}

	req := Req{ID: id, Filters: filters}
	if err := r.send(req); err != nil {
		r.subs.Remove(id)
		return err
	}
	return nil
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
	// r.conn.SetPongHandler(func(_ string) error { r.ping.Disarm(); return nil })

	for {
		if r.isClosing.Load() {
			return
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

			if err := r.subs.RouteEvent(msg.SubID, &msg.Event); err != nil {
				slog.Error("failed to route event", "relay", r.url, "error", err)
			}

		case "CLOSED":
			closed, err := parseClosed(decoder)
			if err != nil {
				slog.Error("failed to parse closed", "relay", r.url, "error", err)
				continue
			}
			r.subs.RouteClosed(closed.ID, closed.Message)

		case "EOSE":
			eose, err := parseEOSE(decoder)
			if err != nil {
				slog.Error("failed to parse eose", "relay", r.url, "error", err)
				continue
			}

			if err := r.subs.RouteEOSE(eose.ID); err != nil {
				slog.Error("failed to route eose", "relay", r.url, "error", err)
			}

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
