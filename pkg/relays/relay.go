package relays

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	ws "github.com/gorilla/websocket"
	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/relays/auth"
)

// T is a read-only representation of a Nostr relay.
// Create one with New, interact with Query or Subscribe, then call Close to close it.
// All methods are safe for concurrent use.
type T struct {
	url      string
	conn     *ws.Conn
	requests chan Request

	subs *subRouter
	auth *auth.Handler
	log  *slog.Logger

	// TODO: we could add a Hooks struct
	settings relaySettings

	wg        sync.WaitGroup
	isClosing atomic.Bool
	done      chan struct{}
	err       error // reason for closing
}

// New returns a connected Relay.
// The context is only used to establish the connection; it does not control the lifetime of the relay.
// Call relay.Close to close the relay and free resources.
func New(ctx context.Context, url string, opts ...RelayOption) (*T, error) {
	if err := ValidateURL(url); err != nil {
		return nil, err
	}

	r := &T{
		url:      url,
		requests: make(chan Request, 100),
		settings: defaultRelaySettings(),
		subs:     newRouter(true),
		log:      slog.Default(),
		done:     make(chan struct{}),
	}

	for _, opt := range opts {
		if err := opt.applyRelay(r); err != nil {
			return nil, err
		}
	}

	conn, resp, err := r.settings.WS.dialer.DialContext(ctx, r.url, nil)
	if err != nil {
		connErr := &ConnectErr{
			URL:   r.url,
			Cause: err,
		}
		if resp != nil {
			connErr.StatusCode = resp.StatusCode
		}
		return nil, connErr
	}
	r.conn = conn

	r.wg.Go(r.read)
	r.wg.Go(r.write)
	return r, nil
}

// URL returns the URL of the relay.
func (r *T) URL() string {
	return r.url
}

// Close disconnects the relay, closing all subscriptions and stopping the read and write goroutines.
// Multiple calls to Close are a no-op.
func (r *T) Close() {
	r.close(nil)
	r.wg.Wait()
}

// close closes the relay with the given error.
func (r *T) close(err error) {
	if r.isClosing.CompareAndSwap(false, true) {
		r.err = err
		close(r.done)
		r.subs.Clear()
	}
}

// Done returns a channel that is closed when the relay is disconnected,
// useful for reacting to relay closures.
func (r *T) Done() <-chan struct{} {
	return r.done
}

// Err returns the reason for the relay disconnection.
// If the relay is still alive (Done hasn't fired), or if it was closed with
// Relay.Close, Err returns nil. Otherwise, it returns the underlying connection error.
func (r *T) Err() error {
	// We can't use r.isClosing directly because it would exist
	// a brief time where the isClosing is true but the err hasn't been set.
	// Using the channel is safe because we always set the error and then close the channel.
	select {
	case <-r.done:
		return r.err
	default:
		return nil
	}
}

// IsAlive returns true if the relay is still alive (not disconnected).
func (r *T) IsAlive() bool {
	return !r.isClosing.Load()
}

// open opens a subscription on the relay.
// This method allows the caller to directly specify the subscription and its channels,
// allowing for deeper control and the ability to re-use channels across subscriptions.
func (r *T) open(s *Subscription) error {
	if s.relay == nil {
		s.relay = r
	}
	if s.relay != r {
		return errors.New("subscription belongs to a different relay")
	}

	if err := r.subs.Add(s); err != nil {
		return err
	}
	if err := r.send(Req{ID: s.id, Filters: s.filters}); err != nil {
		r.subs.Remove(s.id)
		return err
	}
	return nil
}

// Subscribe sends a REQ to the relay with the given ID and filters, returning the underlying subscription.
// Callers can read messages using the [Subscription.Events] channel.
// Callers are responsible for calling [Subscription.Close] when done.
//
// The provided ID is used as the wire subscription ID sent to the relay. Callers should avoid
// reusing the same ID for another subscription or query immediately after closing it,
// as relays may still deliver late messages for the previous subscription.
func (r *T) Subscribe(id string, filters ...nostr.Filter) (*Subscription, error) {
	if r.isClosing.Load() {
		return nil, fmt.Errorf("failed to subscribe: %w", ErrDisconnected)
	}
	if len(filters) == 0 {
		return nil, nil
	}

	s := &Subscription{
		id:      id,
		filters: filters,
		relay:   r,
		events:  make(chan *nostr.Event, 1000),
		eose:    make(chan struct{}),
		done:    make(chan struct{}),
	}

	if err := r.open(s); err != nil {
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}
	return s, nil
}

// Query sends a REQ to the relay with the given ID and filters.
// When it receives an EOSE, it returns all previous events and closes the subscription.
// When it receives a CLOSED, it returns the events collected thus far and the closed reason as an error.
//
// The provided ID is used as the wire subscription ID sent to the relay. Callers should avoid
// reusing the same ID for another query or subscription immediately after Query returns,
// as relays may still deliver late messages for the previous query.
//
// It is always recommended to use this method with a context timeout (e.g. 10s),
// to avoid bad relays that never send an EOSE (or CLOSED) from blocking indefinitely.
func (r *T) Query(ctx context.Context, id string, filters ...nostr.Filter) ([]nostr.Event, error) {
	if r.isClosing.Load() {
		return nil, fmt.Errorf("failed to query: %w", ErrDisconnected)
	}
	if len(filters) == 0 {
		return nil, nil
	}

	s := &Subscription{
		id:      id,
		filters: filters,
		relay:   r,
		events:  make(chan *nostr.Event, 1000),
		eose:    make(chan struct{}),
		done:    make(chan struct{}),
	}

	if err := r.open(s); err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}
	defer s.Close()

	events := make([]nostr.Event, 0, expectedSize(filters))
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
func (r *T) send(request Request) error {
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
func (r *T) read() {
	r.conn.SetReadLimit(r.settings.WS.maxMessageSize)

	for {
		if r.isClosing.Load() {
			return
		}

		msgType, reader, err := r.conn.NextReader()
		if err != nil {
			if isUnexpectedClose(err) {
				r.log.Debug("unexpected close error from relay", "relay", r.url, "error", err)
			}
			r.close(err)
			return
		}

		if msgType != ws.TextMessage {
			r.log.Debug("received binary message", "relay", r.url)
			continue
		}

		decoder := json.NewDecoder(reader)
		label, err := parseLabel(decoder)
		if err != nil {
			r.log.Debug("failed to parse label", "relay", r.url, "error", err)
			continue
		}

		switch label {
		case "EVENT":
			msg, err := parseEvent(decoder)
			if err != nil {
				r.log.Debug("failed to parse event", "relay", r.url, "error", err)
				continue
			}

			if err := verify(&msg.Event); err != nil {
				r.log.Debug("event is invalid", "relay", r.url, "event", msg.Event.ID, "error", err)
				continue
			}

			err = r.subs.Route(msg.SubID, &msg.Event)
			if errors.Is(err, ErrInvalidSubMatch) {
				r.log.Debug("failed to route event", "relay", r.url, "error", err)
				continue
			}
			if err != nil {
				r.log.Error("failed to route event", "relay", r.url, "error", err)
			}

		case "CLOSED":
			closed, err := parseClosed(decoder)
			if err != nil {
				r.log.Debug("failed to parse closed", "relay", r.url, "error", err)
				continue
			}
			r.subs.SignalClosed(closed.ID, closed.Message)

		case "EOSE":
			eose, err := parseEOSE(decoder)
			if err != nil {
				r.log.Debug("failed to parse eose", "relay", r.url, "error", err)
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
				r.log.Debug("failed to parse auth", "relay", r.url, "error", err)
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
				r.log.Error("failed to send auth response", "relay", r.url, "error", err)
			}

		case "OK":
			// this is in response to our AUTH message.
			// We don't need to do anything with it yet.

		case "NOTICE":
			notice, err := parseNotice(decoder)
			if err != nil {
				r.log.Debug("failed to parse notice", "relay", r.url, "error", err)
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
func (r *T) write() {
	defer r.conn.Close()

	for {
		select {
		case <-r.done:
			r.writeClose()
			return

		case request := <-r.requests:
			bytes, err := request.MarshalJSON()
			if err != nil {
				r.log.Error("failed to marshal request", "error", err)
				continue
			}

			if err := r.writeMessage(bytes); err != nil {
				if isUnexpectedClose(err) {
					r.log.Debug("unexpected error when attempting to write", "error", err)
				}
				continue
			}
		}
	}
}

// Compare compares two Relay URLs for sorting.
func Compare(r1, r2 *T) int {
	return cmp.Compare(r1.url, r2.url)
}

// ValidateURL validates a Relay URL.
func ValidateURL(u string) error {
	if u == "" {
		return fmt.Errorf("%w: empty url", ErrInvalidURL)
	}

	parsed, err := url.Parse(u)
	if err != nil {
		return err
	}
	if parsed.Scheme != "wss" && parsed.Scheme != "ws" {
		return fmt.Errorf("%w: invalid scheme: %s", ErrInvalidURL, parsed.Scheme)
	}
	if parsed.Host == "" || parsed.Host == "." {
		return fmt.Errorf("%w: missing host", ErrInvalidURL)
	}
	if parsed.User != nil {
		return fmt.Errorf("%w: userinfo is not allowed", ErrInvalidURL)
	}
	if parsed.RawQuery != "" {
		return fmt.Errorf("%w: query string is not allowed", ErrInvalidURL)
	}
	if parsed.Fragment != "" {
		return fmt.Errorf("%w: fragment is not allowed", ErrInvalidURL)
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

// expectedSize returns the expected number of events that will be returned for the given filters,
// based on their limits.
func expectedSize(filters nostr.Filters) int {
	expected := 0
	for _, f := range filters {
		if f.Limit == 0 && !f.LimitZero {
			return 1000 // no limit specified, default to 1000
		}
		expected += int(f.Limit)
	}
	return expected
}

func (r *T) writeMessage(b []byte) error {
	r.conn.SetWriteDeadline(time.Now().Add(r.settings.WS.writeWait))
	return r.conn.WriteMessage(ws.TextMessage, b)
}

func (r *T) writeClose() error {
	return r.conn.WriteControl(
		ws.CloseMessage,
		ws.FormatCloseMessage(ws.CloseNormalClosure, ""),
		time.Now().Add(r.settings.WS.writeWait),
	)
}

func (r *T) writePing() error {
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
