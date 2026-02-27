package relays

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	ws "github.com/gorilla/websocket"
	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/relays/auth"
	"github.com/vertex-lab/crawler_v2/pkg/relays/subscription"
	"github.com/vertex-lab/crawler_v2/pkg/relays/watchdog"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 45 * time.Second
	maxMessageSize = 500_000 // 0.5MB
)

var (
	ErrRelayClosed        = errors.New("relay is closed")
	ErrSendFailed         = errors.New("failed to send message, channel is full")
	ErrSubscriptionClosed = errors.New("subscription was closed")
)

// Relay is a read-only representation of a Nostr relay.
// Create one with NewRelay, then call Connect to establish the connection, and Disconnect to close it.
type Relay struct {
	url      string
	conn     *ws.Conn
	requests chan Request

	subs *subscription.Manager
	auth *auth.Handler
	ping *watchdog.T

	isClosing atomic.Bool
	done      chan struct{}
}

// NewRelay returns an unconnected Relay.
// Call Connect to establish the connection, and Disconnect to close it.
func NewRelay(url string, sk string) (*Relay, error) {
	auth, err := auth.NewHandler(url, sk)
	if err != nil {
		return nil, err
	}

	r := &Relay{
		url:      url,
		requests: make(chan Request, 1000),
		subs:     subscription.NewManager(),
		auth:     auth,
		ping:     watchdog.New(pongWait, logNoPong(url)),
		done:     make(chan struct{}),
	}
	return r, nil
}

// Connect dials the relay and starts the read and write goroutines.
// The context is used only when connecting to the relay.
func (r *Relay) Connect(ctx context.Context) error {
	var err error
	r.conn, _, err = ws.DefaultDialer.DialContext(ctx, r.url, nil)
	if err != nil {
		return err
	}

	go r.read()
	go r.write()
	return nil
}

// Disconnect closes the done channel, signalling the read and write goroutines to stop.
func (r *Relay) Disconnect() {
	if r.isClosing.CompareAndSwap(false, true) {
		close(r.done)
		r.subs.CloseAll()
	}
}

// Query sends a REQ to the relay with the given id and filters.
// When it receives an EOSE, it returns all previous events and closes the subscription.
// It returns an error if the context is cancelled or the relay has sent a CLOSED message.
//
// It is always recommended to use this method with a context timeout,
// to avoid bad relays that never sent an EOSE (or CLOSED) to block indefinitely the query.
func (r *Relay) Query(ctx context.Context, id string, filters nostr.Filters) ([]nostr.Event, error) {
	req := Req{
		ID:      id,
		Filters: filters,
	}

	if err := r.send(req); err != nil {
		return nil, err
	}

	sub := r.subs.New(req.ID)
	events := make([]nostr.Event, 0)

	defer func() {
		r.send(Close{ID: req.ID})
		r.subs.Close(req.ID)
	}()

	for {
		select {
		case <-ctx.Done():
			return events, ctx.Err()

		case <-r.done:
			return events, ErrRelayClosed

		case event := <-sub.Events:
			events = append(events, event)

		case <-sub.Eose:
			// drain all buffered events, up to a maximum limit of 1000.
			// This avoids busy-reading the events channel, which might always contain events
			// send after the EOSE message has been received, which are likely newly published events.
			for range 1000 {
				select {
				case event := <-sub.Events:
					events = append(events, event)
				default:
					return events, nil
				}
			}
			return events, nil

		case err := <-sub.Closed:
			return events, fmt.Errorf("%w: %w", ErrSubscriptionClosed, err)
		}
	}

}

// Send enqueues a request to be sent to the relay.
// Returns false if the relay is disconnected or the requests channel is full.
func (r *Relay) send(request Request) error {
	select {
	case r.requests <- request:
		return nil

	case <-r.done:
		return ErrRelayClosed

	default:
		return ErrSendFailed
	}
}

// write reads from the requests channel and forwards each message to the websocket connection.
// When done is closed it sends a clean close frame and shuts down the connection.
func (r *Relay) write() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		r.conn.Close()
		ticker.Stop()
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
			r.ping.Arm()
		}
	}
}

// read consumes incoming messages from the websocket connection.
func (r *Relay) read() {
	r.conn.SetReadLimit(maxMessageSize)
	r.conn.SetPongHandler(func(_ string) error { r.ping.Disarm(); return nil })

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
			r.subs.Route(msg.SubID, msg.Event)

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
			r.send(response)

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

func (r *Relay) writeMessage(b []byte) error {
	r.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return r.conn.WriteMessage(ws.TextMessage, b)
}

func (r *Relay) writeClose() error {
	return r.conn.WriteControl(
		ws.CloseMessage,
		ws.FormatCloseMessage(ws.CloseNormalClosure, ""),
		time.Now().Add(writeWait),
	)
}

func (r *Relay) writePing() error {
	return r.conn.WriteControl(
		ws.PingMessage,
		nil,
		time.Now().Add(writeWait),
	)
}

func isUnexpectedClose(err error) bool {
	return ws.IsUnexpectedCloseError(err,
		ws.CloseNormalClosure,
		ws.CloseGoingAway,
		ws.CloseNoStatusReceived,
		ws.CloseAbnormalClosure)
}

func logNoPong(url string) func() {
	return func() {
		slog.Warn("the relay did not respond to a PING", "relay", url, "timeout", pongWait)
	}
}
