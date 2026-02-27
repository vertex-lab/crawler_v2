package relays

import (
	"context"
	"log/slog"
	"time"

	"github.com/goccy/go-json"
	ws "github.com/gorilla/websocket"
	"github.com/vertex-lab/crawler_v2/pkg/relays/auth"
	"github.com/vertex-lab/crawler_v2/pkg/relays/watchdog"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 45 * time.Second
	maxMessageSize = 500_000 // 0.5MB
)

// Relay is a read-only representation of a Nostr relay.
// Create one with NewRelay, then call Connect to establish the connection, and Disconnect to close it.
type Relay struct {
	url      string
	conn     *ws.Conn
	requests chan Request

	auth *auth.Handler
	ping *watchdog.T

	done chan struct{}
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
	close(r.done)
}

// Write enqueues a raw message to be sent to the relay.
// Returns false if the relay is disconnected or the write channel is full.
func (r *Relay) Write(request Request) bool {
	select {
	case r.requests <- request:
		return true

	case <-r.done:
		return false

	default:
		return false
	}
}

// write reads from writeCh and forwards each message to the websocket connection.
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
				slog.Error("unexpected close error from relay", "error", err, "relay", r.url)
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
			slog.Error("failed to parse label", "error", err, "relay", r.url)
			continue
		}

		switch label {
		case "EVENT":
			event, err := parseEvent(decoder)
			if err != nil {
				slog.Error("failed to parse event", "error", err, "relay", r.url)
				continue
			}

			_ = event

		case "CLOSED":
			closed, err := parseClosed(decoder)
			if err != nil {
				slog.Error("failed to parse closed", "error", err, "relay", r.url)
				continue
			}

			_ = closed

		case "AUTH":
			auth, err := parseAuth(decoder)
			if err != nil {
				slog.Error("failed to parse auth", "error", err, "relay", r.url)
				continue
			}

			_ = auth

		case "OK":
			ok, err := parseOK(decoder)
			if err != nil {
				slog.Error("failed to parse OK", "error", err, "relay", r.url)
				continue
			}

			_ = ok

		case "NOTICE":
			notice, err := parseNotice(decoder)
			if err != nil {
				slog.Error("failed to parse notice", "error", err, "relay", r.url)
				continue
			}
			slog.Info("received notice message", "relay", r.url, "message", notice.Message)

		default:
			slog.Warn("received unknown message", "label", label, "relay", r.url)
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
