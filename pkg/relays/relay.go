package relays

import (
	"context"
	"log/slog"
	"time"

	ws "github.com/gorilla/websocket"
	"github.com/vertex-lab/crawler_v2/pkg/relays/watchdog"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 45 * time.Second
	maxMessageSize = 500_000 // 0.5MB
)

// Relay maintains a gorilla-websocket connection to a single Nostr relay.
type Relay struct {
	url          string
	conn         *ws.Conn
	writeCh      chan []byte
	pongWatchdog *watchdog.T

	done chan struct{}
}

// NewRelay returns an unconnected Relay.
// Call Connect to establish the connection, and Disconnect to close it.
func NewRelay(url string) *Relay {
	r := &Relay{
		url:          url,
		writeCh:      make(chan []byte, 1000),
		pongWatchdog: watchdog.New(pongWait, logNoPong(url)),
		done:         make(chan struct{}),
	}
	return r
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
func (r *Relay) Write(msg []byte) bool {
	select {
	case r.writeCh <- msg:
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

		case msg := <-r.writeCh:
			if err := r.writeMessage(msg); err != nil {
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
			r.pongWatchdog.Arm()
		}
	}
}

// read consumes incoming messages from the websocket connection.
func (r *Relay) read() {
	r.conn.SetReadLimit(maxMessageSize)
	r.conn.SetPongHandler(func(_ string) error { r.pongWatchdog.Disarm(); return nil })

	for {
		select {
		case <-r.done:
			return

		default:
			// proceed
		}

		_, _, err := r.conn.ReadMessage()
		if err != nil {
			return
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
