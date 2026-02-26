package relays

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const writeTimeout = 10 * time.Second

// Relay maintains a gorilla-websocket connection to a single Nostr relay.
type Relay struct {
	url     string
	conn    *websocket.Conn
	writeCh chan []byte

	wg   sync.WaitGroup
	done chan struct{}
}

// NewRelay returns an unconnected Relay.
// Call Connect to establish the connection, and Disconnect to close it.
func NewRelay(url string, writeBuf int) *Relay {
	return &Relay{
		url:     url,
		writeCh: make(chan []byte, writeBuf),
		done:    make(chan struct{}),
	}
}

// Connect dials the relay and starts the read and write goroutines.
// The context is used only when connecting to the relay.
func (r *Relay) Connect(ctx context.Context) error {
	var err error
	r.conn, _, err = websocket.DefaultDialer.DialContext(ctx, r.url, nil)
	if err != nil {
		return err
	}

	r.wg.Add(2)
	go r.read()
	go r.write()
	return nil
}

// Disconnect closes the done channel, signalling the read and write goroutines to stop.
func (r *Relay) Disconnect() {
	close(r.done)
	r.wg.Wait()
	r.conn.Close()
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
	defer r.wg.Done()

	for {
		select {
		case <-r.done:
			r.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			r.conn.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			)
			r.conn.Close()
			return

		case msg, ok := <-r.writeCh:
			if !ok {
				return
			}

			r.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := r.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("relay %s: write error: %v", r.url, err)
				return
			}
		}
	}
}

// read consumes incoming messages from the websocket connection and discards them.
func (r *Relay) read() {
	defer r.wg.Done()

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
