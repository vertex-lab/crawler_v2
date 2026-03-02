package relays

import (
	"time"

	"github.com/gorilla/websocket"
	"github.com/vertex-lab/crawler_v2/pkg/relays/auth"
)

type Option func(*Relay) error

// WithAuthKey sets the authentication key, which will be used to sign auth events
// immediately after receiving an AUTH challenge.
func WithAuthKey(sk string) Option {
	return func(r *Relay) error {
		handler, err := auth.NewHandler(r.url, sk)
		if err != nil {
			return err
		}
		r.auth = handler
		return nil
	}
}

// WithWriteWait sets the write wait duration for the WebSocket connection.
func WithWriteWait(writeWait time.Duration) Option {
	return func(r *Relay) error {
		r.settings.WS.writeWait = writeWait
		return nil
	}
}

// WithPongWait sets the pong wait duration for the WebSocket connection.
func WithPongWait(pongWait time.Duration) Option {
	return func(r *Relay) error {
		r.settings.WS.pongWait = pongWait
		return nil
	}
}

// WithPingPeriod sets the ping period for the WebSocket connection.
func WithPingPeriod(pingPeriod time.Duration) Option {
	return func(r *Relay) error {
		r.settings.WS.pingPeriod = pingPeriod
		return nil
	}
}

// WithMaxMessageSize sets the maximum size (in bytes) of a single incoming websocket message
// (e.g., a Nostr EVENT or REQ). Messages larger than this will be rejected.
func WithMaxMessageBytes(s int) Option {
	return func(r *Relay) error {
		r.settings.WS.maxMessageSize = int64(s)
		return nil
	}
}

// WithReadBufferBytes sets the read buffer size (in bytes) for the underlying websocket connection dialer.
func WithReadBufferBytes(s int) Option {
	return func(r *Relay) error {
		r.settings.WS.dialer.ReadBufferSize = s
		return nil
	}
}

// WithWriteBufferBytes sets the write buffer size (in bytes) for the underlying websocket connection dialer.
func WithWriteBufferBytes(s int) Option {
	return func(r *Relay) error {
		r.settings.WS.dialer.WriteBufferSize = s
		return nil
	}
}

// WithHandshakeTimeout sets the handshake timeout for the underlying websocket connection dialer.
func WithHandshakeTimeout(d time.Duration) Option {
	return func(r *Relay) error {
		r.settings.WS.dialer.HandshakeTimeout = d
		return nil
	}
}

type settings struct {
	WS websocketSettings
}

func newSettings() settings {
	return settings{
		WS: newWebsocketSettings(),
	}
}

type websocketSettings struct {
	dialer         websocket.Dialer
	writeWait      time.Duration
	pongWait       time.Duration
	pingPeriod     time.Duration
	maxMessageSize int64
}

func newWebsocketSettings() websocketSettings {
	return websocketSettings{
		dialer: websocket.Dialer{
			ReadBufferSize:   1024, // 1KB
			WriteBufferSize:  1024, // 1KB
			HandshakeTimeout: 10 * time.Second,
		},
		writeWait:      10 * time.Second,
		pongWait:       60 * time.Second,
		pingPeriod:     45 * time.Second,
		maxMessageSize: 500_000,
	}
}
