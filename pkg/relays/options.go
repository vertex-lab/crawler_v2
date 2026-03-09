package relays

import (
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vertex-lab/crawler_v2/pkg/relays/auth"
)

// RelayOption is an option that can be applied to a relay.
type RelayOption interface {
	applyRelay(*T) error
}

// PoolOption is an option that can be applied to a pool.
// All RelayOptions are valid PoolOptions, but the converse is not true.
type PoolOption interface {
	applyPool(*Pool) error
}

// commonOption is an option that can be applied to both relays and pools.
type commonOption interface {
	RelayOption
	PoolOption
}

// WithAuthKey sets the authentication key, which will be used to sign auth events
// immediately after receiving an AUTH challenge.
func WithAuthKey(sk string) commonOption {
	return authOption{sk: sk}
}

type authOption struct {
	sk string
}

func (o authOption) applyRelay(r *T) error {
	handler, err := auth.NewHandler(r.url, o.sk)
	if err != nil {
		return err
	}
	r.auth = handler
	return nil
}

func (o authOption) applyPool(p *Pool) error {
	p.options = append(p.options, o)
	return nil
}

// WithLogger sets the logger for the relay or pool.
func WithLogger(l *slog.Logger) commonOption {
	return loggerOption{l: l}
}

type loggerOption struct {
	l *slog.Logger
}

func (o loggerOption) applyRelay(r *T) error {
	r.log = o.l
	return nil
}

func (o loggerOption) applyPool(p *Pool) error {
	p.log = o.l
	p.options = append(p.options, o)
	return nil
}

// WithRelayRetry sets the interval for retrying relay connections that failed.
// Default is 10 minutes.
func WithRelayRetry(d time.Duration) PoolOption {
	return relayRetryOption{d: d}
}

type relayRetryOption struct {
	d time.Duration
}

func (o relayRetryOption) applyPool(p *Pool) error {
	p.settings.relayRetry = o.d
	return nil
}

// WithSubscriptionRetry sets the interval for retrying subscription connections that failed.
// Default is 10 seconds.
func WithSubscriptionRetry(d time.Duration) PoolOption {
	return subRetryOption{d: d}
}

type subRetryOption struct {
	d time.Duration
}

func (o subRetryOption) applyPool(p *Pool) error {
	p.settings.subRetry = o.d
	return nil
}

// WithWriteWait sets the write wait duration for the WebSocket connection.
func WithWriteWait(d time.Duration) commonOption {
	return writeWaitOption{d: d}
}

type writeWaitOption struct {
	d time.Duration
}

func (o writeWaitOption) applyRelay(r *T) error {
	r.settings.WS.writeWait = o.d
	return nil
}

func (o writeWaitOption) applyPool(p *Pool) error {
	p.options = append(p.options, o)
	return nil
}

// WithMaxMessageSize sets the maximum size (in bytes) of a single incoming websocket message
// (e.g., a Nostr EVENT or REQ). Messages larger than this will be rejected.
func WithMaxMessageBytes(s int) commonOption {
	return maxMessageOption{s: int64(s)}
}

type maxMessageOption struct {
	s int64
}

func (o maxMessageOption) applyRelay(r *T) error {
	r.settings.WS.maxMessageSize = o.s
	return nil
}

func (o maxMessageOption) applyPool(p *Pool) error {
	p.options = append(p.options, o)
	return nil
}

// WithDialer sets the websocket dialer for the relay and pool.
func WithDialer(d websocket.Dialer) commonOption {
	return dialerOption{d: d}
}

type dialerOption struct {
	d websocket.Dialer
}

func (o dialerOption) applyRelay(r *T) error {
	r.settings.WS.dialer = o.d
	return nil
}

func (o dialerOption) applyPool(p *Pool) error {
	p.options = append(p.options, o)
	return nil
}

// WithReadBufferBytes sets the read buffer size (in bytes) for the underlying websocket connection dialer.
func WithReadBufferBytes(s int) commonOption {
	return readBufferOption{s: s}
}

type readBufferOption struct {
	s int
}

func (o readBufferOption) applyRelay(r *T) error {
	r.settings.WS.dialer.ReadBufferSize = o.s
	return nil
}

func (o readBufferOption) applyPool(p *Pool) error {
	p.options = append(p.options, o)
	return nil
}

// WithWriteBufferBytes sets the write buffer size (in bytes) for the underlying websocket connection dialer.
func WithWriteBufferBytes(s int) commonOption {
	return writeBufferOption{s: s}
}

type writeBufferOption struct {
	s int
}

func (o writeBufferOption) applyRelay(r *T) error {
	r.settings.WS.dialer.WriteBufferSize = o.s
	return nil
}

func (o writeBufferOption) applyPool(p *Pool) error {
	p.options = append(p.options, o)
	return nil
}

// WithHandshakeTimeout sets the handshake timeout for the underlying websocket connection dialer.
func WithHandshakeTimeout(d time.Duration) commonOption {
	return handshakeOption{d: d}
}

type handshakeOption struct {
	d time.Duration
}

func (o handshakeOption) applyRelay(r *T) error {
	r.settings.WS.dialer.HandshakeTimeout = o.d
	return nil
}

func (o handshakeOption) applyPool(p *Pool) error {
	p.options = append(p.options, o)
	return nil
}

type relaySettings struct {
	WS websocketSettings
}

func defaultRelaySettings() relaySettings {
	return relaySettings{
		WS: defaultWSSettings(),
	}
}

type websocketSettings struct {
	dialer         websocket.Dialer
	writeWait      time.Duration
	maxMessageSize int64
}

func defaultWSSettings() websocketSettings {
	return websocketSettings{
		dialer: websocket.Dialer{
			ReadBufferSize:   1024, // 1KB
			WriteBufferSize:  1024, // 1KB
			HandshakeTimeout: 10 * time.Second,
		},
		writeWait:      10 * time.Second,
		maxMessageSize: 500_000,
	}
}

type poolSettings struct {
	// how often to retry failed relay connections
	relayRetry time.Duration

	// how often to retry failed subscription connections
	subRetry time.Duration
}

func defaultPoolSettings() poolSettings {
	return poolSettings{
		relayRetry: 10 * time.Minute,
		subRetry:   10 * time.Second,
	}
}
