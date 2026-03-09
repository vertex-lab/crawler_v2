package relays

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/slicex"
)

var (
	ErrPoolClosed = fmt.Errorf("pool is closed")
)

// Pool manages a set of relay connections and presents a unified streaming
// API to the caller. It owns the desired stream state and is responsible
// for maintaining it across relay disconnections and subscription closures.
type Pool struct {
	// The currently active streams, keyed by their ID.
	streams map[string]*Stream

	// active holds the pool sessions, keyed by their relay URL.
	sessions map[string]*session

	// streamOps is the channel for opening/closing streams.
	streamOps chan streamOp

	// relayOps is the channel for adding/removing relays from the pool.
	relayOps chan relayOp

	log      *slog.Logger
	settings poolSettings
	options  []RelayOption

	wg        sync.WaitGroup
	isClosing atomic.Bool
	done      chan struct{}
}

// NewPool creates a new Pool with the given URLs and options.
// The context is only used to establish the connections; it does not control the lifetime of the pool.
// Call pool.Close to close all connections and free resources.
func NewPool(urls []string, opts ...PoolOption) (*Pool, error) {
	urls = slicex.Unique(urls)
	for _, url := range urls {
		if err := ValidateURL(url); err != nil {
			return nil, fmt.Errorf("invalid url: %q: %w", url, err)
		}
	}

	p := &Pool{
		streams:   make(map[string]*Stream),
		sessions:  make(map[string]*session, len(urls)),
		streamOps: make(chan streamOp, 100),
		relayOps:  make(chan relayOp, 100),
		log:       slog.Default(),
		settings:  defaultPoolSettings(),
		done:      make(chan struct{}),
	}

	for _, opt := range opts {
		if err := opt.applyPool(p); err != nil {
			return nil, err
		}
	}

	for _, url := range urls {
		s := p.newSession(url)
		p.sessions[url] = s
		go s.run()
	}

	go p.run()
	return p, nil
}

// Close closes the pool and all the underlying relay connections.
func (p *Pool) Close() {
	if p.isClosing.CompareAndSwap(false, true) {
		close(p.done)
		p.wg.Wait()
	}
}

// Stream creates a new stream with the given id and filters, returning a
// Stream object that can be used to receive events from all connected relays.
// It returns an error if the pool is closed, or if the stream id is duplicated.
func (p *Pool) Stream(id string, filters ...nostr.Filter) (*Stream, error) {
	if p.isClosing.Load() {
		return nil, fmt.Errorf("failed to create stream: %w", ErrPoolClosed)
	}

	s := &Stream{
		id:      id,
		filters: filters,
		pool:    p,
		events:  make(chan *nostr.Event, 10_000),
		done:    make(chan struct{}),
	}

	op := streamOp{
		Stream: s,
		kind:   openOp,
		reply:  make(chan error, 1),
	}

	if err := p.sendStream(op); err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	select {
	case <-p.done:
		return nil, fmt.Errorf("failed to create stream: %w", ErrPoolClosed)

	case err := <-op.reply:
		if err != nil {
			return nil, fmt.Errorf("failed to create stream: %w", err)
		}
		return s, nil
	}
}

// Add adds a relay URL to the pool.
// Add is idempotent, meaning it will not add the same URL multiple times.
func (p *Pool) Add(url string) error {
	if err := ValidateURL(url); err != nil {
		return fmt.Errorf("failed to add relay: %w", err)
	}

	op := relayOp{
		kind: openOp,
		url:  url,
	}

	if err := p.sendRelay(op); err != nil {
		return fmt.Errorf("failed to add relay: %w", err)
	}
	return nil
}

// Remove removes a relay URL from the pool.
// Remove is idempotent, meaning it will not remove the same URL multiple times,
// and won't return an error if the URL is not in the pool.
func (p *Pool) Remove(url string) error {
	if err := ValidateURL(url); err != nil {
		return fmt.Errorf("failed to add relay: %w", err)
	}

	op := relayOp{
		kind: closeOp,
		url:  url,
	}

	if err := p.sendRelay(op); err != nil {
		return fmt.Errorf("failed to add relay: %w", err)
	}
	return nil
}

type opKind int

const (
	openOp  opKind = 0 // opening operation
	closeOp opKind = 1 // closing operation
)

// streamOp represents an operation to be performed on a stream, including
// the stream itself and an optional channel to receive the result.
type streamOp struct {
	*Stream
	kind  opKind
	reply chan error
}

// sendStream sends a stream operation to the pool's streamOps channel.
// It returns an error if the pool is closed or if the send fails due to backpressure.
func (p *Pool) sendStream(op streamOp) error {
	if p.isClosing.Load() {
		return ErrPoolClosed
	}

	select {
	case <-p.done:
		return ErrPoolClosed

	case p.streamOps <- op:
		return nil

	default:
		return ErrSendFailed
	}
}

// relayOp represents an operation to be performed on a relay, including
// the relay URL and an optional channel to receive the result.
type relayOp struct {
	kind opKind
	url  string
}

// sendRelay sends a relay operation to the pool's relayOps channel.
// It returns an error if the pool is closed or if the send fails due to backpressure.
func (p *Pool) sendRelay(op relayOp) error {
	if p.isClosing.Load() {
		return ErrPoolClosed
	}

	select {
	case <-p.done:
		return ErrPoolClosed

	case p.relayOps <- op:
		return nil

	default:
		return ErrSendFailed
	}
}

// cleanup clears all active sessions and streams.
func (p *Pool) cleanup() {
	for _, s := range p.sessions {
		s.close(nil)
	}
	clear(p.sessions)

	for _, s := range p.streams {
		if s.isClosing.CompareAndSwap(false, true) {
			s.err = ErrPoolClosed
			close(s.done)
		}
	}
	clear(p.streams)
}

func (p *Pool) run() {
	defer p.cleanup()

	p.log.Info("relay pool is running")
	defer p.log.Info("relay pool is shutting down")

	retry := time.NewTicker(p.settings.relayRetry)
	defer retry.Stop()

	for {
		if p.isClosing.Load() {
			return
		}

		select {
		case <-p.done:
			return

		case op := <-p.relayOps:
			switch op.kind {
			case openOp:
				if _, ok := p.sessions[op.url]; ok {
					continue
				}

				s := p.newSession(op.url)
				p.sessions[op.url] = s

				for _, stream := range p.streams {
					s.openSub(stream)
				}
				go s.run()

			case closeOp:
				if s, ok := p.sessions[op.url]; ok {
					s.close(nil)
					delete(p.sessions, op.url)
				}
			}

		case op := <-p.streamOps:
			switch op.kind {
			case openOp:
				if _, ok := p.streams[op.id]; ok {
					if op.reply != nil {
						op.reply <- ErrDuplicateStream
					}
					continue
				}

				p.streams[op.id] = op.Stream
				if op.reply != nil {
					op.reply <- nil
				}

				for _, s := range p.sessions {
					if s.isActive() {
						s.openSub(op.Stream)
					}
				}

			case closeOp:
				if _, ok := p.streams[op.id]; !ok {
					continue
				}

				delete(p.streams, op.id)
				for _, s := range p.sessions {
					if s.isActive() {
						s.closeSub(op.Stream)
					}
				}
			}

		case <-retry.C:
			for url, s := range p.sessions {
				if s.isActive() {
					continue
				}

				p.log.Debug("session failed, retrying", "relay", url, "error", s.Err())
				new := p.newSession(url)
				p.sessions[url] = new

				for _, stream := range p.streams {
					new.openSub(stream)
				}
				go new.run()
			}
		}
	}
}

// session manages the connection to a single relay and all its subscriptions.
// When a subscription is closed by the relay, the session restarts it with a backoff.
// When the relay disconnects entirely, the session reports back to the Pool and exits.
type session struct {
	url string

	// subs holds the subscriptions of this session, keyed by their ID.
	subs map[string]*Subscription

	// streamOps receives subscribe/unsubscribe commands from the pool.
	streamOps chan streamOp

	// pool is the pool this session belongs to.
	pool *Pool

	isClosing atomic.Bool
	done      chan struct{}
	err       error // holds the disconnection error
}

// newSession creates a new session for the given relay URL.
// It initialized the streamOps channel with a buffer that is large enough to hold
// the current number of streams and extra, to avoid blocking on streamOps.
func (p *Pool) newSession(url string) *session {
	return &session{
		url:       url,
		pool:      p,
		subs:      make(map[string]*Subscription, len(p.streams)),
		streamOps: make(chan streamOp, max(2*len(p.streams), 100)), // space for current and future streamOps
		done:      make(chan struct{}),
	}
}

func (s *session) close(err error) {
	if s.isClosing.CompareAndSwap(false, true) {
		s.err = err
		close(s.done)
	}
}

// isActive returns true if the session is active.
func (s *session) isActive() bool {
	return !s.isClosing.Load()
}

// Err returns the error that caused the session to close, or nil if it is still active.
func (s *session) Err() error {
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}

func (s *session) run() {
	s.pool.wg.Add(1)
	defer s.pool.wg.Done()

	if s.pool.isClosing.Load() {
		s.close(ErrPoolClosed)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	relay, err := New(ctx, s.url, s.pool.options...)
	if err != nil {
		s.close(err)
		return
	}
	defer relay.Close()

	s.pool.log.Debug("session is running", "url", s.url)
	defer s.pool.log.Debug("session is shutting down", "url", s.url)

	retry := time.NewTicker(s.pool.settings.subRetry)
	defer retry.Stop()

	for {
		if s.isClosing.Load() {
			return
		}

		select {
		case <-s.done:
			return

		case <-relay.Done():
			err := relay.Err()
			s.close(err)
			return

		case op := <-s.streamOps:
			switch op.kind {
			case openOp:

				// the subscription re-uses the stream events channel, so
				// events will be routed by the relay.read() automatically,
				// without the need to manually forward them.
				sub := &Subscription{
					id:      op.Stream.id,
					filters: op.Stream.filters,
					events:  op.Stream.events,
					eose:    make(chan struct{}),
					done:    make(chan struct{}),
				}
				s.subs[op.id] = sub

				if err := relay.open(sub); err != nil {
					// modify directly the subscription fields, which will be used
					// to detect a failed subscription in the retry loop below.
					// It is safe because the subscription hasn't been opened.
					sub.err = err
					sub.isClosing.Store(true)
					continue
				}

			case closeOp:
				if sub, ok := s.subs[op.id]; ok {
					delete(s.subs, op.id)
					sub.Close()
				}
			}

		case <-retry.C:
			for _, sub := range s.subs {
				if sub.IsActive() {
					continue
				}

				s.pool.log.Debug("subscription failed, retrying", "relay", s.url, "id", sub.ID(), "error", sub.Err())

				// make a new subscription and try to open it.
				// To avoid duplicate events, all filters use a since set to the last time
				// the old subscription reported an event.
				new := &Subscription{
					id:      sub.id,
					filters: withSince(sub.filters, sub.LastEvent()),
					events:  sub.events,
					eose:    make(chan struct{}),
					done:    make(chan struct{}),
				}
				s.subs[new.id] = new

				if err := relay.open(new); err != nil {
					new.err = err
					new.isClosing.Store(true)
					continue
				}
			}
		}
	}
}

// openSub opens a subscription for the given stream.
func (s *session) openSub(stream *Stream) {
	if s.isClosing.Load() {
		return
	}

	select {
	case <-s.done:
	case s.streamOps <- streamOp{Stream: stream, kind: openOp}:
	default:
		s.pool.log.Warn("session openSub failed", "session", s.url, "error", "channel is full")
	}
}

// closeSub closes the subscription associated with the given stream.
func (s *session) closeSub(stream *Stream) {
	if s.isClosing.Load() {
		return
	}

	select {
	case <-s.done:
	case s.streamOps <- streamOp{Stream: stream, kind: closeOp}:
	default:
		s.pool.log.Warn("session closeSub failed", "session", s.url, "error", "channel is full")
	}
}

// withSince returns a deep-copy of the given filters, with a since field set to the given time.
func withSince(f nostr.Filters, t time.Time) nostr.Filters {
	c := make(nostr.Filters, len(f))
	for i := range f {
		c[i] = f[i].Clone()
	}

	unix := t.Unix()
	if unix <= 0 {
		// don't add a non-positive since to avoid breaking other implementations.
		// after all, a non-positive since is the same as no since at all.
		return c
	}

	for i := range c {
		ts := nostr.Timestamp(unix) // make a copy to avoid any shared pointer issues
		c[i].Since = &ts
	}
	return c
}
