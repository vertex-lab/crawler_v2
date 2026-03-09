package relays

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/slicex"
	"github.com/pippellia-btc/smallset"
)

var (
	ErrPoolClosed = fmt.Errorf("pool is closed")
	ErrNoRelays   = fmt.Errorf("no relays available")
)

// Pool manages a set of relay connections and presents a unified API to the caller.
// It maintains the desired subscription state, handling relay disconnections and subscription closures.
type Pool struct {
	// streamIDs holds the IDs of all streams that were ever opened by this pool.
	// The session reconnection logic needs the guarantee that stream IDs are unique, otherwise we might have:
	// - session is running
	// - stream with "ID" is opened
	// - session stops running
	// - stream with "ID" is closed
	// - another stream with the same "ID" is opened
	mu        sync.Mutex
	streamIDs smallset.Ordered[string]

	streams   map[string]*Stream
	streamOps chan streamOp

	sessions   map[string]*session
	sessionOps chan sessionOp
	view       chan chan relayView // holds the reply channel for relay views

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
		streams:    make(map[string]*Stream),
		streamOps:  make(chan streamOp, 100),
		sessions:   make(map[string]*session, len(urls)),
		sessionOps: make(chan sessionOp, 100),
		view:       make(chan chan relayView, 100),
		log:        slog.Default(),
		settings:   defaultPoolSettings(),
		done:       make(chan struct{}),
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

// relayView represents a snapshot of the pool's connected and disconnected relays.
type relayView struct {
	connected    []*T
	disconnected []*T
}

// Close closes the pool and all the underlying relay connections.
func (p *Pool) Close() {
	if p.isClosing.CompareAndSwap(false, true) {
		close(p.done)
		p.wg.Wait()
	}
}

// Relays returns a snapshot of the currently connected and disconnected relay URLs in the pool.
func (p *Pool) Relays() (connected, disconnected []string) {
	// don't want to expose a ctx for such a simple operation, so I use a sensible default
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	view, err := p.relays(ctx)
	if err != nil {
		p.log.Error("pool.Relays() failed", "error", err)
		return nil, nil
	}

	connected = make([]string, len(view.connected))
	for i, r := range view.connected {
		connected[i] = r.URL()
	}

	disconnected = make([]string, len(view.disconnected))
	for i, r := range view.disconnected {
		disconnected[i] = r.URL()
	}
	return connected, disconnected
}

// relays returns a snapshot of the currently connected and disconnected relays in the pool.
func (p *Pool) relays(ctx context.Context) (relayView, error) {
	relays := make(chan relayView, 1)
	select {
	case <-ctx.Done():
		return relayView{}, ctx.Err()
	case <-p.done:
		return relayView{}, ErrPoolClosed
	case p.view <- relays:
	default:
		return relayView{}, ErrSendFailed
	}

	select {
	case <-ctx.Done():
		return relayView{}, ctx.Err()
	case <-p.done:
		return relayView{}, ErrPoolClosed
	case view := <-relays:
		return view, nil
	}
}

// Stream creates a new stream with the given id and filters, returning a
// Stream object that can be used to receive events from all connected relays.
// It returns an error if the pool is closed, or if the stream id is duplicated.
func (p *Pool) Stream(id string, filters ...nostr.Filter) (*Stream, error) {
	if p.isClosing.Load() {
		return nil, fmt.Errorf("failed to create stream: %w", ErrPoolClosed)
	}

	p.mu.Lock()
	if p.streamIDs.Contains(id) {
		p.mu.Unlock()
		return nil, fmt.Errorf("failed to create stream: %w", ErrDuplicateStream)
	}
	p.streamIDs.Add(id)
	p.mu.Unlock()

	s := &Stream{
		id:      id,
		filters: filters,
		pool:    p,
		events:  make(chan *nostr.Event, 10_000),
		done:    make(chan struct{}),
	}

	select {
	case <-p.done:
		return nil, fmt.Errorf("failed to create stream: %w", ErrPoolClosed)

	case p.streamOps <- streamOp{Stream: s, close: false}:
		return s, nil

	default:
		return nil, fmt.Errorf("failed to create stream: %w", ErrSendFailed)
	}
}

// steamOp represents an operation that modifies the pool's streams.
type streamOp struct {
	*Stream
	close bool // open = !close
}

// Query concurrently queries all connected relays for events matching the given filters.
// Errors are joined together using [errors.Join], and events are always returned even if errors occur.
//
// It is always recommended to use this method with a context timeout (e.g. 10s),
// to avoid bad relays that never send an EOSE (or CLOSED) from blocking indefinitely.
func (p *Pool) Query(ctx context.Context, id string, filters ...nostr.Filter) ([]nostr.Event, error) {
	if p.isClosing.Load() {
		return nil, fmt.Errorf("failed to query: %w", ErrPoolClosed)
	}

	p.mu.Lock()
	isDuplicate := p.streamIDs.Contains(id)
	p.mu.Unlock()

	if isDuplicate {
		return nil, fmt.Errorf("failed to query: %w", ErrDuplicateStream)
	}

	relays, err := p.relays(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}

	if len(relays.connected) == 0 {
		return nil, fmt.Errorf("failed to query: %w", ErrNoRelays)
	}

	type result struct {
		events []nostr.Event
		err    error
	}

	results := make(chan result, len(relays.connected))
	wg := sync.WaitGroup{}

	for _, r := range relays.connected {
		wg.Add(1)
		go func(r *T) {
			defer wg.Done()
			events, err := r.Query(ctx, id, filters...)
			results <- result{events, err}
		}(r)
	}

	wg.Wait()
	close(results)

	events := make([]nostr.Event, 0, len(relays.connected)*expectedSize(filters))

	for r := range results {
		// TODO: perform deduplication and keep the last replaceable and addressable event only.
		err = errors.Join(err, r.err)
		events = append(events, r.events...)
	}
	return events, nil
}

// Add adds a relay URL to the pool.
// Add is idempotent, meaning it will not add the same URL multiple times.
func (p *Pool) Add(url string) error {
	if err := ValidateURL(url); err != nil {
		return fmt.Errorf("failed to add relay: %w", err)
	}

	select {
	case <-p.done:
		return fmt.Errorf("failed to add relay: %w", ErrPoolClosed)

	case p.sessionOps <- sessionOp{url: url, remove: false}:
		return nil

	default:
		return fmt.Errorf("failed to add relay: %w", ErrSendFailed)
	}
}

// Remove removes a relay URL from the pool.
// Remove is idempotent, meaning it will not remove the same URL multiple times,
// and won't return an error if the URL is not in the pool.
func (p *Pool) Remove(url string) error {
	if err := ValidateURL(url); err != nil {
		return fmt.Errorf("failed to add relay: %w", err)
	}

	select {
	case <-p.done:
		return fmt.Errorf("failed to remove relay: %w", ErrPoolClosed)

	case p.sessionOps <- sessionOp{url: url, remove: true}:
		return nil

	default:
		return fmt.Errorf("failed to remove relay: %w", ErrSendFailed)
	}
}

// sessionOp represents an operation that modifies the pool's sessions.
type sessionOp struct {
	url    string
	remove bool // add = !remove
}

// cleanup clears all active sessions and streams.
func (p *Pool) cleanup() {
	for _, s := range p.sessions {
		s.Close(ErrPoolClosed)
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

		case op := <-p.streamOps:
			if op.close {
				delete(p.streams, op.id)
				for _, s := range p.sessions {
					if s.isActive() {
						s.closeSub(op.id)
					}
				}
				continue
			}

			p.streams[op.id] = op.Stream
			for _, s := range p.sessions {
				if s.isActive() {
					s.openSub(op.id, op.filters, op.events)
				}
			}

		case op := <-p.sessionOps:
			if op.remove {
				if s, ok := p.sessions[op.url]; ok {
					s.Close(nil)
					delete(p.sessions, op.url)
				}
				continue
			}

			if _, ok := p.sessions[op.url]; ok {
				continue
			}

			s := p.newSession(op.url)
			p.sessions[op.url] = s

			for _, stream := range p.streams {
				s.openSub(stream.id, stream.filters, stream.events)
			}
			go s.run()

		case reply := <-p.view:
			view := relayView{
				connected:    make([]*T, 0, len(p.sessions)),
				disconnected: make([]*T, 0, len(p.sessions)),
			}

			for _, s := range p.sessions {
				relay := s.relay.Load()
				if relay == nil {
					continue
				}

				if s.isActive() {
					view.connected = append(view.connected, relay)
				} else {
					view.disconnected = append(view.disconnected, relay)
				}
			}
			reply <- view

		case <-retry.C:
			for url, s := range p.sessions {
				if s.isActive() {
					continue
				}

				if err := s.Err(); err != nil {
					p.log.Debug("session failed", "relay", url, "error", s.Err())
				}

				new := p.newSession(url)
				p.sessions[url] = new

				for _, stream := range p.streams {
					// to avoid duplicate events, retry each subscription where the filters' since
					// reflects the last event of the old subscription.
					// This is possible because the streamIDs are globally unique thanks to pool.streamIDs
					var lastEvent time.Time
					if sub, ok := s.subs[stream.id]; ok {
						lastEvent = sub.LastEvent()
					}
					new.openSub(stream.id, withSince(stream.filters, lastEvent), stream.events)
				}
				go new.run()
			}
		}
	}
}

// session manages the connection to a single relay and all its subscriptions.
// It periodically retries subscriptions that are closed by the relay.
// When the relay disconnects entirely, the session reports back to the Pool and exits.
type session struct {
	url   string
	relay atomic.Pointer[T]

	subs   map[string]*Subscription
	subOps chan subOp

	pool *Pool

	isClosing atomic.Bool
	done      chan struct{}
	err       error // holds the disconnection error
}

// subOp represents an operation that modifies the session subscriptions.
type subOp struct {
	id      string
	filters nostr.Filters
	events  chan *nostr.Event
	close   bool // open = !close
}

// newSession creates a new session for the given relay URL.
// To avoid blocking on the channel send, it initializes the open channel with a buffer
// that is large enough to hold the current number of streams plus extra.
func (p *Pool) newSession(url string) *session {
	return &session{
		url:    url,
		pool:   p,
		subs:   make(map[string]*Subscription, len(p.streams)),
		subOps: make(chan subOp, max(2*len(p.streams), 100)), // space for current and future streams
		done:   make(chan struct{}),
	}
}

func (s *session) Close(err error) {
	if s.isClosing.CompareAndSwap(false, true) {
		s.err = err
		close(s.done)
	}
}

// isActive returns true if the session is active.
func (s *session) isActive() bool {
	return !s.isClosing.Load()
}

// Err returns the error that caused the session to close, or nil if it's still active.
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
		// fast path disconnect to avoid connection attempts during shutdown
		s.Close(ErrPoolClosed)
		return
	}

	relay, err := New(context.Background(), s.url, s.pool.options...)
	if err != nil {
		s.Close(err)
		return
	}
	s.relay.Store(relay)
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
			s.Close(relay.Err())
			return

		case op := <-s.subOps:
			if op.close {
				if sub, ok := s.subs[op.id]; ok {
					delete(s.subs, op.id)
					sub.Close()
				}
				continue
			}

			sub := &Subscription{
				id:      op.id,
				filters: op.filters,
				events:  op.events,
				eose:    make(chan struct{}),
				done:    make(chan struct{}),
			}
			s.subs[sub.id] = sub

			if err := relay.open(sub); err != nil {
				// mark subscription as closed so it can be retried
				sub.isClosing.Store(true)
				s.pool.log.Debug("opening subscription failed", "relay", s.url, "id", sub.ID(), "error", err)
				continue
			}

		case <-retry.C:
			for id, sub := range s.subs {
				if sub.IsActive() {
					continue
				}

				if err := sub.Err(); err != nil {
					s.pool.log.Debug("subscription failed", "relay", s.url, "id", sub.ID(), "error", err)
				}

				// make a new subscription and try to open it.
				// To avoid duplicate events, all filters use a since set to the last time
				// the old subscription reported an event.
				new := &Subscription{
					id:      id,
					filters: withSince(sub.filters, sub.LastEvent()),
					events:  sub.events,
					eose:    make(chan struct{}),
					done:    make(chan struct{}),
				}

				if err := relay.open(new); err != nil {
					s.pool.log.Debug("opening subscription failed", "relay", s.url, "id", id, "error", err)
					continue
				}

				// if it works, update the subscription in the map, othwerwise keep the old
				// because it has the correct LastEvent timestamp.
				s.subs[id] = new
			}
		}
	}
}

// openSub opens a subscription from the given stream.
func (s *session) openSub(id string, filters nostr.Filters, events chan *nostr.Event) {
	if s.isClosing.Load() {
		return
	}

	select {
	case <-s.done:
	case s.subOps <- subOp{id: id, filters: filters, events: events, close: false}:
	default:
		s.pool.log.Warn("session open subscription failed", "session", s.url, "error", "channel is full")
	}
}

// closeSub closes the subscription associated with the given ID.
func (s *session) closeSub(id string) {
	if s.isClosing.Load() {
		return
	}

	select {
	case <-s.done:
	case s.subOps <- subOp{id: id, close: true}:
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
