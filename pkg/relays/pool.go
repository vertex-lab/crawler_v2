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
//
// Callers must always use new and unique IDs for streams, even if old streams were closed.
// This policy is critical for the relay reconnection logic to work correctly.
type Pool struct {
	// streamIDs holds the IDs of all streams that were ever opened by this pool.
	// The session reconnection logic needs the guarantee that stream IDs are unique, otherwise we might have:
	// - session is running
	// - stream with "ID" is opened
	// - session stops running
	// - stream with "ID" is closed
	// - another stream with the same "ID" is opened
	streamIDs smallset.Ordered[string]
	mu        sync.Mutex // only protects streamIDs

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
		p.wg.Go(s.run)
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

// Wait blocks until at least one relay in the pool is connected.
// It returns an error if the context is canceled/deadline exceeded or the pool is closed.
func (p *Pool) Wait(ctx context.Context) error {
	_, err := p.waitConnected(ctx)
	return fmt.Errorf("waiting for relays: %w", err)
}

// waitConnected blocks until at least one relay in the pool is connected, returning all connected relays.
func (p *Pool) waitConnected(ctx context.Context) ([]*T, error) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	for {
		relays, err := p.relays(ctx)
		if err != nil {
			return nil, err
		}
		if len(relays.connected) > 0 {
			return relays.connected, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.done:
			return nil, ErrPoolClosed
		case <-t.C:
		}
	}
}

// Add adds one or more relay URLs to the pool.
// Add is idempotent, meaning duplicate URLs are ignored.
// Invalid URLs are reported and skipped.
func (p *Pool) Add(urls ...string) error {
	if len(urls) == 0 {
		return nil
	}

	urls = slicex.Unique(urls)
	var errs []error

	for i, url := range urls {
		if err := ValidateURL(url); err != nil {
			errs = append(errs, fmt.Errorf("failed to add relay %q: %w", url, err))
			continue
		}

		select {
		case <-p.done:
			errs = append(errs, fmt.Errorf("failed to add %d relays: %w", len(urls)-i, ErrPoolClosed))
			return errors.Join(errs...)

		case p.sessionOps <- sessionOp{url: url, remove: false}:

		default:
			errs = append(errs, fmt.Errorf("failed to add %d relays: %w", len(urls)-i, ErrSendFailed))
			return errors.Join(errs...)
		}
	}
	return errors.Join(errs...)
}

// Remove removes one or more relay URLs from the pool.
// Remove is idempotent, meaning duplicate URLs are ignored, and removing a relay that is
// not in the pool is a no-op. Invalid URLs are reported and skipped.
func (p *Pool) Remove(urls ...string) error {
	if len(urls) == 0 {
		return nil
	}

	urls = slicex.Unique(urls)
	var errs []error

	for i, url := range urls {
		if err := ValidateURL(url); err != nil {
			errs = append(errs, fmt.Errorf("failed to remove relay %q: %w", url, err))
			continue
		}

		select {
		case <-p.done:
			errs = append(errs, fmt.Errorf("failed to remove %d relays: %w", len(urls)-i, ErrPoolClosed))
			return errors.Join(errs...)

		case p.sessionOps <- sessionOp{url: url, remove: true}:

		default:
			errs = append(errs, fmt.Errorf("failed to remove %d relays: %w", len(urls)-i, ErrSendFailed))
			return errors.Join(errs...)
		}
	}
	return errors.Join(errs...)
}

// sessionOp represents an operation that modifies the pool's sessions.
type sessionOp struct {
	url    string
	remove bool // add = !remove
}

// Query concurrently queries all connected relays for events matching the given filters.
// The returned events are deduplicated and present only the newest event per replacement category (replaceable / addresable).
// Errors are joined together using [errors.Join], and events are always returned even if errors occur.
//
// It is always recommended to use this method with a context timeout (e.g. 10s),
// to avoid bad relays that never send an EOSE (or CLOSED) from blocking indefinitely.
func (p *Pool) Query(ctx context.Context, id string, filters ...nostr.Filter) ([]nostr.Event, error) {
	if p.isClosing.Load() {
		return nil, fmt.Errorf("failed to query: %w", ErrPoolClosed)
	}
	if len(filters) == 0 {
		return nil, nil
	}

	p.mu.Lock()
	isDuplicate := p.streamIDs.Contains(id)
	p.mu.Unlock()

	if isDuplicate {
		return nil, fmt.Errorf("failed to query: %w", ErrDuplicateStream)
	}

	relays, err := p.waitConnected(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}

	type result struct {
		events []nostr.Event
		err    error
	}

	results := make(chan result, len(relays))
	wg := sync.WaitGroup{}

	for _, r := range relays {
		wg.Add(1)
		go func(r *T) {
			defer wg.Done()
			events, err := r.Query(ctx, id, filters...)
			results <- result{events, err}
		}(r)
	}

	wg.Wait()
	close(results)

	sizeHint := expectedSize(filters)
	canonical := make(map[string]nostr.Event, sizeHint)

	for r := range results {
		err = errors.Join(err, r.err)

		for _, e := range r.events {
			k := key(e)
			current, found := canonical[k]
			if !found || preferCandidate(current, e) {
				canonical[k] = e
			}
		}
	}

	events := make([]nostr.Event, 0, len(canonical))
	for _, e := range canonical {
		events = append(events, e)
	}
	return events, nil
}

// key returns a key used for deduplicate events and applying replaceable and addressable event rules.
func key(e nostr.Event) string {
	switch {
	case nostr.IsReplaceableKind(e.Kind):
		return fmt.Sprintf("%d:%s", e.Kind, e.PubKey)

	case nostr.IsAddressableKind(e.Kind):
		return fmt.Sprintf("%d:%s:%s", e.Kind, e.PubKey, e.Tags.GetD())
	default:
		return e.ID
	}
}

// preferCandidate reports whether candidate should replace current.
func preferCandidate(current, candidate nostr.Event) bool {
	if candidate.CreatedAt > current.CreatedAt {
		return true
	}
	if candidate.CreatedAt < current.CreatedAt {
		return false
	}
	// tie-breaker: keep lexicographically lowest ID as per NIP-01
	return candidate.ID < current.ID
}

// Stream creates a new stream with the given id and filters, returning a Stream object
// that can be used to receive events from all relays as they are received without deduplication.
// It returns an error if the pool is closed, or if the stream id is duplicated.
func (p *Pool) Stream(id string, filters ...nostr.Filter) (*Stream, error) {
	if p.isClosing.Load() {
		return nil, fmt.Errorf("failed to create stream: %w", ErrPoolClosed)
	}
	if len(filters) == 0 {
		return nil, nil
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

// cleanup clears all active sessions and streams.
func (p *Pool) cleanup() {
	for _, s := range p.sessions {
		s.close()
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
	p.log.Info("relay pool is running")
	defer p.log.Info("relay pool is down")

	var retry <-chan time.Time
	if p.settings.relayRetry > 0 {
		t := time.NewTicker(p.settings.relayRetry)
		retry = t.C
		defer t.Stop()
	}

	defer p.cleanup()

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
					s.close()
					delete(p.sessions, op.url)
				}
				continue
			}

			if _, ok := p.sessions[op.url]; ok {
				continue
			}

			s := p.newSession(op.url)
			p.sessions[op.url] = s
			p.wg.Go(s.run)

			for _, stream := range p.streams {
				s.openSub(stream.id, stream.filters, stream.events)
			}

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

		case <-retry:
			for url, s := range p.sessions {
				if s.isActive() {
					continue
				}

				new := p.newSession(url)
				p.sessions[url] = new
				p.wg.Go(new.run)

				for _, stream := range p.streams {
					// to avoid duplicate events, retry each subscription with filters whose since
					// reflects the last event of the old subscription.
					// This is possible because the IDs are globally unique thanks to pool.streamIDs
					var lastEvent time.Time
					if sub, ok := s.subs[stream.id]; ok {
						lastEvent = sub.LastEvent()
					}
					new.openSub(stream.id, withSince(stream.filters, lastEvent), stream.events)
				}
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

func (s *session) close() {
	if s.isClosing.CompareAndSwap(false, true) {
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
	if s.pool.isClosing.Load() {
		// fast path disconnect to avoid connection attempts during shutdown
		s.close()
		return
	}

	relay, err := New(context.Background(), s.url, s.pool.options...)
	if err != nil {
		s.pool.log.Debug("relay connection failed", "relay", s.url, "error", err)
		s.close()
		return
	}
	s.relay.Store(relay)
	defer relay.Close()

	s.pool.log.Debug("session is running", "relay", s.url)
	defer s.pool.log.Debug("session is down", "relay", s.url)

	var retry <-chan time.Time
	if s.pool.settings.subRetry > 0 {
		t := time.NewTicker(s.pool.settings.subRetry)
		retry = t.C
		defer t.Stop()
	}

	for {
		if s.isClosing.Load() {
			return
		}

		select {
		case <-s.done:
			return

		case <-relay.Done():
			s.pool.log.Debug("relay disconnected", "relay", s.url, "error", relay.Err())
			s.close()
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
				// mark subscription as closed so it can be retried later
				sub.isClosing.Store(true)
				s.pool.log.Debug("opening subscription failed", "relay", s.url, "id", sub.ID(), "error", err)
				continue
			}

		case <-retry:
			for id, sub := range s.subs {
				if sub.IsActive() {
					continue
				}

				if err := sub.Err(); err != nil {
					s.pool.log.Debug("subscription failed", "relay", s.url, "id", sub.ID(), "error", err)
					sub.err = nil // avoid logging the same error repeatedly
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
