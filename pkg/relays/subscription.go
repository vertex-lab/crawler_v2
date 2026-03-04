package relays

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ErrDuplicateSub    = errors.New("subscription with the same id already exists")
	ErrFullSubChannel  = errors.New("subscription channel is full")
	ErrInvalidSubMatch = errors.New("event does not match the subscription filters")
)

type Subscription struct {
	ID      string
	Filters nostr.Filters
	relay   *Relay // pointer to the parent relay, useful for the close

	events chan *nostr.Event

	afterEOSE atomic.Bool
	eose      chan struct{}

	closed atomic.Bool
	done   chan struct{}
	err    error // holds the reason for subscription closure
}

// Close closes the subscription, releasing resources.
func (s *Subscription) Close() {
	if s.closed.CompareAndSwap(false, true) {
		s.relay.subs.Remove(s.ID)
		err := s.relay.send(Close{ID: s.ID})
		if err != nil && !errors.Is(err, ErrDisconnected) {
			s.relay.log.Warn("failed to send close", "relay", s.relay.url, "error", err)
		}
		close(s.done)
	}
}

// Events returns a channel that receives events published by the relay.
func (s *Subscription) Events() <-chan *nostr.Event {
	return s.events
}

// EOSE returns a channel that is closed when the relay received an EOSE.
func (s *Subscription) EOSE() <-chan struct{} {
	return s.eose
}

// IsLive returns true if EOSE already fired,
// which means the subscription is receiving events as they are published live.
func (s *Subscription) IsLive() bool {
	return s.afterEOSE.Load()
}

// IsActive returns true if the subscription is still active.
func (s *Subscription) IsActive() bool {
	return !s.closed.Load()
}

// Done returns a channel that is closed when the subscription is done.
// The reason of the closure is available via the Err method.
func (s *Subscription) Done() <-chan struct{} {
	return s.done
}

// Err returns the reason for the subscription closure.
// If the subscription is still active (Done hasn't fired), Err returns nil.
func (s *Subscription) Err() error {
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}

// subRouter routes incoming relay messages to the appropriate subscription channel.
// All methods are safe for concurrent use.
type subRouter struct {
	mu   sync.RWMutex
	subs map[string]*Subscription

	// verifyMatch controls whether the router verifies that events match
	// the subscription's filters before routing it.
	verifyMatch bool
}

// NewRouter creates a new subRouter. If verifyMatch is true,
// the router will verify that events match the subscription's filters before routing it.
func newRouter(verifyMatch bool) *subRouter {
	return &subRouter{
		subs:        make(map[string]*Subscription),
		verifyMatch: verifyMatch,
	}
}

// Has returns true if the subRouter has a subscription with the given ID.
func (r *subRouter) Has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, ok := r.subs[id]
	return ok
}

// Add registers a subscription channel under the provided id.
// Returns ErrDuplicate if a subscription with the same ID already exists.
func (r *subRouter) Add(s *Subscription) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.subs[s.ID]; ok {
		return ErrDuplicateSub
	}
	r.subs[s.ID] = s
	return nil
}

// Remove unregisters and closes the subscription with the given ID.
func (r *subRouter) Remove(id string) {
	r.mu.Lock()
	delete(r.subs, id)
	r.mu.Unlock()
}

// Clear closes and unregisters all subscriptions.
func (r *subRouter) Clear(err error) {
	r.mu.Lock()
	subs := r.subs
	r.subs = make(map[string]*Subscription)
	r.mu.Unlock()

	for _, s := range subs {
		if s.closed.CompareAndSwap(false, true) {
			s.err = err
			close(s.done)
		}
	}
}

// Route delivers an EVENT message to the subscription with the given ID.
// If the subscription's channel is full, the event is dropped and ErrFullChannel is returned.
func (r *subRouter) Route(id string, e *nostr.Event) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	s, found := r.subs[id]
	if !found {
		return nil
	}

	if r.verifyMatch && !s.Filters.Match(e) {
		return ErrInvalidSubMatch
	}

	select {
	case s.events <- e:
		return nil
	default:
		return ErrFullSubChannel
	}
}

// SignalEOSE signals an EOSE message to the subscription with the given ID.
func (r *subRouter) SignalEOSE(id string) {
	r.mu.RLock()
	s, found := r.subs[id]
	r.mu.RUnlock()

	if found && s.afterEOSE.CompareAndSwap(false, true) {
		close(s.eose)
	}
}

// SignalClosed delivers a CLOSED message to the subscription with the given ID and removes
// it from the subRouter.
func (r *subRouter) SignalClosed(id string, reason string) {
	r.mu.Lock()
	s, found := r.subs[id]
	delete(r.subs, id)
	r.mu.Unlock()

	if found && s.closed.CompareAndSwap(false, true) {
		s.err = errors.New(reason)
		close(s.done)
	}
}
