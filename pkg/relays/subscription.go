package relays

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

type Subscription struct {
	id      string
	filters nostr.Filters
	relay   *T // pointer to the parent relay, useful for the close

	lastEvent atomic.Int64
	events    chan *nostr.Event

	afterEOSE atomic.Bool
	eose      chan struct{}

	isClosing atomic.Bool
	done      chan struct{}
	err       error // holds the reason for subscription closure
}

// ID returns the subscription ID.
func (s *Subscription) ID() string {
	return s.id
}

// Filters returns the subscription filters.
func (s *Subscription) Filters() nostr.Filters {
	f := make(nostr.Filters, len(s.filters))
	copy(f, s.filters)
	return f
}

// Close closes the subscription, releasing resources.
func (s *Subscription) Close() {
	if s.isClosing.CompareAndSwap(false, true) {
		s.relay.subs.Remove(s.id)
		err := s.relay.send(Close{ID: s.id})
		if err != nil && !errors.Is(err, ErrDisconnected) {
			s.relay.log.Error("failed to send close", "relay", s.relay.url, "error", err)
		}
		close(s.done)
	}
}

// LastEvent returns the last time an event was received.
// If the subscription has never received an event, it returns the zero time.
func (s *Subscription) LastEvent() time.Time {
	t := s.lastEvent.Load()
	if t == 0 {
		return time.Time{}
	}
	return time.Unix(t, 0)
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

// IsActive returns true if the subscription is still active (not closing).
func (s *Subscription) IsActive() bool {
	return !s.isClosing.Load()
}

// IsDone returns true if the subscription is done, which appens after the closing process.
// Note that exists a brief time window where s.IsActive() and s.isDone() are both false.
func (s *Subscription) IsDone() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// Done returns a channel that is closed when the subscription is done.
// The reason of the closure is available via the Err method.
func (s *Subscription) Done() <-chan struct{} {
	return s.done
}

// Err returns the reason for the subscription closure.
// If the subscription is still active (Done hasn't fired), or if it was closed with
// Subscription.Close, Err returns nil.
// If the subscription was closed by the relay (with a CLOSED message), Err returns the reason
// wrapped as [ErrClosedSub].
func (s *Subscription) Err() error {
	// We can't use s.isClosing directly because it would exist
	// a brief time where the isClosing is true but the err hasn't been set.
	// Using the channel is safe because we always set the error and then close the channel.
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

	if _, ok := r.subs[s.id]; ok {
		return ErrDuplicateSub
	}
	r.subs[s.id] = s
	return nil
}

// Remove unregisters and closes the subscription with the given ID.
func (r *subRouter) Remove(id string) {
	r.mu.Lock()
	delete(r.subs, id)
	r.mu.Unlock()
}

// Clear closes and unregisters all subscriptions.
func (r *subRouter) Clear() {
	r.mu.Lock()
	subs := r.subs
	r.subs = make(map[string]*Subscription)
	r.mu.Unlock()

	for _, s := range subs {
		if s.isClosing.CompareAndSwap(false, true) {
			s.err = ErrDisconnected
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

	if r.verifyMatch && !s.filters.Match(e) {
		return fmt.Errorf("%w: filters %v, event %s, pubkey %s, kind %d", ErrInvalidSubMatch, s.filters, e.ID, e.PubKey, e.Kind)
	}

	select {
	case s.events <- e:
		s.lastEvent.Store(time.Now().Unix())
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

	if found && s.isClosing.CompareAndSwap(false, true) {
		s.err = fmt.Errorf("%w: %s", ErrClosedSub, reason)
		close(s.done)
	}
}
