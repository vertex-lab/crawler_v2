// Package subscription provides a Router to route relay messages to subscription channels.
package subscription

import (
	"errors"
	"sync"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ErrDuplicate    = errors.New("subscription with the same id already exists")
	ErrFullChannel  = errors.New("subscription channel is full")
	ErrInvalidMatch = errors.New("event does not match the subscription filters")
)

// Message represents a message received from a relay.
// It can be either an EVENT, EOSE, or CLOSED message.
type Message struct {
	Event *nostr.Event // non-nil when the relay sent an EVENT
	EOSE  bool         // true when the relay sent EOSE
	Err   error        // non-nil when the relay sent CLOSED
}

// subscription is an internal representation of a subscription.
type subscription struct {
	Filters nostr.Filters
	C       chan<- Message
}

// Router routes incoming relay messages to the appropriate subscription channel.
// All methods are safe for concurrent use.
type Router struct {
	mu   sync.RWMutex
	subs map[string]subscription

	// verifyMatch controls whether the router verifies that events match
	// the subscription's filters before routing it.
	verifyMatch bool
}

// NewRouter creates a new Router. If verifyMatch is true,
// the router will verify that events match the subscription's filters before routing it.
func NewRouter(verifyMatch bool) *Router {
	return &Router{
		subs:        make(map[string]subscription),
		verifyMatch: verifyMatch,
	}
}

// Has returns true if the Router has a subscription with the given ID.
func (r *Router) Has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, ok := r.subs[id]
	return ok
}

// Add registers a subscription channel under the provided id.
// Returns ErrDuplicate if a subscription with the same ID already exists.
func (r *Router) Add(id string, filters nostr.Filters, ch chan<- Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.subs[id]; ok {
		return ErrDuplicate
	}
	r.subs[id] = subscription{Filters: filters, C: ch}
	return nil
}

// Remove unregisters the subscription with the given ID from the Router.
// It does not close the subscription's channel; the caller is responsible for that.
func (r *Router) Remove(id string) {
	r.mu.Lock()
	delete(r.subs, id)
	r.mu.Unlock()
}

// Clear unregisters all subscriptions from the Router.
// It does not close any subscription channels; callers are responsible for that.
func (r *Router) Clear() {
	r.mu.Lock()
	clear(r.subs)
	r.mu.Unlock()
}

// RouteEvent delivers an EVENT message to the subscription with the given ID.
// If the subscription's channel is full, the event is dropped and a warning is logged.
func (r *Router) RouteEvent(id string, e *nostr.Event) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	sub, found := r.subs[id]
	if !found {
		return nil
	}

	if r.verifyMatch && !sub.Filters.Match(e) {
		return ErrInvalidMatch
	}

	select {
	case sub.C <- Message{Event: e}:
		return nil
	default:
		return ErrFullChannel
	}
}

// RouteEOSE delivers an EOSE message to the subscription with the given ID.
// If the subscription's channel is full, the EOSE is dropped and a warning is logged.
func (r *Router) RouteEOSE(id string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	sub, found := r.subs[id]
	if !found {
		return nil
	}

	select {
	case sub.C <- Message{EOSE: true}:
		return nil
	default:
		return ErrFullChannel
	}
}

// RouteClosed delivers a CLOSED message to the subscription with the given ID and removes
// it from the Router.
// The subscription is removed before the send so that no further messages can be routed to it.
// If there is a reader consuming from the channel, it is guaranteed it will eventually drain
// and the blocking send will complete.
func (r *Router) RouteClosed(id string, reason string) {
	r.mu.Lock()
	sub, found := r.subs[id]
	delete(r.subs, id)
	r.mu.Unlock()

	if found {
		sub.C <- Message{Err: errors.New(reason)}
	}
}
