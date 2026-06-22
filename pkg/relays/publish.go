package relays

import "sync"

// pubRouter routes incoming OK messages to the appropriate pending publish caller.
// All methods are safe for concurrent use.
type pubRouter struct {
	mu         sync.Mutex
	publishers map[string]chan OK
}

// newPubRouter creates a new pubRouter.
func newPubRouter() *pubRouter {
	return &pubRouter{
		publishers: make(map[string]chan OK),
	}
}

// Add registers a channel to receive the OK for the given event ID.
// Returns [ErrDuplicatePub] if the publisher already exists.
func (p *pubRouter) Add(id string, ch chan OK) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.publishers[id]; ok {
		return ErrDuplicatePub
	}
	p.publishers[id] = ch
	return nil
}

// Remove unregisters the publisher for the given event ID.
func (p *pubRouter) Remove(id string) {
	p.mu.Lock()
	delete(p.publishers, id)
	p.mu.Unlock()
}

// Deliver sends an OK to the publisher registered for ok.ID, if any.
// Returns true if a publisher was found, false otherwise.
func (p *pubRouter) Deliver(ok OK) bool {
	p.mu.Lock()
	ch, found := p.publishers[ok.ID]
	p.mu.Unlock()

	if !found {
		return false
	}

	select {
	case ch <- ok:
	default:
	}
	return true
}

// Clear clears the map of pending publishers.
func (p *pubRouter) Clear() {
	p.mu.Lock()
	p.publishers = make(map[string]chan OK)
	p.mu.Unlock()
}
