package subscription

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/nbd-wtf/go-nostr"
)

// T represents an active subscription to a relay.
//
// - Events are streamed on the Events channel.
//
// - Eose is closed when the relay signals that all stored events have been sent.
//
// - Closed receives exactly one value when the subscription ends, then is closed.
// This value is nil if the subscription context was cancelled, and it's non-nil when
// the relay sent a CLOSED message.
type T struct {
	Events chan nostr.Event
	Eose   chan struct{}
	Closed chan error
}

// Manager represents a manager for subscriptions.
// It exposes methods to manage the lifecycle of subscriptions.
// All methods are safe for concurrent use.
type Manager struct {
	mu   sync.Mutex
	subs map[string]*T
}

func NewManager() *Manager {
	return &Manager{
		subs: make(map[string]*T),
	}
}

// New creates a new subscription and adds it to the manager.
func (m *Manager) New(id string) *T {
	sub := &T{
		Events: make(chan nostr.Event),
		Eose:   make(chan struct{}),
		Closed: make(chan error),
	}

	m.mu.Lock()
	m.subs[id] = sub
	m.mu.Unlock()
	return sub
}

// Close closes the subscription with the given ID.
func (m *Manager) Close(id string) {
	m.mu.Lock()
	sub, found := m.subs[id]
	m.mu.Unlock()

	if !found {
		return
	}

	sub.Closed <- nil
	close(sub.Closed)

	m.mu.Lock()
	delete(m.subs, id)
	m.mu.Unlock()
}

// CloseAll closes all subscriptions.
func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, sub := range m.subs {
		sub.Closed <- nil
		close(sub.Closed)
		delete(m.subs, id)
	}
}

// ForceClose reacts to an unexpected closure of the subscription with the given ID and reason.
func (m *Manager) ForceClose(id string, reason string) {
	m.mu.Lock()
	sub, found := m.subs[id]
	m.mu.Unlock()

	if !found {
		return
	}

	sub.Closed <- errors.New(reason)
	close(sub.Closed)

	m.mu.Lock()
	delete(m.subs, id)
	m.mu.Unlock()
}

// Route routes an event to the subscription with the given ID.
func (m *Manager) Route(id string, event nostr.Event) {
	m.mu.Lock()
	sub, found := m.subs[id]
	m.mu.Unlock()

	if !found {
		return
	}

	select {
	case sub.Events <- event:
	default:
		slog.Warn("subscription events channel is full", "subscription", id, "event", event.ID)
	}
}

// EOSE signals the end of stored events for the subscription with the given ID.
func (m *Manager) EOSE(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sub, found := m.subs[id]
	if !found {
		return
	}
	close(sub.Eose)
}
