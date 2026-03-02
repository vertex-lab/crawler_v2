package subscription

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/nbd-wtf/go-nostr"
)

var ErrDuplicate = errors.New("subscription with the same id already exists")

// Message represents a message received from a relay.
// It can be either an EVENT, EOSE, or CLOSED message.
type Message struct {
	Event *nostr.Event // non-nil when the relay sent an EVENT
	EOSE  bool         // true when the relay sent EOSE
	Err   error        // non-nil when the relay sent CLOSED
}

func Event(e *nostr.Event) Message { return Message{Event: e} }
func Closed(reason string) Message { return Message{Err: errors.New(reason)} }
func EOSE() Message                { return Message{EOSE: true} }

// T represents an active subscription to a relay.
// Read the subscription messages using the Messages method.
type T struct {
	ID string
	c  chan Message
}

// Messages returns a channel that receives messages from the subscription.
func (s *T) Messages() <-chan Message {
	return s.c
}

// Manager represents a manager for subscriptions.
// It exposes methods to manage the lifecycle of subscriptions.
// All methods are safe for concurrent use.
type Manager struct {
	mu       sync.RWMutex
	subs     map[string]*T
	capacity int
}

// NewManager creates a new Manager. Each subscription will have a buffer of capacity messages.
func NewManager(capacity int) *Manager {
	return &Manager{
		subs:     make(map[string]*T),
		capacity: capacity,
	}
}

// New creates a new subscription and adds it to the manager.
func (m *Manager) New(id string) (*T, error) {
	sub := &T{
		ID: id,
		c:  make(chan Message, m.capacity),
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.subs[id]; exists {
		return nil, ErrDuplicate
	}
	m.subs[id] = sub
	return sub, nil
}

// Close closes the subscription with the given ID.
func (m *Manager) Close(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sub, found := m.subs[id]
	if !found {
		return
	}

	close(sub.c)
	delete(m.subs, id)
}

// CloseAll closes all subscriptions.
func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, sub := range m.subs {
		close(sub.c)
		delete(m.subs, id)
	}
}

// ForceClose reacts to an unexpected closure of the subscription with the given ID and reason.
func (m *Manager) ForceClose(id string, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sub, found := m.subs[id]
	if !found {
		return
	}

	select {
	case sub.c <- Closed(reason):
	default:
		slog.Warn("subscription channel is full, dropped CLOSED", "subscription", id, "reason", reason)
	}

	close(sub.c)
	delete(m.subs, id)
}

// Route routes an event to the subscription with the given ID.
func (m *Manager) Route(id string, event *nostr.Event) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sub, found := m.subs[id]
	if !found {
		return
	}

	select {
	case sub.c <- Event(event):
	default:
		slog.Warn("subscription channel is full, dropped EVENT", "subscription", id, "event", event.ID)
	}
}

// EOSE signals the end of stored events for the subscription with the given ID.
func (m *Manager) EOSE(id string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sub, found := m.subs[id]
	if !found {
		return
	}

	select {
	case sub.c <- EOSE():
	default:
		slog.Warn("subscription channel is full, dropped EOSE", "subscription", id)
	}
}
