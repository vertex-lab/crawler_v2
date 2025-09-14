package pipe

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"slices"
	"syscall"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ErrEventTooBig = errors.New("event is too big")
	maxTags        = 20_000
	maxContent     = 50_000
)

// EventTooBig is a [nastro.EventPolicy] that errs if the event is too big.
func EventTooBig(e *nostr.Event) error {
	if len(e.Tags) > maxTags {
		return fmt.Errorf("%w: event with ID %s has too many tags: %d", ErrEventTooBig, e.ID, len(e.Tags))
	}
	if len(e.Content) > maxContent {
		return fmt.Errorf("%w: event with ID %s has too much content: %d", ErrEventTooBig, e.ID, len(e.Content))
	}
	return nil
}

// HandleSignals listens for OS signals and triggers context cancellation.
func HandleSignals(cancel context.CancelFunc) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals

	log.Println("signal received. shutting down...")
	cancel()
}

// Shutdown iterates over the relays in the pool and closes all connections.
func shutdown(pool *nostr.SimplePool) {
	pool.Relays.Range(func(_ string, relay *nostr.Relay) bool {
		relay.Close()
		return true
	})
}

type buffer struct {
	IDs      []string
	capacity int
	write    int
}

func newBuffer(capacity int) *buffer {
	return &buffer{
		IDs:      make([]string, capacity),
		capacity: capacity,
	}
}

func (b *buffer) Add(ID string) {
	b.IDs[b.write] = ID
	b.write = (b.write + 1) % b.capacity
}

func (b *buffer) Contains(ID string) bool {
	return slices.Contains(b.IDs, ID)
}
