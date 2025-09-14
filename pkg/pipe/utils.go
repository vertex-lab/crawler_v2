package pipe

import (
	"context"
	"log"
	"os"
	"os/signal"
	"slices"
	"syscall"
)

// HandleSignals listens for OS signals and triggers context cancellation.
func HandleSignals(cancel context.CancelFunc) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals

	log.Println("signal received. shutting down...")
	cancel()
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
