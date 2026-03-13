package relays

import (
	"sync/atomic"

	"github.com/nbd-wtf/go-nostr"
)

// Stream represents a stream of events matching a set of filters across a pool of relays.
type Stream struct {
	id      string
	filters nostr.Filters
	events  chan *nostr.Event

	pool *Pool

	isClosing atomic.Bool
	done      chan struct{}
	err       error // holds the reason for closure
}

// ID returns the stream's unique identifier.
func (s *Stream) ID() string {
	return s.id
}

// Filters returns the stream filters.
func (s *Stream) Filters() nostr.Filters {
	f := make(nostr.Filters, len(s.filters))
	copy(f, s.filters)
	return f
}

// Events returns the channel from which events can be read.
func (s *Stream) Events() <-chan *nostr.Event {
	return s.events
}

// Done returns a channel that is closed when the stream is done.
func (s *Stream) Done() <-chan struct{} {
	return s.done
}

// Err returns the reason for the stream closure.
// If the stream is still active (Done hasn't fired), or if it was closed with
// Stream.Close, Err returns nil.
func (s *Stream) Err() error {
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}

// IsActive returns true if the stream is still active (not closing).
func (s *Stream) IsActive() bool {
	return !s.isClosing.Load()
}

// IsDone returns true if the stream is done, which appens after the closing process.
// Note that exists a brief time window where s.IsActive() and s.isDone() are both false.
func (s *Stream) IsDone() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// Close closes the stream, releasing resources.
func (s *Stream) Close() {
	if s.isClosing.CompareAndSwap(false, true) {
		select {
		case <-s.pool.done:
		case s.pool.streamOps <- streamOp{Stream: s, close: true}:
		}
		close(s.done)
	}
}
