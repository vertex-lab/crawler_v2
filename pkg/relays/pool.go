package relays

// The pool manages a set of relay connections and presents a unified
// subscription API to the caller. Responsibilities are layered:
//
//	Pool   — owns the desired subscription state (source of truth).
//	         Supervises workers and respawns them with backoff when they
//	         die, replaying its subscription state to each new instance.
//
//	worker — owns the active subscriptions for one relay connection.
//	         Supervises forward goroutines and retries individual
//	         subscriptions closed by the relay, with backoff.
//	         Dies when the relay disconnects; reports back to the pool.
//
//	forward — moves events from one subscription to the pool's event
//	          stream. Dies when the subscription closes; reports the
//	          subscription ID back to the worker.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/slicex"
)

var (
	ErrPoolClosed   = fmt.Errorf("pool is closed")
	errWorkerClosed = fmt.Errorf("worker is closed")
)

type Pool struct {
	// For each stream, workers are expected to maintain the corresponding
	// subscription on their respective relays. The pool replays this map to any new worker.
	streams map[string]*Stream

	// workers is the set of active workers, keyed by their relay URL.
	workers map[string]*worker

	// URLs of relays that have disconnected are sent here.
	disconnections chan disconnection

	// operations is the channel for subscribe/unsubscribe requests from the caller.
	operations chan streamOp

	isClosing atomic.Bool
	done      chan struct{}
}

func NewPool(urls ...string) (*Pool, error) {
	p := &Pool{
		streams:        make(map[string]*Stream),
		workers:        make(map[string]*worker, len(urls)),
		disconnections: make(chan disconnection, 100),
		operations:     make(chan streamOp, 100),
	}

	for _, u := range slicex.Unique(urls) {
		if err := ValidateURL(u); err != nil {
			return nil, fmt.Errorf("invalid url %q: %w", u, err)
		}

		w := &worker{
			url:        u,
			pool:       p,
			subs:       make(map[string]*Subscription),
			operations: make(chan streamOp, 100),
			closed:     make(chan *Stream, 100),
			done:       make(chan struct{}),
		}

		go w.run()
		p.workers[u] = w
	}

	go p.run()
	return p, nil
}

// Close closes the pool and all the underlying relay connections.
func (p *Pool) Close() {
	if p.isClosing.CompareAndSwap(false, true) {
		close(p.done)
	}
}

type opKind int

const (
	kindOpen  opKind = 0
	kindClose opKind = 1
)

// streamOp represents an operation to be performed on a stream, including
// the stream itself and an optional channel to receive the result nil for success, non-nil for error.
type streamOp struct {
	*Stream
	kind  opKind
	reply chan error
}

// sendOp sends a stream operation to the pool's operations channel.
// It returns an error if the pool is closed or if the send fails due to backpressure.
func (p *Pool) sendOp(op streamOp) error {
	if p.isClosing.Load() {
		return ErrPoolClosed
	}

	select {
	case <-p.done:
		return ErrPoolClosed

	case p.operations <- op:
		return nil

	default:
		return ErrSendFailed
	}
}

type disconnection struct {
	url string
	err error
}

// Stream creates a new stream with the given id and filters, returning a
// Stream object that can be used to receive events from all connected relays.
// It returns an error if the pool is closed, or if the stream id is duplicated.
func (p *Pool) Stream(id string, filters nostr.Filters) (*Stream, error) {
	if p.isClosing.Load() {
		return nil, fmt.Errorf("failed to create stream: %w", ErrDisconnected)
	}

	s := &Stream{
		id:      id,
		filters: filters,
		pool:    p,
		events:  make(chan *nostr.Event, 1000),
		done:    make(chan struct{}),
	}

	op := streamOp{
		Stream: s,
		kind:   kindOpen,
		reply:  make(chan error, 1),
	}

	if err := p.sendOp(op); err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	select {
	case <-p.done:
		return nil, fmt.Errorf("failed to create stream: %w", ErrPoolClosed)

	case err := <-op.reply:
		if err != nil {
			return nil, fmt.Errorf("failed to create stream: %w", err)
		}
		return s, nil
	}
}

func (p *Pool) run() {
	for {
		select {
		case <-p.done:
			for _, w := range p.workers {
				w.close()
			}
			clear(p.workers)

			for _, s := range p.streams {
				s.err = ErrPoolClosed
				close(s.done)
			}
			clear(p.streams)
			return

		case op := <-p.operations:
			switch op.kind {
			case kindOpen:
				if _, ok := p.streams[op.id]; ok {
					if op.reply != nil {
						op.reply <- ErrDuplicateStream
					}
					continue
				}

				p.streams[op.id] = op.Stream
				if op.reply != nil {
					op.reply <- nil
				}

				for _, w := range p.workers {
					w.sendOp(op)
				}

			case kindClose:
				if _, ok := p.streams[op.id]; ok {
					delete(p.streams, op.id)
					for _, w := range p.workers {
						w.sendOp(op)
					}
				}
			}

		case d := <-p.disconnections:
			delete(p.workers, d.url)
			if d.err != nil {
				slog.Warn("relay disconnected", "relay", d.url, "error", d.err)
			}
			// TODO: Respawn it after a backoff and replay the desired subs.
		}
	}
}

type worker struct {
	url   string
	relay *Relay

	// pool is the pool this worker belongs to, useful in disconnections
	pool *Pool

	// subs holds the active subscriptions on this relay.
	// This is the worker's own view of reality: a subset of the pool's
	// desired state. Entries are missing when the relay has closed them
	// and the worker is waiting to retry.
	subs map[string]*Subscription

	// operations receives subscribe/unsubscribe commands from the pool.
	operations chan streamOp

	// subscriptions closed by the relay are sent here, so the worker can retry them.
	closed chan *Stream

	isClosing atomic.Bool
	done      chan struct{}
}

func (w *worker) close() {
	if w.isClosing.CompareAndSwap(false, true) {
		close(w.done)
	}
}

// sendOp sends a stream operation to the worker's operations channel.
// Because workers are ephemeral entities, this methods doesn't return an error.
func (w *worker) sendOp(op streamOp) {
	if w.isClosing.Load() {
		return
	}

	select {
	case <-w.done:
		return

	case w.operations <- op:
		return

	default:
		slog.Warn("worker sending operation failed", "worker", w.url, "op", op)
		return
	}
}

func (w *worker) signalDisconnect(err error) {
	if w.isClosing.Load() {
		return
	}

	select {
	case <-w.done:
		return

	case w.pool.disconnections <- disconnection{url: w.url, err: err}:
		return
	}
}

func (w *worker) run() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	relay, err := New(ctx, w.url)
	if err != nil {
		w.signalDisconnect(err)
		return
	}

	defer relay.Close()
	w.relay = relay

	for {
		select {
		case <-w.done:
			return

		case <-w.relay.Done():
			// TODO: add method relay.Err() method to know the reason why Done was fired.
			w.signalDisconnect(nil)
			return

		case op := <-w.operations:
			switch op.kind {
			case kindOpen:
				sub, err := w.relay.Subscribe(op.id, op.filters)
				if err != nil {
					slog.Error("failed to open subscription", "url", w.url, "id", op.id, "error", err)
					return
				}

				w.subs[op.id] = sub
				go w.forward(sub, op.Stream)

			case kindClose:
				if sub, ok := w.subs[op.id]; ok {
					sub.Close()
					delete(w.subs, op.id)
				}
			}

		case stream := <-w.closed:
			// the relay closed the subscription associated with the stream.
			_ = stream // TODO: retry with backoff
		}
	}
}

func (w *worker) forward(sub *Subscription, stream *Stream) {
	for {
		select {
		case <-w.done:
			return

		case <-sub.Done():
			// drain any remaining event
			n := len(sub.Events())
			for range n {
				e := <-sub.Events()
				stream.events <- e
			}

			err := sub.Err()
			if err != nil && !errors.Is(err, ErrDisconnected) {
				// the subscription was closed by a CLOSED sent by the relay.
				// send to the worker's closed channel to retry the subscription.
				slog.Warn("subscription was closed by relay", "url", w.url, "id", sub.id, "error", err)
				w.closed <- stream
				return
			}

		case e := <-sub.Events():
			stream.events <- e

			// drain all events to avoid blocking the subscription
			n := len(sub.Events())
			for range n {
				e = <-sub.Events()
				stream.events <- e
			}
		}
	}
}
