package relays

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
	ErrPoolClosed = fmt.Errorf("pool is closed")
)

// Pool manages a set of relay connections and presents a unified streaming
// API to the caller. It owns the desired stream state and is responsible
// for maintaining it across relay disconnections: when a session dies, the
// Pool respawns it with backoff and replays its streams to the new instance.
type Pool struct {
	// The currently active streams, keyed by their ID.
	streams map[string]*Stream

	// sessions holds the currently active sessions, keyed by their relay URL.
	sessions map[string]*session

	// dead receives sessions that are no longer active.
	dead chan *session

	// operations is the channel for subscribe/unsubscribe requests from the caller.
	operations chan streamOp

	isClosing atomic.Bool
	done      chan struct{}
}

func NewPool(urls ...string) (*Pool, error) {
	size := max(len(urls), 100)
	p := &Pool{
		streams:    make(map[string]*Stream),
		sessions:   make(map[string]*session, size),
		dead:       make(chan *session, size),
		operations: make(chan streamOp, 100),
		done:       make(chan struct{}),
	}

	for _, u := range slicex.Unique(urls) {
		if err := ValidateURL(u); err != nil {
			return nil, fmt.Errorf("invalid url %q: %w", u, err)
		}

		s := &session{
			url:        u,
			pool:       p,
			pipes:      make(map[string]*pipe),
			dead:       make(chan *pipe, 100),
			operations: make(chan streamOp, 100),
			done:       make(chan struct{}),
		}

		go s.run()
		p.sessions[u] = s
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

// notifyDeath sends a session to the pool's dead channel, unless the pool is closing.
func (p *Pool) notifyDeath(s *session) {
	if p.isClosing.Load() {
		return
	}

	select {
	case <-p.done:
	case p.dead <- s:
	}
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
			for _, s := range p.sessions {
				s.close()
			}
			clear(p.sessions)

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

				for _, s := range p.sessions {
					s.sendOp(op)
				}

			case kindClose:
				if _, ok := p.streams[op.id]; ok {
					delete(p.streams, op.id)
					for _, s := range p.sessions {
						s.sendOp(op)
					}
				}
			}

		case s := <-p.dead:
			delete(p.sessions, s.url)
			if s.err != nil {
				slog.Warn("relay disconnected", "relay", s.url, "error", s.err)
			}
			// TODO: Respawn it after a backoff and replay the desired subs.
		}
	}
}

// session manages the connection to a single relay.
// It owns the pipes that send events from one subscription to a stream.
// When a pipe dies because the relay closed the subscription, the session
// retries it with backoff. When the relay disconnects entirely, the session
// reports back to the Pool and exits.
type session struct {
	url   string
	relay *Relay

	// pipes holds the active pipes of this session, keyed by subscription/stream ID.
	pipes map[string]*pipe

	// dead holds the pipes that have died and need to be retried.
	dead chan *pipe

	// operations receives subscribe/unsubscribe commands from the pool.
	operations chan streamOp

	// pool is the pool this session belongs to.
	pool *Pool

	isClosing atomic.Bool
	done      chan struct{}
	err       error // holds the disconnection error
}

func (s *session) close() {
	if s.isClosing.CompareAndSwap(false, true) {
		if s.relay != nil {
			s.relay.Close()
		}
		close(s.done)
	}
}

// sendOp sends a stream operation to the session's operations channel.
// Because sessions are ephemeral entities, this methods doesn't return an error.
func (s *session) sendOp(op streamOp) {
	if s.isClosing.Load() {
		return
	}

	select {
	case <-s.done:
	case s.operations <- op:
	default:
		slog.Warn("session sending operation failed", "session", s.url, "op", op)
	}
}

// notifyDeath sends a pipe to the session's dead channel, unless the session is closing.
func (s *session) notifyDeath(p *pipe) {
	if s.isClosing.Load() {
		return
	}

	select {
	case <-s.done:
	case s.dead <- p:
	}
}

func (s *session) run() {
	defer s.close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	relay, err := New(ctx, s.url)
	if err != nil {
		s.err = err
		s.pool.notifyDeath(s)
		return
	}
	s.relay = relay

	for {
		select {
		case <-s.done:
			return

		case <-s.relay.Done():
			// TODO: add method relay.Err() method to know the reason why Done was fired.
			s.pool.notifyDeath(s)
			return

		case op := <-s.operations:
			switch op.kind {
			case kindOpen:
				sub, err := s.relay.Subscribe(op.id, op.filters)
				if err != nil {
					slog.Error("failed to open subscription", "url", s.url, "id", op.id, "error", err)
					continue
				}

				p := &pipe{
					sub:     sub,
					stream:  op.Stream,
					session: s,
				}

				s.pipes[op.id] = p
				go p.run()

			case kindClose:
				if p, ok := s.pipes[op.id]; ok {
					p.sub.Close()
					delete(s.pipes, op.id)
				}
			}

		case pipe := <-s.dead:
			// the relay closed the subscription associated with the stream.
			_ = pipe // TODO: retry with backoff
		}
	}
}

// pipe forwards events from a subscription to its parent stream.
// When the subscription is closed, the pipe reports back to the session,
// unless the closure is due to the relay disconnecting.
type pipe struct {
	sub    *Subscription
	stream *Stream

	// session is the parent session of this pipe.
	session *session
}

func (p *pipe) run() {
	for {
		select {
		case <-p.sub.Done():
			err := p.sub.Err()
			if err != nil && !errors.Is(err, ErrDisconnected) {
				// the subscription was closed by a CLOSED sent by the relay.
				// send to the session's closed channel to retry the subscription.
				p.session.notifyDeath(p)
			}

			// drain any remaining event
			n := len(p.sub.Events())
			for range n {
				e := <-p.sub.Events()
				p.stream.events <- e
			}
			return

		case e := <-p.sub.Events():
			p.stream.events <- e

			// drain all events to avoid blocking the subscription
			n := len(p.sub.Events())
			for range n {
				e = <-p.sub.Events()
				p.stream.events <- e
			}
		}
	}
}
