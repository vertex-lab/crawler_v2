package relays

import (
	"context"
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

	options []Option

	isClosing atomic.Bool
	done      chan struct{}
}

func NewPool(urls []string, opts ...Option) (*Pool, error) {
	p := &Pool{
		streams:    make(map[string]*Stream),
		sessions:   make(map[string]*session, len(urls)),
		dead:       make(chan *session, 100),
		operations: make(chan streamOp, 100),
		options:    opts,
		done:       make(chan struct{}),
	}

	for _, url := range slicex.Unique(urls) {
		if err := ValidateURL(url); err != nil {
			return nil, fmt.Errorf("invalid url %q: %w", url, err)
		}

		s := &session{
			url:        url,
			pool:       p,
			subs:       make(map[string]*Subscription),
			dead:       make(chan string, 100),
			operations: make(chan streamOp, 100),
			done:       make(chan struct{}),
		}

		go s.run()
		p.sessions[url] = s
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
		events:  make(chan *nostr.Event, 10_000),
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

// notifyDeath reports a dead session to the pool.
func (p *Pool) notifyDeath(s *session) {
	if p.isClosing.Load() {
		return
	}

	select {
	case <-p.done:
	case p.dead <- s:
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
				if _, exists := p.streams[op.id]; exists {
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
				if _, exists := p.streams[op.id]; exists {
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

// session manages the connection to a single relay and all its subscriptions.
// When a subscription is closed by the relay, the session restarts it with a backoff.
// When the relay disconnects entirely, the session reports back to the Pool and exits.
type session struct {
	url   string
	relay *Relay

	// subs holds the active subscriptions of this session, keyed by their ID.
	subs map[string]*Subscription

	// dead holds the IDs of subscriptions that have died and need to be restarted.
	dead chan string

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
// Because sessions are ephemeral entities, this methods doesn't return errors.
func (s *session) sendOp(op streamOp) {
	if s.isClosing.Load() {
		return
	}

	select {
	case <-s.done:
	case s.operations <- op:
	default:
		slog.Warn("session sendOp: operations channel full", "session", s.url)
	}
}

// notifyDeath notifies the session when a subscription dies.
func (s *session) notifyDeath(id string) {
	if s.isClosing.Load() {
		return
	}

	select {
	case <-s.done:
	case s.dead <- id:
	}
}

// options returns the session options.
// These are copied from the pool, with the expection of the last WithOnClosed option.
// The WithOnClosed option is fundamental to the session lifecycle, as it's the only way the
// session can react to subscriptions that have been closed by the relay.
func (s *session) options() []Option {
	opts := make([]Option, len(s.pool.options)+1)
	copy(opts, s.pool.options)
	opts = append(opts, WithOnClosed(func(id, reason string) { s.notifyDeath(id) }))
	return opts
}

func (s *session) run() {
	defer s.close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	relay, err := New(ctx, s.url, s.options()...)
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

		case id := <-s.dead:
			// the relay run the onClosed, returning here the subscription ID
			sub, exists := s.subs[id]
			if exists {
				delete(s.subs, id)
				slog.Error("relay closed subscription", "url", s.url, "id", id, "error", sub.Err())

				// TODO: decide whether to retry the subscription or not
			}

		case op := <-s.operations:
			switch op.kind {
			case kindOpen:

				// the subscription re-uses the stream events channel, so
				// events will be routed by the relay.read() automatically,
				// without the need to manually forward them.
				sub := &Subscription{
					id:      op.Stream.id,
					filters: op.Stream.filters,
					events:  op.Stream.events,
					eose:    make(chan struct{}),
					done:    make(chan struct{}),
				}

				if err := s.relay.open(sub); err != nil {
					slog.Error("failed to open subscription", "url", s.url, "id", op.id, "error", err)
					continue
				}
				s.subs[op.id] = sub

			case kindClose:
				sub, exists := s.subs[op.id]
				if exists {
					delete(s.subs, op.id)
					sub.Close()
				}
			}
		}
	}
}
