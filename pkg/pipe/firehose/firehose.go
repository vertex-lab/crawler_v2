// Package firehose provides a background goroutine that reads the stream of events
// from a [relays.Pool] and forwards them to a caller-provided function.
package firehose

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/relays"
)

// T represents a firehose that reads the stream of events of a [relays.Pool].
// It deduplicates events using a simple ring-buffer, applies the pubkey policy and forwards the rest.
type T struct {
	pool   *relays.Pool
	policy Policy
	config Config
}

// Policy decides whether to accept an event from the given pubkey.
type Policy interface {
	Allow(ctx context.Context, pubkey string) bool
}

// New creates a new [T]. The config is assumed to already be validated.
func New(c Config, pool *relays.Pool, policy Policy) *T {
	return &T{
		pool:   pool,
		policy: policy,
		config: c,
	}
}

// Run connects to the relays and forwards incoming events until the context is cancelled.
func (f *T) Run(ctx context.Context, forward func(*nostr.Event) error) {
	slog.Info("Firehose: ready")
	defer slog.Info("Firehose: shut down")

	stream, err := f.pool.Stream("vertex-firehose", f.config.Filter())
	if err != nil {
		slog.Error("T: failed to create stream", "error", err)
		return
	}

	// It is unlikely that we'll see the same event ID twice after all of these events.
	seen := newRingBuffer(2048)

	for {
		select {
		case <-ctx.Done():
			return

		case <-stream.Done():
			slog.Error("T: stream closed", "error", stream.Err())
			return

		case e := <-stream.Events():
			if seen.Contains(e.ID) {
				continue
			}
			seen.Add(e.ID)

			if !f.policy.Allow(ctx, e.PubKey) {
				continue
			}

			if err := forward(e); err != nil {
				slog.Error("T: failed to forward", "error", err)
			}
		}
	}
}

// db is the subset of the database behaviour required by [ExistPolicy].
type db interface {
	Exists(ctx context.Context, pubkey string) (bool, error)
}

// ExistPolicy is a [Policy] that allows a pubkey if and only if it already
// exists in the database. It keeps an LRU cache because the assumption is that
// keys are never removed from the database.
type existPolicy struct {
	cache *lru.Cache[string, struct{}]
	db    db
}

// ExistPolicy is a [Policy] that allows a pubkey if and only if it already
// exists in the database. It keeps an LRU cache because the assumption is that
// keys are never removed from the database.
func ExistPolicy(db db, cacheSize int) *existPolicy {
	cache, err := lru.New[string, struct{}](cacheSize)
	if err != nil {
		panic(fmt.Errorf("Firehose ExistPolicy: %w", err))
	}
	return &existPolicy{cache: cache, db: db}
}

func (p *existPolicy) Allow(ctx context.Context, pubkey string) bool {
	if p.cache.Contains(pubkey) {
		return true
	}

	exists, err := p.db.Exists(ctx, pubkey)
	if err != nil {
		slog.Error("T: ExistPolicy: failed to check existence", "pubkey", pubkey, "error", err)
		return false
	}

	if exists {
		p.cache.Add(pubkey, struct{}{})
		return true
	}
	return false
}

// ringBuffer is a fixed-capacity ring-buffer used for event deduplication.
type ringBuffer struct {
	IDs      []string
	capacity int
	write    int
}

func newRingBuffer(capacity int) *ringBuffer {
	return &ringBuffer{
		IDs:      make([]string, capacity),
		capacity: capacity,
	}
}

func (b *ringBuffer) Add(ID string) {
	b.IDs[b.write] = ID
	b.write = (b.write + 1) % b.capacity
}

func (b *ringBuffer) Contains(ID string) bool {
	return slices.Contains(b.IDs, ID)
}
