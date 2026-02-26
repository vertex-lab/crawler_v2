package firehose

import (
	"context"
	"log/slog"
	"slices"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nbd-wtf/go-nostr"
)

// Firehose connects to a list of relays and streams all new events matching the config.
// It deduplicates events using a simple ring-buffer, applies the Filter to remove
// events from undesired pubkeys, and forwards the rest.
type Firehose struct {
	pool   *nostr.SimplePool
	filter Filter
	seen   *ringBuffer
	config Config
}

// Filter decides whether to accept an event from the given pubkey.
type Filter interface {
	Allow(ctx context.Context, pubkey string) bool
}

// New creates a new [Firehose]. The config is assumed to already be validated.
func New(config Config, filter Filter) *Firehose {
	return &Firehose{
		filter: filter,
		seen:   newRingBuffer(1024),
		config: config,
	}
}

// Run connects to the relays and forwards incoming events to forward until ctx is cancelled.
func (f *Firehose) Run(ctx context.Context, forward func(*nostr.Event) error) {
	slog.Info("Firehose: ready")
	defer slog.Info("Firehose: shut down")

	f.pool = nostr.NewSimplePool(ctx)
	defer shutdown(f.pool)

	since := nostr.Timestamp(time.Now().Add(-f.config.Offset).Unix())
	sub := nostr.Filter{
		Kinds: f.config.Kinds,
		Since: &since,
	}

	for event := range f.pool.SubscribeMany(ctx, f.config.Relays, sub) {
		if f.seen.Contains(event.ID) {
			continue
		}
		f.seen.Add(event.ID)

		if !f.filter.Allow(ctx, event.PubKey) {
			continue
		}

		if err := forward(event.Event); err != nil {
			slog.Error("Firehose: failed to forward", "error", err)
		}
	}
}

// shutdown closes all relay connections in the pool.
func shutdown(pool *nostr.SimplePool) {
	pool.Relays.Range(func(_ string, relay *nostr.Relay) bool {
		relay.Close()
		return true
	})
}

// NodeStore is the subset of the database behaviour required by [ExistenceGate].
type NodeStore interface {
	Exists(ctx context.Context, pubkey string) (bool, error)
}

// ExistenceGate is a [Filter] that allows a pubkey if and only if it already
// exists in the database. It keeps an LRU cache because the assumption is that
// keys are never removed from the database.
type ExistenceGate struct {
	cache    *lru.Cache[string, struct{}]
	fallback NodeStore
}

func NewExistenceGate(fallback NodeStore, cacheSize int) (*ExistenceGate, error) {
	cache, err := lru.New[string, struct{}](cacheSize)
	if err != nil {
		return nil, err
	}
	return &ExistenceGate{cache: cache, fallback: fallback}, nil
}

func (g *ExistenceGate) Allow(ctx context.Context, pubkey string) bool {
	if g.cache.Contains(pubkey) {
		return true
	}

	exists, err := g.fallback.Exists(ctx, pubkey)
	if err != nil {
		slog.Error("ExistenceGate: failed to check existence", "pubkey", pubkey, "error", err)
		return false
	}

	if exists {
		g.cache.Add(pubkey, struct{}{})
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
