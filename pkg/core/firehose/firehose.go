package firehose

import (
	"context"
	"log/slog"
	"slices"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/relays"
)

// Firehose connects to the [realays.Pool] and streams all new events matching the config.
// It deduplicates events using a simple ring-buffer, applies the Filter to remove
// events from undesired pubkeys, and forwards the rest.
type Firehose struct {
	pool   *relays.Pool
	policy Policy
	config Config
}

// Policy decides whether to accept an event from the given pubkey.
type Policy interface {
	Allow(ctx context.Context, pubkey string) bool
}

// New creates a new [Firehose]. The config is assumed to already be validated.
func New(c Config, pool *relays.Pool, policy Policy) *Firehose {
	return &Firehose{
		pool:   pool,
		policy: policy,
		config: c,
	}
}

// Run connects to the relays and forwards incoming events until the context is cancelled.
func (f *Firehose) Run(ctx context.Context, forward func(*nostr.Event) error) {
	slog.Info("Firehose: ready")
	defer slog.Info("Firehose: shut down")

	stream, err := f.pool.Stream("vertex-firehose", f.config.Filter())
	if err != nil {
		slog.Error("Firehose: failed to create stream", "error", err)
		return
	}

	// It is unlikely that we'll see the same event ID twice after all of these events.
	seen := newRingBuffer(2048)

	for {
		select {
		case <-ctx.Done():
			return

		case <-stream.Done():
			slog.Error("Firehose: stream closed", "error", stream.Err())
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
				slog.Error("Firehose: failed to forward", "error", err)
			}
		}
	}
}

// DB is the subset of the database behaviour required by [ExistPolicy].
type DB interface {
	Exists(ctx context.Context, pubkey string) (bool, error)
}

// ExistPolicy is a [Policy] that allows a pubkey if and only if it already
// exists in the database. It keeps an LRU cache because the assumption is that
// keys are never removed from the database.
type ExistPolicy struct {
	cache *lru.Cache[string, struct{}]
	db    DB
}

func NewExistPolicy(db DB, cacheSize int) (*ExistPolicy, error) {
	cache, err := lru.New[string, struct{}](cacheSize)
	if err != nil {
		return nil, err
	}
	return &ExistPolicy{cache: cache, db: db}, nil
}

func (p *ExistPolicy) Allow(ctx context.Context, pubkey string) bool {
	if p.cache.Contains(pubkey) {
		return true
	}

	exists, err := p.db.Exists(ctx, pubkey)
	if err != nil {
		slog.Error("Firehose: ExistPolicy: failed to check existence", "pubkey", pubkey, "error", err)
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
