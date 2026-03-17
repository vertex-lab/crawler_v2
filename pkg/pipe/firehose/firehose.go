// Package firehose provides a background goroutine that reads the stream of events
// from a [relays.Pool] and forwards them to a caller-provided function.
package firehose

import (
	"context"
	"fmt"
	"log/slog"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/relays"
)

// T represents a firehose that reads the stream of events of a [relays.Pool].
// It deduplicates events using a LRU cache, applies the pubkey policy and forwards the rest.
type T struct {
	pool   *relays.Pool
	seen   *lru.Cache[string, struct{}] // holds the seen event IDs
	policy Policy
	config Config
}

// New creates a new [T]. The config is assumed to already be validated.
func New(c Config, pool *relays.Pool, policy Policy) *T {
	seen, err := lru.New[string, struct{}](c.SeenCache)
	if err != nil {
		panic(fmt.Errorf("Firehose: failed to create seen cache: %w", err))
	}

	return &T{
		pool:   pool,
		seen:   seen,
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
		slog.Error("Firehose: failed to create stream", "error", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-stream.Done():
			slog.Error("Firehose: stream closed", "error", stream.Err())
			return

		case e := <-stream.Events():
			if f.seen.Contains(e.ID) {
				continue
			}
			f.seen.Add(e.ID, struct{}{})

			if !f.policy.Allow(ctx, e.PubKey) {
				continue
			}

			if err := forward(e); err != nil {
				slog.Error("Firehose: failed to forward", "error", err)
			}
		}
	}
}

// Policy decides whether to accept an event from the given pubkey.
type Policy interface {
	Allow(ctx context.Context, pubkey string) bool
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
func ExistPolicy(db db, cacheSize int) Policy {
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
		slog.Error("Firehose: ExistPolicy: failed to check existence", "pubkey", pubkey, "error", err)
		return false
	}

	if exists {
		p.cache.Add(pubkey, struct{}{})
		return true
	}
	return false
}
