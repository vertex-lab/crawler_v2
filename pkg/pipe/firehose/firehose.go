// Package firehose provides a background goroutine that reads the stream of events
// from a [relays.Pool] and forwards them to a caller-provided function.
package firehose

import (
	"context"
	"fmt"
	"log/slog"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/events"
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

			if f.policy.Allow(ctx, e) {
				if err := forward(e); err != nil {
					slog.Error("Firehose: failed to forward", "error", err)
				}
			}
		}
	}
}

// Policy decides whether to allow an event.
type Policy interface {
	Allow(ctx context.Context, e *nostr.Event) bool
}

// TrustPolicy is a [Policy] that allows a pubkey if and only if it already
// exists in the database. The assumptions are:
// - pubkeys in the database are somewhat trusted
// - pubkeys are never removed from the database
func TrustPolicy(db db, cacheSize int) Policy {
	cache, err := lru.New[string, struct{}](cacheSize)
	if err != nil {
		panic(fmt.Errorf("Firehose ExistPolicy: %w", err))
	}
	return &trustPolicy{cache: cache, db: db}
}

type trustPolicy struct {
	cache *lru.Cache[string, struct{}]
	db    db
}

// db is the subset of the database behaviour required by [TrustPolicy].
type db interface {
	Exists(ctx context.Context, pubkey string) (bool, error)
}

func (p *trustPolicy) Allow(ctx context.Context, e *nostr.Event) bool {
	if p.cache.Contains(e.PubKey) {
		return true
	}

	exists, err := p.db.Exists(ctx, e.PubKey)
	if err != nil {
		slog.Error("Firehose: TrustPolicy: failed to check existence", "pubkey", e.PubKey, "error", err)
		return false
	}

	if exists {
		p.cache.Add(e.PubKey, struct{}{})
		return true
	}
	return false
}

// LeakPolicy is a [Policy] that allows events that might contain a leak,
// as defined by the [leaks.Found] package.
func LeakPolicy() Policy {
	return leakPolicy{}
}

type leakPolicy struct{}

func (p leakPolicy) Allow(ctx context.Context, e *nostr.Event) bool {
	return events.ContainsLeak(e)
}

// OrPolicy is a [Policy] that allows an event if any of the given policies allow it.
func OrPolicy(policies ...Policy) Policy {
	return orPolicy{policies}
}

type orPolicy struct {
	policies []Policy
}

func (p orPolicy) Allow(ctx context.Context, e *nostr.Event) bool {
	for _, policy := range p.policies {
		if policy.Allow(ctx, e) {
			return true
		}
	}
	return false
}

// AndPolicy is a [Policy] that allows an event if all of the given policies allow it.
func AndPolicy(policies ...Policy) Policy {
	return andPolicy{policies}
}

type andPolicy struct {
	policies []Policy
}

func (p andPolicy) Allow(ctx context.Context, e *nostr.Event) bool {
	for _, policy := range p.policies {
		if !policy.Allow(ctx, e) {
			return false
		}
	}
	return true
}
