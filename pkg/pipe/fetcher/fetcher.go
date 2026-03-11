package fetcher

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/relays"
)

var (
	ErrQueueFull     = errors.New("fetcher queue is full")
	ErrInvalidPubkey = errors.New("invalid pubkey")

	QueryID = "vertex-fetcher" // the REQ id that will be used with a relay or pool
)

// T represents a fetcher that fetches nostr events from a source.
type T struct {
	source  Source
	pubkeys chan string
	config  Config
}

// Source represents a source of nostr events, which can either:
// - a single relay
// - a pool of relays
// - a nostr database like nostr-sqlite
type Source interface {
	Query(ctx context.Context, filters ...nostr.Filter) ([]nostr.Event, error)
}

func New(c Config, source Source) *T {
	return &T{
		config:  c,
		source:  source,
		pubkeys: make(chan string, c.Queue),
	}
}

// Enqueue adds one or more pubkeys to the fetcher's queue.
// Invalid pubkeys are skipped but reported as an error.
func (f *T) Enqueue(pubkeys ...string) error {
	if len(pubkeys) == 0 {
		return nil
	}

	var errs []error
	for _, pk := range pubkeys {
		if len(pk) != 64 || !nostr.IsValidPublicKey(pk) {
			errs = append(errs, ErrInvalidPubkey)
			continue
		}

		select {
		case f.pubkeys <- pk:
		default:
			errs = append(errs, ErrQueueFull)
			return errors.Join(errs...)
		}
	}
	return errors.Join(errs...)
}

// Run starts the fetcher loop, which fetches events for pubkeys from the queue and forwards them.
func (f *T) Run(ctx context.Context, forward func(*nostr.Event) error) {
	slog.Info("Fetcher: ready")
	defer slog.Info("Fetcher: shut down")

	handle := func(pubkeys []string) error {
		events, err := f.fetch(ctx, pubkeys)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}

		for i := range events {
			if err := forward(&events[i]); err != nil {
				return err
			}
		}
		return nil
	}

	batch := make([]string, 0, f.config.Batch)
	timer := time.After(f.config.Interval)

	for {
		select {
		case <-ctx.Done():
			return

		case pubkey := <-f.pubkeys:
			batch = append(batch, pubkey)
			if len(batch) < f.config.Batch {
				continue
			}

			if err := handle(batch); err != nil {
				slog.Error("Fetcher: failed to handle batch", "error", err)
			}

			batch = make([]string, 0, f.config.Batch)
			timer = time.After(f.config.Interval)

		case <-timer:
			timer = time.After(f.config.Interval)
			if len(batch) == 0 {
				continue
			}

			if err := handle(batch); err != nil {
				slog.Error("Fetcher: failed to handle batch", "error", err)
			}

			batch = make([]string, 0, f.config.Batch)
		}
	}
}

func (f *T) fetch(ctx context.Context, pubkeys []string) ([]nostr.Event, error) {
	if len(pubkeys) == 0 {
		return nil, nil
	}

	queryCtx, cancel := context.WithTimeout(ctx, f.config.Timeout)
	defer cancel()

	filter := nostr.Filter{
		Kinds:   f.config.Kinds,
		Authors: pubkeys,
	}
	return f.source.Query(queryCtx, filter)
}

// SourceRelay returns a Source that fetches events from a relay.
func SourceRelay(r *relays.T) Source {
	if r == nil {
		panic("SourceRelay: relay is nil")
	}
	return &sourceRelay{relay: r}
}

type sourceRelay struct {
	relay *relays.T
}

func (s *sourceRelay) Query(ctx context.Context, filters ...nostr.Filter) ([]nostr.Event, error) {
	return s.relay.Query(ctx, QueryID, filters...)
}

// SourcePool returns a Source that fetches events from a relay pool.
func SourcePool(p *relays.Pool) Source {
	if p == nil {
		panic("SourcePool: pool is nil")
	}
	return &sourcePool{pool: p}
}

type sourcePool struct {
	pool *relays.Pool
}

func (s *sourcePool) Query(ctx context.Context, filters ...nostr.Filter) ([]nostr.Event, error) {
	return s.pool.Query(ctx, QueryID, filters...)
}
