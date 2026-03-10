package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/slicex"
	"github.com/vertex-lab/crawler_v2/pkg/core"
	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
	"github.com/vertex-lab/crawler_v2/pkg/walks"
	sqlite "github.com/vertex-lab/nostr-sqlite"
)

var (
	ErrIsClosing = errors.New("engine is closing")
	ErrQueueFull = errors.New("engine queue is full")
)

// T is the unified engine that consumes events from an internal queue and
// dispatches logic by event kind.
type T struct {
	store *sqlite.Store
	graph regraph.DB
	cache *walks.CachedWalker

	isClosing atomic.Bool
	events    chan *nostr.Event

	// Updated by kind:3 processing; read/drained by Arbiter polling.
	walksUpdated atomic.Int64
	config       Config
}

// New creates a new Engine instance.
// Config is assumed to be validated by the caller.
func New(c Config, store *sqlite.Store, graph regraph.DB) *T {
	return &T{
		config: c,
		store:  store,
		graph:  graph,
		events: make(chan *nostr.Event, c.Queue),
		cache: walks.NewWalker(
			walks.WithCapacity(c.CacheCapacity),
			walks.WithFallback(graph),
		),
	}
}

// Enqueue adds an event to the engine queue.
// It is non-blocking and returns ErrQueueFull when the queue is saturated.
func (e *T) Enqueue(event *nostr.Event) error {
	if event == nil {
		return errors.New("event is nil")
	}
	if e.isClosing.Load() {
		return ErrIsClosing
	}

	select {
	case e.events <- event:
		return nil
	default:
		return ErrQueueFull
	}
}

// WalksUpdated returns how many walk updates happened since the previous call.
func (e *T) WalksUpdated() int {
	return int(e.walksUpdated.Swap(0))
}

func (e *T) Run(ctx context.Context) {
	slog.Info("Engine: ready")
	defer slog.Info("Engine: shut down")

	processed := 0
	handle := func(event *nostr.Event) {
		if event == nil || !slices.Contains(e.config.Kinds, event.Kind) {
			return
		}
		if err := e.process(event); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Engine: failed to process event", "error", err, "id", event.ID, "kind", event.Kind, "pubkey", event.PubKey)
			return
		}
		processed++
		if e.config.PrintEvery > 0 && processed%e.config.PrintEvery == 0 {
			slog.Info("Engine", "processed", processed)
		}
	}

	for {
		select {
		case <-ctx.Done():
			e.isClosing.Store(true)

			// process buffered items
			for {
				select {
				case event := <-e.events:
					handle(event)
				default:
					return
				}
			}

		case event := <-e.events:
			handle(event)
		}
	}
}

func (e *T) process(event *nostr.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.config.Timeout)
	defer cancel()

	switch event.Kind {
	case nostr.KindFollowList:
		return e.updateGraph(ctx, event)

	default:
		// other kinds are archived normally.
		return e.archive(ctx, event)
	}
}

// updateGraph updates the graph using the given follow-list event:
// - archive/replace event in sqlite
// - on replacement, update graph + walks
func (e *T) updateGraph(ctx context.Context, event *nostr.Event) error {
	if event.Kind != nostr.KindFollowList {
		return errors.New("update Graph received an event that is not a kind:3 follow-list")
	}

	replaced, err := e.store.Replace(ctx, event)
	if err != nil {
		return err
	}
	if !replaced {
		return nil
	}

	delta, err := e.computeDelta(ctx, event)
	if err != nil {
		return err
	}
	updated, err := e.updateWalks(ctx, delta)
	if err != nil {
		return err
	}
	if err := e.graph.Update(ctx, delta); err != nil {
		return err
	}
	if err := e.cache.Update(ctx, delta); err != nil {
		return err
	}
	if updated > 0 {
		e.walksUpdated.Add(int64(updated))
	}
	return nil
}

// archive applies nostr kind persistence semantics:
// - regular kinds: Save
// - replaceable/addressable kinds: Replace
func (e *T) archive(ctx context.Context, event *nostr.Event) error {
	ctx, cancel := context.WithTimeout(ctx, e.config.Timeout)
	defer cancel()

	switch {
	case nostr.IsRegularKind(event.Kind):
		_, err := e.store.Save(ctx, event)
		return err

	case nostr.IsReplaceableKind(event.Kind) || nostr.IsAddressableKind(event.Kind):
		_, err := e.store.Replace(ctx, event)
		return err

	default:
		return nil
	}
}

// Compute the delta from the "p" tags in the follow list.
func (e *T) computeDelta(ctx context.Context, event *nostr.Event) (graph.Delta, error) {
	author, err := e.graph.NodeByKey(ctx, event.PubKey)
	if err != nil {
		return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
	}

	oldFollows, err := e.cache.Follows(ctx, author.ID)
	if err != nil {
		return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
	}

	pubkeys := parsePubkeys(event)
	onMissing := regraph.Ignore
	if author.Status == graph.StatusActive {
		// active nodes are the only ones that can add new pubkeys to the database
		onMissing = regraph.AddValid
	}

	newFollows, err := e.graph.Resolve(ctx, pubkeys, onMissing)
	if err != nil {
		return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
	}
	return graph.NewDelta(event.Kind, author.ID, oldFollows, newFollows), nil
}

// updateWalks updates random walks based on delta and returns number of newly written walks.
func (e *T) updateWalks(ctx context.Context, delta graph.Delta) (int, error) {
	if delta.Size() == 0 {
		return 0, nil
	}

	visiting, err := e.graph.WalksVisiting(ctx, delta.Node, -1)
	if err != nil {
		return 0, fmt.Errorf("failed to update walks: %w", err)
	}

	old, new, err := walks.ToUpdate(ctx, e.cache, delta, visiting)
	if err != nil {
		return 0, fmt.Errorf("failed to update walks: %w", err)
	}

	if err := e.graph.ReplaceWalks(ctx, old, new); err != nil {
		return 0, fmt.Errorf("failed to update walks: %w", err)
	}
	return len(new), nil
}

// Parse unique pubkeys (excluding author) from the "p" tags in the event.
func parsePubkeys(event *nostr.Event) []string {
	size := min(len(event.Tags), core.MaxTags)
	pubkeys := make([]string, 0, size)

	for _, tag := range event.Tags {
		if len(pubkeys) > core.MaxTags {
			break
		}

		if len(tag) < 2 {
			continue
		}

		prefix, pubkey := tag[0], tag[1]
		if prefix != "p" {
			continue
		}

		if pubkey == event.PubKey {
			// remove self-follows
			continue
		}
		pubkeys = append(pubkeys, pubkey)
	}
	return slicex.Unique(pubkeys)
}
