// Package engine provides a unified event processing engine that consumes events
// from an internal queue and dispatches logic by event kind. Engine can be
// configured with [AfterHooks] to observe and react to events after they are
// processed.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/slicex"
	"github.com/vertex-lab/crawler_v2/pkg/events"
	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/leaks"
	"github.com/vertex-lab/crawler_v2/pkg/pipe"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
	"github.com/vertex-lab/crawler_v2/pkg/walks"
	sqlite "github.com/vertex-lab/nostr-sqlite"
)

var (
	ErrNotRunning = errors.New("engine is not running")
	ErrQueueFull  = errors.New("engine queue is full")
)

// T is the unified engine that consumes events from an internal queue and
// dispatches logic by event kind. Engine can be configured with [AfterHooks]
// to observe and react to events after they are processed.
// Such configuration must happen before the engine is started with Ingest or Sync.
type T struct {
	graph regraph.DB
	cache *walks.CachedWalker

	store *sqlite.Store
	leaks *leaks.DB

	events chan *nostr.Event

	After  AfterHooks // user configurable hooks
	config Config

	mu   sync.RWMutex
	mode engineMode
}

// AfterHooks groups optional callbacks invoked by the engine after specific
// processing steps complete successfully.
//
// These hooks are observational and are intended for follow-up side effects
// outside the engine's core responsibilities.
type AfterHooks struct {
	// PubkeysAdded is called after the engine has added newly discovered pubkeys
	// to the graph storage while processing an event.
	PubkeysAdded func(pubkeys ...string) error

	// KeysLeaked is called after the engine has discovered leaked keys while processing an event.
	KeysLeaked func(pubkeys ...string) error

	// RelaysDiscovered is called after the engine has extracted relay URLs from an
	// accepted relay-list event.
	RelaysDiscovered func(relays ...string) error

	// WalksUpdated is called after the engine has updated random walks in the graph.
	// old represents the old walks, and new represents the walks after the update.
	WalksUpdated func(old, new []walks.Walk) error
}

type engineMode struct {
	name    string
	process func(*nostr.Event)
}

func (m engineMode) IsSet() bool {
	return m.name != ""
}

// New creates a new Engine instance.
// Config is assumed to be validated by the caller.
func New(c Config, store *sqlite.Store, graph regraph.DB) *T {
	return &T{
		config: c,
		store:  store,
		graph:  graph,
		leaks:  leaks.NewDB(graph.Client),
		events: make(chan *nostr.Event, c.Queue),
		cache: walks.NewWalker(
			walks.WithCapacity(c.CacheCapacity),
			walks.WithFallback(graph),
		),
	}
}

// Enqueue adds an event to the engine queue if it's running.
// It is non-blocking and returns ErrQueueFull when the queue is saturated.
func (e *T) Enqueue(event *nostr.Event) error {
	if event == nil {
		return errors.New("event is nil")
	}

	e.mu.RLock()
	mode := e.mode
	e.mu.RUnlock()

	if !mode.IsSet() {
		return ErrNotRunning
	}

	select {
	case e.events <- event:
		return nil
	default:
		return ErrQueueFull
	}
}

// Ingest starts the engine in live ingestion mode. It's a blocking operation.
// Queued events are first archived according to their storage semantics, and are
// subsequently used to update the derived state (e.g. follow-list graph in redis).
func (e *T) Ingest(ctx context.Context) {
	e.mu.Lock()
	if e.mode.IsSet() {
		panic("engine.Ingest called while the engine is already running")
	}

	e.mode = engineMode{
		name:    "ingest",
		process: e.processIngest,
	}

	e.mu.Unlock()
	e.run(ctx)
}

// Sync starts the engine in sync mode. It's a blocking operation.
// Queued events are not archived, as they are assumed to already come from the storage layer, but
// are only used to recreate the derived state (e.g. follow-list graph in redis).
func (e *T) Sync(ctx context.Context) {
	e.mu.Lock()
	if e.mode.IsSet() {
		panic("engine.Sync called while the engine is already running")
	}

	e.mode = engineMode{
		name:    "sync",
		process: e.processSync,
	}

	e.mu.Unlock()
	e.run(ctx)
}

// run starts the engine loop, processing events with the given process function.
func (e *T) run(ctx context.Context) {
	slog.Info("Engine: ready")
	defer slog.Info("Engine: shut down")

	process := e.mode.process
	processed := 0

	handle := func(event *nostr.Event) {
		process(event)
		processed++
		if e.config.PrintEvery > 0 && processed%e.config.PrintEvery == 0 {
			slog.Info("Engine", "processed", processed)
		}
	}

	for {
		select {
		case <-ctx.Done():
			// signal shutdown and process buffered items
			e.mu.Lock()
			e.mode = engineMode{}
			e.mu.Unlock()

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

// processIngest is the process function for the engine in the [T.Ingest] method.
func (e *T) processIngest(event *nostr.Event) {
	ctx, cancel := context.WithTimeout(context.Background(), e.config.Timeout)
	defer cancel()

	if events.ContainsLeak(event) {
		// found a candidate leak
		if err := e.updateLeaks(ctx, event); err != nil {
			logErrEvent(err, event)
		}
	}

	switch event.Kind {
	case nostr.KindFollowList:
		replaced, err := e.store.Replace(ctx, event)
		if err != nil {
			logErrEvent(err, event)
			return
		}

		if replaced {
			if err := e.updateGraph(ctx, event); err != nil {
				logErrEvent(err, event)
				return
			}
		}

	case nostr.KindRelayListMetadata:
		replaced, err := e.store.Replace(ctx, event)
		if err != nil {
			logErrEvent(err, event)
			return
		}

		if replaced && e.After.RelaysDiscovered != nil {
			relays := events.ParseRelays(event.Tags)
			if err := e.After.RelaysDiscovered(relays...); err != nil {
				logErrEvent(err, event)
			}
		}

	default:
		if slices.Contains(pipe.ProfileKinds, event.Kind) {
			if err := e.archive(ctx, event); err != nil {
				logErrEvent(err, event)
			}
		}
	}
}

// processSync is the process function for the engine in the [T.Sync] method.
func (e *T) processSync(event *nostr.Event) {
	ctx, cancel := context.WithTimeout(context.Background(), e.config.Timeout)
	defer cancel()

	if events.ContainsLeak(event) {
		// found a candidate leak
		if err := e.updateLeaks(ctx, event); err != nil {
			logErrEvent(err, event)
		}
	}

	if event.Kind == nostr.KindFollowList {
		if err := e.updateGraph(ctx, event); err != nil {
			logErrEvent(err, event)
		}
	}
}

// archive applies nostr kind persistence semantics:
// - regular kinds: Save
// - replaceable/addressable kinds: Replace
func (e *T) archive(ctx context.Context, event *nostr.Event) error {
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

// updateLeaks updates the leaks database with any leaked keys found in the event.
func (e *T) updateLeaks(ctx context.Context, event *nostr.Event) error {
	seckeys := events.ParseLeaks(event)
	if len(seckeys) == 0 {
		return nil
	}

	filtered, err := e.filterSeckeys(ctx, seckeys)
	if err != nil {
		return fmt.Errorf("failed to update leaks: %w", err)
	}
	slog.Info("found leaked keys", "event", event.ID, "total", len(seckeys), "filtered", len(filtered))
	if len(filtered) == 0 {
		return nil
	}

	added, err := e.leaks.Store(ctx, filtered, time.Now())
	if err != nil {
		return err
	}

	if e.After.KeysLeaked != nil {
		return e.After.KeysLeaked(added...)
	}
	return nil
}

// filterSeckeys returns the subset of seckeys whose associated public keys are in the graph.
// If any of the seckeys are invalid, an error is returned.
func (e *T) filterSeckeys(ctx context.Context, seckeys []string) ([]string, error) {
	if len(seckeys) == 0 {
		return nil, nil
	}

	pubkeys := make([]string, 0, len(seckeys))
	for _, sk := range seckeys {
		pk, err := nostr.GetPublicKey(sk)
		if err != nil {
			return nil, fmt.Errorf("failed to filter seckeys: %w", err)
		}
		if !nostr.IsValidPublicKey(pk) {
			return nil, fmt.Errorf("failed to filter seckeys: invalid secret key: %s", sk)
		}
		pubkeys = append(pubkeys, pk)
	}

	IDs, err := e.graph.NodeIDs(ctx, pubkeys...)
	if err != nil {
		return nil, fmt.Errorf("failed to filter seckeys: %w", err)
	}

	known := make([]string, 0, len(seckeys))
	for i, sk := range seckeys {
		if IDs[i] != "" {
			known = append(known, sk)
		}
	}
	return known, nil
}

// updateGraph updates the redis graph and walks using the given follow-list event.
func (e *T) updateGraph(ctx context.Context, event *nostr.Event) error {
	if event.Kind != nostr.KindFollowList {
		return errors.New("updateGraph received an event that is not a kind:3 follow-list")
	}

	delta, err := e.computeDelta(ctx, event)
	if err != nil {
		return err
	}
	if delta.Size() == 0 {
		return nil
	}

	if err := e.graph.Update(ctx, delta); err != nil {
		return err
	}
	if err := e.cache.Update(ctx, delta); err != nil {
		return err
	}
	if err := e.updateWalks(ctx, delta); err != nil {
		return err
	}
	return nil
}

// computeDelta computes the delta from the "p" tags in the follow list.
func (e *T) computeDelta(ctx context.Context, event *nostr.Event) (graph.Delta, error) {
	author, err := e.graph.NodeByKey(ctx, event.PubKey)
	if err != nil {
		return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
	}

	oldFollows, err := e.cache.Follows(ctx, author.ID)
	if err != nil {
		return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
	}

	pubkeys := events.ParseTags("p", event.Tags)
	pubkeys = slicex.Exclude(pubkeys, event.PubKey)

	nodes, err := e.graph.NodeIDs(ctx, pubkeys...)
	if err != nil {
		return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
	}

	newFollows := make([]graph.ID, 0, len(nodes))
	var addedPks []string

	for i, ID := range nodes {
		if ID != "" {
			newFollows = append(newFollows, ID)
		}

		if ID == "" && author.Status == graph.StatusActive {
			// add unknown pubkeys to the graph only if the author
			// of the follow-list is active / trustworthy
			pk := pubkeys[i]
			if len(pk) != 64 || !nostr.IsValidPublicKey(pk) {
				continue
			}

			newID, err := e.graph.AddNode(ctx, pk)
			if err != nil {
				return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
			}

			newFollows = append(newFollows, newID)
			addedPks = append(addedPks, pk)
		}
	}

	if e.After.PubkeysAdded != nil {
		if err := e.After.PubkeysAdded(addedPks...); err != nil {
			logErrEvent(err, event)
		}
	}

	delta := graph.NewDelta(event.Kind, author.ID, oldFollows, newFollows)
	return delta, nil
}

// updateWalks updates random walks based on delta.
func (e *T) updateWalks(ctx context.Context, delta graph.Delta) error {
	if delta.Size() == 0 {
		return nil
	}

	visiting, err := e.graph.WalksVisiting(ctx, delta.Node, -1)
	if err != nil {
		return fmt.Errorf("failed to update walks: %w", err)
	}

	old, new, err := walks.ToUpdate(ctx, e.cache, delta, visiting)
	if err != nil {
		return fmt.Errorf("failed to update walks: %w", err)
	}

	if err := e.graph.ReplaceWalks(ctx, old, new); err != nil {
		return fmt.Errorf("failed to update walks: %w", err)
	}

	if e.After.WalksUpdated != nil {
		if err := e.After.WalksUpdated(old, new); err != nil {
			slog.Error("Engine: call to After.WalksUpdated failed", "error", err)
		}
	}
	return nil
}

// logErrEvent logs an error event if the error is not context.Canceled.
func logErrEvent(err error, e *nostr.Event) {
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("Engine: failed to process event", "error", err, "id", e.ID, "kind", e.Kind, "pubkey", e.PubKey)
	}
}
