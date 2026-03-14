package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/slicex"
	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/pipe"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
	"github.com/vertex-lab/crawler_v2/pkg/relays"
	"github.com/vertex-lab/crawler_v2/pkg/walks"
	sqlite "github.com/vertex-lab/nostr-sqlite"
)

var (
	ErrNotRunning = errors.New("engine is not running")
	ErrQueueFull  = errors.New("engine queue is full")

	// IngestKinds are the event kinds the engine accepts in ingest mode.
	IngestKinds = pipe.ProfileKinds

	// SyncKinds are the event kinds the engine accepts in sync mode.
	SyncKinds = []int{nostr.KindFollowList}
)

// T is the unified engine that consumes events from an internal queue and
// dispatches logic by event kind. Engine can be configured with [AfterHooks]
// to observe and react to events after they are processed.
// Such configuration must happen before the engine is started with Ingest or Sync.
type T struct {
	store  *sqlite.Store
	graph  regraph.DB
	cache  *walks.CachedWalker
	events chan *nostr.Event

	After  AfterHooks // user configurable hooks
	config Config

	mu   sync.RWMutex
	mode engineMode
}

// either not running, ingest or sync mode.
type engineMode uint8

const (
	modeNotRunning engineMode = 0
	modeIngest     engineMode = 1
	modeSync       engineMode = 2
)

// AfterHooks groups optional callbacks invoked by the engine after specific
// processing steps complete successfully.
//
// These hooks are observational and are intended for follow-up side effects
// outside the engine's core responsibilities.
type AfterHooks struct {
	// PubkeysAdded is called after the engine has added newly discovered pubkeys
	// to the graph storage while processing an event.
	PubkeysAdded func(pubkeys ...string) error

	// RelaysDiscovered is called after the engine has extracted relay URLs from an
	// accepted relay-list event.
	RelaysDiscovered func(relays ...string) error

	// WalksUpdated is called after the engine has updated random walks in the graph.
	// old represents the old walks, and new represents the walks after the update.
	WalksUpdated func(old, new []walks.Walk) error
}

// New creates a new Engine instance.
// Config is assumed to be validated by the caller.
func New(c Config, store *sqlite.Store, graph regraph.DB) *T {
	return &T{
		config: c,
		mode:   modeNotRunning,
		store:  store,
		graph:  graph,
		events: make(chan *nostr.Event, c.Queue),
		cache: walks.NewWalker(
			walks.WithCapacity(c.CacheCapacity),
			walks.WithFallback(graph),
		),
	}
}

// AcceptedKinds returns the event kinds the engine accepts in the current mode.
func (e *T) AcceptedKinds() []int {
	e.mu.RLock()
	mode := e.mode
	e.mu.RUnlock()

	switch mode {
	case modeNotRunning:
		return nil
	case modeIngest:
		return IngestKinds
	case modeSync:
		return SyncKinds
	default:
		panic("engine in unknown mode")
	}
}

// Enqueue adds an event to the engine queue if it's accepted by the current mode.
// It is non-blocking and returns ErrQueueFull when the queue is saturated.
func (e *T) Enqueue(event *nostr.Event) error {
	if event == nil {
		return errors.New("event is nil")
	}

	e.mu.RLock()
	mode := e.mode
	e.mu.RUnlock()

	var acceptedKinds []int
	switch mode {
	case modeNotRunning:
		return ErrNotRunning
	case modeIngest:
		acceptedKinds = IngestKinds
	case modeSync:
		acceptedKinds = SyncKinds
	default:
		panic("engine in unknown mode")
	}

	if !slices.Contains(acceptedKinds, event.Kind) {
		// event kind is not accepted, just skip it
		return nil
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
	if e.mode != modeNotRunning {
		panic("engine.Ingest called while the engine is already running")
	}
	e.mode = modeIngest
	e.mu.Unlock()
	e.run(ctx, e.processIngest)
}

// Sync starts the engine in sync mode. It's a blocking operation.
// Queued events are not archived, as they are assumed to already come from the storage layer, but
// are only used to recreate the derived state (e.g. follow-list graph in redis).
func (e *T) Sync(ctx context.Context) {
	e.mu.Lock()
	if e.mode != modeNotRunning {
		panic("engine.Sync called while the engine is already running")
	}
	e.mode = modeSync
	e.mu.Unlock()
	e.run(ctx, e.processSync)
}

// run starts the engine loop, processing events with the given process function.
func (e *T) run(ctx context.Context, process func(*nostr.Event) error) {
	slog.Info("Engine: ready")
	defer slog.Info("Engine: shut down")

	processed := 0
	handle := func(event *nostr.Event) {
		if err := process(event); err != nil {
			logErrEvent(err, event)
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
			// signal shutdown and process buffered items
			e.mu.Lock()
			e.mode = modeNotRunning
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
func (e *T) processIngest(event *nostr.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.config.Timeout)
	defer cancel()

	switch event.Kind {
	case nostr.KindFollowList:
		replaced, err := e.store.Replace(ctx, event)
		if err != nil {
			return err
		}

		if replaced {
			return e.updateGraph(ctx, event)
		}
		return nil

	case nostr.KindRelayListMetadata:
		replaced, err := e.store.Replace(ctx, event)
		if err != nil {
			return err
		}

		if replaced && e.After.RelaysDiscovered != nil {
			relays := ParseRelays(event)
			if err := e.After.RelaysDiscovered(relays...); err != nil {
				logErrEvent(err, event)
			}
		}
		return nil

	default:
		// other kinds are archived normally.
		return e.archive(ctx, event)
	}
}

// processSync is the process function for the engine in the [T.Sync] method.
func (e *T) processSync(event *nostr.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.config.Timeout)
	defer cancel()

	if event.Kind == nostr.KindFollowList {
		return e.updateGraph(ctx, event)
	}
	return nil
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

	pubkeys := ParsePubkeys(event)
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

// ParsePubkeys parses unique pubkeys (excluding author) from the "p" tags in the event.
// Pubkeys are not validated.
func ParsePubkeys(e *nostr.Event) []string {
	size := min(len(e.Tags), pipe.MaxTags)
	pubkeys := make([]string, 0, size)

	for _, tag := range e.Tags {
		if len(pubkeys) > pipe.MaxTags {
			break
		}

		if len(tag) < 2 {
			continue
		}

		prefix, pubkey := tag[0], tag[1]
		if prefix != "p" {
			continue
		}

		if pubkey == e.PubKey {
			// remove self-follows
			continue
		}
		pubkeys = append(pubkeys, pubkey)
	}
	return slicex.Unique(pubkeys)
}

// ParseRelays parses unique and valid relay URLs from the "r" tags in the event.
func ParseRelays(e *nostr.Event) []string {
	size := min(len(e.Tags), pipe.MaxTags)
	urls := make([]string, 0, size)

	for _, tag := range e.Tags {
		if len(tag) < 2 || tag[0] != "r" {
			continue
		}

		url := tag[1]
		if err := relays.ValidateURL(url); err != nil {
			continue
		}
		urls = append(urls, url)
	}
	return slicex.Unique(urls)
}

// logErrEvent logs an error event if the error is not context.Canceled.
func logErrEvent(err error, e *nostr.Event) {
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("Engine: failed to process event", "error", err, "id", e.ID, "kind", e.Kind, "pubkey", e.PubKey)
	}
}
