// The pipe package defines high-level pipeline functions (e.g. [Firehose], [Engine])
package pipe

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/pippellia-btc/slicex"
	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
	"github.com/vertex-lab/crawler_v2/pkg/walks"
	sqlite "github.com/vertex-lab/nostr-sqlite"

	"github.com/nbd-wtf/go-nostr"
)

type EngineConfig struct {
	ChannelCapacity int `envconfig:"CHANNEL_CAPACITY"`
	Archiver        ArchiverConfig
	Grapher         GrapherConfig
}

func NewEngineConfig() EngineConfig {
	return EngineConfig{
		Archiver: NewArchiverConfig(),
		Grapher:  NewGrapherConfig(),
	}
}

func (c EngineConfig) Validate() error {
	if c.ChannelCapacity < 0 {
		return errors.New("channel capacity cannot be negative")
	}

	if err := c.Archiver.Validate(); err != nil {
		return fmt.Errorf("Archiver: %w", err)
	}

	if err := c.Grapher.Validate(); err != nil {
		return fmt.Errorf("Grapher: %w", err)
	}

	return nil
}

func (c EngineConfig) Print() {
	fmt.Printf("Engine:\n")
	fmt.Printf("  ChannelCapacity: %d\n", c.ChannelCapacity)
	c.Archiver.Print()
	c.Grapher.Print()
}

// Engine is responsible for cohordinating the [Archiver] with the [Grapher].
func Engine(
	ctx context.Context,
	config EngineConfig,
	events chan *nostr.Event,
	store *sqlite.Store,
	db regraph.DB,
) {
	graphEvents := make(chan *nostr.Event, config.ChannelCapacity)
	defer close(graphEvents)

	sendGraphEvents := func(e *nostr.Event) error {
		if e.Kind == nostr.KindFollowList {
			return Send(graphEvents)(e)
		}
		return nil
	}

	go Grapher(ctx, config.Grapher, graphEvents, db)
	Archiver(ctx, config.Archiver, events, store, sendGraphEvents)
}

type ArchiverConfig struct {
	Kinds      []int `envconfig:"ARCHIVER_KINDS"`
	PrintEvery int   `envconfig:"ARCHIVER_PRINT_EVERY"`
}

func NewArchiverConfig() ArchiverConfig {
	return ArchiverConfig{
		Kinds:      profileKinds,
		PrintEvery: 10_000,
	}
}

func (c ArchiverConfig) Validate() error {
	if len(c.Kinds) < 1 {
		return errors.New("kind list cannot be empty")
	}

	if c.PrintEvery < 0 {
		return errors.New("print every cannot be negative")
	}
	return nil
}

func (c ArchiverConfig) Print() {
	fmt.Printf("Archiver:\n")
	fmt.Printf("  Kinds: %v\n", c.Kinds)
	fmt.Printf("  PrintEvery: %d\n", c.PrintEvery)
}

// Archiver stores events in the event store according to their kind.
// If a replacement happened, it calls the provided onReplace function.
func Archiver(
	ctx context.Context,
	config ArchiverConfig,
	events chan *nostr.Event,
	store *sqlite.Store,
	onReplace Forward[*nostr.Event],
) {
	slog.Info("Archiver: ready")
	defer slog.Info("Archiver: shut down")

	var processed int

	for {
		select {
		case <-ctx.Done():
			return

		case event, ok := <-events:
			if !ok {
				return
			}

			if !slices.Contains(config.Kinds, event.Kind) {
				continue
			}

			err := archive(ctx, event, store, onReplace)
			if err != nil && ctx.Err() == nil {
				slog.Error("Archiver: failed to archive event", "error", err, "id", event.ID, "kind", event.Kind, "pubkey", event.PubKey)
			}

			processed++
			if processed%config.PrintEvery == 0 {
				slog.Info("Archiver", "processed", processed)
			}
		}
	}
}

// Archive an event based on its kind.
// If a replacement happened, it calls the provided onReplace
func archive(
	ctx context.Context,
	event *nostr.Event,
	store *sqlite.Store,
	onReplace Forward[*nostr.Event],
) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	switch {
	case nostr.IsRegularKind(event.Kind):
		_, err := store.Save(ctx, event)
		return err

	case nostr.IsReplaceableKind(event.Kind) || nostr.IsAddressableKind(event.Kind):
		replaced, err := store.Replace(ctx, event)
		if err != nil {
			return err
		}

		if replaced && onReplace != nil {
			return onReplace(event)
		}
		return nil

	default:
		return nil
	}
}

type GrapherConfig struct {
	CacheCapacity int `envconfig:"GRAPHER_CACHE_CAPACITY"`
	PrintEvery    int `envconfig:"GRAPHER_PRINT_EVERY"`
}

func NewGrapherConfig() GrapherConfig {
	return GrapherConfig{
		CacheCapacity: 100_000,
		PrintEvery:    10_000,
	}
}

func (c GrapherConfig) Validate() error {
	if c.CacheCapacity < 0 {
		return errors.New("cache capacity cannot be negative")
	}

	if c.PrintEvery < 0 {
		return errors.New("print every cannot be negative")
	}
	return nil
}

func (c GrapherConfig) Print() {
	fmt.Printf("Grapher:\n")
	fmt.Printf("  CacheCapacity: %v\n", c.CacheCapacity)
	fmt.Printf("  PrintEvery: %d\n", c.PrintEvery)
}

// Grapher consumes events to update the graph and random walks.
func Grapher(
	ctx context.Context,
	config GrapherConfig,
	events chan *nostr.Event,
	db regraph.DB,
) {
	slog.Info("Grapher: ready")
	defer slog.Info("Grapher: shut down")

	cache := walks.NewWalker(
		walks.WithCapacity(config.CacheCapacity),
		walks.WithFallback(db),
	)

	var processed int

	for {
		select {
		case <-ctx.Done():
			return

		case event, ok := <-events:
			if !ok {
				return
			}

			if event.Kind != nostr.KindFollowList {
				slog.Error("Grapher: received unexpected event kind", "id", event.ID, "kind", event.Kind, "pubkey", event.PubKey)
				continue
			}

			err := func() error {
				opctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				delta, err := computeDelta(opctx, db, cache, event)
				if err != nil {
					return err
				}

				if err := updateWalks(opctx, db, cache, delta); err != nil {
					return err
				}

				if err := db.Update(opctx, delta); err != nil {
					return err
				}

				return cache.Update(opctx, delta)
			}()

			if err != nil && ctx.Err() == nil {
				slog.Error("Grapher: failed to process event", "error", err, "id", event.ID, "kind", event.Kind, "pubkey", event.PubKey)
			}

			processed++
			if processed%config.PrintEvery == 0 {
				slog.Info("Grapher", "processed", processed)
			}
		}
	}
}

// Compute the delta from the "p" tags in the follow list.
func computeDelta(ctx context.Context, db regraph.DB, cache *walks.CachedWalker, event *nostr.Event) (graph.Delta, error) {
	author, err := db.NodeByKey(ctx, event.PubKey)
	if err != nil {
		return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
	}

	oldFollows, err := cache.Follows(ctx, author.ID)
	if err != nil {
		return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
	}

	pubkeys := ParsePubkeys(event)
	onMissing := regraph.Ignore
	if author.Status == graph.StatusActive {
		// active nodes are the only ones that can add new pubkeys to the database
		onMissing = regraph.AddValid
	}

	newFollows, err := db.Resolve(ctx, pubkeys, onMissing)
	if err != nil {
		return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
	}
	return graph.NewDelta(event.Kind, author.ID, oldFollows, newFollows), nil
}

// updateWalks uses the delta to update the random walks.
func updateWalks(ctx context.Context, db regraph.DB, cache *walks.CachedWalker, delta graph.Delta) error {
	if delta.Size() == 0 {
		// nothing to change, stop
		return nil
	}

	visiting, err := db.WalksVisiting(ctx, delta.Node, -1)
	if err != nil {
		return fmt.Errorf("failed to update walks: %w", err)
	}

	old, new, err := walks.ToUpdate(ctx, cache, delta, visiting)
	if err != nil {
		return fmt.Errorf("failed to update walks: %w", err)
	}

	if err := db.ReplaceWalks(ctx, old, new); err != nil {
		return fmt.Errorf("failed to update walks: %w", err)
	}

	WalksTracker.Add(int32(len(new)))
	return nil
}

// Parse unique pubkeys (excluding author) from the "p" tags in the event.
func ParsePubkeys(event *nostr.Event) []string {
	pubkeys := make([]string, 0, min(len(event.Tags), maxTags))
	for _, tag := range event.Tags {
		if len(pubkeys) > maxTags {
			// stop processing, list is too big
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
