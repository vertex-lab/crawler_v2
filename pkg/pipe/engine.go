// The pipe package defines high-level pipeline functions (e.g. [Firehose], [Engine])
package pipe

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/pippellia-btc/nastro"
	"github.com/pippellia-btc/slicex"
	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/redb"
	"github.com/vertex-lab/crawler_v2/pkg/walks"

	"github.com/nbd-wtf/go-nostr"
)

type EngineConfig struct {
	Archiver        ArchiverConfig
	Builder         GraphBuilderConfig
	ChannelCapacity int `envconfig:"CHANNEL_CAPACITY"`
}

func NewEngineConfig() EngineConfig {
	return EngineConfig{
		Archiver: NewArchiverConfig(),
		Builder:  NewGraphBuilderConfig(),
	}
}

func (c EngineConfig) Validate() error {
	if err := c.Archiver.Validate(); err != nil {
		return fmt.Errorf("Archiver: %w", err)
	}

	if err := c.Builder.Validate(); err != nil {
		return fmt.Errorf("GraphBuilder: %w", err)
	}

	return nil
}

func (c EngineConfig) Print() {
	fmt.Printf("Engine:\n")
	fmt.Printf("  ChannelCapacity: %d\n", c.ChannelCapacity)
	c.Archiver.Print()
	c.Builder.Print()
}

// Engine is responsible for cohordinating the [Archiver] with the [GraphBuilder].
func Engine(
	ctx context.Context,
	config EngineConfig,
	store nastro.Store,
	db redb.RedisDB,
	events chan *nostr.Event,
) {
	graphEvents := make(chan *nostr.Event, config.ChannelCapacity)
	defer close(graphEvents)

	sendFollowList := func(e *nostr.Event) error {
		if e.Kind == nostr.KindFollowList {
			return Send(graphEvents)(e)
		}
		return nil
	}

	go GraphBuilder(ctx, config.Builder, db, graphEvents)
	Archiver(ctx, config.Archiver, store, events, sendFollowList)
}

type ArchiverConfig struct {
	Kinds      []int `envconfig:"ARCHIVER_KINDS"`
	PrintEvery int   `envconfig:"ENGINE_PRINT_EVERY"`
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

// Archiver stores events in the event store.
func Archiver(
	ctx context.Context,
	config ArchiverConfig,
	store nastro.Store,
	events chan *nostr.Event,
	onReplace Forward[*nostr.Event],
) {
	log.Println("Archiver: ready")
	defer log.Println("Archiver: shut down")

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

			err := archive(ctx, store, event, onReplace)
			if err != nil && ctx.Err() == nil {
				log.Printf("Archiver: event ID %s, kind %d by %s: %v", event.ID, event.Kind, event.PubKey, err)
			}

			processed++
			if processed%config.PrintEvery == 0 {
				log.Printf("Archiver: processed %d events", processed)
			}
		}
	}
}

// Archive an event based on its kind.
// If a replacement happened, it calls the provided onReplace
func archive(
	ctx context.Context,
	store nastro.Store,
	event *nostr.Event,
	onReplace Forward[*nostr.Event],
) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	switch {
	case nostr.IsRegularKind(event.Kind):
		return store.Save(ctx, event)

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

type GraphBuilderConfig struct {
	CacheCapacity int `envconfig:"ENGINE_CACHE_CAPACITY"`
	PrintEvery    int `envconfig:"ENGINE_PRINT_EVERY"`
}

func NewGraphBuilderConfig() GraphBuilderConfig {
	return GraphBuilderConfig{
		CacheCapacity: 100_000,
		PrintEvery:    10_000,
	}
}

func (c GraphBuilderConfig) Validate() error {
	if c.CacheCapacity < 0 {
		return errors.New("cache capacity cannot be negative")
	}

	if c.PrintEvery < 0 {
		return errors.New("print every cannot be negative")
	}
	return nil
}

func (c GraphBuilderConfig) Print() {
	fmt.Printf("GraphBuilder:\n")
	fmt.Printf("  CacheCapacity: %v\n", c.CacheCapacity)
	fmt.Printf("  PrintEvery: %d\n", c.PrintEvery)
}

// GraphBuilder consumes events to update the graph and random walks.
func GraphBuilder(
	ctx context.Context,
	config GraphBuilderConfig,
	db redb.RedisDB,
	events chan *nostr.Event,
) {
	log.Println("GraphBuilder: ready")
	defer log.Println("GraphBuilder: shut down")

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
				log.Printf("GraphBuilder: event ID %s, kind %d by %s: %v", event.ID, event.Kind, event.PubKey, "unexpected kind")
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
				log.Printf("GraphBuilder: event ID %s, kind %d by %s: %v", event.ID, event.Kind, event.PubKey, err)
			}

			processed++
			if processed%config.PrintEvery == 0 {
				log.Printf("GraphBuilder: processed %d events", processed)
			}
		}
	}
}

// Compute the delta from the "p" tags in the follow list.
func computeDelta(ctx context.Context, db redb.RedisDB, cache *walks.CachedWalker, event *nostr.Event) (graph.Delta, error) {
	author, err := db.NodeByKey(ctx, event.PubKey)
	if err != nil {
		return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
	}

	oldFollows, err := cache.Follows(ctx, author.ID)
	if err != nil {
		return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
	}

	pubkeys := ParsePubkeys(event)
	onMissing := redb.Ignore
	if author.Status == graph.StatusActive {
		// active nodes are the only ones that can add new pubkeys to the database
		onMissing = redb.AddValid
	}

	newFollows, err := db.Resolve(ctx, pubkeys, onMissing)
	if err != nil {
		return graph.Delta{}, fmt.Errorf("failed to compute delta: %w", err)
	}
	return graph.NewDelta(event.Kind, author.ID, oldFollows, newFollows), nil
}

// updateWalks uses the delta to update the random walks.
func updateWalks(ctx context.Context, db redb.RedisDB, cache *walks.CachedWalker, delta graph.Delta) error {
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
