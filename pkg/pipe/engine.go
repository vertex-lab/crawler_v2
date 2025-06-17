// The pipe package defines high-level pipeline functions (e.g. [Firehose], [Engine])
package pipe

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/redb"
	"github.com/vertex-lab/crawler_v2/pkg/walks"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/relay/pkg/eventstore"
)

type EngineConfig struct {
	PrintEvery int

	// GraphBuilder params
	BuilderCapacity int
	CacheCapacity   int
}

func NewEngineConfig() EngineConfig {
	return EngineConfig{
		PrintEvery:      5000,
		BuilderCapacity: 1000,
		CacheCapacity:   100_000,
	}
}

func (c EngineConfig) Print() {
	fmt.Printf("Engine\n")
	fmt.Printf("  PrintEvery: %d\n", c.PrintEvery)
	fmt.Printf("  BuilderCapacity: %d\n", c.BuilderCapacity)
	fmt.Printf("  CacheCapacity: %d\n", c.CacheCapacity)
}

// Engine is responsible for cohordinating the [Archiver] with the [GraphBuilder].
func Engine(
	ctx context.Context,
	config EngineConfig,
	store *eventstore.Store,
	db redb.RedisDB,
	events chan *nostr.Event) {

	graphEvents := make(chan *nostr.Event, config.BuilderCapacity)
	defer close(graphEvents)

	log.Println("Engine: ready to process events")
	defer log.Println("Engine: shutting down...")

	go GraphBuilder(ctx, config, db, graphEvents)

	Archiver(ctx, config, store, events, func(e *nostr.Event) error {
		if e.Kind == nostr.KindFollowList {
			select {
			case graphEvents <- e:
			default:
				return errors.New("channel is full")
			}
		}
		return nil
	})
}

// Archiver stores events in the event store.
func Archiver(
	ctx context.Context,
	config EngineConfig,
	store *eventstore.Store,
	events chan *nostr.Event,
	onReplace func(*nostr.Event) error) {

	var processed int

	for {
		select {
		case <-ctx.Done():
			return

		case event, ok := <-events:
			if !ok {
				return
			}

			err := func() error {
				opctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				switch {
				case nostr.IsRegularKind(event.Kind):
					return store.Save(opctx, event)

				case nostr.IsReplaceableKind(event.Kind):
					replaced, err := store.Replace(opctx, event)
					if err != nil {
						return err
					}

					if replaced {
						return onReplace(event)
					}
					return nil

				default:
					return nil
				}
			}()

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

// GraphBuilder consumes events to update the graph and random walks.
func GraphBuilder(
	ctx context.Context,
	config EngineConfig,
	db redb.RedisDB,
	events chan *nostr.Event) {

	cache := walks.NewWalker(
		walks.WithCapacity(config.CacheCapacity),
		walks.WithFallback(db),
		walks.WithLogFile("cache.log"),
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

	pubkeys := parsePubkeys(event)
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

const (
	followPrefix = "p"
	maxFollows   = 50000
)

// parse unique pubkeys (excluding author) from the "p" tags in the event.
func parsePubkeys(event *nostr.Event) []string {
	pubkeys := make([]string, 0, min(len(event.Tags), maxFollows))
	for _, tag := range event.Tags {
		if len(pubkeys) > maxFollows {
			// stop processing, list is too big
			break
		}

		if len(tag) < 2 {
			continue
		}

		prefix, pubkey := tag[0], tag[1]
		if prefix != followPrefix {
			continue
		}

		if pubkey == event.PubKey {
			// remove self-follows
			continue
		}

		pubkeys = append(pubkeys, pubkey)
	}

	return unique(pubkeys)
}

func logEvent(prefix string, e *nostr.Event, extra any) {
	log.Printf("%s: event ID %s, kind %d by %s: %v", prefix, e.ID, e.Kind, e.PubKey, extra)
}

// Unique returns a slice of unique elements of the input slice.
func unique[E cmp.Ordered](slice []E) []E {
	if len(slice) == 0 {
		return nil
	}

	slices.Sort(slice)
	unique := make([]E, 0, len(slice))
	unique = append(unique, slice[0])

	for i := 1; i < len(slice); i++ {
		if slice[i] != slice[i-1] {
			unique = append(unique, slice[i])
		}
	}

	return unique
}
