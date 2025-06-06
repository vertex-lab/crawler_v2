package pipe

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/redb"
	"github/pippellia-btc/crawler/pkg/walks"
	"log"
	"slices"
	"sync/atomic"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/relay/pkg/eventstore"
)

// EventTracker tracks the number of events processed
var EventTracker atomic.Int32

var ErrUnsupportedKind = errors.New("unsupported event kind")

type EngineConfig struct {
	PrintEvery int

	// for the GraphUpdater
	UpdaterCapacity int
	CacheCapacity   int

	// for the archiveEngine
	ArchiverCapacity int
}

func NewEngineConfig() EngineConfig {
	return EngineConfig{
		PrintEvery:       5000,
		UpdaterCapacity:  1000,
		CacheCapacity:    100_000,
		ArchiverCapacity: 1000,
	}
}

func (c EngineConfig) Print() {
	fmt.Printf("Engine\n")
	fmt.Printf("  PrintEvery: %d\n", c.PrintEvery)
	fmt.Printf("  UpdaterCapacity: %d\n", c.UpdaterCapacity)
	fmt.Printf("  CacheCapacity: %d\n", c.CacheCapacity)
	fmt.Printf("  ArchiveCapacity: %d\n", c.ArchiverCapacity)
}

// Engine is responsible for dispacting the correct events to the [Archiver] or [GraphUpdater].
func Engine(
	ctx context.Context,
	config EngineConfig,
	store *eventstore.Store,
	db redb.RedisDB,
	events chan *nostr.Event) {

	defer log.Println("Engine: shutting down...")

	graphEvents := make(chan *nostr.Event, config.UpdaterCapacity)
	archiveEvents := make(chan *nostr.Event, config.ArchiverCapacity)
	defer close(graphEvents)
	defer close(archiveEvents)

	go GraphUpdater(ctx, config, store, db, graphEvents)
	go Archiver(ctx, config, store, archiveEvents)

	log.Println("Engine: ready to process events")

	for {
		select {
		case <-ctx.Done():
			return

		case event, ok := <-events:
			if !ok {
				return
			}

			switch event.Kind {
			case nostr.KindFollowList:
				graphEvents <- event

			case nostr.KindProfileMetadata:
				archiveEvents <- event

			default:
				logEvent(event, ErrUnsupportedKind)
			}
		}
	}
}

// Archiver consumes events that are not graph-related and stores them.
func Archiver(
	ctx context.Context,
	config EngineConfig,
	store *eventstore.Store,
	events chan *nostr.Event) {

	for {
		select {
		case <-ctx.Done():
			return

		case event, ok := <-events:
			if !ok {
				return
			}

			err := func() error {
				opctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				switch {
				case nostr.IsRegularKind(event.Kind):
					return store.Save(opctx, event)

				case nostr.IsReplaceableKind(event.Kind):
					_, err := store.Replace(opctx, event)
					return err

				default:
					return nil
				}
			}()

			if err != nil {
				logEvent(event, err)
			}

			EventTracker.Add(1)
		}
	}
}

// GraphUpdater consumes events to update the graph and random walks.
func GraphUpdater(
	ctx context.Context,
	config EngineConfig,
	store *eventstore.Store,
	db redb.RedisDB,
	events chan *nostr.Event) {

	cache := walks.NewWalker(
		walks.WithCapacity(config.CacheCapacity),
		walks.WithFallback(db),
		walks.WithLogFile("cache.log"),
	)

	for {
		select {
		case <-ctx.Done():
			return

		case event, ok := <-events:
			if !ok {
				return
			}

			err := func() error {
				opctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()

				replaced, err := store.Replace(opctx, event)
				if err != nil {
					return err
				}

				if replaced {
					return processFollowList(opctx, db, cache, event)
				}
				return nil
			}()

			if err != nil {
				logEvent(event, err)
			}

			EventTracker.Add(1)
		}
	}
}

// processFollowList parses the pubkeys listed in the event, and uses them to:
// - update the follows of the author (db and cache)
// - update the author's random walks and signal the number to the [WalksTracker]
func processFollowList(ctx context.Context, db redb.RedisDB, cache *walks.CachedWalker, event *nostr.Event) error {
	author, err := db.NodeByKey(ctx, event.PubKey)
	if err != nil {
		return err
	}

	oldFollows, err := cache.Follows(ctx, author.ID)
	if err != nil {
		return err
	}

	pubkeys := parsePubkeys(event)
	onMissing := redb.Ignore
	if author.Status == graph.StatusActive {
		// active nodes are the only ones that can add new pubkeys to the database
		onMissing = redb.AddValid
	}

	newFollows, err := db.Resolve(ctx, pubkeys, onMissing)
	if err != nil {
		return err
	}

	delta := graph.NewDelta(event.Kind, author.ID, oldFollows, newFollows)
	if delta.Size() == 0 {
		// old and new follows are the same, stop
		return nil
	}

	visiting, err := db.WalksVisiting(ctx, author.ID, -1)
	if err != nil {
		return err
	}

	old, new, err := walks.ToUpdate(ctx, cache, delta, visiting)
	if err != nil {
		return err
	}

	if err := db.ReplaceWalks(ctx, old, new); err != nil {
		return err
	}

	if err := db.Update(ctx, delta); err != nil {
		return err
	}

	WalksTracker.Add(int32(len(new)))
	return cache.Update(ctx, delta)
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

func logEvent(e *nostr.Event, extra any) {
	msg := fmt.Sprintf("Engine: event ID %s, kind %d by %s: ", e.ID, e.Kind, e.PubKey)
	log.Printf(msg+"%v", extra)
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
