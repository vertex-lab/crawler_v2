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
	"time"

	"github.com/nbd-wtf/go-nostr"
)

var ErrUnsupportedKind = errors.New("unsupported event kind")

type ProcessorConfig struct {
	PrintEvery int
}

func NewProcessorConfig() ProcessorConfig {
	return ProcessorConfig{PrintEvery: 5000}
}

func (c ProcessorConfig) Print() {
	fmt.Printf("Processor\n")
	fmt.Printf("  PrintEvery: %d\n", c.PrintEvery)
}

// Processor() process one event at the time from the eventChannel, based on their kind.
func Processor(
	ctx context.Context,
	config ProcessorConfig,
	db redb.RedisDB,
	//store *eventstore.Store,
	events chan *nostr.Event) {

	var err error
	var processed int

	cache := walks.NewWalker(
		walks.WithCapacity(10000),
		walks.WithFallback(db),
	)

	for {
		select {
		case <-ctx.Done():
			log.Println("Processor: shutting down...")
			return

		case event := <-events:
			switch event.Kind {
			case nostr.KindFollowList:
				err = processFollowList(cache, db, event)

			case nostr.KindProfileMetadata:
				err = nil //HandleProfileMetadata(eventStore, event)

			default:
				err = ErrUnsupportedKind
			}

			if err != nil {
				log.Printf("Processor: event ID %s, kind %d by %s: %v", event.ID, event.Kind, event.PubKey, err)
			}

			processed++
			if processed%config.PrintEvery == 0 {
				log.Printf("Processor: processed %d events", processed)
			}
		}
	}
}

func processFollowList(cache *walks.CachedWalker, db redb.RedisDB, event *nostr.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

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

	walksTracker.Add(int32(len(new)))
	return cache.Update(ctx, delta)
}

const (
	followPrefix = "p"
	maxFollows   = 50000
)

// ParsePubkeys returns the slice of pubkeys that are correctly listed in the nostr.Tags.
// - Badly formatted tags are ignored.
// - Pubkeys will be uniquely added (no repetitions).
// - The author of the event will be removed from the followed pubkeys if present.
// - NO CHECKING the validity of the pubkeys
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
