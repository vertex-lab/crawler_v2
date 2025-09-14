package pipe

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/nastro"
)

type FetcherConfig struct {
	Kinds    []int
	Relays   []string
	Batch    int
	Interval time.Duration
}

func NewFetcherConfig() FetcherConfig {
	return FetcherConfig{
		Kinds: []int{
			nostr.KindProfileMetadata,
			nostr.KindFollowList,
		},
		Relays:   defaultRelays,
		Batch:    100,
		Interval: time.Minute,
	}
}

func (c FetcherConfig) Print() {
	fmt.Printf("Fetcher\n")
	fmt.Printf("  Kinds: %v\n", c.Kinds)
	fmt.Printf("  Relays: %v\n", c.Relays)
	fmt.Printf("  Batch: %d\n", c.Batch)
	fmt.Printf("  Interval: %v\n", c.Interval)
}

// Fetcher extracts pubkeys from the channel and queries relays for their events when:
// - the batch is bigger than config.Batch
// - more than config.Interval has passed since the last query
func Fetcher(
	ctx context.Context,
	config FetcherConfig,
	pubkeys <-chan string,
	forward Forward[*nostr.Event],
) {
	log.Println("Fetcher: ready")
	defer log.Println("Fetcher: shut down")

	batch := make([]string, 0, config.Batch)
	timer := time.After(config.Interval)

	for {
		select {
		case <-ctx.Done():
			return

		case pubkey, ok := <-pubkeys:
			if !ok {
				return
			}

			batch = append(batch, pubkey)
			if len(batch) < config.Batch {
				continue
			}

			events, err := fetch(ctx, config, batch)
			if err != nil && ctx.Err() == nil {
				log.Printf("Fetcher: %v", err)
				continue
			}

			for _, event := range events {
				if err := forward(event); err != nil {
					log.Printf("Fetcher: %v", err)
				}
			}

			batch = make([]string, 0, config.Batch)
			timer = time.After(config.Interval)

		case <-timer:
			events, err := fetch(ctx, config, batch)
			if err != nil && ctx.Err() == nil {
				log.Printf("Fetcher: %v", err)
				continue
			}

			for _, event := range events {
				if err := forward(event); err != nil {
					log.Printf("Fetcher: %v", err)
				}
			}

			batch = make([]string, 0, config.Batch)
			timer = time.After(config.Interval)
		}
	}
}

// fetch queries the config.Relays for the config.Kinds of the specified pubkeys.
func fetch(ctx context.Context, config FetcherConfig, pubkeys []string) ([]*nostr.Event, error) {
	if len(pubkeys) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	pool := nostr.NewSimplePool(ctx)
	defer shutdown(pool)

	filter := nostr.Filter{
		Kinds:   config.Kinds,
		Authors: pubkeys,
		Limit:   len(config.Kinds) * len(pubkeys),
	}

	latest := make(map[string]*nostr.Event, len(pubkeys)*len(config.Kinds))
	for event := range pool.FetchMany(ctx, config.Relays, filter) {

		key := fmt.Sprintf("%s:%d", event.PubKey, event.Kind)
		e, exists := latest[key]
		if !exists || event.CreatedAt > e.CreatedAt {
			latest[key] = event.Event
		}
	}

	events := make([]*nostr.Event, 0, len(latest))
	for _, event := range latest {
		events = append(events, event)
	}

	return events, nil
}

// FetcherDB extracts pubkeys from the channel and queries the store for their events:
// - when the batch is bigger than config.Batch
// - after config.Interval since the last query.
func FetcherDB(
	ctx context.Context,
	config FetcherConfig,
	store nastro.Store,
	pubkeys <-chan string,
	forward Forward[*nostr.Event],
) {
	log.Println("FetcherDB: ready")
	defer log.Println("FetcherDB: shut down")

	batch := make([]string, 0, config.Batch)
	timer := time.After(config.Interval)

	for {
		select {
		case <-ctx.Done():
			return

		case pubkey, ok := <-pubkeys:
			if !ok {
				return
			}

			batch = append(batch, pubkey)
			if len(batch) < config.Batch {
				continue
			}

			filter := nostr.Filter{
				Kinds:   config.Kinds,
				Authors: batch,
				Limit:   len(config.Kinds) * len(batch),
			}

			events, err := store.Query(ctx, filter)
			if err != nil {
				log.Printf("FetcherDB: %v", err)
			}

			for _, event := range events {
				if err := forward(&event); err != nil {
					log.Printf("FetcherDB: %v", err)
				}
			}

			batch = make([]string, 0, config.Batch)
			timer = time.After(config.Interval)

		case <-timer:
			if len(batch) == 0 {
				continue
			}

			filter := nostr.Filter{
				Kinds:   config.Kinds,
				Authors: batch,
				Limit:   len(config.Kinds) * len(batch),
			}

			events, err := store.Query(ctx, filter)
			if err != nil {
				log.Printf("FetcherDB: %v", err)
			}

			for _, event := range events {
				if err := forward(&event); err != nil {
					log.Printf("FetcherDB: %v", err)
				}
			}

			batch = make([]string, 0, config.Batch)
			timer = time.After(config.Interval)
		}
	}
}
