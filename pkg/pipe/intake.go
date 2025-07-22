package pipe

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/nastro"
)

var (
	Kinds = []int{
		nostr.KindProfileMetadata,
		nostr.KindFollowList,
	}

	defaultRelays = []string{
		"wss://purplepag.es",
		"wss://njump.me",
		"wss://relay.snort.social",
		"wss://relay.damus.io",
		"wss://relay.primal.net",
		"wss://relay.nostr.band",
		"wss://nostr-pub.wellorder.net",
		"wss://relay.nostr.net",
		"wss://nostr.lu.ke",
		"wss://nostr.at",
		"wss://e.nos.lol",
		"wss://nostr.lopp.social",
		"wss://nostr.vulpem.com",
		"wss://relay.nostr.bg",
		"wss://wot.utxo.one",
		"wss://nostrelites.org",
		"wss://wot.nostr.party",
		"wss://wot.sovbit.host",
		"wss://wot.girino.org",
		"wss://relay.lnau.net",
		"wss://wot.siamstr.com",
		"wss://wot.sudocarlos.com",
		"wss://relay.otherstuff.fyi",
		"wss://relay.lexingtonbitcoin.org",
		"wss://wot.azzamo.net",
		"wss://wot.swarmstr.com",
		"wss://zap.watch",
		"wss://satsage.xyz",
	}
)

type FirehoseConfig struct {
	Relays []string
	Offset time.Duration
}

func NewFirehoseConfig() FirehoseConfig {
	return FirehoseConfig{
		Relays: defaultRelays,
		Offset: time.Minute,
	}
}

func (c FirehoseConfig) Since() *nostr.Timestamp {
	since := nostr.Timestamp(time.Now().Add(-c.Offset).Unix())
	return &since
}

func (c FirehoseConfig) Print() {
	fmt.Printf("Firehose\n")
	fmt.Printf("  Relays: %v\n", c.Relays)
	fmt.Printf("  Offset: %v\n", c.Offset)
}

type PubkeyChecker interface {
	Exists(ctx context.Context, pubkey string) (bool, error)
}

// buffer is a minimalistic ring buffer used to keep track of the latest event IDs
type buffer struct {
	IDs      []string
	capacity int
	write    int
}

func newBuffer(capacity int) *buffer {
	return &buffer{
		IDs:      make([]string, capacity),
		capacity: capacity,
	}
}

func (b *buffer) Add(ID string) {
	b.IDs[b.write] = ID
	b.write = (b.write + 1) % b.capacity
}

func (b *buffer) Contains(ID string) bool {
	return slices.Contains(b.IDs, ID)
}

// Firehose connects to a list of relays and pulls [Kinds] events that are newer than [FirehoseConfig.Since].
// It discards events from unknown pubkeys as an anti-spam mechanism.
func Firehose(ctx context.Context, config FirehoseConfig, check PubkeyChecker, send func(*nostr.Event) error) {
	defer log.Println("Firehose: shutting down...")

	pool := nostr.NewSimplePool(ctx)
	defer shutdown(pool)

	filter := nostr.Filter{
		Kinds: Kinds,
		Since: config.Since(),
	}

	seen := newBuffer(1024)
	for event := range pool.SubscribeMany(ctx, config.Relays, filter) {
		if seen.Contains(event.ID) {
			// event already seen, skip
			continue
		}
		seen.Add(event.ID)

		exists, err := check.Exists(ctx, event.PubKey)
		if err != nil {
			log.Printf("Firehose: %v", err)
			continue
		}

		if !exists {
			// event from unknown pubkey, skip
			continue
		}

		if err := send(event.Event); err != nil {
			log.Printf("Firehose: %v", err)
		}
	}
}

type FetcherConfig struct {
	Relays   []string
	Batch    int
	Interval time.Duration
}

func NewFetcherConfig() FetcherConfig {
	return FetcherConfig{
		Relays:   defaultRelays,
		Batch:    100,
		Interval: time.Minute,
	}
}

func (c FetcherConfig) Print() {
	fmt.Printf("Fetcher\n")
	fmt.Printf("  Relays: %v\n", c.Relays)
	fmt.Printf("  Batch: %d\n", c.Batch)
	fmt.Printf("  Interval: %v\n", c.Interval)
}

// Fetcher extracts pubkeys from the channel and queries relays for their events:
// - when the batch is bigger than config.Batch
// - after config.Interval since the last query.
func Fetcher(ctx context.Context, config FetcherConfig, pubkeys <-chan string, send func(*nostr.Event) error) {
	defer log.Println("Fetcher: shutting down...")

	batch := make([]string, 0, config.Batch)
	timer := time.After(config.Interval)

	pool := nostr.NewSimplePool(ctx)
	defer shutdown(pool)

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

			events, err := fetch(ctx, pool, config.Relays, batch)
			if err != nil && ctx.Err() == nil {
				log.Printf("Fetcher: %v", err)
				continue
			}

			for _, event := range events {
				if err := send(event); err != nil {
					log.Printf("Fetcher: %v", err)
				}
			}

			batch = make([]string, 0, config.Batch)
			timer = time.After(config.Interval)

		case <-timer:
			events, err := fetch(ctx, pool, config.Relays, batch)
			if err != nil && ctx.Err() == nil {
				log.Printf("Fetcher: %v", err)
				continue
			}

			for _, event := range events {
				if err := send(event); err != nil {
					log.Printf("Fetcher: %v", err)
				}
			}

			batch = make([]string, 0, config.Batch)
			timer = time.After(config.Interval)
		}
	}
}

// fetch queries the [Kinds] of the specified pubkeys.
func fetch(ctx context.Context, pool *nostr.SimplePool, relays, pubkeys []string) ([]*nostr.Event, error) {
	if len(pubkeys) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	filter := nostr.Filter{
		Kinds:   Kinds,
		Authors: pubkeys,
	}

	latest := make(map[string]*nostr.Event, len(pubkeys)*len(filter.Kinds))
	for event := range pool.FetchMany(ctx, relays, filter) {

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

// Fetcher extracts pubkeys from the channel and queries the store for their events:
// - when the batch is bigger than config.Batch
// - after config.Interval since the last query.
func FetcherDB(
	ctx context.Context,
	config FetcherConfig,
	store nastro.Store,
	pubkeys <-chan string,
	send func(*nostr.Event) error) {

	defer log.Println("FetcherDB: shutting down...")

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
				Kinds:   Kinds,
				Authors: batch,
				Limit:   len(Kinds) * len(batch),
			}

			events, err := store.Query(ctx, filter)
			if err != nil {
				log.Printf("FetcherDB: %v", err)
			}

			for _, event := range events {
				if err := send(&event); err != nil {
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
				Kinds:   Kinds,
				Authors: batch,
				Limit:   len(Kinds) * len(batch),
			}

			events, err := store.Query(ctx, filter)
			if err != nil {
				log.Printf("FetcherDB: %v", err)
			}

			for _, event := range events {
				if err := send(&event); err != nil {
					log.Printf("FetcherDB: %v", err)
				}
			}

			batch = make([]string, 0, config.Batch)
			timer = time.After(config.Interval)
		}
	}
}

// Shutdown iterates over the relays in the pool and closes all connections.
func shutdown(pool *nostr.SimplePool) {
	pool.Relays.Range(func(_ string, relay *nostr.Relay) bool {
		relay.Close()
		return true
	})
}

var (
	ErrEventTooBig = errors.New("event is too big")
	maxTags        = 20_000
	maxContent     = 50_000
)

// EventTooBig is a [nastro.EventPolicy] that errs if the event is too big.
func EventTooBig(e *nostr.Event) error {
	if len(e.Tags) > maxTags {
		return fmt.Errorf("%w: event with ID %s has too many tags: %d", ErrEventTooBig, e.ID, len(e.Tags))
	}
	if len(e.Content) > maxContent {
		return fmt.Errorf("%w: event with ID %s has too much content: %d", ErrEventTooBig, e.ID, len(e.Content))
	}
	return nil
}
