package pipe

import (
	"context"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

var (
	relevantKinds = []int{
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
		Offset: 10 * time.Second,
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

// Firehose connects to a list of relays and pulls [relevantKinds] events that are newer than [FirehoseConfig.Since].
// It discards events from unknown pubkeys as an anti-spam mechanism.
func Firehose(ctx context.Context, config FirehoseConfig, check PubkeyChecker, send func(e *nostr.Event) error) {
	pool := nostr.NewSimplePool(ctx)
	defer close(pool)

	filter := nostr.Filter{
		Kinds: relevantKinds,
		Since: config.Since(),
	}

	seen := newBuffer(2048)
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

// Close iterates over the relays in the pool and closes all connections.
func close(pool *nostr.SimplePool) {
	pool.Relays.Range(func(_ string, relay *nostr.Relay) bool {
		relay.Close()
		return true
	})
}
