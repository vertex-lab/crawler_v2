package pipe

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

var (
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
	Kinds  []int
	Relays []string
	Offset time.Duration
}

func NewFirehoseConfig() FirehoseConfig {
	return FirehoseConfig{
		Kinds: []int{
			nostr.KindProfileMetadata,
			nostr.KindFollowList,
		},
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
	fmt.Printf("  Kinds: %v\n", c.Kinds)
	fmt.Printf("  Relays: %v\n", c.Relays)
	fmt.Printf("  Offset: %v\n", c.Offset)
}

type PubkeyChecker interface {
	Exists(ctx context.Context, pubkey string) (bool, error)
}

type Forward[T any] func(T) error

// Firehose connects to a list of relays and pulls config.Kinds events that are newer than config.Since.
// It deduplicate events using a simple ring-buffer.
// It discards events from unknown pubkeys as an anti-spam mechanism.
// It forwards the rest using the provided [Forward] function.
func Firehose(
	ctx context.Context,
	config FirehoseConfig,
	check PubkeyChecker,
	forward Forward[*nostr.Event],
) {
	defer log.Println("Firehose: shut down")

	pool := nostr.NewSimplePool(ctx)
	defer shutdown(pool)

	filter := nostr.Filter{
		Kinds: config.Kinds,
		Since: config.Since(),
	}

	seen := newBuffer(1024)
	for event := range pool.SubscribeMany(ctx, config.Relays, filter) {
		if seen.Contains(event.ID) {
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

		if err := forward(event.Event); err != nil {
			log.Printf("Firehose: %v", err)
		}
	}
}
