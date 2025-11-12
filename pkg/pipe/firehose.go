package pipe

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
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

	contentKinds = []int{
		nostr.KindTextNote,
		nostr.KindComment,
		nostr.KindArticle,
		20, // Picture Event
		21, // Video Event
		22, // "Tik-Tok" Video Event
	}

	engagementKinds = []int{
		nostr.KindReaction,
		nostr.KindRepost,
		nostr.KindGenericRepost,
		nostr.KindZap,
		nostr.KindNutZap,
	}

	profileKinds = []int{
		nostr.KindProfileMetadata,
		nostr.KindFollowList,
		nostr.KindMuteList,
		nostr.KindRelayListMetadata,
		nostr.KindUserServerList,
	}

	allKinds = slices.Concat(
		contentKinds,
		engagementKinds,
		profileKinds,
	)
)

type FirehoseConfig struct {
	Kinds  []int         `envconfig:"FIREHOSE_KINDS"`
	Relays []string      `envconfig:"RELAYS"`
	Offset time.Duration `envconfig:"FIREHOSE_OFFSET"`
}

func NewFirehoseConfig() FirehoseConfig {
	return FirehoseConfig{
		Kinds:  allKinds,
		Relays: defaultRelays,
		Offset: time.Minute,
	}
}

func (c FirehoseConfig) Validate() error {
	if len(c.Kinds) < 1 {
		return errors.New("kind list cannot be empty")
	}

	for _, relay := range c.Relays {
		if !nostr.IsValidRelayURL(relay) {
			return fmt.Errorf("\"%s\" is not a valid relay url", relay)
		}
	}
	return nil
}

func (c FirehoseConfig) Since() *nostr.Timestamp {
	since := nostr.Timestamp(time.Now().Add(-c.Offset).Unix())
	return &since
}

func (c FirehoseConfig) Print() {
	fmt.Printf("Firehose:\n")
	fmt.Printf("  Kinds: %v\n", c.Kinds)
	fmt.Printf("  Relays: %v\n", c.Relays)
	fmt.Printf("  Offset: %v\n", c.Offset)
}

type Forward[T any] func(T) error

// Send returns a [Forward] function that will attempt to send values into the given channel.
// It returns an error if the channel is full.
func Send[T any](ch chan T) Forward[T] {
	return func(t T) error {
		select {
		case ch <- t:
			return nil
		default:
			return fmt.Errorf("pipe.Send: channel is full, dropping %v", t)
		}
	}
}

type PubkeyGate interface {
	Allows(ctx context.Context, pubkey string) bool
}

// ExistenceGate is a [PubkeyGate] that allows pubkeys if they exists in the database.
// The assumption is that keys can't be removed from the database.
type ExistenceGate struct {
	exists   map[string]struct{}
	fallback regraph.RedisDB
}

func NewExistenceGate(fallback regraph.RedisDB) *ExistenceGate {
	return &ExistenceGate{
		exists:   make(map[string]struct{}),
		fallback: fallback,
	}
}

func (g *ExistenceGate) Allows(ctx context.Context, pubkey string) bool {
	if _, ok := g.exists[pubkey]; ok {
		return true
	}

	exists, err := g.fallback.Exists(ctx, pubkey)
	if err != nil {
		log.Printf("ExistanceGate: %v", err)
		return false
	}

	if exists {
		g.exists[pubkey] = struct{}{}
		return true
	}
	return false
}

// Firehose connects to a list of relays and pulls config.Kinds events that are newer than config.Since.
// It deduplicate events using a simple ring-buffer.
// It applies the [PubkeyGate] to remove events from undesired pubkeys.
// It forwards the rest using the provided [Forward] function.
func Firehose(
	ctx context.Context,
	config FirehoseConfig,
	gate PubkeyGate,
	forward Forward[*nostr.Event],
) {
	log.Println("Firehose: ready")
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

		if !gate.Allows(ctx, event.PubKey) {
			continue
		}

		if err := forward(event.Event); err != nil {
			log.Printf("Firehose: %v", err)
		}
	}
}

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
