package firehose

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
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

type Config struct {
	// Kinds is the list of event kinds to fetch from the relays.
	Kinds []int `envconfig:"FIREHOSE_KINDS"`

	// Relays is the list of relays to fetch events from.
	Relays []string `envconfig:"RELAYS"`

	// Offset is the time offset to apply when fetching events.
	// The firehose will fetch events newer than now - offset.
	Offset time.Duration `envconfig:"FIREHOSE_OFFSET"`

	// FilterCacheSize is the maximum number of pubkeys the filter cache can hold.
	FilterCacheSize int `envconfig:"FIREHOSE_FILTER_CACHE_SIZE"`
}

func NewConfig() Config {
	return Config{
		Kinds:           allKinds,
		Relays:          defaultRelays,
		Offset:          time.Minute,
		FilterCacheSize: 100_000,
	}
}

func (c Config) Validate() error {
	if len(c.Kinds) < 1 {
		return errors.New("kind list cannot be empty")
	}
	for _, relay := range c.Relays {
		if !nostr.IsValidRelayURL(relay) {
			return fmt.Errorf("\"%s\" is not a valid relay url", relay)
		}
	}
	if c.Offset < 0 {
		return errors.New("offset cannot be negative")
	}
	if c.Offset > 2*time.Minute {
		slog.Warn("firehose: offset is greater than 2 minutes, which might trigger relay rate limits")
	}
	return nil
}

func (c Config) String() string {
	return fmt.Sprintf("Firehose:\n"+
		"\tKinds: %v\n"+
		"\tRelays: %v\n"+
		"\tOffset: %v\n",
		c.Kinds, c.Relays, c.Offset)
}
