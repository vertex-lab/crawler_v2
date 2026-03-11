package pool

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/relays"
)

var DefaultRelays = []string{
	"wss://relay.damus.io",
	"wss://relay.primal.net",
	"wss://purplepag.es",
	"wss://njump.me",
	"wss://relay.snort.social",
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

type Config struct {
	// authKey is the nostr secret key used to authenticate with all relays in the pool.
	AuthKey string `env:"POOL_AUTH_KEY"`

	// InitRelays is a list of relay URLs to be added to the pool on startup.
	InitRelays []string `env:"POOL_RELAYS"`

	// RelayRetry is the duration to wait before retrying a failed relay connection. Default is 10 minutes.
	RelayRetry time.Duration `env:"POOL_RELAY_RETRY"`

	// SubRetry is the duration to wait before retrying a closed subscription. Default is 10 seconds.
	SubRetry time.Duration `env:"POOL_SUB_RETRY"`
}

func NewConfig() Config {
	return Config{
		InitRelays: DefaultRelays,
		RelayRetry: 10 * time.Minute,
		SubRetry:   10 * time.Second,
	}
}

func (c Config) Validate() error {
	if c.AuthKey == "" {
		slog.Warn("pool authentication key is not configured, which might cause more severe rate limiting on some relays")
	} else {
		if len(c.AuthKey) != 64 {
			return fmt.Errorf("invalid auth key: invalid lenght")
		}
		if _, err := nostr.GetPublicKey(c.AuthKey); err != nil {
			return fmt.Errorf("invalid auth key: %w", err)
		}
	}

	if len(c.InitRelays) == 0 {
		return fmt.Errorf("no initial relays configured")
	} else {
		for _, url := range c.InitRelays {
			if err := relays.ValidateURL(url); err != nil {
				return fmt.Errorf("invalid relay URL %q: %w", url, err)
			}
		}
	}

	if c.RelayRetry < time.Second {
		return fmt.Errorf("relay retry duration must be at least 1 second to function reliably")
	}
	if c.SubRetry < time.Second {
		return fmt.Errorf("subscription retry duration must be at least 1 second to function reliably")
	}
	return nil
}

func (c Config) String() string {
	return fmt.Sprintf("Pool:\n"+
		"\tAuth Key: %s\n"+
		"\tInit Relays: %v\n"+
		"\tRelay Retry: %s\n"+
		"\tSub Retry: %s\n",
		c.AuthKey, c.InitRelays, c.RelayRetry, c.SubRetry)
}
