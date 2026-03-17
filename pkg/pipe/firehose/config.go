package firehose

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/pipe"
)

type Config struct {
	// Kinds is the list of event kinds to fetch from the relays.
	Kinds []int `env:"FIREHOSE_KINDS"`

	// Offset is the time offset to apply when fetching events.
	// The firehose will fetch events newer than (now - offset).
	Offset time.Duration `env:"FIREHOSE_OFFSET"`

	// FilterCache is the maximum number of pubkeys the filter cache can hold.
	FilterCache int `env:"FIREHOSE_FILTER_CACHE_SIZE"`

	// SeenCache is the maximum number of pubkeys the seen cache can hold.
	SeenCache int `env:"FIREHOSE_SEEN_CACHE_SIZE"`
}

func NewConfig() Config {
	return Config{
		Kinds:       pipe.AllKinds,
		Offset:      time.Minute,
		FilterCache: 100_000,
		SeenCache:   100_000,
	}
}

// Filter returns a nostr.Filter configured with the firehose's kind list and offset.
func (c Config) Filter() nostr.Filter {
	since := nostr.Timestamp(time.Now().Add(-c.Offset).Unix())
	return nostr.Filter{
		Kinds: c.Kinds,
		Since: &since,
	}
}

func (c Config) Validate() error {
	if len(c.Kinds) < 1 {
		return errors.New("kind list cannot be empty")
	}

	if c.Offset < 0 {
		return errors.New("offset cannot be negative")
	}
	if c.Offset > 2*time.Minute {
		slog.Warn("firehose: offset is greater than 2 minutes, which might trigger relay rate limits")
	}

	if c.FilterCache < 1 {
		return errors.New("filter cache size cannot be negative")
	}
	if c.SeenCache < 1 {
		return errors.New("seen cache size cannot be negative")
	}
	return nil
}

func (c Config) String() string {
	return fmt.Sprintf("Firehose:\n"+
		"\tKinds: %v\n"+
		"\tOffset: %v\n"+
		"\tFilter Cache: %v\n"+
		"\tSeen Cache: %v\n",
		c.Kinds, c.Offset, c.FilterCache, c.SeenCache)
}
