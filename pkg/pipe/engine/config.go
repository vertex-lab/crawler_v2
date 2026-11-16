package engine

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/pipe"
)

type Config struct {
	// ArchiveKinds selects event kinds to persist in addition to the follow and
	// relay lists required by graph processing and relay discovery.
	ArchiveKinds []int `env:"ENGINE_ARCHIVE_KINDS"`

	// Queue is the capacity of the internal engine events queue.
	Queue int `env:"ENGINE_QUEUE"`

	// CacheCapacity is the capacity of the in-memory walk cache.
	CacheCapacity int `env:"ENGINE_CACHE_CAPACITY"`

	// PrintEvery controls how often processing stats are logged.
	PrintEvery int `env:"ENGINE_PRINT_EVERY"`

	// Timeout is the maximum per-event processing time.
	Timeout time.Duration `env:"ENGINE_TIMEOUT"`
}

func NewConfig() Config {
	return Config{
		ArchiveKinds:  slices.Clone(pipe.ProfileKinds),
		Queue:         10_000,
		CacheCapacity: 100_000,
		PrintEvery:    10_000,
		Timeout:       10 * time.Second,
	}
}

func (c Config) Validate() error {
	for _, kind := range c.ArchiveKinds {
		if kind < 0 || kind > 65535 || nostr.IsEphemeralKind(kind) {
			return fmt.Errorf("archive kind must be a persistent kind between 0 and 65535: %d", kind)
		}
	}
	if c.Queue < 0 {
		return errors.New("queue value must be non-negative")
	}
	if c.CacheCapacity < 0 {
		return errors.New("cache capacity must be non-negative")
	}
	if c.PrintEvery < 0 {
		return errors.New("print every must be non-negative")
	}
	if c.Timeout < time.Second {
		return errors.New("timeout must be at least 1 second to function reliably")
	}
	return nil
}

func (c Config) String() string {
	return fmt.Sprintf("Engine:\n"+
		"\tArchive Kinds: %v\n"+
		"\tQueue: %d\n"+
		"\tCache Capacity: %d\n"+
		"\tPrint Every: %d\n"+
		"\tTimeout: %v\n",
		c.ArchiveKinds,
		c.Queue,
		c.CacheCapacity,
		c.PrintEvery,
		c.Timeout,
	)
}
