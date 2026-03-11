package engine

import (
	"errors"
	"fmt"
	"time"

	"github.com/vertex-lab/crawler_v2/pkg/pipe"
)

type Config struct {
	// Kinds is the list of event kinds to be processed.
	// If an event has a kind that is not in this list, it will be simple skipped.
	Kinds []int `env:"ENGINE_KINDS"`

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
		Kinds:         pipe.ProfileKinds,
		Queue:         10_000,
		CacheCapacity: 100_000,
		PrintEvery:    10_000,
		Timeout:       10 * time.Second,
	}
}

func (c Config) Validate() error {
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
		"\tQueue: %d\n"+
		"\tCache Capacity: %d\n"+
		"\tPrint Every: %d\n"+
		"\tTimeout: %v\n",
		c.Queue,
		c.CacheCapacity,
		c.PrintEvery,
		c.Timeout,
	)
}
