package fetcher

import (
	"errors"
	"fmt"
	"time"

	"github.com/vertex-lab/crawler_v2/pkg/core"
)

type Config struct {
	// Kinds represents the list of kinds to fetch.
	Kinds []int `env:"FETCHER_KINDS"`

	// Queue represents the capacity of the fetcher pubkeys queue. Default is 10k.
	Queue int `env:"FETCHER_QUEUE"`

	// Batch represents the maximum number of pubkeys that can be fetched in a single batch. Default is 100.
	Batch int `env:"FETCHER_BATCH"`

	// Interval represents the maximum time interval between fetches. Default is 1 minute.
	Interval time.Duration `env:"FETCHER_INTERVAL"`

	// Timeout represents the maximum time to wait for a query to complete. Default is 10 seconds.
	Timeout time.Duration `env:"FETCHER_TIMEOUT"`
}

func NewConfig() Config {
	return Config{
		Kinds:    core.ProfileKinds,
		Queue:    10_000,
		Batch:    100,
		Interval: time.Minute,
		Timeout:  20 * time.Second,
	}
}

func (c Config) Validate() error {
	if len(c.Kinds) < 1 {
		return errors.New("kind list cannot be empty")
	}
	if c.Queue < 0 {
		return errors.New("queue value must be non-negative")
	}
	if c.Batch < 0 {
		return errors.New("batch value must be non-negative")
	}
	if c.Batch < 10 {
		return errors.New("batch value must be at least 10 to avoid relay rate-limiting")
	}
	if c.Interval < time.Second {
		return errors.New("interval must be at least 1 second to avoid relay rate-limiting")
	}
	if c.Timeout < time.Second {
		return errors.New("timeout must be at least 1 second to function reliably")
	}
	return nil
}

func (c Config) String() string {
	return fmt.Sprintf("Fetcher:\n"+
		"\tKinds: %v\n"+
		"\tQueue: %d\n"+
		"\tBatch: %d\n"+
		"\tInterval: %v\n",
		c.Kinds, c.Queue, c.Batch, c.Interval)
}
