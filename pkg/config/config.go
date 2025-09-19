// The config package loads and validates the variables in the enviroment into a [Config]
package config

import (
	"errors"
	"fmt"

	"github.com/kelseyhightower/envconfig"
	"github.com/vertex-lab/crawler_v2/pkg/pipe"

	_ "github.com/joho/godotenv/autoload" // autoloading .env
	"github.com/nbd-wtf/go-nostr"
)

type SystemConfig struct {
	RedisAddress    string   `envconfig:"REDIS_ADDRESS"`
	SQLiteURL       string   `envconfig:"SQLITE_URL"`
	EventsCapacity  int      `envconfig:"EVENTS_CAPACITY"`
	PubkeysCapacity int      `envconfig:"PUBKEYS_CAPACITY"`
	InitPubkeys     []string `envconfig:"INIT_PUBKEYS"`
	PrintStats      bool     `envconfig:"PRINT_STATS"`
}

func NewSystemConfig() SystemConfig {
	return SystemConfig{
		RedisAddress:    "localhost:6379",
		SQLiteURL:       "events.sqlite",
		EventsCapacity:  1000,
		PubkeysCapacity: 1000,
	}
}

func (c SystemConfig) Validate() error {
	if c.EventsCapacity < 0 {
		return errors.New("events: value cannot be negative")
	}

	for _, pk := range c.InitPubkeys {
		if !nostr.IsValidPublicKey(pk) {
			return fmt.Errorf("init pubkeys: \"%s\" is not valid hex pubkey", pk)
		}
	}
	return nil
}

func (c SystemConfig) Print() {
	fmt.Println("System:")
	fmt.Printf("  RedisAddress: %s\n", c.RedisAddress)
	fmt.Printf("  SQLiteURL: %s\n", c.SQLiteURL)
	fmt.Printf("  EventsCapacity: %d\n", c.EventsCapacity)
	fmt.Printf("  PubkeysCapacity: %d\n", c.PubkeysCapacity)
	fmt.Printf("  InitPubkeys: %v\n", c.InitPubkeys)
	fmt.Printf("  PrintStats: %v\n", c.PrintStats)
}

// The configuration parameters for the system and the main processes
type Config struct {
	SystemConfig
	Firehose pipe.FirehoseConfig
	Fetcher  pipe.FetcherConfig
	Arbiter  pipe.ArbiterConfig
	Engine   pipe.EngineConfig
}

// New returns a config with default parameters
func New() Config {
	return Config{
		SystemConfig: NewSystemConfig(),
		Firehose:     pipe.NewFirehoseConfig(),
		Fetcher:      pipe.NewFetcherConfig(),
		Arbiter:      pipe.NewArbiterConfig(),
		Engine:       pipe.NewEngineConfig(),
	}
}

func (c Config) Validate() error {
	if err := c.SystemConfig.Validate(); err != nil {
		return fmt.Errorf("System: %w", err)
	}

	if err := c.Firehose.Validate(); err != nil {
		return fmt.Errorf("Firehose: %w", err)
	}

	if err := c.Fetcher.Validate(); err != nil {
		return fmt.Errorf("Fetcher: %w", err)
	}

	if err := c.Arbiter.Validate(); err != nil {
		return fmt.Errorf("Arbiter: %w", err)
	}

	if err := c.Engine.Validate(); err != nil {
		return fmt.Errorf("Engine: %w", err)
	}
	return nil
}

func (c Config) Print() {
	c.SystemConfig.Print()
	c.Firehose.Print()
	c.Fetcher.Print()
	c.Arbiter.Print()
	c.Engine.Print()
}

// Load creates a new [Config] with default parameters.
// Then, if the corresponding environment variable is set, it overwrites them.
func Load() (Config, error) {
	config := New()

	if err := envconfig.Process("", &config); err != nil {
		return Config{}, fmt.Errorf("config.Load: %w", err)
	}

	if err := config.Validate(); err != nil {
		return Config{}, fmt.Errorf("config.Load: %w", err)
	}

	return config, nil
}
