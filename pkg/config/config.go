// The config package loads and validates the variables in the enviroment into a [Config]
package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/arbiter"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/engine"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/fetcher"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/firehose"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/pool"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/recorder"

	_ "github.com/joho/godotenv/autoload" // autoloading .env
	"github.com/nbd-wtf/go-nostr"
)

type System struct {
	// The address of the Redis server
	RedisAddress string `env:"REDIS_ADDRESS"`

	// The path to the Sqlite database file
	SqlitePath string `env:"SQLITE_PATH"`

	// The list of public keys to initialize the graph with
	InitPubkeys []string `env:"INIT_PUBKEYS"`
}

func NewSystem() System {
	return System{
		RedisAddress: "localhost:6379",
		SqlitePath:   "events.sqlite",
	}
}

func (c System) Validate() error {
	for _, pk := range c.InitPubkeys {
		if !nostr.IsValidPublicKey(pk) {
			return fmt.Errorf("init pubkeys: \"%s\" is not valid hex pubkey", pk)
		}
	}
	return nil
}

func (c System) String() string {
	return fmt.Sprintf("System:\n"+
		"\tRedisAddress: %s\n"+
		"\tSqlitePath:   %s\n"+
		"\tInitPubkeys:  %v\n",
		c.RedisAddress, c.SqlitePath, c.InitPubkeys)
}

// The configuration parameters for the system and the main processes
type Config struct {
	System
	Pool     pool.Config
	Firehose firehose.Config
	Recorder recorder.Config
	Fetcher  fetcher.Config
	Arbiter  arbiter.Config
	Engine   engine.Config
}

// New returns a config with default parameters
func New() Config {
	return Config{
		System:   NewSystem(),
		Pool:     pool.NewConfig(),
		Firehose: firehose.NewConfig(),
		Recorder: recorder.NewConfig(),
		Fetcher:  fetcher.NewConfig(),
		Arbiter:  arbiter.NewConfig(),
		Engine:   engine.NewConfig(),
	}
}

// Load creates a new [Config] with default parameters.
// Then, if the corresponding environment variable is set, it overwrites them.
func Load() (Config, error) {
	config := New()
	if err := env.Parse(&config); err != nil {
		return Config{}, fmt.Errorf("config.Load: %w", err)
	}
	return config, nil
}

func (c Config) Validate() error {
	if err := c.System.Validate(); err != nil {
		return fmt.Errorf("System: %w", err)
	}
	if err := c.Pool.Validate(); err != nil {
		return fmt.Errorf("Pool: %w", err)
	}
	if err := c.Firehose.Validate(); err != nil {
		return fmt.Errorf("Firehose: %w", err)
	}
	if err := c.Recorder.Validate(); err != nil {
		return fmt.Errorf("Recorder: %w", err)
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

func (c Config) String() string {
	return c.System.String() +
		c.Pool.String() +
		c.Firehose.String() +
		c.Recorder.String() +
		c.Fetcher.String() +
		c.Arbiter.String() +
		c.Engine.String()
}
