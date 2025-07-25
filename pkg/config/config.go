// The config package loads and validates the variables in the enviroment into a [Config]
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/vertex-lab/crawler_v2/pkg/pipe"

	_ "github.com/joho/godotenv/autoload" // autoloading .env
	"github.com/nbd-wtf/go-nostr"
)

type SystemConfig struct {
	RedisAddress string
	SQLiteURL    string

	EventsCapacity  int
	PubkeysCapacity int

	InitPubkeys []string // only used during initialization
	PrintStats  bool
}

func NewSystemConfig() SystemConfig {
	return SystemConfig{
		RedisAddress:    "localhost:6379",
		SQLiteURL:       "events.sqlite",
		EventsCapacity:  1000,
		PubkeysCapacity: 1000,
	}
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
func New() *Config {
	return &Config{
		SystemConfig: NewSystemConfig(),
		Firehose:     pipe.NewFirehoseConfig(),
		Fetcher:      pipe.NewFetcherConfig(),
		Arbiter:      pipe.NewArbiterConfig(),
		Engine:       pipe.NewEngineConfig(),
	}
}

func (c *Config) Print() {
	c.SystemConfig.Print()
	c.Firehose.Print()
	c.Fetcher.Print()
	c.Arbiter.Print()
	c.Engine.Print()
}

// Load reads the enviroment variables and parses them into a [Config] struct
func Load() (*Config, error) {
	var config = New()
	var err error

	for _, item := range os.Environ() {
		keyVal := strings.SplitN(item, "=", 2)
		key, val := keyVal[0], keyVal[1]

		switch key {
		case "REDIS_ADDRESS":
			config.RedisAddress = val

		case "SQLITE_URL":
			config.SQLiteURL = val

		case "EVENTS_CAPACITY":
			config.EventsCapacity, err = strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "PUBKEYS_CAPACITY":
			config.PubkeysCapacity, err = strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "INIT_PUBKEYS":
			pubkeys := strings.Split(val, ",")
			for _, pk := range pubkeys {
				if !nostr.IsValidPublicKey(pk) {
					return nil, fmt.Errorf("pubkey %s is not valid", pk)
				}
			}

			config.InitPubkeys = pubkeys

		case "PRINT_STATS":
			config.PrintStats, err = strconv.ParseBool(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "FIREHOSE_OFFSET":
			offset, err := strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}
			config.Fetcher.Interval = time.Duration(offset) * time.Second

		case "RELAYS":
			relays := strings.Split(val, ",")
			if len(relays) == 0 {
				return nil, fmt.Errorf("relay list is empty")
			}

			for _, relay := range relays {
				if !nostr.IsValidRelayURL(relay) {
					return nil, fmt.Errorf("relay \"%s\" is not a valid url", relay)
				}
			}

			config.Firehose.Relays = relays
			config.Fetcher.Relays = relays

		case "FETCHER_BATCH":
			config.Fetcher.Batch, err = strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "FETCHER_INTERVAL":
			interval, err := strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}
			config.Fetcher.Interval = time.Duration(interval) * time.Second

		case "ARBITER_ACTIVATION":
			config.Arbiter.Activation, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "ARBITER_PROMOTION":
			config.Arbiter.Promotion, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "ARBITER_DEMOTION":
			config.Arbiter.Demotion, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "ARBITER_PING_WAIT":
			wait, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}
			config.Arbiter.PingWait = time.Duration(wait) * time.Second

		case "ARBITER_PROMOTION_WAIT":
			wait, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}
			config.Arbiter.PromotionWait = time.Duration(wait) * time.Second

		case "ENGINE_PRINT_EVERY":
			config.Engine.PrintEvery, err = strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "ENGINE_BUILDER_CAPACITY":
			config.Engine.BuilderCapacity, err = strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "ENGINE_CACHE_CAPACITY":
			config.Engine.CacheCapacity, err = strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}
		}
	}

	return config, nil
}
