package arbiter

import (
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type Config struct {
	// Activation controls how often the Arbiter activates, as the % of random walks that changed
	// since the last scan. Default is 0.01 (1%).
	Activation float64 `env:"ARBITER_ACTIVATION"`

	// Promotion is the threshold for promoting a pubkey, as a % of the [basePagerank].
	Promotion float64 `env:"ARBITER_PROMOTION"`

	// Demotion is the threshold for demoting a pubkey, as a % of the [basePagerank].
	Demotion float64 `env:"ARBITER_DEMOTION"`

	// PromotionWait is the duration to wait before promoting a pubkey.
	PromotionWait time.Duration `env:"ARBITER_PROMOTION_WAIT"`

	// PollInterval is the interval duration for checking the number of walks that changed since the last check.
	PollInterval time.Duration `env:"ARBITER_PING_WAIT"`

	// PrintStats controls whether the Arbiter prints stats after every scan.
	PrintStats bool `env:"ARBITER_PRINT_STATS"`

	// ScanTimeout is the timeout for the scan operation. Default is 2 minutes.
	ScanTimeout time.Duration `env:"ARBITER_SCAN_TIMEOUT"`
}

func NewConfig() Config {
	return Config{
		Activation:    0.01,
		Promotion:     0.1,
		Demotion:      1.05,
		PromotionWait: 24 * time.Hour,
		PollInterval:  time.Minute,
		PrintStats:    true,
		ScanTimeout:   2 * time.Minute,
	}
}

func (c Config) Validate() error {
	if c.Activation < 0 || c.Activation > 1 {
		return errors.New("activation ratio must be in [0,1]")
	}

	if c.Promotion < 0 {
		return errors.New("promotion multiplier cannot be negative")
	}
	if c.Demotion < 0 {
		return errors.New("demotion multiplier cannot be negative")
	}
	if c.Demotion <= 1 {
		slog.Warn("Arbiter: demotion <= 1 means active nodes cannot be demoted")
	}
	if 1+c.Promotion <= c.Demotion {
		slog.Warn("Arbiter: (1 + promotion) <= demotion may cause cyclical promotions/demotions")
	}

	if c.PollInterval < time.Second {
		return errors.New("ping wait must be at least 1 second")
	}
	if c.ScanTimeout < time.Second {
		return errors.New("scan timeout must be at least 1 second")
	}

	if c.PromotionWait < 24*time.Hour {
		slog.Warn("Arbiter: promotion wait is less than 24h")
	}
	return nil
}

func (c Config) String() string {
	return fmt.Sprintf("Arbiter:\n"+
		"\tActivation: %f\n"+
		"\tPromotion: %f\n"+
		"\tDemotion: %f\n"+
		"\tPromotion Wait: %v\n"+
		"\tPoll Interval: %v\n"+
		"\tPrint Stats: %v\n"+
		"\tScan Timeout: %v\n",
		c.Activation, c.Promotion, c.Demotion, c.PromotionWait, c.PollInterval, c.PrintStats, c.ScanTimeout,
	)
}
