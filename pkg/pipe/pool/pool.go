// Package pool is a wrapper around [relays.Pool], adding to it a config and stats logging.
package pool

import (
	"log/slog"
	"time"

	"github.com/vertex-lab/crawler_v2/pkg/relays"
)

func New(c Config, opts ...relays.PoolOption) (*relays.Pool, error) {
	opts = append(opts,
		relays.WithRelayRetry(c.RelayRetry),
		relays.WithSubscriptionRetry(c.SubRetry),
	)
	if c.AuthKey != "" {
		opts = append(opts, relays.WithAuthKey(c.AuthKey))
	}

	pool, err := relays.NewPool(c.InitRelays, opts...)
	if err != nil {
		return nil, err
	}

	if c.StatsInterval > 0 {
		go logStats(pool, c.StatsInterval)
	}
	return pool, nil
}

// logStats logs the relay pool stats every given interval.
func logStats(pool *relays.Pool, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			connected, disconnected := pool.Relays()
			slog.Info("relay pool stats", "connected", len(connected), "disconnected", len(disconnected))

		case <-pool.Done():
			return
		}
	}
}
