package pool

import (
	"log/slog"
	"os"

	"github.com/vertex-lab/crawler_v2/pkg/relays"
)

func New(c Config) (*relays.Pool, error) {
	options := []relays.PoolOption{
		relays.WithLogger(slog.New(
			slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)),
		relays.WithRelayRetry(c.RelayRetry),
		relays.WithSubscriptionRetry(c.SubRetry),
	}
	if c.AuthKey != "" {
		options = append(options, relays.WithAuthKey(c.AuthKey))
	}
	return relays.NewPool(c.InitRelays, options...)
}
