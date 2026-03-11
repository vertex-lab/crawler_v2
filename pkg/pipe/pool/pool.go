package pool

import (
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
	return relays.NewPool(c.InitRelays, opts...)
}
