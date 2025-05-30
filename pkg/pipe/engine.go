package pipe

import (
	"context"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/pagerank"
	"github/pippellia-btc/crawler/pkg/redb"
	"github/pippellia-btc/crawler/pkg/walks"
	"log"
	"sync/atomic"
	"time"
)

// walksTracker tracks the number of walks that have been updated by [Processor].
// It's used to wake-up the [Arbiter], which performs work and then resets it to 0.
var walksTracker *atomic.Int32

type ArbiterConfig struct {
	Activation float64
	Promotion  float64
	Demotion   float64

	PingPeriod time.Duration
	WaitPeriod time.Duration
}

func NewArbiterConfig() ArbiterConfig {
	return ArbiterConfig{
		Activation: 0.01,
		Promotion:  0.1,
		Demotion:   1.05,

		PingPeriod: time.Minute,
		WaitPeriod: time.Hour,
	}
}

func (c ArbiterConfig) Print() {
	fmt.Printf("Arbiter\n")
	fmt.Printf("  Activation: %f\n", c.Activation)
	fmt.Printf("  Promotion: %f\n", c.Promotion)
	fmt.Printf("  Demotion: %f\n", c.Demotion)
	fmt.Printf("  WaitPeriod: %v\n", c.WaitPeriod)
}

// Arbiter activates when the % of walks changed is greater than a threshold. Then it:
// - scans through all the nodes in the database
// - promotes or demotes nodes
func Arbiter(ctx context.Context, config ArbiterConfig, db redb.RedisDB, send func(pk string) error) {
	ticker := time.NewTicker(config.PingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Arbiter: shutting down...")
			return

		case <-ticker.C:
			total, err := db.TotalWalks(ctx)
			if err != nil {
				log.Printf("Arbiter: %v", err)
				continue
			}

			changed := walksTracker.Load()
			changeRatio := float64(changed) / float64(total)

			if changeRatio > config.Activation {
				promoted, demoted, err := arbiterScan(ctx, config, db, send)
				if err != nil {
					log.Printf("Arbiter: %v", err)
					continue
				}

				walksTracker.Store(0) // resets tracker
				log.Printf("Arbiter: promoted %d, demoted %d", promoted, demoted)
			}
		}
	}
}

// ArbiterScan performs one entire database scan, promoting or demoting nodes based on their pagerank.
func arbiterScan(ctx context.Context, config ArbiterConfig, db redb.RedisDB, send func(pk string) error) (promoted, demoted int, err error) {
	maxTime := 60 * time.Second
	ctx, cancel := context.WithTimeout(ctx, maxTime)
	defer cancel()

	baseRank, err := minPagerank(ctx, db)
	if err != nil {
		return promoted, demoted, err
	}

	promotionThreshold := baseRank * config.Promotion
	demotionThreshold := baseRank * config.Demotion

	var IDs []graph.ID
	var cursor uint64

	for {
		select {
		case <-ctx.Done():
			return promoted, demoted, fmt.Errorf("failed to finish the scan in %v", maxTime)
		default:
			// proceed with the scan
		}

		IDs, cursor, err = db.ScanNodes(ctx, cursor, 3000) // ~1000 nodes
		if err != nil {
			return promoted, demoted, err
		}

		nodes, err := db.Nodes(ctx, IDs...)
		if err != nil {
			return promoted, demoted, err
		}

		ranks, err := pagerank.Global(ctx, db, IDs...)
		if err != nil {
			return promoted, demoted, err
		}

		for i, node := range nodes {
			switch node.Status {
			case graph.StatusActive:
				// active --> inactive
				if ranks[i] < demotionThreshold {
					if err := demote(db, node.ID); err != nil {
						return promoted, demoted, err
					}

					demoted++
				}

			case graph.StatusInactive:
				// inactive --> active
				added, found := node.Added()
				if !found {
					return promoted, demoted, fmt.Errorf("node %s doesn't have an addition record", node.ID)
				}

				if ranks[i] >= promotionThreshold && time.Since(added) > config.WaitPeriod {
					if err := promote(db, node.ID); err != nil {
						return promoted, demoted, err
					}

					promoted++
					if err := send(node.Pubkey); err != nil {
						return promoted, demoted, err
					}
				}
			}
		}

		if cursor == 0 {
			// returns to 0, the scan is complete
			return promoted, demoted, nil
		}
	}
}

// minPagerank returns the minimum possible pagerank value for an active node.
// An active node is visited by at least its own walks (the one starting from itself)
// which are [walks.N].
func minPagerank(ctx context.Context, db redb.RedisDB) (float64, error) {
	total, err := db.TotalVisits(ctx)
	if err != nil {
		return -1, err
	}
	return float64(walks.N) / float64(total), nil
}

// demote removes all walks that start from the node and changes its status to inactive.
func demote(db redb.RedisDB, node graph.ID) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	visiting, err := db.WalksVisiting(ctx, node, -1)
	if err != nil {
		return fmt.Errorf("failed to demote node %s: %v", node, err)
	}

	toRemove, err := walks.ToRemove(node, visiting)
	if err != nil {
		return fmt.Errorf("failed to demote node %s: %v", node, err)
	}

	if err := db.RemoveWalks(ctx, toRemove...); err != nil {
		return fmt.Errorf("failed to demote node %s: %v", node, err)
	}

	return db.Demote(ctx, node)
}

// promote generates random walks for the node and changes its status to active.
func promote(db redb.RedisDB, node graph.ID) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	walks, err := walks.Generate(ctx, db, node)
	if err != nil {
		return fmt.Errorf("failed to promote node %s: %v", node, err)
	}

	if err := db.AddWalks(ctx, walks...); err != nil {
		return fmt.Errorf("failed to promote node %s: %v", node, err)
	}

	return db.Promote(ctx, node)
}
