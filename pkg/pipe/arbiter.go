package pipe

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/pagerank"
	"github.com/vertex-lab/crawler_v2/pkg/redb"
	"github.com/vertex-lab/crawler_v2/pkg/walks"
)

// WalksTracker tracks the number of walks that have been updated by the [Grapher].
// It's used to wake-up the [Arbiter], which performs work and then resets it to 0.
var WalksTracker atomic.Int32

type ArbiterConfig struct {
	Activation    float64       `envconfig:"ARBITER_ACTIVATION"`
	Promotion     float64       `envconfig:"ARBITER_PROMOTION"`
	Demotion      float64       `envconfig:"ARBITER_DEMOTION"`
	PromotionWait time.Duration `envconfig:"ARBITER_PROMOTION_WAIT"`
	PingWait      time.Duration `envconfig:"ARBITER_PING_WAIT"`
}

func NewArbiterConfig() ArbiterConfig {
	return ArbiterConfig{
		Activation:    0.01,
		Promotion:     0.1,
		Demotion:      1.05,
		PromotionWait: time.Hour,
		PingWait:      time.Minute,
	}
}

func (c ArbiterConfig) Validate() error {
	if c.Activation < 0 {
		return errors.New("activation ratio cannot be negative")
	}

	if c.Promotion < 0 {
		return errors.New("promotion multiplier cannot be negative")
	}

	if c.Demotion < 0 {
		return errors.New("demotion multiplier cannot be negative")
	}

	if c.Demotion <= 1 {
		log.Println("WARN: Arbiter: demotion multiplier is smaller than 1." +
			"This implies it's impossible for an active node to be demoted")
	}

	if 1+c.Promotion <= c.Demotion {
		log.Println("WARN: Arbiter: the inequality (1 + promotion) > demotion is not satisfied." +
			"This implies there will be cyclical promotions -> demotions -> promotions...")
	}

	if c.PromotionWait < 24*time.Hour {
		log.Println("WARN: Arbiter: the promotion wait is less than 24hrs." +
			"This implies a reputable attacker could add to the db several bots in a short period of time")
	}
	return nil
}

func (c ArbiterConfig) Print() {
	fmt.Printf("Arbiter: \n")
	fmt.Printf("  Activation: %f\n", c.Activation)
	fmt.Printf("  Promotion: %f\n", c.Promotion)
	fmt.Printf("  Demotion: %f\n", c.Demotion)
	fmt.Printf("  PromotionWait: %v\n", c.PromotionWait)
	fmt.Printf("  PingWait: %v\n", c.PingWait)
}

// Arbiter activates when the % of walks changed is greater than a threshold. Then it:
// - scans through all the nodes in the database
// - promotes or demotes nodes
func Arbiter(
	ctx context.Context,
	config ArbiterConfig,
	db redb.RedisDB,
	forward Forward[string],
) {
	log.Println("Arbiter: ready")
	defer log.Println("Arbiter: shut down")

	ticker := time.NewTicker(config.PingWait)
	defer ticker.Stop()

	WalksTracker.Add(100_000_000) // trigger a scan at startup

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			total, err := db.TotalWalks(ctx)
			if err != nil && ctx.Err() == nil {
				log.Printf("Arbiter: %v", err)
				continue
			}

			changed := WalksTracker.Load()
			changeRatio := float64(changed) / float64(total)

			if changeRatio > config.Activation {
				promoted, demoted, err := arbiterScan(ctx, config, db, forward)
				if err != nil && ctx.Err() == nil {
					log.Printf("Arbiter: %v", err)
				}

				WalksTracker.Store(0) // resets tracker
				log.Printf("Arbiter: promoted %d, demoted %d", promoted, demoted)
			}
		}
	}
}

// ArbiterScan performs one entire database scan, promoting or demoting nodes based on their pagerank.
func arbiterScan(
	ctx context.Context,
	config ArbiterConfig,
	db redb.RedisDB,
	forward Forward[string],
) (promoted, demoted int, err error) {

	maxTime := 2 * time.Minute
	ctx, cancel := context.WithTimeout(ctx, maxTime)
	defer cancel()

	baseRank, err := minPagerank(ctx, db)
	if err != nil {
		return 0, 0, err
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
					if err := Demote(db, node.ID); err != nil {
						return promoted, demoted, err
					}

					demoted++
				}

			case graph.StatusInactive:
				// inactive --> active
				added, found := node.Addition()
				if !found {
					return promoted, demoted, fmt.Errorf("node %s doesn't have an addition record", node.ID)
				}

				if ranks[i] >= promotionThreshold && time.Since(added) > config.PromotionWait {
					if err := Promote(db, node.ID); err != nil {
						return promoted, demoted, err
					}

					promoted++
					if err := forward(node.Pubkey); err != nil {
						return promoted, demoted, err
					}
				}
			}
		}

		if cursor == 0 {
			// the scan is complete
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

// Demote removes all walks that start from the node and changes its status to inactive.
func Demote(db redb.RedisDB, node graph.ID) error {
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

// Promote generates random walks for the node and changes its status to active.
func Promote(db redb.RedisDB, node graph.ID) error {
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
