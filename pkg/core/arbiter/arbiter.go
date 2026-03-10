package arbiter

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/pagerank"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
	"github.com/vertex-lab/crawler_v2/pkg/walks"
)

// T represents the Arbiter, which periodically scans the graph
// for nodes to promote or demote based on their pagerank.
type T struct {
	config Config
	db     regraph.DB

	// Typically wired as engine.WalksUpdated.
	walksUpdated func() int
}

func New(c Config, db regraph.DB, walksUpdated func() int) *T {
	if walksUpdated == nil {
		panic("arbiter.New: walksUpdated is nil")
	}
	return &T{
		config:       c,
		db:           db,
		walksUpdated: walksUpdated,
	}
}

func (a *T) Run(ctx context.Context, onPromotion func(string) error) {
	slog.Info("Arbiter: ready")
	defer slog.Info("Arbiter: shut down")

	scan := func() {
		promoted, demoted, err := a.scan(ctx, onPromotion)
		if err != nil && ctx.Err() == nil {
			slog.Error("Arbiter: failed to scan", "error", err)
		}
		if a.config.PrintStats {
			slog.Info("Arbiter: scan completed", "promoted", promoted, "demoted", demoted)
		}
	}

	scan() // scan at startup

	ticker := time.NewTicker(a.config.PollInterval)
	defer ticker.Stop()

	updated := 0

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			total, err := a.db.TotalWalks(ctx)
			if err != nil && ctx.Err() == nil {
				slog.Error("Arbiter: failed to fetch total walks", "error", err)
				continue
			}

			if total <= 0 {
				slog.Error("Arbiter: total walks are non-positive", "total", total)
				continue
			}

			updated += a.walksUpdated()
			change := float64(updated) / float64(total)

			if change >= a.config.Activation {
				scan()
				updated = 0
			}
		}
	}
}

// scan performs a single scan of the graph to promote/demote nodes.
func (a *T) scan(ctx context.Context, onPromotion func(string) error) (promoted, demoted int, err error) {
	ctx, cancel := context.WithTimeout(ctx, a.config.ScanTimeout)
	defer cancel()

	baseRank, err := basePagerank(ctx, a.db)
	if err != nil {
		return 0, 0, err
	}

	promotionT := baseRank * a.config.Promotion
	demotionT := baseRank * a.config.Demotion

	var ids []graph.ID
	var cursor uint64

	for {
		select {
		case <-ctx.Done():
			return promoted, demoted, ctx.Err()
		default:
		}

		ids, cursor, err = a.db.ScanNodes(ctx, cursor, 3000) // ~1000 nodes
		if err != nil {
			return promoted, demoted, err
		}

		nodes, err := a.db.Nodes(ctx, ids...)
		if err != nil {
			return promoted, demoted, err
		}

		ranks, err := pagerank.Global(ctx, a.db, ids...)
		if err != nil {
			return promoted, demoted, err
		}

		for i := range nodes {
			node := nodes[i]
			rank := ranks[i]

			if node.Status == graph.StatusActive && rank < demotionT {
				if err := Demote(a.db, node.ID); err != nil {
					return promoted, demoted, err
				}
				demoted++
			}

			if node.Status == graph.StatusInactive && rank >= promotionT {
				added, found := node.Addition()
				if !found {
					return promoted, demoted, fmt.Errorf("node %s doesn't have an addition record", node.ID)
				}
				if time.Since(added) < a.config.PromotionWait {
					continue
				}

				if err := Promote(a.db, node.ID); err != nil {
					return promoted, demoted, err
				}

				promoted++
				if onPromotion != nil {
					if err := onPromotion(node.Pubkey); err != nil {
						return promoted, demoted, err
					}
				}
			}
		}

		if cursor == 0 {
			return promoted, demoted, nil
		}
	}
}

// basePagerank returns the minimum pagerank value for a node, based on the total number of visits.
func basePagerank(ctx context.Context, db regraph.DB) (float64, error) {
	total, err := db.TotalVisits(ctx)
	if err != nil {
		return -1, err
	}
	return float64(walks.N) / float64(total), nil
}

// Demote removes all walks that start from the node and changes its status to inactive.
func Demote(db regraph.DB, node graph.ID) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
func Promote(db regraph.DB, node graph.ID) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ws, err := walks.Generate(ctx, db, node)
	if err != nil {
		return fmt.Errorf("failed to promote node %s: %v", node, err)
	}
	if err := db.AddWalks(ctx, ws...); err != nil {
		return fmt.Errorf("failed to promote node %s: %v", node, err)
	}
	return db.Promote(ctx, node)
}
