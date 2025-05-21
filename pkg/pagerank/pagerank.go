package pagerank

import (
	"context"
	"errors"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
)

var ErrEmptyWalkStore = errors.New("the walk store is empty")

type VisitCounter interface {
	// TotalVisits returns the total number of visits, which is the sum of the lengths of all walks.
	TotalVisits(ctx context.Context) (int, error)

	// Visits returns the number of times each specified node was visited during the walks.
	// The returned slice contains counts in the same order as the input nodes.
	Visits(ctx context.Context, nodes ...graph.ID) ([]int, error)
}

// Global computes the global pagerank score for the specified nodes.
// If a node is not found, its pagerank is assumed to be 0.
func Global(ctx context.Context, count VisitCounter, nodes ...graph.ID) ([]float64, error) {
	if len(nodes) == 0 {
		return nil, nil
	}

	total, err := count.TotalVisits(ctx)
	if err != nil {
		return nil, fmt.Errorf("Global: failed to get the visits total: %w", err)
	}

	if total == 0 {
		return nil, ErrEmptyWalkStore
	}

	visits, err := count.Visits(ctx, nodes...)
	if err != nil {
		return nil, fmt.Errorf("Global: failed to get the nodes visits: %w", err)
	}

	pageranks := make([]float64, len(visits))
	for i, v := range visits {
		pageranks[i] = float64(v) / float64(total)

	}

	return pageranks, nil
}
