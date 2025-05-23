package pagerank

import (
	"context"
	"errors"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/walks"
	"math/rand/v2"
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

	if total <= 0 {
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

// pWalk is a personalized walk, which is a random walk that resets to a specified node
// and continues until it reaches a specified target lenght.
type pWalk struct {
	start graph.ID // the starting node
	node  graph.ID // the current node

	ongoing walks.Walk // the current walk
	union   []graph.ID // the sum of all previous walk paths
}

func newPersonalizedWalk(start graph.ID, target int) *pWalk {
	return &pWalk{
		start:   start,
		node:    start,
		ongoing: walks.Walk{Path: []graph.ID{start}},
		union:   make([]graph.ID, 0, target),
	}
}

// Reached returns whether the personalized walk is long enough
func (w *pWalk) Reached(lenght int) bool {
	return len(w.union) >= lenght
}

// Reset the walk to its base state after appending the ongoing walk to the union
func (w *pWalk) Reset() {
	w.union = append(w.union, w.ongoing.Path...)
	w.ongoing = walks.Walk{Path: []graph.ID{w.start}}
	w.node = w.start
}

// WalkPool makes sure a walk is returned only once, avoiding bias in the [Personalized]
type WalkPool interface {
	// Next returns a path that starts with the provided node
	Next(node graph.ID) ([]graph.ID, bool)
}

// The personalizedWalk() function simulates a long personalized random walk
// starting from a node with reset to itself. Whenever possible, walks from the
// [WalkCache] are used to speed up the computation.
func personalizedWalk(
	ctx context.Context,
	walker walks.Walker,
	pool WalkPool,
	start graph.ID,
	lenght int) ([]graph.ID, error) {

	var path []graph.ID
	var exists bool
	walk := newPersonalizedWalk(start, lenght)

	for {
		if walk.Reached(lenght) {
			return walk.union, nil
		}

		if rand.Float64() > walks.Alpha {
			walk.Reset()
			continue
		}

		path, exists = pool.Next(walk.node)
		switch exists {
		case true:
			// graft the given path
			walk.ongoing.Graft(path)
			walk.Reset()

		case false:
			// perform one manual step
			follows, err := walker.Follows(ctx, walk.node)
			if err != nil {
				return nil, err
			}

			if len(follows) == 0 {
				// found a dandling node, stop
				walk.Reset()
				continue
			}

			node := randomElement(follows)
			if walk.ongoing.Visits(node) {
				// found a cycle, stop
				walk.Reset()
				continue
			}

			walk.node = node
			walk.ongoing.Append(node)
		}
	}
}

// returns a random element of a slice. It panics if the slice is empty or nil.
func randomElement[S []E, E any](s S) E {
	return s[rand.IntN(len(s))]
}
