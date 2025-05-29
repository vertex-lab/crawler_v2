// The pagerank package uses the random walks to compute graph algorithms like
// global and personalized pageranks.
package pagerank

import (
	"context"
	"errors"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/walks"
	"math/rand/v2"
	"slices"
)

var ErrEmptyWalkStore = errors.New("the walk store is empty")

type VisitCounter interface {
	// TotalVisits returns the total number of visits, which is the sum of the lengths of all walks.
	TotalVisits(ctx context.Context) (int, error)

	// Visits returns the number of times each specified node was visited during the walks.
	// The returned slice contains counts in the same order as the input nodes.
	// If a node is not found, it returns 0 visits.
	Visits(ctx context.Context, nodes ...graph.ID) ([]int, error)
}

// Global computes the global pagerank score for each target node, as the frequency of visits.
// If a node is not found, its pagerank is assumed to be 0.
func Global(ctx context.Context, count VisitCounter, targets ...graph.ID) ([]float64, error) {
	if len(targets) == 0 {
		return nil, nil
	}

	total, err := count.TotalVisits(ctx)
	if err != nil {
		return nil, fmt.Errorf("Global: failed to get the visits total: %w", err)
	}

	if total <= 0 {
		return nil, ErrEmptyWalkStore
	}

	visits, err := count.Visits(ctx, targets...)
	if err != nil {
		return nil, fmt.Errorf("Global: failed to get the nodes visits: %w", err)
	}

	pageranks := make([]float64, len(visits))
	for i, v := range visits {
		pageranks[i] = float64(v) / float64(total)

	}

	return pageranks, nil
}

type PersonalizedLoader interface {
	// Follows returns the follow-list of the node
	Follows(ctx context.Context, node graph.ID) ([]graph.ID, error)

	// BulkFollows returns the follow-lists of the specified nodes
	BulkFollows(ctx context.Context, nodes []graph.ID) ([][]graph.ID, error)

	// WalksVisitingAny returns up to limit walks that visit the specified nodes.
	// The walks are distributed evenly among the nodes:
	// - if limit == -1, all walks are returned.
	// - if limit < len(nodes), no walks are returned
	WalksVisitingAny(ctx context.Context, nodes []graph.ID, limit int) ([]walks.Walk, error)
}

func PersonalizedWithTargets(
	ctx context.Context,
	loader PersonalizedLoader,
	source graph.ID,
	targets []graph.ID,
	targetLenght int) ([]float64, error) {

	if len(targets) == 0 {
		return nil, nil
	}

	pp, err := Personalized(ctx, loader, source, targetLenght)
	if err != nil {
		return nil, err
	}

	pageranks := make([]float64, len(targets))
	for i, t := range targets {
		pageranks[i] = pp[t]
	}

	return pageranks, nil
}

/*
Personalized computes the personalized pagerank of node by simulating a
long random walk starting at and resetting to itself. This long walk is generated
using the random walks in the storage layer whenever possible.

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func Personalized(
	ctx context.Context,
	loader PersonalizedLoader,
	source graph.ID,
	targetLenght int) (map[graph.ID]float64, error) {

	follows, err := loader.Follows(ctx, source)
	if err != nil {
		return nil, fmt.Errorf("Personalized: failed to fetch the follows of source: %w", err)
	}

	if len(follows) == 0 {
		// the special distribution of a dandling node
		return map[graph.ID]float64{source: 1.0}, nil
	}

	followByNode, err := loader.BulkFollows(ctx, follows)
	if err != nil {
		return nil, fmt.Errorf("Personalized: failed to fetch the two-hop network of source: %w", err)
	}

	targetWalks := int(float64(targetLenght) * (1 - walks.Alpha))
	walks, err := loader.WalksVisitingAny(ctx, append(follows, source), targetWalks)
	if err != nil {
		return nil, fmt.Errorf("Personalized: failed to fetch the walk: %w", err)
	}

	walker := newCachedWalker(follows, followByNode, loader)
	pool := newWalkPool(walks)

	walk, err := personalizedWalk(ctx, walker, pool, source, targetLenght)
	if err != nil {
		return nil, err
	}

	return frequencyMap(walk), nil
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

// // WalkPool makes sure a walk is returned only once, avoiding bias in the [Personalized]
// type WalkPool interface {
// 	// Next returns a path that starts with the provided node
// 	Next(node graph.ID) ([]graph.ID, bool)
// }

// The personalizedWalk() function simulates a long personalized random walk
// starting from a node with reset to itself. Whenever possible, walks from the
// [WalkCache] are used to speed up the computation.
func personalizedWalk(
	ctx context.Context,
	walker walks.Walker,
	pool *walkPool,
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
			// use the pre-computed walk when available
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

// frequencyMap returns a map node --> frequency of visits.
func frequencyMap(path []graph.ID) map[graph.ID]float64 {
	if len(path) == 0 {
		return nil
	}

	total := len(path)
	freq := 1.0 / float64(total)
	freqs := make(map[graph.ID]float64, total/100)

	for _, node := range path {
		freqs[node] += freq
	}

	return freqs
}

// targetFrequency returns the frequency of visits for each target
func targetFrequency(targets []graph.ID, path []graph.ID) []float64 {
	if len(targets) == 0 || len(path) == 0 {
		return nil
	}

	total := len(path)
	freq := 1.0 / float64(total)
	freqs := make([]float64, len(targets))

	for _, node := range path {
		idx := slices.Index(targets, node)
		if idx == -1 {
			continue
		}

		freqs[idx] += freq
	}

	return freqs
}

// returns a random element of a slice. It panics if the slice is empty or nil.
func randomElement[S []E, E any](s S) E {
	return s[rand.IntN(len(s))]
}
