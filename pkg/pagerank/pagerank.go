// The pagerank package uses the random walks to compute graph algorithms like
// global and personalized pageranks.
package pagerank

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/pippellia-btc/slicex"
	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/walks"
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

	bulk, err := loader.BulkFollows(ctx, follows)
	if err != nil {
		return nil, fmt.Errorf("Personalized: failed to fetch the two-hop network of source: %w", err)
	}

	walker := walks.NewWalker(
		walks.WithCapacity(10000),
		walks.WithFallback(loader),
	)

	if err := walker.Load(follows, bulk); err != nil {
		return nil, fmt.Errorf("Personalized: failed to load the two-hop network of source: %w", err)
	}

	targetWalks := int(float64(targetLenght) * (1 - walks.Alpha))
	visiting, err := loader.WalksVisitingAny(ctx, append(follows, source), targetWalks)
	if err != nil {
		return nil, fmt.Errorf("Personalized: failed to fetch the walk: %w", err)
	}

	pool := newWalkPool(visiting)

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

type walkPool struct {
	walks []walks.Walk

	// walkIndexes maps each node to the list of indices in walks where that node appears.
	// That is, if walkIndexes[v] = [0, 2], then v is visited by walks[0] and walks[2].
	walkIndexes map[graph.ID][]int
}

func newWalkPool(walks []walks.Walk) *walkPool {
	walkIndexes := make(map[graph.ID][]int, len(walks)/100)

	// add the index of the walk to each node that it visits
	// excluding the last one which will be cropped out anyway.
	for i, walk := range walks {
		for j := range walk.Len() - 1 {
			node := walk.Path[j]
			walkIndexes[node] = append(walkIndexes[node], i)
		}
	}

	return &walkPool{
		walks:       walks,
		walkIndexes: walkIndexes,
	}
}

// Next returns a path of nodes that starts immediately after node, making sure
// that the same walk is only used once to avoid bias in the sampling.
// For example, if the walk is [0,1,2,3,4], node = 1, it returns [2,3,4].
func (w *walkPool) Next(node graph.ID) ([]graph.ID, bool) {
	indexes, exists := w.walkIndexes[node]
	if !exists || len(indexes) == 0 {
		return nil, false
	}

	for i, idx := range indexes {
		walk := w.walks[idx]
		cut := walk.Index(node)
		if cut == -1 {
			// walk already used, skip
			continue
		}

		// zero the walk so it can't be reused, and reslice the walk indexes
		// so we don't spend time looking at walks already used.
		w.walks[idx].Path = nil
		w.walkIndexes[node] = indexes[i+1:]
		return walk.Path[cut+1:], true
	}

	// all walks where already used
	delete(w.walkIndexes, node)
	return nil, false
}

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

			node := slicex.RandomElement(follows)
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
