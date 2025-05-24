package pagerank

import (
	"context"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/walks"
)

type cachedWalker struct {
	follows  map[graph.ID][]graph.ID
	fallback walks.Walker
}

func newCachedWalker(followsMap map[graph.ID][]graph.ID, fallback walks.Walker) *cachedWalker {
	return &cachedWalker{
		follows:  followsMap,
		fallback: fallback,
	}
}

func (w *cachedWalker) Follows(ctx context.Context, node graph.ID) ([]graph.ID, error) {
	follows, exists := w.follows[node]
	if !exists {
		var err error
		follows, err = w.fallback.Follows(ctx, node)
		if err != nil {
			return nil, err
		}

		w.follows[node] = follows
	}

	return follows, nil
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
