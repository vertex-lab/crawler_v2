package walks

import (
	"context"
	"github/pippellia-btc/crawler/pkg/graph"
	"strconv"
)

type Walker interface {
	// Follows returns the follow-list of the node, used  for generating random walks
	Follows(ctx context.Context, node graph.ID) ([]graph.ID, error)
}

type MapWalker struct {
	follows map[graph.ID][]graph.ID
}

func NewWalker(m map[graph.ID][]graph.ID) *MapWalker {
	return &MapWalker{follows: m}
}

func (m *MapWalker) Follows(ctx context.Context, node graph.ID) ([]graph.ID, error) {
	return m.follows[node], nil
}

func (m *MapWalker) Update(ctx context.Context, delta graph.Delta) {
	m.follows[delta.Node] = delta.New()
}

func NewCyclicWalker(n int) *MapWalker {
	follows := make(map[graph.ID][]graph.ID, n)
	for i := range n {
		node := graph.ID(strconv.Itoa(i))
		next := graph.ID(strconv.Itoa((i + 1) % n))
		follows[node] = []graph.ID{next}
	}

	return &MapWalker{follows: follows}
}

// CachedWalker is a walker with optional fallback that stores follow relationships
// in a compact format (uint32) for reduced memory footprint.
type cachedWalker struct {
	follows  map[graph.ID][]graph.ID
	fallback Walker
}

func NewCachedWalker(nodes []graph.ID, follows [][]graph.ID, fallback Walker) *cachedWalker {
	w := cachedWalker{
		follows:  make(map[graph.ID][]graph.ID, len(nodes)),
		fallback: fallback,
	}

	for i, node := range nodes {
		w.follows[node] = follows[i]
	}

	return &w
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
