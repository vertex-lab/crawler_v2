package walks

import (
	"context"
	"github/pippellia-btc/crawler/pkg/graph"
	"strconv"
)

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
