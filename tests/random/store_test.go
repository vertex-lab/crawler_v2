package random_test

import (
	"context"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/walks"
	"math"
	"strconv"
)

type WalkStore struct {
	nextID int
	Walks  map[walks.ID]walks.Walk
}

func NewWalkStore() *WalkStore {
	return &WalkStore{
		Walks: make(map[walks.ID]walks.Walk, 1000),
	}
}

// AddWalks adds walks with sequentials IDs
func (s *WalkStore) AddWalks(w []walks.Walk) {
	for _, walk := range w {
		ID := walks.ID(strconv.Itoa(s.nextID))
		s.nextID++

		walk.ID = ID
		s.Walks[ID] = walk
	}
}

// ReplaceWalks reassigns the ID --> walk
func (s *WalkStore) ReplaceWalks(w []walks.Walk) {
	for _, walk := range w {
		s.Walks[walk.ID] = walk
	}
}

func (s *WalkStore) WalksVisiting(node graph.ID) []walks.Walk {
	visiting := make([]walks.Walk, 0, walks.N)
	for _, walk := range s.Walks {
		if walk.Visits(node) {
			visiting = append(visiting, walk)
		}
	}

	return visiting
}

func (s *WalkStore) TotalVisits(ctx context.Context) (int, error) {
	total := 0
	for _, walk := range s.Walks {
		total += walk.Len()
	}
	return total, nil
}

func (s *WalkStore) Visits(ctx context.Context, nodes ...graph.ID) ([]int, error) {
	if len(nodes) == 0 {
		return nil, nil
	}

	count := make(map[graph.ID]int, len(nodes))
	for _, walk := range s.Walks {
		for _, node := range walk.Path {
			count[node]++
		}
	}

	visits := make([]int, len(nodes))
	for i, node := range nodes {
		visits[i] = count[node]
	}

	return visits, nil
}

// Distance returns the L1 distance between two lists of ranks.
func Distance(r1, r2 []float64) float64 {
	if len(r1) != len(r2) {
		return math.MaxFloat64
	}

	var dist float64 = 0
	for i := range r1 {
		dist += math.Abs(r1[i] - r2[i])
	}

	return dist
}
