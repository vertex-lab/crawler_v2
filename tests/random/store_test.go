package random_test

import (
	"context"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/walks"
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

func (s *WalkStore) WalksVisiting(node graph.ID, limit int) []walks.Walk {
	if limit == -1 {
		limit = 1000000
	}

	visiting := make([]walks.Walk, 0, walks.N)
	for _, walk := range s.Walks {
		if len(visiting) >= limit {
			break
		}

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

type mockLoader struct {
	walker walks.Walker
	store  *WalkStore
}

func NewMockLoader(walker walks.Walker) *mockLoader {
	return &mockLoader{
		walker: walker,
		store:  NewWalkStore(),
	}
}

func (l *mockLoader) Follows(ctx context.Context, node graph.ID) ([]graph.ID, error) {
	return l.walker.Follows(ctx, node)
}

func (l *mockLoader) BulkFollows(ctx context.Context, nodes []graph.ID) ([][]graph.ID, error) {
	var err error
	follows := make([][]graph.ID, len(nodes))

	for i, node := range nodes {
		follows[i], err = l.walker.Follows(ctx, node)
		if err != nil {
			return nil, err
		}
	}

	return follows, nil
}

func (l *mockLoader) AddWalks(w []walks.Walk) {
	l.store.AddWalks(w)
}

func (l *mockLoader) WalksVisitingAny(ctx context.Context, nodes []graph.ID, limit int) ([]walks.Walk, error) {
	if len(nodes) == 0 {
		return nil, nil
	}

	if limit == -1 {
		limit = 1000000
	}

	limitPerNode := limit / len(nodes)
	if limitPerNode <= 0 {
		return nil, nil
	}

	visiting := make([]walks.Walk, 0, limit)
	for _, node := range nodes {
		visiting = append(visiting, l.store.WalksVisiting(node, limitPerNode)...)
	}

	return visiting, nil
}
