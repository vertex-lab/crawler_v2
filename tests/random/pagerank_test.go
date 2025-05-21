package random_test

import (
	"context"
	"github/pippellia-btc/crawler/pkg/pagerank"
	"github/pippellia-btc/crawler/pkg/walks"
	"math/rand/v2"
	"testing"
)

func TestPagerankStatic(t *testing.T) {
	expectedDistance := 0.01
	walks.Alpha = 0.85
	walks.N = 5000

	tests := []struct {
		name string
		Setup
	}{
		{name: "all dandling nodes", Setup: Dandlings(11)},
		{name: "triangle graph", Setup: Triangle},
		{name: "long cycle", Setup: Cyclic(30)},
		{name: "acyclic graph 1", Setup: Acyclic1},
		{name: "acyclic graph 2", Setup: Acyclic2},
		{name: "acyclic graph 3", Setup: Acyclic3},
		{name: "acyclic graph 4", Setup: Acyclic4},
		{name: "acyclic graph 5", Setup: Acyclic5},
		{name: "acyclic graph 6", Setup: Acyclic6},
		{name: "acyclic graph 7", Setup: Acyclic7},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := pagerank.NewWalkStore()

			walks, err := walks.Generate(ctx, test.walker, test.nodes...)
			if err != nil {
				t.Fatalf("failed to generate the walks: %v", err)
			}
			store.AddWalks(walks)

			ranks, err := pagerank.Global(ctx, store, test.nodes...)
			if err != nil {
				t.Fatalf("expected nil, pr %v", err)
			}

			distance := pagerank.Distance(ranks, test.ranks)
			if distance > expectedDistance {
				t.Errorf("expected distance %f, got %f\n", expectedDistance, distance)
				t.Errorf("expected ranks %v, got %v", test.ranks, ranks)
			}
		})
	}
}

/*
It is a known phenomenon that [walks.ToUpdate] does not return correct results
when the probability of cycles involving node --> removed is high.

Therefore, we only test with acyclic graphs, or graphs large enough that the
probability of such cycles is very low.
*/
func TestPagerankDynamic(t *testing.T) {
	expectedDistance := 0.01
	walks.Alpha = 0.85
	walks.N = 5000

	tests := []struct {
		name string
		Setup
	}{
		{name: "all dandling nodes", Setup: Dandlings(11)},
		{name: "long cycle", Setup: Cyclic(50)},
		{name: "acyclic graph 1", Setup: Acyclic1},
		{name: "acyclic graph 2", Setup: Acyclic2},
		{name: "acyclic graph 3", Setup: Acyclic3},
		{name: "acyclic graph 4", Setup: Acyclic4},
		{name: "acyclic graph 5", Setup: Acyclic5},
		{name: "acyclic graph 6", Setup: Acyclic6},
		{name: "acyclic graph 7", Setup: Acyclic7},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := pagerank.NewWalkStore()

			// apply a random delta to the graph
			delta := randomElement(test.deltas)
			test.walker.Update(ctx, delta)

			rwalks, err := walks.Generate(ctx, test.walker, test.nodes...)
			if err != nil {
				t.Fatalf("failed to generate the walks: %v", err)
			}

			store.AddWalks(rwalks)
			rwalks = store.WalksVisiting(delta.Node)

			// apply the opposite delta, returning to the original state
			inv := delta.Inverse()
			test.walker.Update(ctx, inv)

			toUpdate, err := walks.ToUpdate(ctx, test.walker, inv, rwalks)
			if err != nil {
				t.Fatalf("failed to update the walks: %v", err)
			}
			store.ReplaceWalks(toUpdate)

			ranks, err := pagerank.Global(ctx, store, test.nodes...)
			if err != nil {
				t.Fatalf("expected nil, pr %v", err)
			}

			distance := pagerank.Distance(ranks, test.ranks)
			if distance > expectedDistance {
				t.Errorf("inverse delta %v; expected distance %f, got %f\n", inv, expectedDistance, distance)
				t.Errorf("expected ranks %v,\n got %v", test.ranks, ranks)
			}
		})
	}
}

// returns a random element of a slice. It panics if the slice is empty or nil.
func randomElement[S []E, E any](s S) E {
	return s[rand.IntN(len(s))]
}
