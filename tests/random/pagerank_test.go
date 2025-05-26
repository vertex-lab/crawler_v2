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
			store := NewWalkStore()

			walks, err := walks.Generate(ctx, test.walker, test.nodes...)
			if err != nil {
				t.Fatalf("failed to generate the walks: %v", err)
			}
			store.AddWalks(walks)

			global, err := pagerank.Global(ctx, store, test.nodes...)
			if err != nil {
				t.Fatalf("expected nil, pr %v", err)
			}

			distance := Distance(global, test.global)
			if distance > expectedDistance {
				t.Errorf("expected distance %f, got %f\n", expectedDistance, distance)
				t.Errorf("expected ranks %v,\n got %v", test.global, global)
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
			store := NewWalkStore()

			// apply a random delta to the graph
			delta := randomElement(test.deltas)
			test.walker.Update(ctx, delta)

			rwalks, err := walks.Generate(ctx, test.walker, test.nodes...)
			if err != nil {
				t.Fatalf("failed to generate the walks: %v", err)
			}

			store.AddWalks(rwalks)
			rwalks = store.WalksVisiting(delta.Node, -1)

			// apply the opposite delta, returning to the original state
			inv := delta.Inverse()
			test.walker.Update(ctx, inv)

			toUpdate, err := walks.ToUpdate(ctx, test.walker, inv, rwalks)
			if err != nil {
				t.Fatalf("failed to update the walks: %v", err)
			}
			store.ReplaceWalks(toUpdate)

			global, err := pagerank.Global(ctx, store, test.nodes...)
			if err != nil {
				t.Fatalf("expected nil, pr %v", err)
			}

			distance := Distance(global, test.global)
			if distance > expectedDistance {
				t.Errorf("inverse delta %v; expected distance %f, got %f\n", inv, expectedDistance, distance)
				t.Errorf("expected ranks %v,\n got %v", test.global, global)
			}
		})
	}
}

func TestPersonalized(t *testing.T) {
	expectedDistance := 0.01
	targetLenght := 1000000
	walks.Alpha = 0.85

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
			// the number of walks should only make the algorithm faster,
			// without changing its precision. To test this we simply randomize it
			walks.N = rand.IntN(3000)

			ctx := context.Background()
			loader := NewMockLoader(test.walker)

			rwalks, err := walks.Generate(ctx, test.walker, test.nodes...)
			if err != nil {
				t.Fatalf("failed to generate the walks: %v", err)
			}
			loader.AddWalks(rwalks)

			personalized, err := pagerank.PersonalizedWithTargets(ctx, loader, "0", test.nodes, targetLenght)
			if err != nil {
				t.Fatalf("expected nil, pr %v", err)
			}

			distance := Distance(personalized, test.personalized)
			if distance > expectedDistance {
				t.Errorf("expected distance %f, got %f\n", expectedDistance, distance)
				t.Errorf("walks per node %d", walks.N)
				t.Errorf("expected ranks %v,\n got %v", test.personalized, personalized)
			}
		})
	}
}

// returns a random element of a slice. It panics if the slice is empty or nil.
func randomElement[S []E, E any](s S) E {
	return s[rand.IntN(len(s))]
}
