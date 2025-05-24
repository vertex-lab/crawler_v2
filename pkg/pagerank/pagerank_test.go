package pagerank

import (
	"context"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/walks"
	"math/rand/v2"
	"reflect"
	"strconv"
	"testing"
)

func TestPersonalized(t *testing.T) {
	ctx := context.Background()
	walks.Alpha = 1 // making the test deterministic

	walker := walks.NewCyclicWalker(3)
	pool := newWalkPool([]walks.Walk{
		{Path: []graph.ID{"0", "1", "X"}},
		{Path: []graph.ID{"0", "1", "Y"}},
	})

	expected := []graph.ID{
		"0", "1", "X",
		"0", "1", "Y",
		"0", "1", "2",
		"0", "1", "2",
		"0", "1", "2"}

	walk, err := personalizedWalk(ctx, walker, pool, "0", 13)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	if !reflect.DeepEqual(walk, expected) {
		t.Fatalf("expected %v, got %v", expected, walk)
	}
}

func BenchmarkFrequencyMap(b *testing.B) {
	sizes := []int{10000, 100000, 1000000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {

			path := make([]graph.ID, size)
			for i := range sizes {
				n := rand.IntN(size / 10)
				path[i] = graph.ID(strconv.Itoa(n))
			}

			b.ResetTimer()
			for range b.N {
				frequencyMap(path)
			}
		})
	}
}

func BenchmarkTargetFrequency(b *testing.B) {
	targets := make([]graph.ID, 10)
	for i := range 10 {
		targets[i] = randomID(1000)
	}

	sizes := []int{10000, 100000, 1000000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {

			path := make([]graph.ID, size)
			for i := range sizes {
				path[i] = randomID(size / 10)
			}

			b.ResetTimer()
			for range b.N {
				targetFrequency(targets, path)
			}
		})
	}
}

func randomID(n int) graph.ID {
	return graph.ID(strconv.Itoa(rand.IntN(n)))
}
