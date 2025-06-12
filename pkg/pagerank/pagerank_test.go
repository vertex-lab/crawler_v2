package pagerank

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"testing"

	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/walks"
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

func randomID(n int) graph.ID {
	return graph.ID(strconv.Itoa(rand.IntN(n)))
}
