package walks

import (
	"context"
	"errors"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
	"math/rand/v2"
	"reflect"
	"strconv"
	"testing"
)

var ctx = context.Background()

func TestFollows(t *testing.T) {
	tests := []struct {
		name     string
		node     graph.ID
		fallback Walker
		expected []graph.ID
		err      error
	}{
		{
			name: "node not found, no fallback",
			node: "69",
			err:  graph.ErrNodeNotFound,
		},
		{
			name:     "node not found, fallback",
			node:     "1",
			fallback: NewSimpleWalker(map[graph.ID][]graph.ID{"1": {"2"}}),
			expected: []graph.ID{"2"},
		},
		{
			name:     "node found",
			node:     "0",
			expected: []graph.ID{"1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			walker := NewWalker(
				WithCapacity(1),
				WithFallback(test.fallback),
			)
			walker.add(0, []uint32{1})

			follows, err := walker.Follows(ctx, test.node)
			if !errors.Is(err, test.err) {
				t.Fatalf("expected error %v, got %v", test.err, err)
			}

			if !reflect.DeepEqual(follows, test.expected) {
				t.Fatalf("expected follows %v, got %v", test.expected, follows)
			}

			if walker.Size() != 1 {
				t.Fatalf("failed to evict keys %d", walker.Size())
			}
		})
	}
}

func BenchmarkAdd(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			walker := NewWalker(
				WithCapacity(size),
			)

			for {
				// fill-up the cached walker outside the benchmark
				if walker.Size() >= size {
					break
				}

				node := rand.Uint32N(1000000)
				follows := randomCompactIDs(1000)
				walker.add(node, follows)
			}

			node := rand.Uint32N(1000000)
			follows := randomCompactIDs(1000)

			b.ResetTimer()
			for range b.N {
				walker.add(node, follows)
			}
		})
	}

	for range b.N {

	}
}

func BenchmarkCompactIDs(b *testing.B) {
	follows := randomFollows(1000)
	b.ResetTimer()

	for range b.N {
		IDs, err := compactIDs(follows)
		if err != nil {
			b.Fatalf("failed to convert to compact IDs: %v", err)
		}
		nodes(IDs)
	}
}

func randomFollows(size int) []graph.ID {
	follows := make([]graph.ID, size)
	for i := range size {
		node := rand.IntN(10000000)
		follows[i] = graph.ID(strconv.Itoa(node))
	}
	return follows
}

func randomCompactIDs(size int) []uint32 {
	follows := make([]uint32, size)
	for i := range size {
		follows[i] = rand.Uint32N(1000000)
	}
	return follows
}
