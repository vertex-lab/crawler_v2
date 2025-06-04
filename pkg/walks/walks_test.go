package walks

import (
	"context"
	"errors"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
	"math"
	"reflect"
	"strconv"
	"testing"
)

func TestGenerate(t *testing.T) {
	t.Run("cyclic stop", func(t *testing.T) {
		Alpha = 1 // so walks won't stop
		walker := NewCyclicWalker(3)
		expected := Walk{Path: []graph.ID{"0", "1", "2"}}

		walks, err := Generate(context.Background(), walker, "0")
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		for _, walk := range walks {
			if !reflect.DeepEqual(walk, expected) {
				t.Fatalf("expected walk %v, got %v", expected, walk)
			}
		}
	})

	t.Run("average lenght", func(t *testing.T) {
		maxError := 0.1
		Alpha = 0.85
		N = 10000

		walker := NewCyclicWalker(1000)
		expectedLenght := (1.0 / (1.0 - Alpha))

		walks, err := Generate(context.Background(), walker, "0")
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}

		sumLenght := 0
		for _, walk := range walks {
			sumLenght += len(walk.Path)
		}

		averageLenght := float64(sumLenght) / float64(N)
		if math.Abs(averageLenght-expectedLenght) > maxError {
			t.Fatalf("expected average lenght %f, got %f", expectedLenght, averageLenght)
		}
	})
}

func TestToRemove(t *testing.T) {
	N = 3
	tests := []struct {
		name     string
		walks    []Walk
		toRemove []Walk
		err      error
	}{
		{
			name: "no walks",
			err:  ErrInvalidRemoval,
		},
		{
			name:  "too few walks to remove",
			walks: []Walk{{Path: []graph.ID{"0", "1"}}},
			err:   ErrInvalidRemoval,
		},
		{
			name: "valid",
			walks: []Walk{
				{Path: []graph.ID{"0", "1"}},
				{Path: []graph.ID{"0", "2"}},
				{Path: []graph.ID{"0", "3"}},
				{Path: []graph.ID{"1", "0"}},
			},
			toRemove: []Walk{
				{Path: []graph.ID{"0", "1"}},
				{Path: []graph.ID{"0", "2"}},
				{Path: []graph.ID{"0", "3"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			toRemove, err := ToRemove("0", test.walks)
			if !errors.Is(err, test.err) {
				t.Fatalf("expected error %v, got %v", test.err, err)
			}

			if !reflect.DeepEqual(toRemove, test.toRemove) {
				t.Fatalf("expected walks to remove %v, got %v", test.toRemove, toRemove)
			}
		})
	}
}

func TestUpdateRemove(t *testing.T) {
	walker := NewSimpleWalker(
		map[graph.ID][]graph.ID{
			"0": {"3"},
			"1": {"2"},
			"2": {"0"},
			"3": {"2"},
		})

	delta := graph.Delta{
		Node:   "0",
		Remove: []graph.ID{"1"}, // the old follows were "1" and "3"
		Keep:   []graph.ID{"3"},
	}

	walks := []Walk{
		{ID: "0", Path: []graph.ID{"0", "1", "2"}}, // this is invalid
		{ID: "1", Path: []graph.ID{"0", "3", "2"}},
	}

	Alpha = 1 // avoid early stopping, which makes the test deterministic
	expected := []Walk{{ID: "0", Path: []graph.ID{"0", "3", "2"}}}

	old, new, err := ToUpdate(context.Background(), walker, delta, walks)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	if !reflect.DeepEqual(old, walks[:1]) {
		t.Errorf("expected old %v, got %v", walks[:1], old)
	}

	if !reflect.DeepEqual(new, expected) {
		t.Errorf("expected new %v, got %v", expected, new)
	}
}

func TestDivergence(t *testing.T) {
	tests := []struct {
		w1       Walk
		w2       Walk
		expected int
	}{
		{w1: Walk{Path: []graph.ID{"0"}}, w2: Walk{Path: []graph.ID{"0", "1"}}, expected: 1},
		{w1: Walk{Path: []graph.ID{"0", "1", "69"}}, w2: Walk{Path: []graph.ID{"0", "1"}}, expected: 2},
		{w1: Walk{Path: []graph.ID{"0", "1", "69"}}, w2: Walk{Path: []graph.ID{"0", "1", "420"}}, expected: 2},
		{w1: Walk{Path: []graph.ID{"a", "b", "c"}}, w2: Walk{Path: []graph.ID{"a", "b", "c"}}, expected: -1},
		{expected: -1},
	}

	for i, test := range tests {
		if div := Divergence(test.w1, test.w2); div != test.expected {
			t.Fatalf("test %d: expected %d, got %v", i, test.expected, div)
		}
	}
}

func TestFindCycle(t *testing.T) {
	tests := []struct {
		list     []graph.ID
		expected int
	}{
		{list: []graph.ID{"0", "1", "2", "3", "4", "5"}, expected: -1},
		{list: []graph.ID{"0", "1", "2", "3", "1", "5"}, expected: 4},
		{list: []graph.ID{"0", "1", "2", "3", "1", "0"}, expected: 4},
		{list: []graph.ID{"0", "1", "3", "3", "4", "5"}, expected: 3},
	}

	for _, test := range tests {
		if pos := findCycle(test.list); pos != test.expected {
			t.Fatalf("list %v; expected %d, got %d", test.list, test.expected, pos)
		}
	}
}

// func TestMemoryUsage(t *testing.T) {
// 	var before runtime.MemStats
// 	runtime.ReadMemStats(&before)

// 	follows := make(map[int][]int)
// 	for i := 0; i < 1000000; i++ {
// 		node := int(i)
// 		follows[node] = randomFollows(100)
// 	}

// 	var after runtime.MemStats
// 	runtime.ReadMemStats(&after)

// 	used := float64(after.Alloc-before.Alloc) / 1024 / 1024
// 	t.Fatalf("Approx. memory used by map: %.2f MB\n", used)
// }

func BenchmarkFindCycle(b *testing.B) {
	sizes := []int{10, 100, 1000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			path := make([]graph.ID, size)
			for i := range size {
				path[i] = graph.ID(strconv.Itoa(i))
			}

			b.ResetTimer()
			for range b.N {
				findCycle(path)
			}
		})
	}
}
