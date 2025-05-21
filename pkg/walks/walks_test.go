package walks

import (
	"context"
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

func TestUpdateRemove(t *testing.T) {
	walker := NewWalker(map[graph.ID][]graph.ID{
		"0": {"3"},
		"1": {"2"},
		"2": {"0"},
		"3": {"2"},
	})

	delta := graph.Delta{
		Node:    "0",
		Removed: []graph.ID{"1"}, // the old follows were "1" and "3"
		Common:  []graph.ID{"3"},
	}

	walks := []Walk{
		{ID: "0", Path: []graph.ID{"0", "1", "2"}}, // this is invalid
		{ID: "1", Path: []graph.ID{"0", "3", "2"}},
	}

	Alpha = 1 // avoid early stopping, which makes the test deterministic
	expected := []Walk{{ID: "0", Path: []graph.ID{"0", "3", "2"}}}

	toUpdate, err := ToUpdate(context.Background(), walker, delta, walks)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	if !reflect.DeepEqual(toUpdate, expected) {
		t.Errorf("expected %v, got %v", expected, toUpdate)
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
