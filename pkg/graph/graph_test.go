package graph

import (
	"fmt"
	"reflect"
	"testing"
)

func TestNewDelta(t *testing.T) {
	testCases := []struct {
		name     string
		old      []ID
		new      []ID
		expected Delta
	}{
		{
			name:     "nil slices",
			expected: Delta{Kind: 3, Node: "0", Remove: []ID{}, Keep: []ID{}, Add: []ID{}},
		},
		{
			name:     "empty slices",
			expected: Delta{Kind: 3, Node: "0", Remove: []ID{}, Keep: []ID{}, Add: []ID{}},
		},
		{
			name:     "only removals",
			old:      []ID{"0", "1", "2", "19", "111"},
			new:      []ID{"2", "19"},
			expected: Delta{Kind: 3, Node: "0", Remove: []ID{"0", "1", "111"}, Keep: []ID{"2", "19"}, Add: []ID{}},
		},
		{
			name:     "only additions",
			old:      []ID{"0", "1"},
			new:      []ID{"420", "0", "1", "69"},
			expected: Delta{Kind: 3, Node: "0", Remove: []ID{}, Keep: []ID{"0", "1"}, Add: []ID{"420", "69"}},
		},
		{
			name:     "both",
			old:      []ID{"0", "1", "111"},
			new:      []ID{"420", "0", "1", "69"},
			expected: Delta{Kind: 3, Node: "0", Remove: []ID{"111"}, Keep: []ID{"0", "1"}, Add: []ID{"420", "69"}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			delta := NewDelta(3, "0", test.old, test.new)
			if !reflect.DeepEqual(delta, test.expected) {
				t.Errorf("expected delta %v, got %v", test.expected, delta)
			}
		})
	}
}

func BenchmarkNewDelta(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			old := RandomIDs(size)
			new := RandomIDs(size)

			b.ResetTimer()
			for range b.N {
				NewDelta(3, "0", old, new)
			}
		})
	}
}

func BenchmarkNewDeltaSets(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			old := RandomIDs(size)
			new := RandomIDs(size)

			b.ResetTimer()
			for range b.N {
				newDeltaSet(3, "0", old, new)
			}
		})
	}
}

func newDeltaSet(kind int, node ID, old, new []ID) Delta {
	delta := Delta{
		Kind: kind,
		Node: node,
	}

	oldMap := make(map[ID]struct{}, len(old))
	newMap := make(map[ID]struct{}, len(new))

	// Fill maps
	for _, id := range old {
		oldMap[id] = struct{}{}
	}
	for _, id := range new {
		newMap[id] = struct{}{}
	}

	// Find removed and kept
	for _, id := range old {
		if _, found := newMap[id]; found {
			delta.Keep = append(delta.Keep, id)
		} else {
			delta.Remove = append(delta.Remove, id)
		}
	}

	// Find added
	for _, id := range new {
		if _, found := oldMap[id]; !found {
			delta.Add = append(delta.Add, id)
		}
	}

	return delta
}
