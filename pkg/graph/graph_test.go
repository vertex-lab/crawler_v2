package graph

import (
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
