package graph

type ID string

// Delta represent the changes a Node made to its follow list.
// It Removed some nodes, and Added some others.
// This means the old follow list is Removed + Common, while the new is Common + Added
type Delta struct {
	Node    ID
	Removed []ID
	Common  []ID
	Added   []ID
}

// Old returns the old state of the delta
func (d Delta) Old() []ID {
	return append(d.Common, d.Removed...)
}

// New returns the new state of the delta
func (d Delta) New() []ID {
	return append(d.Common, d.Added...)
}

// Inverse of the delta. If a delta and it's inverse are applied, the graph returns to its original state.
func (d Delta) Inverse() Delta {
	return Delta{
		Node:    d.Node,
		Common:  d.Common,
		Removed: d.Added,
		Added:   d.Removed,
	}
}
