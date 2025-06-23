// The graph package defines the fundamental structures (e.g. [Node], [Delta])
// that are used across packages.
package graph

import (
	"errors"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/pippellia-btc/slicex"
)

const (
	// types of status
	StatusActive   string = "active" // meaning, we generate random walks for this node
	StatusInactive string = "inactive"

	// internal record kinds
	Addition  int = -3
	Promotion int = -2
	Demotion  int = -1
)

var (
	ErrNodeNotFound      = errors.New("node not found")
	ErrNodeAlreadyExists = errors.New("node already exists")
)

type ID string

func (id ID) MarshalBinary() ([]byte, error) { return []byte(id), nil }

// Node contains the metadata about a node, including a collection of Records.
type Node struct {
	ID      ID
	Pubkey  string
	Status  string // either [StatusActive] or [StatusInactive]
	Records []Record
}

// Record contains the timestamp of a node update.
type Record struct {
	Kind      int // either [Addition], [Promotion] or [Demotion]
	Timestamp time.Time
}

func (n *Node) Addition() (time.Time, bool)  { return n.find(Addition) }
func (n *Node) Promotion() (time.Time, bool) { return n.find(Promotion) }
func (n *Node) Demotion() (time.Time, bool)  { return n.find(Demotion) }

func (n *Node) find(kind int) (time.Time, bool) {
	for _, record := range n.Records {
		if record.Kind == kind {
			return record.Timestamp, true
		}
	}
	return time.Time{}, false
}

// Delta represents updates to apply to a Node.
// Add and Remove contain node IDs to add to or remove from the node’s relationships (e.g., follow list).
type Delta struct {
	Kind   int
	Node   ID
	Remove []ID
	Keep   []ID
	Add    []ID
}

// NewDelta returns a delta by computing the relationships to remove, keep and add.
// Time complexity O(n * logn + m * logm), where n and m are the lengths of the slices.
// This function is much faster than converting to sets for sizes (n, m) smaller than ~10^6.
func NewDelta(kind int, node ID, old, new []ID) Delta {
	delta := Delta{
		Kind: kind,
		Node: node,
	}

	delta.Remove, delta.Keep, delta.Add = slicex.Partition(old, new)
	return delta
}

// Size returns the number of relationships changed by delta
func (d Delta) Size() int {
	return len(d.Remove) + len(d.Add)
}

// Old returns the old state of the delta
func (d Delta) Old() []ID {
	return append(d.Keep, d.Remove...)
}

// New returns the new state of the delta
func (d Delta) New() []ID {
	return append(d.Keep, d.Add...)
}

// Inverse of the delta. If a delta and it's inverse are applied, the graph returns to its original state.
func (d Delta) Inverse() Delta {
	return Delta{
		Kind:   d.Kind,
		Node:   d.Node,
		Keep:   d.Keep,
		Remove: d.Add,
		Add:    d.Remove,
	}
}

// RandomIDs of the provided size.
func RandomIDs(size int) []ID {
	IDs := make([]ID, size)
	for i := range size {
		node := rand.IntN(10000000)
		IDs[i] = ID(strconv.Itoa(node))
	}
	return IDs
}
