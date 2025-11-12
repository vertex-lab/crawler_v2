// The walks package is responsible for defining, generating, removing and updating random walks.
package walks

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"

	"github.com/pippellia-btc/slicex"
	"github.com/vertex-lab/crawler_v2/pkg/graph"
)

var (
	Alpha = 0.85 // the dampening factor
	N     = 100  // the walks per node

	ErrInvalidRemoval = fmt.Errorf("the walks to be removed are different than the expected number (%d)", N)
)

// ID represent how walks are identified in the storage layer
type ID string

func (id ID) MarshalBinary() ([]byte, error) { return []byte(id), nil }

// Walk is an ordered list of node IDs
type Walk struct {
	ID   ID
	Path []graph.ID
}

// Len returns the lenght of the walk
func (w Walk) Len() int {
	return len(w.Path)
}

// Visits returns whether the walk visits any of the nodes
func (w Walk) Visits(nodes ...graph.ID) bool {
	for _, node := range nodes {
		if slices.Contains(w.Path, node) {
			return true
		}
	}
	return false
}

// VisitsAt returns whether the walk visits any of the nodes at the specified step.
// If the step is outside the bouds of the walk, it returns false.
func (w Walk) VisitsAt(step int, nodes ...graph.ID) bool {
	if step < 0 || step >= w.Len() {
		return false
	}
	return slices.Contains(nodes, w.Path[step])
}

// Index returns the index of node in the walk, or -1 if not present
func (w Walk) Index(node graph.ID) int {
	return slices.Index(w.Path, node)
}

// Copy returns a deep copy of the walk
func (w Walk) Copy() Walk {
	path := make([]graph.ID, len(w.Path))
	copy(path, w.Path)
	return Walk{ID: w.ID, Path: path}
}

// Append some nodes to the end of the walk
func (w *Walk) Append(nodes ...graph.ID) {
	w.Path = append(w.Path, nodes...)
}

// Prune the walk at the specified index (excluded).
// It panics if the index is not within the bounds of the walk
func (w *Walk) Prune(cut int) {
	if cut < 0 || cut > len(w.Path) {
		panic("cut index must be within the bounds of the walk")
	}
	w.Path = w.Path[:cut]
}

// Graft the walk by appending a path, and removing cycles (if any)
func (w *Walk) Graft(path []graph.ID) {
	w.Path = append(w.Path, path...)
	pos := findCycle(w.Path)
	if pos == -1 {
		return
	}

	w.Path = w.Path[:pos]
}

// Divergence returns the first index where w1 and w2 are different, -1 if equal.
func Divergence(w1, w2 Walk) int {
	min := min(w1.Len(), w2.Len())
	for i := range min {
		if w1.Path[i] != w2.Path[i] {
			return i
		}
	}

	if w1.Len() == w2.Len() {
		// they are all equal, so no divergence
		return -1
	}
	return min
}

// Generate [N] random walks for the specified node, using dampening factor [Alpha].
// A walk stops early if a cycle is encountered.
// Walk IDs are not set, because it's the responsibility of the storage layer.
func Generate(ctx context.Context, walker Walker, nodes ...graph.ID) ([]Walk, error) {
	if len(nodes) == 0 {
		return nil, nil
	}

	walks := make([]Walk, 0, N*len(nodes))
	var path []graph.ID
	var err error

	for _, node := range nodes {
		for range N {
			path, err = generate(ctx, walker, node)
			if err != nil {
				return nil, fmt.Errorf("failed to generate walk: %w", err)
			}

			walks = append(walks, Walk{Path: path})
		}
	}

	return walks, nil
}

// Generate a random path of nodes, by:
// - starting from one of the provided nodes, chosen at random
// - walking along the social graph
// - stopping with probabiliy 1-Alpha, on dandling nodes, and on cycles
func generate(ctx context.Context, walker Walker, start ...graph.ID) ([]graph.ID, error) {
	if len(start) == 0 {
		return nil, nil
	}

	node := slicex.RandomElement(start)
	path := make([]graph.ID, 0, expectedLenght(Alpha))
	path = append(path, node)

	for {
		if rand.Float64() > Alpha {
			break
		}

		follows, err := walker.Follows(ctx, node)
		if err != nil {
			return nil, err
		}

		if len(follows) == 0 {
			// found a dandling node, stop
			break
		}

		node = slicex.RandomElement(follows)
		if slices.Contains(path, node) {
			// found a cycle, stop
			break
		}

		path = append(path, node)
	}

	return path, nil
}

// ToRemove returns the walks that need to be removed.
func ToRemove(node graph.ID, walks []Walk) ([]Walk, error) {
	toRemove := make([]Walk, 0, N)
	for _, walk := range walks {
		if walk.Index(node) == 0 {
			toRemove = append(toRemove, walk)
		}
	}

	if len(toRemove) != N {
		slog.Error("walks.ToRemove", "error", ErrInvalidRemoval, "to-remove", len(toRemove))
	}
	return toRemove, nil
}

// ToUpdate returns how the old walks need to be updated to reflect the changes in the graph.
// In particular, it corrects invalid steps and resamples in order to maintain the correct distribution.
func ToUpdate(ctx context.Context, walker Walker, delta graph.Delta, walks []Walk) (old, new []Walk, err error) {
	resampleProbability := resampleProbability(delta)
	old = make([]Walk, 0, expectedUpdates(walks, delta))
	new = make([]Walk, 0, expectedUpdates(walks, delta))

	for _, walk := range walks {
		pos := walk.Index(delta.Node)
		if pos == -1 {
			// the walk doesn't visit node, skip
			continue
		}

		resample := rand.Float64() < resampleProbability
		invalid := walk.VisitsAt(pos+1, delta.Remove...)

		switch {
		case resample:
			// prune and graft with the added nodes to avoid oversampling of common nodes
			updated := walk.Copy()
			updated.Prune(pos + 1)

			if rand.Float64() < Alpha {
				path, err := generate(ctx, walker, delta.Add...)
				if err != nil {
					return nil, nil, fmt.Errorf("ToUpdate: failed to generate new segment: %w", err)
				}

				updated.Graft(path)
			}

			old = append(old, walk)
			new = append(new, updated)

		case invalid:
			// prune and graft invalid steps with the common nodes
			updated := walk.Copy()
			updated.Prune(pos + 1)

			path, err := generate(ctx, walker, delta.Keep...)
			if err != nil {
				return nil, nil, fmt.Errorf("ToUpdate: failed to generate new segment: %w", err)
			}

			updated.Graft(path)
			old = append(old, walk)
			new = append(new, updated)
		}
	}

	return old, new, nil
}

// The resample probability that a walk needs to be changed to avoid an oversampling of common nodes.
// Consider the simple graph 0 -> 1; all the walks that continue from 0 will reach 1.
// Now imagine 0 added 2 and 3 to its successors;
// Our goal is to have 1/3 of the walks that continue go to each of 1, 2 and 3.
// This means we have to re-do 2/3 of the walks and make them continue towards 2 or 3.
func resampleProbability(delta graph.Delta) float64 {
	if len(delta.Add) == 0 {
		return 0
	}

	k := float64(len(delta.Keep))
	a := float64(len(delta.Add))
	return a / (a + k)
}

func expectedUpdates(walks []Walk, delta graph.Delta) int {
	if len(delta.Keep) == 0 {
		// no nodes have remained, all walks must be re-computed
		return len(walks)
	}

	r := float64(len(delta.Remove))
	k := float64(len(delta.Keep))
	a := float64(len(delta.Add))

	invalidP := Alpha * r / (r + k)
	resampleP := a / (a + k)
	updateP := invalidP + resampleP - invalidP*resampleP
	expected := float64(len(walks)) * updateP
	return int(expected + 0.5)
}

// Find the position of the first repetition in a slice. If there are no cycles, -1 is returned
func findCycle[S []K, K comparable](s S) int {
	seen := make(map[K]struct{})
	for i, e := range s {
		if _, ok := seen[e]; ok {
			return i
		}

		seen[e] = struct{}{}
	}

	return -1
}

func expectedLenght(alpha float64) int {
	switch {
	case alpha < 0 || alpha > 1:
		panic("alpha must be between 0 and 1 (excluded)")

	case alpha == 1:
		// this case should only happen in tests, so return a default value
		return 100

	default:
		return int(1.0/(1-alpha) + 0.5)
	}
}
