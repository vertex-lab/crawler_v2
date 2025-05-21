package walks

import (
	"context"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
	"math/rand/v2"
	"slices"
)

var (
	Alpha = 0.85 // the dampening factor
	N     = 100  // the walks per node
)

// ID represent how walks are identified in the storage layer
type ID string

// Walk is an ordered list of node IDs
type Walk struct {
	ID   ID
	Path []graph.ID
	// Stop  int
}

type Walker interface {
	// Follows returns the follow-list of the specified node, which will be used in
	// generating random walks
	Follows(ctx context.Context, node graph.ID) ([]graph.ID, error)
}

// Len returns the lenght of the walk
func (w Walk) Len() int {
	return len(w.Path)
}

// Visits returns whether the walk visited node
func (w Walk) Visits(node graph.ID) bool {
	return slices.Contains(w.Path, node)
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

// Generate N random walks for the specified node, using dampening factor alpha.
// A walk stops early if a cycle is encountered. Walk IDs will be overwritten by the storage layer.
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
				return nil, fmt.Errorf("failed to Generate: %w", err)
			}

			walks = append(walks, Walk{Path: path})
		}
	}

	return walks, nil
}

// Generate a random path of nodes, by:
// - starting from one of the provided nodes, chosen at random
// - walking along the social graph
// - stopping with probabiliy 1-alpha, on dandling nodes, and on cycles
func generate(ctx context.Context, walker Walker, start ...graph.ID) ([]graph.ID, error) {
	if len(start) == 0 {
		return nil, nil
	}

	node := randomElement(start)
	path := make([]graph.ID, 0, averageLenght(Alpha))
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

		node = randomElement(follows)
		if slices.Contains(path, node) {
			// found a cycle, stop
			break
		}

		path = append(path, node)
	}

	return path, nil
}

// ToRemove returns the IDs of walks that needs to be removed.
// It returns an error if the number of walks to remove differs from the expected [N].
func ToRemove(node graph.ID, walks []Walk) ([]ID, error) {
	toRemove := make([]ID, 0, N)

	for _, walk := range walks {
		if walk.Index(node) != -1 {
			toRemove = append(toRemove, walk.ID)
		}
	}

	if len(toRemove) != N {
		return toRemove, fmt.Errorf("walks to be removed (%d) are less than expected (%d)", len(toRemove), N)
	}

	return toRemove, nil
}

func ToUpdate(ctx context.Context, walker Walker, delta graph.Delta, walks []Walk) ([]Walk, error) {
	toUpdate := make([]Walk, 0, expectedUpdates(walks, delta))
	resampleProbability := resampleProbability(delta)

	var pos int
	var isInvalid, shouldResample bool

	for _, walk := range walks {
		pos = walk.Index(delta.Node)
		if pos == -1 {
			// the walk doesn't visit node, skip
			continue
		}

		shouldResample = rand.Float64() < resampleProbability
		isInvalid = (pos < walk.Len()-1) && slices.Contains(delta.Removed, walk.Path[pos+1])

		switch {
		case shouldResample:
			// prune and graft with the added nodes to avoid oversampling of common nodes
			updated := walk.Copy()
			updated.Prune(pos + 1)

			if rand.Float64() < Alpha {
				new, err := generate(ctx, walker, delta.Added...)
				if err != nil {
					return nil, fmt.Errorf("ToUpdate: failed to generate new segment: %w", err)
				}

				updated.Graft(new)
			}

			toUpdate = append(toUpdate, updated)

		case isInvalid:
			// prune and graft invalid steps with the common nodes
			updated := walk.Copy()
			updated.Prune(pos + 1)

			new, err := generate(ctx, walker, delta.Common...)
			if err != nil {
				return nil, fmt.Errorf("ToUpdate: failed to generate new segment: %w", err)
			}

			updated.Graft(new)
			toUpdate = append(toUpdate, updated)
		}

	}

	return toUpdate, nil
}

// The resample probability that a walk needs to be changed to avoid an oversampling of common nodes.
// Consider the simple graph 0 -> 1; all the walks that continue from 0 will reach 1.
// Now imagine 0 added 2 and 3 to its successors;
// Our goal is to have 1/3 of the walks that continue go to each of 1, 2 and 3.
// This means we have to re-do 2/3 of the walks and make them continue towards 2 or 3.
func resampleProbability(delta graph.Delta) float64 {
	if len(delta.Added) == 0 {
		return 0
	}

	c := float64(len(delta.Common))
	a := float64(len(delta.Added))
	return a / (a + c)
}

func expectedUpdates(walks []Walk, delta graph.Delta) int {
	if len(delta.Common) == 0 {
		// no nodes have remained, all walks must be re-computed
		return len(walks)
	}

	r := float64(len(delta.Removed))
	c := float64(len(delta.Common))
	a := float64(len(delta.Added))

	invalidProbability := Alpha * r / (r + c)
	resampleProbability := a / (a + c)
	updateProbability := invalidProbability + resampleProbability - invalidProbability*resampleProbability
	expectedUpdates := float64(len(walks)) * updateProbability
	return int(expectedUpdates + 0.5)
}

// returns a random element of a slice. It panics if the slice is empty or nil.
func randomElement[S []E, E any](s S) E {
	return s[rand.IntN(len(s))]
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

func averageLenght(alpha float64) int {
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
