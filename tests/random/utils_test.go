package random_test

import (
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/walks"
	"math"
	"strconv"
)

type Setup struct {
	walker *walks.MapWalker
	deltas []graph.Delta

	nodes        []graph.ID
	global       []float64
	personalized []float64 // according to node "0"
}

// Distance returns the L1 distance between two lists of ranks.
func Distance(r1, r2 []float64) float64 {
	if len(r1) != len(r2) {
		return math.MaxFloat64
	}

	var dist float64 = 0
	for i := range r1 {
		dist += math.Abs(r1[i] - r2[i])
	}

	return dist
}

// Dandlings returns a setup consisting of n dandling nodes
func Dandlings(n int) Setup {
	nodes := make([]graph.ID, n)
	global := make([]float64, n)

	personalized := make([]float64, n)
	personalized[0] = 1

	add := make([]graph.ID, 0, n-1)
	deltas := make([]graph.Delta, 0, n-1)

	for i := range n {
		node := graph.ID(strconv.Itoa(i))
		nodes[i] = node
		global[i] = 1.0 / float64(n)

		if i > 0 {
			// all the possible deltas modulo graph isomorphism; 0 --> [1,2, ... k] for 1 <= k <= n
			add = append(add, node)
			deltas = append(deltas, graph.Delta{Node: "0", Add: add})
		}
	}

	return Setup{
		walker:       walks.NewWalker(make(map[graph.ID][]graph.ID)),
		deltas:       deltas,
		nodes:        nodes,
		global:       global,
		personalized: personalized,
	}
}

// Cyclic returns a setup consisting of a single cycle of n nodes.
func Cyclic(n int) Setup {
	mid := graph.ID(strconv.Itoa(n / 2))
	nodes := make([]graph.ID, n)
	global := make([]float64, n)
	personalized := make([]float64, n)
	a := walks.Alpha

	for i := range n {
		nodes[i] = graph.ID(strconv.Itoa(i))
		global[i] = 1.0 / float64(n)
		personalized[i] = math.Pow(a, float64(i)) * (1.0 - a) / (1.0 - math.Pow(a, float64(n)))
	}

	return Setup{
		walker: walks.NewCyclicWalker(n),
		deltas: []graph.Delta{
			{Node: "0", Remove: []graph.ID{"1"}},
			{Node: "0", Keep: []graph.ID{"1"}, Add: []graph.ID{mid}},
			{Node: "0", Remove: []graph.ID{"1"}, Add: []graph.ID{mid}},
		},

		nodes:        nodes,
		global:       global,
		personalized: personalized,
	}
}

var Triangle = Cyclic(3)

var Acyclic1 = Setup{
	walker: walks.NewWalker(map[graph.ID][]graph.ID{
		"0": {"1", "2"},
		"1": {},
		"2": {"3"},
		"3": {"1"},
		"4": {},
	}),
	deltas: []graph.Delta{
		// removals
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}},
		{Node: "0", Remove: []graph.ID{"2"}, Keep: []graph.ID{"1"}},
		{Node: "0", Remove: []graph.ID{"1", "2"}},
		{Node: "2", Remove: []graph.ID{"3"}},
		{Node: "3", Remove: []graph.ID{"1"}},
		// additions
		{Node: "0", Keep: []graph.ID{"1", "2"}, Add: []graph.ID{"3"}},
		{Node: "0", Keep: []graph.ID{"1", "2"}, Add: []graph.ID{"4"}},
		{Node: "0", Keep: []graph.ID{"1", "2"}, Add: []graph.ID{"3", "4"}},
		{Node: "4", Add: []graph.ID{"0"}},
		{Node: "4", Add: []graph.ID{"1"}},
		{Node: "4", Add: []graph.ID{"2"}},
		{Node: "4", Add: []graph.ID{"3"}},
		{Node: "4", Add: []graph.ID{"1", "2"}},
		{Node: "4", Add: []graph.ID{"2", "3"}},
		{Node: "4", Add: []graph.ID{"3", "4"}},
		{Node: "4", Add: []graph.ID{"0", "1", "2"}},
		{Node: "4", Add: []graph.ID{"0", "1", "2", "3"}},
		// removals and additions
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}, Add: []graph.ID{"4"}},
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}, Add: []graph.ID{"3"}},
		{Node: "0", Remove: []graph.ID{"1", "2"}, Add: []graph.ID{"3"}},
		{Node: "0", Remove: []graph.ID{"1", "2"}, Add: []graph.ID{"4"}},
		{Node: "0", Remove: []graph.ID{"1", "2"}, Add: []graph.ID{"3", "4"}},
		{Node: "2", Remove: []graph.ID{"3"}, Add: []graph.ID{"1"}},
		{Node: "2", Remove: []graph.ID{"3"}, Add: []graph.ID{"4"}},
		{Node: "2", Remove: []graph.ID{"3"}, Add: []graph.ID{"1", "4"}},
	},
	nodes:        []graph.ID{"0", "1", "2", "3", "4"},
	global:       []float64{0.11185, 0.36950, 0.15943, 0.24736, 0.11185},
	personalized: []float64{0.39709, 0.29070, 0.16876, 0.14345, 0.0},
}

var Acyclic2 = Setup{
	walker: walks.NewWalker(map[graph.ID][]graph.ID{
		"0": {"1", "2"},
		"1": {},
		"2": {},
		"3": {},
		"4": {"3", "5"},
		"5": {},
	}),
	deltas: []graph.Delta{
		// removals
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}},
		{Node: "0", Remove: []graph.ID{"1", "2"}},
		// additions
		{Node: "0", Keep: []graph.ID{"1", "2"}, Add: []graph.ID{"3"}},
		{Node: "0", Keep: []graph.ID{"1", "2"}, Add: []graph.ID{"4"}},
		{Node: "0", Keep: []graph.ID{"1", "2"}, Add: []graph.ID{"3", "4"}},
		{Node: "0", Keep: []graph.ID{"1", "2"}, Add: []graph.ID{"3", "5"}},
		{Node: "0", Keep: []graph.ID{"1", "2"}, Add: []graph.ID{"3", "4", "5"}},
		// removals and additions
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}, Add: []graph.ID{"3"}},
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}, Add: []graph.ID{"4"}},
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}, Add: []graph.ID{"3", "4"}},
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}, Add: []graph.ID{"3", "5"}},
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}, Add: []graph.ID{"3", "4", "5"}},
	},
	nodes:        []graph.ID{"0", "1", "2", "3", "4", "5"},
	global:       []float64{0.12987, 0.18506, 0.18506, 0.18506, 0.12987, 0.18506},
	personalized: []float64{0.54054, 0.22973, 0.22973, 0.0, 0.0, 0.0},
}

var Acyclic3 = Setup{
	walker: walks.NewWalker(map[graph.ID][]graph.ID{
		"0": {"1", "2"},
		"1": {},
		"2": {},
		"3": {"1", "2"},
	}),
	deltas: []graph.Delta{
		// removals
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}},
		{Node: "0", Remove: []graph.ID{"1", "2"}},
		// additions
		{Node: "0", Keep: []graph.ID{"1", "2"}, Add: []graph.ID{"3"}},
		{Node: "2", Add: []graph.ID{"1"}},
		// removals and additions
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}, Add: []graph.ID{"3"}},
		{Node: "0", Remove: []graph.ID{"1", "2"}, Add: []graph.ID{"3"}},
	},
	nodes:        []graph.ID{"0", "1", "2", "3"},
	global:       []float64{0.17544, 0.32456, 0.32456, 0.17544},
	personalized: []float64{0.54054, 0.22973, 0.22973, 0.0},
}

var Acyclic4 = Setup{
	walker: walks.NewWalker(map[graph.ID][]graph.ID{
		"0": {"1", "2"},
		"1": {},
		"2": {},
		"3": {"1"},
	}),
	deltas: []graph.Delta{
		// removals
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}},
		{Node: "0", Remove: []graph.ID{"1", "2"}},
		{Node: "3", Remove: []graph.ID{"1"}},
		// additions
		{Node: "0", Keep: []graph.ID{"1", "2"}, Add: []graph.ID{"3"}},
		{Node: "2", Add: []graph.ID{"1"}},
		{Node: "2", Add: []graph.ID{"3"}},
		{Node: "3", Keep: []graph.ID{"1"}, Add: []graph.ID{"0"}},
		// removals and additions
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2"}, Add: []graph.ID{"3"}},
		{Node: "0", Remove: []graph.ID{"1", "2"}, Add: []graph.ID{"3"}},
		{Node: "3", Remove: []graph.ID{"1"}, Add: []graph.ID{"0"}},
		{Node: "3", Remove: []graph.ID{"1"}, Add: []graph.ID{"0", "2"}},
	},
	nodes:        []graph.ID{"0", "1", "2", "3"},
	global:       []float64{0.17544, 0.39912, 0.25, 0.17544},
	personalized: []float64{0.54054, 0.22973, 0.22973, 0.0},
}

var Acyclic5 = Setup{
	walker: walks.NewWalker(map[graph.ID][]graph.ID{
		"0": {"3"},
		"1": {"0"},
		"2": {},
		"3": {"2"},
	}),
	deltas: []graph.Delta{
		// removals
		{Node: "0", Remove: []graph.ID{"3"}},
		{Node: "1", Remove: []graph.ID{"0"}},
		{Node: "3", Remove: []graph.ID{"2"}},
		// additions
		{Node: "0", Keep: []graph.ID{"3"}, Add: []graph.ID{"2"}},
		{Node: "1", Keep: []graph.ID{"0"}, Add: []graph.ID{"2"}},
		{Node: "1", Keep: []graph.ID{"0"}, Add: []graph.ID{"3"}},
		{Node: "1", Keep: []graph.ID{"0"}, Add: []graph.ID{"2", "3"}},
		// removals and additions
		{Node: "0", Remove: []graph.ID{"3"}, Add: []graph.ID{"2"}},
		{Node: "1", Remove: []graph.ID{"0"}, Add: []graph.ID{"2"}},
		{Node: "1", Remove: []graph.ID{"0"}, Add: []graph.ID{"3"}},
		{Node: "1", Remove: []graph.ID{"0"}, Add: []graph.ID{"2", "3"}},
	},
	nodes:        []graph.ID{"0", "1", "2", "3"},
	global:       []float64{0.21489, 0.11616, 0.37015, 0.29881},
	personalized: []float64{0.38873, 0.0, 0.28085, 0.33042},
}

var Acyclic6 = Setup{
	walker: walks.NewWalker(map[graph.ID][]graph.ID{
		"0": {"4"},
		"1": {"0"},
		"2": {},
		"3": {"1", "4"},
		"4": {"2"},
	}),
	deltas: []graph.Delta{
		// removals
		{Node: "0", Remove: []graph.ID{"4"}},
		{Node: "1", Remove: []graph.ID{"0"}},
		{Node: "3", Remove: []graph.ID{"1"}, Keep: []graph.ID{"4"}},
		{Node: "3", Remove: []graph.ID{"4"}, Keep: []graph.ID{"1"}},
		{Node: "3", Remove: []graph.ID{"1", "4"}},
		{Node: "4", Remove: []graph.ID{"2"}},
		// additions
		{Node: "0", Keep: []graph.ID{"4"}, Add: []graph.ID{"2"}},
		{Node: "1", Keep: []graph.ID{"0"}, Add: []graph.ID{"2"}},
		{Node: "1", Keep: []graph.ID{"0"}, Add: []graph.ID{"4"}},
		{Node: "1", Keep: []graph.ID{"0"}, Add: []graph.ID{"2", "4"}},
		{Node: "3", Keep: []graph.ID{"1", "4"}, Add: []graph.ID{"0"}},
		{Node: "3", Keep: []graph.ID{"1", "4"}, Add: []graph.ID{"2"}},
		{Node: "3", Keep: []graph.ID{"1", "4"}, Add: []graph.ID{"0", "2"}},
		// removals and additions
		{Node: "0", Remove: []graph.ID{"4"}, Add: []graph.ID{"2"}},
		{Node: "1", Remove: []graph.ID{"0"}, Add: []graph.ID{"2"}},
		{Node: "1", Remove: []graph.ID{"0"}, Add: []graph.ID{"4"}},
		{Node: "1", Remove: []graph.ID{"0"}, Add: []graph.ID{"2", "4"}},
		{Node: "3", Remove: []graph.ID{"1"}, Keep: []graph.ID{"4"}, Add: []graph.ID{"0"}},
		{Node: "3", Remove: []graph.ID{"1"}, Keep: []graph.ID{"4"}, Add: []graph.ID{"2"}},
		{Node: "3", Remove: []graph.ID{"1"}, Keep: []graph.ID{"4"}, Add: []graph.ID{"0", "2"}},
		{Node: "3", Remove: []graph.ID{"4"}, Keep: []graph.ID{"1"}, Add: []graph.ID{"0"}},
		{Node: "3", Remove: []graph.ID{"4"}, Keep: []graph.ID{"1"}, Add: []graph.ID{"2"}},
		{Node: "3", Remove: []graph.ID{"4"}, Keep: []graph.ID{"1"}, Add: []graph.ID{"0", "2"}},
		{Node: "3", Remove: []graph.ID{"1", "4"}, Add: []graph.ID{"0"}},
		{Node: "3", Remove: []graph.ID{"1", "4"}, Add: []graph.ID{"2"}},
		{Node: "3", Remove: []graph.ID{"1", "4"}, Add: []graph.ID{"0", "2"}},
	},
	nodes:        []graph.ID{"0", "1", "2", "3", "4"},
	global:       []float64{0.18820, 0.12128, 0.32417, 0.08511, 0.28125},
	personalized: []float64{0.38873, 0.0, 0.28086, 0.0, 0.33042},
}

var Acyclic7 = Setup{
	walker: walks.NewWalker(map[graph.ID][]graph.ID{
		"0": {"1", "2", "3"},
		"1": {},
		"2": {},
		"3": {},
		"4": {"0", "1", "2", "3"},
	}),
	deltas: []graph.Delta{
		// removals
		{Node: "0", Remove: []graph.ID{"1"}, Keep: []graph.ID{"2", "3"}},
		{Node: "0", Remove: []graph.ID{"1", "2"}, Keep: []graph.ID{"3"}},
		{Node: "0", Remove: []graph.ID{"1", "2", "3"}},
		{Node: "4", Remove: []graph.ID{"0"}, Keep: []graph.ID{"1", "2", "3"}},
		{Node: "4", Remove: []graph.ID{"1"}, Keep: []graph.ID{"0", "2", "3"}},
		{Node: "4", Remove: []graph.ID{"1", "2"}, Keep: []graph.ID{"0", "3"}},
		{Node: "4", Remove: []graph.ID{"1", "2", "3"}, Keep: []graph.ID{"0"}},
		{Node: "4", Remove: []graph.ID{"0", "1", "2", "3"}},
		// additions
		{Node: "1", Add: []graph.ID{"2"}},
		{Node: "1", Add: []graph.ID{"2", "3"}},
	},
	nodes:        []graph.ID{"0", "1", "2", "3", "4"},
	global:       []float64{0.17622, 0.22615, 0.22615, 0.22615, 0.14534},
	personalized: []float64{0.54054, 0.15315, 0.15315, 0.15315, 0.0},
}
