package random_test

import (
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/walks"
	"strconv"
)

type Setup struct {
	walker *walks.MapWalker
	nodes  []graph.ID
	ranks  []float64
	deltas []graph.Delta
}

// Dandlings returns a setup consisting of n dandling nodes
func Dandlings(n int) Setup {
	nodes := make([]graph.ID, n)
	ranks := make([]float64, n)

	added := make([]graph.ID, 0, n-1)
	deltas := make([]graph.Delta, 0, n-1)

	for i := range n {
		node := graph.ID(strconv.Itoa(i))
		nodes[i] = node
		ranks[i] = 1.0 / float64(n)

		if i > 0 {
			// all the possible deltas modulo graph isomorphism; 0 --> [1,2, ... k] for 1 <= k <= n
			added = append(added, node)
			deltas = append(deltas, graph.Delta{Node: "0", Added: added})
		}
	}

	return Setup{
		walker: walks.NewWalker(make(map[graph.ID][]graph.ID)),
		nodes:  nodes,
		ranks:  ranks,
		deltas: deltas,
	}
}

// Cyclic returns a setup consisting of a single cycle of n nodes.
func Cyclic(n int) Setup {
	mid := graph.ID(strconv.Itoa(n / 2))
	nodes := make([]graph.ID, n)
	ranks := make([]float64, n)

	for i := range n {
		nodes[i] = graph.ID(strconv.Itoa(i))
		ranks[i] = 1.0 / float64(n)
	}

	return Setup{
		walker: walks.NewCyclicWalker(n),
		nodes:  nodes,
		ranks:  ranks,
		deltas: []graph.Delta{
			{Node: "0", Removed: []graph.ID{"1"}},
			{Node: "0", Common: []graph.ID{"1"}, Added: []graph.ID{mid}},
			{Node: "0", Removed: []graph.ID{"1"}, Added: []graph.ID{mid}},
		},
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
	nodes: []graph.ID{"0", "1", "2", "3", "4"},
	ranks: []float64{0.11185, 0.36950, 0.15943, 0.24736, 0.11185},
	deltas: []graph.Delta{
		// removals
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}},
		{Node: "0", Removed: []graph.ID{"2"}, Common: []graph.ID{"1"}},
		{Node: "0", Removed: []graph.ID{"1", "2"}},
		{Node: "2", Removed: []graph.ID{"3"}},
		{Node: "3", Removed: []graph.ID{"1"}},
		// additions
		{Node: "0", Common: []graph.ID{"1", "2"}, Added: []graph.ID{"3"}},
		{Node: "0", Common: []graph.ID{"1", "2"}, Added: []graph.ID{"4"}},
		{Node: "0", Common: []graph.ID{"1", "2"}, Added: []graph.ID{"3", "4"}},
		{Node: "4", Added: []graph.ID{"0"}},
		{Node: "4", Added: []graph.ID{"1"}},
		{Node: "4", Added: []graph.ID{"2"}},
		{Node: "4", Added: []graph.ID{"3"}},
		{Node: "4", Added: []graph.ID{"1", "2"}},
		{Node: "4", Added: []graph.ID{"2", "3"}},
		{Node: "4", Added: []graph.ID{"3", "4"}},
		{Node: "4", Added: []graph.ID{"0", "1", "2"}},
		{Node: "4", Added: []graph.ID{"0", "1", "2", "3"}},
		// removals and additions
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}, Added: []graph.ID{"4"}},
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}, Added: []graph.ID{"3"}},
		{Node: "0", Removed: []graph.ID{"1", "2"}, Added: []graph.ID{"3"}},
		{Node: "0", Removed: []graph.ID{"1", "2"}, Added: []graph.ID{"4"}},
		{Node: "0", Removed: []graph.ID{"1", "2"}, Added: []graph.ID{"3", "4"}},
		{Node: "2", Removed: []graph.ID{"3"}, Added: []graph.ID{"1"}},
		{Node: "2", Removed: []graph.ID{"3"}, Added: []graph.ID{"4"}},
		{Node: "2", Removed: []graph.ID{"3"}, Added: []graph.ID{"1", "4"}},
	},
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
	nodes: []graph.ID{"0", "1", "2", "3", "4", "5"},
	ranks: []float64{0.12987, 0.18506, 0.18506, 0.18506, 0.12987, 0.18506},
	deltas: []graph.Delta{
		// removals
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}},
		{Node: "0", Removed: []graph.ID{"1", "2"}},
		// additions
		{Node: "0", Common: []graph.ID{"1", "2"}, Added: []graph.ID{"3"}},
		{Node: "0", Common: []graph.ID{"1", "2"}, Added: []graph.ID{"4"}},
		{Node: "0", Common: []graph.ID{"1", "2"}, Added: []graph.ID{"3", "4"}},
		{Node: "0", Common: []graph.ID{"1", "2"}, Added: []graph.ID{"3", "5"}},
		{Node: "0", Common: []graph.ID{"1", "2"}, Added: []graph.ID{"3", "4", "5"}},
		// removals and additions
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}, Added: []graph.ID{"3"}},
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}, Added: []graph.ID{"4"}},
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}, Added: []graph.ID{"3", "4"}},
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}, Added: []graph.ID{"3", "5"}},
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}, Added: []graph.ID{"3", "4", "5"}},
	},
}

var Acyclic3 = Setup{
	walker: walks.NewWalker(map[graph.ID][]graph.ID{
		"0": {"1", "2"},
		"1": {},
		"2": {},
		"3": {"1", "2"},
	}),
	nodes: []graph.ID{"0", "1", "2", "3"},
	ranks: []float64{0.17544, 0.32456, 0.32456, 0.17544},
	deltas: []graph.Delta{
		// removals
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}},
		{Node: "0", Removed: []graph.ID{"1", "2"}},
		// additions
		{Node: "0", Common: []graph.ID{"1", "2"}, Added: []graph.ID{"3"}},
		{Node: "2", Added: []graph.ID{"1"}},
		// removals and additions
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}, Added: []graph.ID{"3"}},
		{Node: "0", Removed: []graph.ID{"1", "2"}, Added: []graph.ID{"3"}},
	},
}

var Acyclic4 = Setup{
	walker: walks.NewWalker(map[graph.ID][]graph.ID{
		"0": {"1", "2"},
		"1": {},
		"2": {},
		"3": {"1"},
	}),
	nodes: []graph.ID{"0", "1", "2", "3"},
	ranks: []float64{0.17544, 0.39912, 0.25, 0.17544},
	deltas: []graph.Delta{
		// removals
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}},
		{Node: "0", Removed: []graph.ID{"1", "2"}},
		{Node: "3", Removed: []graph.ID{"1"}},
		// additions
		{Node: "0", Common: []graph.ID{"1", "2"}, Added: []graph.ID{"3"}},
		{Node: "2", Added: []graph.ID{"1"}},
		{Node: "2", Added: []graph.ID{"3"}},
		{Node: "3", Common: []graph.ID{"1"}, Added: []graph.ID{"0"}},
		// removals and additions
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2"}, Added: []graph.ID{"3"}},
		{Node: "0", Removed: []graph.ID{"1", "2"}, Added: []graph.ID{"3"}},
		{Node: "3", Removed: []graph.ID{"1"}, Added: []graph.ID{"0"}},
		{Node: "3", Removed: []graph.ID{"1"}, Added: []graph.ID{"0", "2"}},
	},
}

var Acyclic5 = Setup{
	walker: walks.NewWalker(map[graph.ID][]graph.ID{
		"0": {"3"},
		"1": {"0"},
		"2": {},
		"3": {"2"},
	}),
	nodes: []graph.ID{"0", "1", "2", "3"},
	ranks: []float64{0.21489, 0.11616, 0.37015, 0.29881},
	deltas: []graph.Delta{
		// removals
		{Node: "0", Removed: []graph.ID{"3"}},
		{Node: "1", Removed: []graph.ID{"0"}},
		{Node: "3", Removed: []graph.ID{"2"}},
		// additions
		{Node: "0", Common: []graph.ID{"3"}, Added: []graph.ID{"2"}},
		{Node: "1", Common: []graph.ID{"0"}, Added: []graph.ID{"2"}},
		{Node: "1", Common: []graph.ID{"0"}, Added: []graph.ID{"3"}},
		{Node: "1", Common: []graph.ID{"0"}, Added: []graph.ID{"2", "3"}},
		// removals and additions
		{Node: "0", Removed: []graph.ID{"3"}, Added: []graph.ID{"2"}},
		{Node: "1", Removed: []graph.ID{"0"}, Added: []graph.ID{"2"}},
		{Node: "1", Removed: []graph.ID{"0"}, Added: []graph.ID{"3"}},
		{Node: "1", Removed: []graph.ID{"0"}, Added: []graph.ID{"2", "3"}},
	},
}

var Acyclic6 = Setup{
	walker: walks.NewWalker(map[graph.ID][]graph.ID{
		"0": {"4"},
		"1": {"0"},
		"2": {},
		"3": {"1", "4"},
		"4": {"2"},
	}),
	nodes: []graph.ID{"0", "1", "2", "3", "4"},
	ranks: []float64{0.18820, 0.12128, 0.32417, 0.08511, 0.28125},
	deltas: []graph.Delta{
		// removals
		{Node: "0", Removed: []graph.ID{"4"}},
		{Node: "1", Removed: []graph.ID{"0"}},
		{Node: "3", Removed: []graph.ID{"1"}, Common: []graph.ID{"4"}},
		{Node: "3", Removed: []graph.ID{"4"}, Common: []graph.ID{"1"}},
		{Node: "3", Removed: []graph.ID{"1", "4"}},
		{Node: "4", Removed: []graph.ID{"2"}},
		// additions
		{Node: "0", Common: []graph.ID{"4"}, Added: []graph.ID{"2"}},
		{Node: "1", Common: []graph.ID{"0"}, Added: []graph.ID{"2"}},
		{Node: "1", Common: []graph.ID{"0"}, Added: []graph.ID{"4"}},
		{Node: "1", Common: []graph.ID{"0"}, Added: []graph.ID{"2", "4"}},
		{Node: "3", Common: []graph.ID{"1", "4"}, Added: []graph.ID{"0"}},
		{Node: "3", Common: []graph.ID{"1", "4"}, Added: []graph.ID{"2"}},
		{Node: "3", Common: []graph.ID{"1", "4"}, Added: []graph.ID{"0", "2"}},
		// removals and additions
		{Node: "0", Removed: []graph.ID{"4"}, Added: []graph.ID{"2"}},
		{Node: "1", Removed: []graph.ID{"0"}, Added: []graph.ID{"2"}},
		{Node: "1", Removed: []graph.ID{"0"}, Added: []graph.ID{"4"}},
		{Node: "1", Removed: []graph.ID{"0"}, Added: []graph.ID{"2", "4"}},
		{Node: "3", Removed: []graph.ID{"1"}, Common: []graph.ID{"4"}, Added: []graph.ID{"0"}},
		{Node: "3", Removed: []graph.ID{"1"}, Common: []graph.ID{"4"}, Added: []graph.ID{"2"}},
		{Node: "3", Removed: []graph.ID{"1"}, Common: []graph.ID{"4"}, Added: []graph.ID{"0", "2"}},
		{Node: "3", Removed: []graph.ID{"4"}, Common: []graph.ID{"1"}, Added: []graph.ID{"0"}},
		{Node: "3", Removed: []graph.ID{"4"}, Common: []graph.ID{"1"}, Added: []graph.ID{"2"}},
		{Node: "3", Removed: []graph.ID{"4"}, Common: []graph.ID{"1"}, Added: []graph.ID{"0", "2"}},
		{Node: "3", Removed: []graph.ID{"1", "4"}, Added: []graph.ID{"0"}},
		{Node: "3", Removed: []graph.ID{"1", "4"}, Added: []graph.ID{"2"}},
		{Node: "3", Removed: []graph.ID{"1", "4"}, Added: []graph.ID{"0", "2"}},
	},
}

var Acyclic7 = Setup{
	walker: walks.NewWalker(map[graph.ID][]graph.ID{
		"0": {"1", "2", "3"},
		"1": {},
		"2": {},
		"3": {},
		"4": {"0", "1", "2", "3"},
	}),
	nodes: []graph.ID{"0", "1", "2", "3", "4"},
	ranks: []float64{0.17622, 0.22615, 0.22615, 0.22615, 0.14534},
	deltas: []graph.Delta{
		// removals
		{Node: "0", Removed: []graph.ID{"1"}, Common: []graph.ID{"2", "3"}},
		{Node: "0", Removed: []graph.ID{"1", "2"}, Common: []graph.ID{"3"}},
		{Node: "0", Removed: []graph.ID{"1", "2", "3"}},
		{Node: "4", Removed: []graph.ID{"0"}, Common: []graph.ID{"1", "2", "3"}},
		{Node: "4", Removed: []graph.ID{"1"}, Common: []graph.ID{"0", "2", "3"}},
		{Node: "4", Removed: []graph.ID{"1", "2"}, Common: []graph.ID{"0", "3"}},
		{Node: "4", Removed: []graph.ID{"1", "2", "3"}, Common: []graph.ID{"0"}},
		{Node: "4", Removed: []graph.ID{"0", "1", "2", "3"}},
		// additions
		{Node: "1", Added: []graph.ID{"2"}},
		{Node: "1", Added: []graph.ID{"2", "3"}},
	},
}
