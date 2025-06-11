package e2e_test

import (
	"context"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/pagerank"
	"github/pippellia-btc/crawler/pkg/redb"
	"github/pippellia-btc/crawler/pkg/walks"
	test "github/pippellia-btc/crawler/tests/random"
	"math"
	"testing"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// TestWalks will perform multiple iterations of the following:
// - fetch walks in batches
// - verify their consistency, meaning each node in a walk should contain its walk ID
func TestWalks(t *testing.T) {
	fmt.Println("-----------------------------")
	fmt.Println("Testing the walks consistency")
	fmt.Printf("-----------------------------\n\n")

	db := redb.New(&redis.Options{Addr: "localhost:6379"})

	var iteration int
	var limit int = 10000

	var batch []walks.Walk
	var cursor uint64
	var err error

	for {
		iteration++
		fmt.Printf("\033[1A")
		fmt.Print("\033[J")
		fmt.Printf("iteration %d...\n", iteration)

		batch, cursor, err = db.ScanWalks(ctx, cursor, limit)
		if err != nil {
			t.Fatal(err)
		}

		pipe := db.Client.Pipeline()
		cmds := make(map[string]*redis.BoolCmd)

		for _, walk := range batch {
			for _, node := range walk.Path {
				// check that the walks visiting node contain this walk ID
				key := string(node) + ":" + string(walk.ID)
				cmds[key] = pipe.SIsMember(ctx, redb.KeyWalksVisitingPrefix+string(node), walk.ID)
			}
		}

		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("pipeline failed: %v", err)
		}

		for key, cmd := range cmds {
			if !cmd.Val() {
				t.Errorf("expected true, got %v: %v", cmd.Val(), key)
			}
		}

		if cursor == 0 {
			break
		}
	}

	fmt.Println("passed!")
	fmt.Println("-----------------------------")
}

// TestPagerank regenerate all walks for all active nodes to compute pagerank.
// The resulting distribution is compared with the one in Redis.
func TestPagerank(t *testing.T) {
	fmt.Println("---------------------------------")
	fmt.Println("Testing the pagerank distribution")

	db := redb.New(&redis.Options{Addr: "localhost:6379"})
	nodes, err := db.AllNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}

	original, err := pagerank.Global(ctx, db, nodes...)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(" > original pagerank computed")

	// copy the db into a map to speed up random walks generation
	followMap := make(map[graph.ID][]graph.ID, len(nodes))
	for _, node := range nodes {
		follows, err := db.Follows(ctx, node)
		if err != nil {
			t.Fatal(err)
		}

		followMap[node] = follows
	}

	walker := walks.NewSimpleWalker(followMap)
	store := test.NewWalkStore()

	fmt.Println(" > db copied")
	fmt.Printf(" > generating walks...\n")
	fmt.Printf("---------------------------------\n\n")

	var active int
	for i, ID := range nodes {
		if i%1000 == 0 {
			fmt.Printf("\033[1A")
			fmt.Print("\033[J")
			fmt.Printf("progress %d/%d...\n", i+1, len(nodes))
		}

		node, err := db.NodeByID(ctx, ID)
		if err != nil {
			t.Fatal(err)
		}

		if node.Status == graph.StatusActive {
			walks, err := walks.Generate(ctx, walker, ID)
			if err != nil {
				t.Fatal(err)
			}

			store.AddWalks(walks)
			active++
		}
	}

	recomputed, err := pagerank.Global(ctx, store, nodes...)
	if err != nil {
		t.Fatal(err)
	}

	expected := expectedDistance(active, len(nodes))
	distance := test.Distance(original, recomputed)
	fmt.Printf("expected distance %f, got %f\n", expected, distance)

	if distance > expected {
		t.Fatalf("distance is higher than expected!")
	}

	fmt.Println("passed!")
	fmt.Println("-----------------------------")
}

/*
ExpectedDistance between the real pagerank and the Monte-Carlo pagerank.
Such distance goes as ~N/sqrt(R), where N is the number of nodes and R is the number of walks.

# REFERENCES:
[1] K. Avrachenkov, N. Litvak, D. Nemirovsky, N. Osipova; "Monte Carlo methods in PageRank computation"
URL: https://www-sop.inria.fr/members/Konstantin.Avratchenkov/pubs/mc.pdf
*/
func expectedDistance(activeNodes, totalNodes int) float64 {
	const errorConstant = 0.00035 // empirically derived

	walks := float64(activeNodes * walks.N)
	return errorConstant * float64(totalNodes) / math.Sqrt(walks)
}
