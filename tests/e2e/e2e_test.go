package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/leaks"
	"github.com/vertex-lab/crawler_v2/pkg/pagerank"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
	"github.com/vertex-lab/crawler_v2/pkg/walks"
	test "github.com/vertex-lab/crawler_v2/tests/random"

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

	rClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rClient.Close()

	db, err := regraph.New(rClient)
	if err != nil {
		t.Fatalf("setup failed %v", err)
	}

	var iteration int
	var limit int = 10000

	var batch []walks.Walk
	var cursor uint64

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
				cmds[key] = pipe.SIsMember(ctx, regraph.KeyWalksVisitingPrefix+string(node), walk.ID)
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

	redis := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer redis.Close()

	db, err := regraph.New(redis)
	if err != nil {
		t.Fatalf("setup failed %v", err)
	}

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

	var active int
	for i, ID := range nodes {
		if i%1000 == 0 {
			fmt.Printf("\033[1A")
			fmt.Print("\033[J")
			fmt.Printf(" > generating walks %d/%d...\n", i+1, len(nodes))
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

	fmt.Println("---------------------------------")
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
	const errorConstant = 2 * 0.00035 // empirically derived

	walks := float64(activeNodes * walks.N)
	return errorConstant * float64(totalNodes) / math.Sqrt(walks)
}

func TestLeaks(t *testing.T) {
	fmt.Println("---------------------------------")
	fmt.Println("Testing the leaked nodes")

	redis := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer redis.Close()

	leaks := leaks.NewDB(redis)
	db, err := regraph.New(redis)
	if err != nil {
		t.Fatalf("setup failed %v", err)
	}

	records, err := leaks.All(ctx)
	if err != nil {
		t.Fatalf("failed to get leaked keys: %v", err)
	}

	for _, r := range records {
		if err := r.Validate(); err != nil {
			t.Errorf("invalid record %v: %v", r, err)
			continue
		}

		// if the pubkey is in the graph, it must be associated
		// with a "leaked" node, which must not have any walks starting from it
		node, err := db.NodeByKey(ctx, r.Pubkey)
		if errors.Is(err, graph.ErrNodeNotFound) {
			continue
		}
		if err != nil {
			t.Errorf("failed to get node of %s: %v", r.Pubkey, err)
			continue
		}
		if node.Status != graph.StatusLeaked {
			t.Errorf("node %s has status %s", node.ID, node.Status)
			continue
		}

		walksVisiting, err := db.WalksVisiting(ctx, node.ID, -1)
		if err != nil {
			t.Errorf("failed to get walks visiting %s: %v", r.Pubkey, err)
			continue
		}

		for _, walk := range walksVisiting {
			if walk.Path[0] == node.ID {
				t.Errorf("leaked node %s has walk: %v", r.Pubkey, walk)
			}
		}
	}
}
