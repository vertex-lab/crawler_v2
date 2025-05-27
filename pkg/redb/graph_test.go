package redb

import (
	"context"
	"errors"
	"github/pippellia-btc/crawler/pkg/graph"
	"reflect"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func TestParseNode(t *testing.T) {
	tests := []struct {
		name     string
		fields   map[string]string
		expected *graph.Node
		err      error
	}{
		{
			name: "nil map",
		},
		{
			name:   "empty map",
			fields: map[string]string{},
		},
		{
			name: "valid no records",
			fields: map[string]string{
				NodeID:     "19",
				NodePubkey: "nineteen",
				NodeStatus: graph.StatusActive,
			},
			expected: &graph.Node{
				ID:     "19",
				Pubkey: "nineteen",
				Status: graph.StatusActive,
			},
		},
		{
			name: "valid with record",
			fields: map[string]string{
				NodeID:      "19",
				NodePubkey:  "nineteen",
				NodeStatus:  graph.StatusActive,
				NodeAddedTS: "1",
			},
			expected: &graph.Node{
				ID:     "19",
				Pubkey: "nineteen",
				Status: graph.StatusActive,
				Records: []graph.Record{
					{Kind: graph.Addition, Timestamp: time.Unix(1, 0)},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node, err := parseNode(test.fields)
			if !errors.Is(err, test.err) {
				t.Fatalf("expected %v got %v", test.err, err)
			}

			if !reflect.DeepEqual(node, test.expected) {
				t.Fatalf("ParseNode(): expected node %v got %v", test.expected, node)
			}
		})
	}
}

func TestAddNode(t *testing.T) {
	t.Run("node already exists", func(t *testing.T) {
		db, err := OneNode()
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer db.flushAll()

		if _, err = db.AddNode(ctx, "0"); !errors.Is(err, ErrNodeAlreadyExists) {
			t.Fatalf("expected error %v, got %v", ErrNodeAlreadyExists, err)
		}
	})

	t.Run("valid", func(t *testing.T) {
		db, err := OneNode()
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer db.flushAll()

		ID, err := db.AddNode(ctx, "xxx")
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}

		expected := &graph.Node{
			ID:      "1",
			Pubkey:  "xxx",
			Status:  graph.StatusInactive,
			Records: []graph.Record{{Kind: graph.Addition, Timestamp: time.Unix(time.Now().Unix(), 0)}},
		}

		if ID != expected.ID {
			t.Fatalf("expected ID %s, got %s", expected.ID, ID)
		}

		node, err := db.NodeByKey(ctx, "xxx")
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(node, expected) {
			t.Fatalf("expected node %v, got %v", expected, node)
		}
	})
}

func TestMembers(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (RedisDB, error)
		node     graph.ID
		expected []graph.ID
		err      error
	}{
		{
			name:  "empty database",
			setup: Empty,
			node:  "0",
			err:   ErrNodeNotFound,
		},
		{
			name:  "node not found",
			setup: OneNode,
			node:  "1",
			err:   ErrNodeNotFound,
		},
		{
			name:     "dandling node",
			setup:    OneNode,
			node:     "0",
			expected: []graph.ID{},
		},
		{
			name:     "valid",
			setup:    Simple,
			node:     "0",
			expected: []graph.ID{"1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := test.setup()
			if err != nil {
				t.Fatalf("setup failed: %v", err)
			}
			defer db.flushAll()

			follows, err := db.members(ctx, follows, test.node)
			if !errors.Is(err, test.err) {
				t.Fatalf("expected error %v, got %v", test.err, err)
			}

			if !reflect.DeepEqual(follows, test.expected) {
				t.Errorf("expected follows %v, got %v", test.expected, follows)
			}
		})
	}
}

func TestUpdateFollows(t *testing.T) {
	db, err := Simple()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer db.flushAll()

	delta := &graph.Delta{
		Kind:   nostr.KindFollowList,
		Node:   "0",
		Remove: []graph.ID{"1"},
		Add:    []graph.ID{"2"},
	}

	if err := db.Update(ctx, delta); err != nil {
		t.Fatalf("expected error nil, got %v", err)
	}

	follows, err := db.Follows(ctx, "0")
	if err != nil {
		t.Fatalf("expected nil got %v", err)
	}

	if !reflect.DeepEqual(follows, []graph.ID{"2"}) {
		t.Fatalf("expected follows(0) %v, got %v", []graph.ID{"2"}, follows)
	}

	followers, err := db.Followers(ctx, "1")
	if err != nil {
		t.Fatalf("expected nil got %v", err)
	}

	if !reflect.DeepEqual(followers, []graph.ID{}) {
		t.Fatalf("expected followers(1) %v, got %v", []graph.ID{}, followers)
	}

	followers, err = db.Followers(ctx, "2")
	if err != nil {
		t.Fatalf("expected nil got %v", err)
	}

	if !reflect.DeepEqual(followers, []graph.ID{"0"}) {
		t.Fatalf("expected followers(2) %v, got %v", []graph.ID{"0"}, followers)
	}
}

func TestNodeIDs(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (RedisDB, error)
		pubkeys  []string
		expected []graph.ID
	}{
		{
			name:     "empty database",
			setup:    Empty,
			pubkeys:  []string{"0"},
			expected: []graph.ID{""},
		},
		{
			name:     "node not found",
			setup:    OneNode,
			pubkeys:  []string{"1"},
			expected: []graph.ID{""},
		},
		{
			name:     "valid",
			setup:    Simple,
			pubkeys:  []string{"0", "1", "69"},
			expected: []graph.ID{"0", "1", ""}, // last is not found
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := test.setup()
			if err != nil {
				t.Fatalf("setup failed: %v", err)
			}
			defer db.flushAll()

			nodes, err := db.NodeIDs(ctx, test.pubkeys...)
			if err != nil {
				t.Fatalf("expected error nil, got %v", err)
			}

			if !reflect.DeepEqual(nodes, test.expected) {
				t.Fatalf("expected nodes %v, got %v", test.expected, nodes)
			}
		})
	}
}

func TestPubkeys(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (RedisDB, error)
		nodes    []graph.ID
		expected []string
	}{
		{
			name:     "empty database",
			setup:    Empty,
			nodes:    []graph.ID{"0"},
			expected: []string{""},
		},
		{
			name:     "node not found",
			setup:    OneNode,
			nodes:    []graph.ID{"1"},
			expected: []string{""},
		},
		{
			name:     "valid",
			setup:    Simple,
			nodes:    []graph.ID{"0", "1", "69"},
			expected: []string{"0", "1", ""}, // last is not found
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := test.setup()
			if err != nil {
				t.Fatalf("setup failed: %v", err)
			}
			defer db.flushAll()

			pubkeys, err := db.Pubkeys(ctx, test.nodes...)
			if err != nil {
				t.Fatalf("expected error nil, got %v", err)
			}

			if !reflect.DeepEqual(pubkeys, test.expected) {
				t.Fatalf("expected pubkeys %v, got %v", test.expected, pubkeys)
			}
		})
	}
}

// ------------------------------------- HELPERS -------------------------------

func Empty() (RedisDB, error) {
	return New(&redis.Options{Addr: testAddress}), nil
}

func OneNode() (RedisDB, error) {
	db := New(&redis.Options{Addr: testAddress})
	if _, err := db.AddNode(context.Background(), "0"); err != nil {
		db.flushAll()
		return RedisDB{}, err
	}

	return db, nil
}

func Simple() (RedisDB, error) {
	ctx := context.Background()
	db := New(&redis.Options{Addr: testAddress})

	for _, pk := range []string{"0", "1", "2"} {
		if _, err := db.AddNode(ctx, pk); err != nil {
			db.flushAll()
			return RedisDB{}, err
		}
	}

	// 0 ---> 1
	if err := db.client.SAdd(ctx, follows("0"), "1").Err(); err != nil {
		db.flushAll()
		return RedisDB{}, err
	}

	if err := db.client.SAdd(ctx, followers("1"), "0").Err(); err != nil {
		db.flushAll()
		return RedisDB{}, err
	}

	return db, nil
}
