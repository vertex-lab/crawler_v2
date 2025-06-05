package redb

import (
	"errors"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/walks"
	"reflect"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
)

// func TestValidate(t *testing.T) {
// 	tests := []struct {
// 		name  string
// 		setup func() (RedisDB, error)
// 		err   error
// 	}{
// 		{name: "empty", setup: Empty, err: ErrValueIsNil},
// 		{name: "valid", setup: SomeWalks(0)},
// 	}

// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			db, err := test.setup()
// 			if err != nil {
// 				t.Fatalf("setup failed: %v", err)
// 			}
// 			defer db.flushAll()

// 			if err = db.validateWalks(); !errors.Is(err, test.err) {
// 				t.Fatalf("expected error %v, got %v", test.err, err)
// 			}
// 		})
// 	}
// }

func TestWalksVisiting(t *testing.T) {
	tests := []struct {
		name          string
		setup         func() (RedisDB, error)
		limit         int
		expectedWalks int // the number of [defaultWalk] returned
	}{
		{
			name:          "empty",
			setup:         SomeWalks(0),
			limit:         1,
			expectedWalks: 0,
		},
		{
			name:          "all walks",
			setup:         SomeWalks(10),
			limit:         -1,
			expectedWalks: 10,
		},
		{
			name:          "some walks",
			setup:         SomeWalks(100),
			limit:         33,
			expectedWalks: 33,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := test.setup()
			if err != nil {
				t.Fatalf("setup failed: %v", err)
			}
			defer db.flushAll()

			visiting, err := db.WalksVisiting(ctx, "0", test.limit)
			if err != nil {
				t.Fatalf("expected error nil, got %v", err)
			}

			if len(visiting) != test.expectedWalks {
				t.Fatalf("expected %d walks, got %d", test.expectedWalks, len(visiting))
			}

			for _, walk := range visiting {
				if !reflect.DeepEqual(walk.Path, defaultWalk.Path) {
					// compare only the paths, not the IDs
					t.Fatalf("expected walk %v, got %v", defaultWalk, walk)
				}
			}
		})
	}
}

func TestWalksVisitingAny(t *testing.T) {
	tests := []struct {
		name          string
		setup         func() (RedisDB, error)
		limit         int
		expectedWalks int // the number of [defaultWalk] returned
	}{
		{
			name:          "empty",
			setup:         SomeWalks(0),
			limit:         1,
			expectedWalks: 0,
		},
		{
			name:          "all walks",
			setup:         SomeWalks(10),
			limit:         -1,
			expectedWalks: 10,
		},
		{
			name:          "some walks",
			setup:         SomeWalks(100),
			limit:         20,
			expectedWalks: 20,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := test.setup()
			if err != nil {
				t.Fatalf("setup failed: %v", err)
			}
			defer db.flushAll()

			nodes := []graph.ID{"0", "1"}
			visiting, err := db.WalksVisitingAny(ctx, nodes, test.limit)
			if err != nil {
				t.Fatalf("expected error nil, got %v", err)
			}

			if len(visiting) > test.expectedWalks {
				t.Fatalf("expected %d walks, got %d", test.expectedWalks, len(visiting))
			}

			for _, walk := range visiting {
				if !reflect.DeepEqual(walk.Path, defaultWalk.Path) {
					// compare only the paths, not the IDs
					t.Fatalf("expected walk %v, got %v", defaultWalk, walk)
				}
			}
		})
	}
}

func TestAddWalks(t *testing.T) {
	db, err := SomeWalks(1)()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer db.flushAll()

	walks := []walks.Walk{
		{ID: "1", Path: []graph.ID{"1", "2", "3"}},
		{ID: "2", Path: []graph.ID{"4", "5"}},
		{ID: "3", Path: []graph.ID{"a", "b", "c"}},
	}

	if err := db.AddWalks(ctx, walks...); err != nil {
		t.Fatalf("expected error nil, got %v", err)
	}

	stored, err := db.Walks(ctx, "1", "2", "3")
	if err != nil {
		t.Fatalf("expected error nil, got %v", err)
	}

	if !reflect.DeepEqual(stored, walks) {
		t.Fatalf("expected walks %v, got %v", walks, stored)
	}

	total, err := db.TotalVisits(ctx)
	if err != nil {
		t.Fatalf("expected error nil, got %v", err)
	}

	if total != 10 {
		t.Fatalf("expected total visits %d, got %d", 10, total)
	}
}

func TestRemoveWalks(t *testing.T) {
	db, err := SomeWalks(10)()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer db.flushAll()

	walks := []walks.Walk{
		{ID: "0", Path: defaultWalk.Path},
		{ID: "1", Path: defaultWalk.Path},
	}

	if err := db.RemoveWalks(ctx, walks...); err != nil {
		t.Fatalf("expected error nil, got %v", err)
	}

	total, err := db.TotalVisits(ctx)
	if err != nil {
		t.Fatalf("expected error nil, got %v", err)
	}

	expected := (10 - 2) * defaultWalk.Len()
	if total != expected {
		t.Fatalf("expected total %d, got %d", expected, total)
	}

	visits, err := db.Visits(ctx, "0")
	if err != nil {
		t.Fatalf("expected error nil, got %v", err)
	}

	expected = (10 - 2)
	if visits[0] != expected {
		t.Fatalf("expected visits %d, got %d", expected, visits[0])
	}
}

func TestReplaceWalks(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		db, err := SomeWalks(2)()
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer db.flushAll()

		before := []walks.Walk{
			{ID: "0", Path: []graph.ID{"0", "1"}},
			{ID: "1", Path: []graph.ID{"0", "1"}},
		}

		after := []walks.Walk{
			{ID: "0", Path: []graph.ID{"0", "2", "3"}}, // changed
			{ID: "1", Path: []graph.ID{"0", "1"}},
		}

		if err := db.ReplaceWalks(ctx, before, after); err != nil {
			t.Fatalf("expected error nil, got %v", err)
		}

		walks, err := db.Walks(ctx, "0", "1")
		if err != nil {
			t.Fatalf("expected error nil, got %v", err)
		}

		if !reflect.DeepEqual(walks, after) {
			t.Fatalf("expected walks %v, got %v", after, walks)
		}

		expected := []int{2, 1, 1, 1}
		visits, err := db.Visits(ctx, "0", "1", "2", "3")
		if err != nil {
			t.Fatalf("expected error nil, got %v", err)
		}

		if !reflect.DeepEqual(visits, expected) {
			t.Fatalf("expected visits %v, got %v", expected, visits)
		}

		total, err := db.TotalVisits(ctx)
		if err != nil {
			t.Fatalf("expected error nil, got %v", err)
		}

		if total != 5 {
			t.Fatalf("expected total %d, got %d", 8, total)
		}
	})

	t.Run("mass removal", func(t *testing.T) {
		num := 10000
		db, err := SomeWalks(num)()
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer db.flushAll()

		before := make([]walks.Walk, num)
		after := make([]walks.Walk, num)
		for i := range num {
			ID := walks.ID(strconv.Itoa(i))
			before[i] = walks.Walk{ID: ID, Path: defaultWalk.Path} // the walk in the DB
			after[i] = walks.Walk{ID: ID}                          // empty walk
		}

		if err := db.ReplaceWalks(ctx, before, after); err != nil {
			t.Fatalf("expected error nil, got %v", err)
		}
		visits, err := db.Visits(ctx, "0", "1")
		if err != nil {
			t.Fatalf("expected error nil, got %v", err)
		}

		if !reflect.DeepEqual(visits, []int{0, 0}) {
			t.Fatalf("expected visits %v, got %v", []int{0, 0}, visits)
		}

		total, err := db.TotalVisits(ctx)
		if err != nil {
			t.Fatalf("expected error nil, got %v", err)
		}

		if total != 0 {
			t.Fatalf("expected total %d, got %d", 0, total)
		}
	})
}

func TestValidateReplacement(t *testing.T) {
	tests := []struct {
		name string
		old  []walks.Walk
		new  []walks.Walk
		err  error
	}{
		{
			name: "no walks",
		},
		{
			name: "different lenght",
			old:  []walks.Walk{{}},
			err:  ErrInvalidReplacement,
		},
		{
			name: "different IDs",
			old:  []walks.Walk{{ID: "0"}, {ID: "1"}},
			new:  []walks.Walk{{ID: "1"}, {ID: "0"}},
			err:  ErrInvalidReplacement,
		},
		{
			name: "repeated IDs",
			old:  []walks.Walk{{ID: "0"}, {ID: "0"}},
			new:  []walks.Walk{{ID: "0"}, {ID: "0"}},
			err:  ErrInvalidReplacement,
		},
		{
			name: "valid IDs",
			old:  []walks.Walk{{ID: "0"}, {ID: "1"}},
			new:  []walks.Walk{{ID: "0"}, {ID: "1"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateReplacement(test.old, test.new)
			if !errors.Is(err, test.err) {
				t.Fatalf("expected error %v, got %v", test.err, err)
			}
		})
	}
}

func TestUnique(t *testing.T) {
	tests := []struct {
		slice    []walks.ID
		expected []walks.ID
	}{
		{slice: nil, expected: nil},
		{slice: []walks.ID{}, expected: nil},
		{slice: []walks.ID{"1", "2", "0"}, expected: []walks.ID{"0", "1", "2"}},
		{slice: []walks.ID{"1", "2", "0", "3", "1", "0"}, expected: []walks.ID{"0", "1", "2", "3"}},
	}

	for _, test := range tests {
		unique := unique(test.slice)
		if !reflect.DeepEqual(unique, test.expected) {
			t.Errorf("expected %v, got %v", test.expected, unique)
		}
	}
}

func BenchmarkUnique(b *testing.B) {
	size := 1000000
	IDs := make([]walks.ID, size)
	for i := 0; i < size; i++ {
		IDs[i] = walks.ID(strconv.Itoa(i))
	}

	b.ResetTimer()
	for range b.N {
		unique(IDs)
	}
}

var defaultWalk = walks.Walk{Path: []graph.ID{"0", "1"}}

func SomeWalks(n int) func() (RedisDB, error) {
	return func() (RedisDB, error) {
		db := RedisDB{Client: redis.NewClient(&redis.Options{Addr: testAddress})}
		if err := db.Client.HSet(ctx, KeyRWS, KeyAlpha, walks.Alpha, KeyWalksPerNode, walks.N).Err(); err != nil {
			return RedisDB{}, err
		}

		for range n {
			if err := db.AddWalks(ctx, defaultWalk); err != nil {
				return RedisDB{}, err
			}
		}

		return db, nil
	}
}
