package redb

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/pippellia-btc/slicex"
	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/walks"

	"github.com/redis/go-redis/v9"
)

const (
	KeyRWS                 = "RWS"          // TODO: this can be removed
	KeyAlpha               = "alpha"        // TODO: walks:alpha
	KeyWalksPerNode        = "walksPerNode" // TODO: walks:N or another
	KeyLastWalkID          = "lastWalkID"   // TODO: walks:next
	KeyTotalVisits         = "totalVisits"  // TODO: walks:total_visits
	KeyWalks               = "walks"
	KeyWalksVisitingPrefix = "walksVisiting:" // TODO: walks_visiting:
)

var (
	ErrInvalidWalkParameters = errors.New("invalid walk parameters")
	ErrWalkNotFound          = errors.New("walk not found")
	ErrInvalidReplacement    = errors.New("invalid walk replacement")
	ErrInvalidLimit          = errors.New("limit must be a positive integer, or -1 to fetch all walks")
)

// init the walk store checking the existence of [KeyRWS].
// If it exists, check its fields for consistency
// If it doesn't, store [walks.Alpha] and [walks.N]
func (db RedisDB) init() error {
	ctx := context.Background()
	exists, err := db.Client.Exists(ctx, KeyRWS).Result()
	if err != nil {
		return fmt.Errorf("failed to check for existence of %s %w", KeyRWS, err)
	}

	switch exists {
	case 1:
		// exists, check the values
		vals, err := db.Client.HMGet(ctx, KeyRWS, KeyAlpha, KeyWalksPerNode).Result()
		if err != nil {
			return fmt.Errorf("failed to fetch alpha and walksPerNode %w", err)
		}

		alpha, err := parseFloat(vals[0])
		if err != nil {
			return fmt.Errorf("failed to parse alpha: %w", err)
		}

		N, err := parseInt(vals[1])
		if err != nil {
			return fmt.Errorf("failed to parse walksPerNode: %w", err)
		}

		if alpha != walks.Alpha {
			return fmt.Errorf("%w: alpha and walks.Alpha are different", ErrInvalidWalkParameters)
		}

		if N != walks.N {
			return fmt.Errorf("%w: N and walks.N are different", ErrInvalidWalkParameters)
		}

	case 0:
		// doesn't exists, seed the values
		err := db.Client.HSet(ctx, KeyRWS, KeyAlpha, walks.Alpha, KeyWalksPerNode, walks.N).Err()
		if err != nil {
			return fmt.Errorf("failed to set alpha and walksPerNode %w", err)
		}

	default:
		return fmt.Errorf("unexpected exists: %d", exists)
	}

	return nil
}

// Walks returns the walks associated with the IDs.
func (db RedisDB) Walks(ctx context.Context, IDs ...walks.ID) ([]walks.Walk, error) {
	switch {
	case len(IDs) == 0:
		return nil, nil

	case len(IDs) <= 100000:
		vals, err := db.Client.HMGet(ctx, KeyWalks, toStrings(IDs)...).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to fetch walks: %w", err)
		}

		walks := make([]walks.Walk, len(vals))
		for i, val := range vals {
			if val == nil {
				// walk was not found, so return an error
				return nil, fmt.Errorf("failed to fetch walk with ID %s: %w", IDs[i], ErrWalkNotFound)
			}

			walks[i] = parseWalk(val.(string))
			walks[i].ID = IDs[i]
		}

		return walks, nil

	default:
		// too many walks for a single call, so we split them in two batches
		mid := len(IDs) / 2
		batch1, err := db.Walks(ctx, IDs[:mid]...)
		if err != nil {
			return nil, err
		}

		batch2, err := db.Walks(ctx, IDs[mid:]...)
		if err != nil {
			return nil, err
		}

		return append(batch1, batch2...), nil
	}
}

// WalksVisiting returns up-to limit walks that visit node.
// Use limit = -1 to fetch all the walks visiting node.
func (db RedisDB) WalksVisiting(ctx context.Context, node graph.ID, limit int) ([]walks.Walk, error) {
	switch {
	case limit == -1:
		// return all walks visiting node
		IDs, err := db.Client.SMembers(ctx, walksVisiting(node)).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to fetch %s: %w", walksVisiting(node), err)
		}

		return db.Walks(ctx, toWalks(IDs)...)

	case limit > 0:
		IDs, err := db.Client.SRandMemberN(ctx, walksVisiting(node), int64(limit)).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to fetch %s: %w", walksVisiting(node), err)
		}

		return db.Walks(ctx, toWalks(IDs)...)

	default:
		return nil, ErrInvalidLimit
	}
}

// WalksVisitingAny returns up to limit walks that visit the specified nodes.
// The walks are distributed evenly among the nodes:
// - if limit == -1, all walks are returned (use with few nodes)
// - if limit < len(nodes), no walks are returned
func (db RedisDB) WalksVisitingAny(ctx context.Context, nodes []graph.ID, limit int) ([]walks.Walk, error) {
	switch {
	case limit == -1:
		// return all walks visiting all nodes
		pipe := db.Client.Pipeline()
		cmds := make([]*redis.StringSliceCmd, len(nodes))

		for i, node := range nodes {
			cmds[i] = pipe.SMembers(ctx, walksVisiting(node))
		}

		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("failed to fetch all walks visiting %d nodes: %w", len(nodes), err)
		}

		IDs := make([]string, 0, walks.N*len(nodes))
		for _, cmd := range cmds {
			IDs = append(IDs, cmd.Val()...)
		}

		unique := slicex.Unique(IDs)
		return db.Walks(ctx, toWalks(unique)...)

	case limit > 0:
		// return limit walks uniformely distributed across all nodes
		nodeLimit := int64(limit / len(nodes))
		if nodeLimit == 0 {
			return nil, nil
		}

		pipe := db.Client.Pipeline()
		cmds := make([]*redis.StringSliceCmd, len(nodes))

		for i, node := range nodes {
			cmds[i] = pipe.SRandMemberN(ctx, walksVisiting(node), nodeLimit)
		}

		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("failed to fetch %d walks visiting %d nodes: %w", limit, len(nodes), err)
		}

		IDs := make([]string, 0, limit)
		for _, cmd := range cmds {
			IDs = append(IDs, cmd.Val()...)
		}

		unique := slicex.Unique(IDs)
		return db.Walks(ctx, toWalks(unique)...)

	default:
		// invalid limit
		return nil, fmt.Errorf("failed to fetch walks visiting any: %w", ErrInvalidLimit)
	}
}

// AddWalks adds all the walks to the database assigning them progressive IDs.
func (db RedisDB) AddWalks(ctx context.Context, walks ...walks.Walk) error {
	if len(walks) == 0 {
		return nil
	}

	// get the IDs outside the transaction, which implies there might be "holes",
	// meaning IDs not associated with any walk
	next, err := db.Client.HIncrBy(ctx, KeyRWS, KeyLastWalkID, int64(len(walks))).Result()
	if err != nil {
		return fmt.Errorf("failed to add walks: failed to increment ID: %w", err)
	}

	var visits, ID int
	pipe := db.Client.TxPipeline()

	for i, walk := range walks {
		visits += walk.Len()
		ID = int(next) - len(walks) + i // assigning IDs in the same order

		pipe.HSet(ctx, KeyWalks, ID, formatWalk(walk))
		for _, node := range walk.Path {
			pipe.SAdd(ctx, walksVisiting(node), ID)
		}
	}

	pipe.HIncrBy(ctx, KeyRWS, KeyTotalVisits, int64(visits))

	if _, err = pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to add walks: pipeline failed %w", err)
	}

	return nil
}

// RemoveWalks removes all the walks from the database.
func (db RedisDB) RemoveWalks(ctx context.Context, walks ...walks.Walk) error {
	if len(walks) == 0 {
		return nil
	}

	var visits int
	pipe := db.Client.TxPipeline()

	for _, walk := range walks {
		pipe.HDel(ctx, KeyWalks, string(walk.ID))
		for _, node := range walk.Path {
			pipe.SRem(ctx, walksVisiting(node), string(walk.ID))
		}

		visits += walk.Len()
	}

	pipe.HIncrBy(ctx, KeyRWS, KeyTotalVisits, -int64(visits))

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to remove walks: pipeline failed %w", err)
	}

	return nil
}

// ReplaceWalks replaces the old walks with the new ones.
func (db RedisDB) ReplaceWalks(ctx context.Context, before, after []walks.Walk) error {
	if err := validateReplacement(before, after); err != nil {
		return err
	}

	var visits int64
	pipe := db.Client.TxPipeline()

	for i := range before {
		div := walks.Divergence(before[i], after[i])
		if div == -1 {
			// the two walks are equal, skip
			continue
		}

		prev := before[i]
		next := after[i]
		ID := after[i].ID

		pipe.HSet(ctx, KeyWalks, ID, formatWalk(next))

		for _, node := range prev.Path[div:] {
			pipe.SRem(ctx, walksVisiting(node), ID)
			visits--
		}

		for _, node := range next.Path[div:] {
			pipe.SAdd(ctx, walksVisiting(node), ID)
			visits++
		}

		if pipe.Len() > 5000 {
			// execute a partial update when it's too big
			pipe.HIncrBy(ctx, KeyRWS, KeyTotalVisits, visits)

			if _, err := pipe.Exec(ctx); err != nil {
				return fmt.Errorf("failed to replace walks: pipeline failed %w", err)
			}

			pipe = db.Client.TxPipeline()
			visits = 0
		}
	}

	pipe.HIncrBy(ctx, KeyRWS, KeyTotalVisits, visits)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to replace walks: pipeline failed %w", err)
	}

	return nil
}

func validateReplacement(old, new []walks.Walk) error {
	if len(old) != len(new) {
		return fmt.Errorf("%w: old and new walks must have the same lenght", ErrInvalidReplacement)
	}

	seen := make(map[walks.ID]struct{})
	for i := range old {
		if old[i].ID != new[i].ID {
			return fmt.Errorf("%w: IDs don't match at index %d: old=%s, new=%s", ErrInvalidReplacement, i, old[i].ID, new[i].ID)
		}

		if _, ok := seen[old[i].ID]; ok {
			return fmt.Errorf("%w: repeated walk ID %s", ErrInvalidReplacement, old[i].ID)
		}

		seen[old[i].ID] = struct{}{}
	}

	return nil
}

// TotalVisits returns the total number of visits, which is the sum of the lengths of all walks.
func (db RedisDB) TotalVisits(ctx context.Context) (int, error) {
	total, err := db.Client.HGet(ctx, KeyRWS, KeyTotalVisits).Result()
	if err != nil {
		return -1, fmt.Errorf("failed to get the total number of visits: %w", err)
	}

	tot, err := strconv.Atoi(total)
	if err != nil {
		return -1, fmt.Errorf("failed to parse the total number of visits: %w", err)
	}

	return tot, nil
}

// TotalWalks returns the total number of walks.
func (db RedisDB) TotalWalks(ctx context.Context) (int, error) {
	total, err := db.Client.HLen(ctx, KeyWalks).Result()
	if err != nil {
		return -1, fmt.Errorf("failed to get the total number of walks: %w", err)
	}
	return int(total), nil
}

// Visits returns the number of times each specified node was visited during the walks.
// The returned slice contains counts in the same order as the input nodes.
// If a node is not found, it returns 0 visits.
func (db RedisDB) Visits(ctx context.Context, nodes ...graph.ID) ([]int, error) {
	return db.counts(ctx, walksVisiting, nodes...)
}

// ScanWalks to return a batch of walks of size roughly proportional to limit.
// Limit controls how much "work" is invested in fetching the batch, hence it's not precise.
// Learn more about scan: https://redis.io/docs/latest/commands/hscan/
func (db RedisDB) ScanWalks(ctx context.Context, cursor uint64, limit int) ([]walks.Walk, uint64, error) {
	keyVals, cursor, err := db.Client.HScan(ctx, KeyWalks, cursor, "*", int64(limit)).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to scan for walks: %w", err)
	}

	if len(keyVals)%2 != 0 {
		return nil, 0, fmt.Errorf("unexpected HSCAN result length: got %d elements", len(keyVals))
	}

	batch := make([]walks.Walk, 0, len(keyVals)/2)
	for i := 0; i < len(keyVals); i += 2 {
		walk := parseWalk(keyVals[i+1])
		walk.ID = walks.ID(keyVals[i])
		batch = append(batch, walk)
	}

	return batch, cursor, nil
}
