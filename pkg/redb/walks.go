package redb

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/walks"
	"math"
	"slices"
	"strconv"

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
	ErrWalkNotFound       = errors.New("walk not found")
	ErrInvalidReplacement = errors.New("invalid walk replacement")
	ErrInvalidLimit       = errors.New("limit must be a positive integer, or -1 to fetch all walks")
)

// Walks returns the walks associated with the IDs.
func (r RedisDB) Walks(ctx context.Context, IDs ...walks.ID) ([]walks.Walk, error) {
	switch {
	case len(IDs) == 0:
		return nil, nil

	case len(IDs) <= 100000:
		vals, err := r.client.HMGet(ctx, KeyWalks, toStrings(IDs)...).Result()
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
		batch1, err := r.Walks(ctx, IDs[:mid]...)
		if err != nil {
			return nil, err
		}

		batch2, err := r.Walks(ctx, IDs[mid:]...)
		if err != nil {
			return nil, err
		}

		return append(batch1, batch2...), nil
	}
}

// WalksVisiting returns up-to limit walks that visit node.
// Use limit = -1 to fetch all the walks visiting node.
func (r RedisDB) WalksVisiting(ctx context.Context, node graph.ID, limit int) ([]walks.Walk, error) {
	switch {
	case limit == -1:
		// return all walks visiting node
		IDs, err := r.client.SMembers(ctx, walksVisiting(node)).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to fetch %s: %w", walksVisiting(node), err)
		}

		return r.Walks(ctx, toWalks(IDs)...)

	case limit > 0:
		IDs, err := r.client.SRandMemberN(ctx, walksVisiting(node), int64(limit)).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to fetch %s: %w", walksVisiting(node), err)
		}

		return r.Walks(ctx, toWalks(IDs)...)

	default:
		return nil, ErrInvalidLimit
	}
}

// WalksVisitingAny returns up to limit walks that visit the specified nodes.
// The walks are distributed evenly among the nodes:
// - if limit == -1, all walks are returned (use with few nodes)
// - if limit < len(nodes), no walks are returned
func (r RedisDB) WalksVisitingAny(ctx context.Context, nodes []graph.ID, limit int) ([]walks.Walk, error) {
	switch {
	case limit == -1:
		// return all walks visiting all nodes
		pipe := r.client.Pipeline()
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

		unique := unique(IDs)
		return r.Walks(ctx, toWalks(unique)...)

	case limit > 0:
		// return limit walks uniformely distributed across all nodes
		nodeLimit := int64(limit / len(nodes))
		if nodeLimit == 0 {
			return nil, nil
		}

		pipe := r.client.Pipeline()
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

		unique := unique(IDs)
		return r.Walks(ctx, toWalks(unique)...)

	default:
		// invalid limit
		return nil, fmt.Errorf("failed to fetch walks visiting any: %w", ErrInvalidLimit)
	}
}

// AddWalks adds all the walks to the database assigning them progressive IDs.
func (r RedisDB) AddWalks(ctx context.Context, walks ...walks.Walk) error {
	if len(walks) == 0 {
		return nil
	}

	// get the IDs outside the transaction, which implies there might be "holes",
	// meaning IDs not associated with any walk
	next, err := r.client.HIncrBy(ctx, KeyRWS, KeyLastWalkID, int64(len(walks))).Result()
	if err != nil {
		return fmt.Errorf("failed to add walks: failed to increment ID: %w", err)
	}

	var visits, ID int
	pipe := r.client.TxPipeline()

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
func (r RedisDB) RemoveWalks(ctx context.Context, walks ...walks.Walk) error {
	if len(walks) == 0 {
		return nil
	}

	var visits int
	pipe := r.client.TxPipeline()

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

func (r RedisDB) ReplaceWalks(ctx context.Context, before, after []walks.Walk) error {
	if err := validateReplacement(before, after); err != nil {
		return err
	}

	var visits int64
	pipe := r.client.TxPipeline()

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

			pipe = r.client.TxPipeline()
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
func (r RedisDB) TotalVisits(ctx context.Context) (int, error) {
	total, err := r.client.HGet(ctx, KeyRWS, KeyTotalVisits).Result()
	if err != nil {
		return -1, fmt.Errorf("failed to get the total number of visits: %w", err)
	}

	tot, err := strconv.Atoi(total)
	if err != nil {
		return -1, fmt.Errorf("failed to parse the total number of visits: %w", err)
	}

	return tot, nil
}

// Visits returns the number of times each specified node was visited during the walks.
// The returned slice contains counts in the same order as the input nodes.
// If a node is not found, it returns 0 visits.
func (r RedisDB) Visits(ctx context.Context, nodes ...graph.ID) ([]int, error) {
	return r.counts(ctx, walksVisiting, nodes...)
}

func (r RedisDB) validateWalks() error {
	vals, err := r.client.HMGet(context.Background(), KeyRWS, KeyAlpha, KeyWalksPerNode).Result()
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

	if math.Abs(alpha-walks.Alpha) > 1e-10 {
		return errors.New("alpha and walks.Alpha are different")
	}

	if N != walks.N {
		return errors.New("N and walks.N are different")
	}

	return nil
}

// unique returns a slice of unique elements of the input slice.
func unique[E cmp.Ordered](slice []E) []E {
	if len(slice) == 0 {
		return nil
	}

	slices.Sort(slice)
	unique := make([]E, 0, len(slice))
	unique = append(unique, slice[0])

	for i := 1; i < len(slice); i++ {
		if slice[i] != slice[i-1] {
			unique = append(unique, slice[i])
		}
	}

	return unique
}
