// The package redb defines the redis implementation of the database, which stores
// graph relationships (e.g. follows) as well as random walks.
package redb

import (
	"context"
	"errors"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
	"strconv"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
)

const (
	// redis variable names
	KeyDatabase        = "database"   // TODO: this can be removed
	KeyLastNodeID      = "lastNodeID" // TODO: change it to "next" inside "node" hash
	KeyKeyIndex        = "keyIndex"   // TODO: change to key_index
	KeyNodePrefix      = "node:"
	KeyFollowsPrefix   = "follows:"
	KeyFollowersPrefix = "followers:"

	// redis node HASH fields
	NodeID          = "id"
	NodePubkey      = "pubkey"
	NodeStatus      = "status"
	NodePromotionTS = "promotion_TS" // TODO: change to promotion
	NodeDemotionTS  = "demotion_TS"  // TODO: change to demotion
	NodeAddedTS     = "added_TS"     // TODO: change to addition
)

var (
	ErrNodeNotFound      = errors.New("node not found")
	ErrNodeAlreadyExists = errors.New("node already exists")
)

type RedisDB struct {
	client *redis.Client
}

func New(opt *redis.Options) RedisDB {
	r := RedisDB{client: redis.NewClient(opt)}
	if err := r.validateWalks(); err != nil {
		panic(err)
	}
	return r
}

// Size returns the DBSize of redis, which is the total number of keys
func (r RedisDB) Size(ctx context.Context) (int, error) {
	size, err := r.client.DBSize(ctx).Result()
	if err != nil {
		return 0, err
	}
	return int(size), nil
}

// NodeCount returns the number of nodes stored in redis (in the keyIndex)
func (r RedisDB) NodeCount(ctx context.Context) (int, error) {
	nodes, err := r.client.HLen(ctx, KeyKeyIndex).Result()
	if err != nil {
		return 0, err
	}
	return int(nodes), nil
}

// NodeByID fetches a node by its ID
func (r RedisDB) NodeByID(ctx context.Context, ID graph.ID) (*graph.Node, error) {
	fields, err := r.client.HGetAll(ctx, node(ID)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %s: %w", node(ID), err)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("failed to fetch %s: %w", node(ID), ErrNodeNotFound)
	}

	return parseNode(fields)
}

// NodeByKey fetches a node by its pubkey
func (r RedisDB) NodeByKey(ctx context.Context, pubkey string) (*graph.Node, error) {
	ID, err := r.client.HGet(ctx, KeyKeyIndex, pubkey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch ID of node with pubkey %s: %w", pubkey, err)
	}

	fields, err := r.client.HGetAll(ctx, node(ID)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch node with pubkey %s: %w", pubkey, err)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("failed to fetch node with pubkey %s: %w", pubkey, ErrNodeNotFound)
	}

	return parseNode(fields)
}

func (r RedisDB) containsNode(ctx context.Context, ID graph.ID) (bool, error) {
	exists, err := r.client.Exists(ctx, node(ID)).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check for the existence of %v: %w", node(ID), err)
	}
	return exists == 1, nil
}

// AddNode adds a new inactive node to the database and returns its assigned ID
func (r RedisDB) AddNode(ctx context.Context, pubkey string) (graph.ID, error) {
	exists, err := r.client.HExists(ctx, KeyKeyIndex, pubkey).Result()
	if err != nil {
		return "", fmt.Errorf("failed to check for existence of pubkey %s: %w", pubkey, err)
	}

	if exists {
		return "", fmt.Errorf("failed to add node with pubkey %s: %w", pubkey, ErrNodeAlreadyExists)
	}

	// get the ID outside the transaction, which implies there might be "holes",
	// meaning IDs not associated with any node
	next, err := r.client.HIncrBy(ctx, KeyDatabase, KeyLastNodeID, 1).Result()
	if err != nil {
		return "", fmt.Errorf("failed to add node with pubkey %s: failed to increment ID", pubkey)
	}
	ID := strconv.FormatInt(next-1, 10)

	pipe := r.client.TxPipeline()
	pipe.HSetNX(ctx, KeyKeyIndex, pubkey, ID)
	pipe.HSet(ctx, node(ID), NodeID, ID, NodePubkey, pubkey, NodeStatus, graph.StatusInactive, NodeAddedTS, time.Now().Unix())
	if _, err := pipe.Exec(ctx); err != nil {
		return "", fmt.Errorf("failed to add node with pubkey %s: pipeline failed: %w", pubkey, err)
	}

	return graph.ID(ID), nil
}

// Promote changes the node status to active
func (r RedisDB) Promote(ctx context.Context, ID graph.ID) error {
	err := r.client.HSet(ctx, node(ID), NodeStatus, graph.StatusActive, NodePromotionTS, time.Now().Unix()).Err()
	if err != nil {
		return fmt.Errorf("failed to promote %s: %w", node(ID), err)
	}
	return nil
}

// Demote changes the node status to inactive
func (r RedisDB) Demote(ctx context.Context, ID graph.ID) error {
	err := r.client.HSet(ctx, node(ID), NodeStatus, graph.StatusInactive, NodeDemotionTS, time.Now().Unix()).Err()
	if err != nil {
		return fmt.Errorf("failed to promote %s: %w", node(ID), err)
	}
	return nil
}

// Follows returns the follow list of node. If node is not found, it returns [ErrNodeNotFound].
func (r RedisDB) Follows(ctx context.Context, node graph.ID) ([]graph.ID, error) {
	return r.members(ctx, follows, node)
}

// Followers returns the list of followers of node. If node is not found, it returns [ErrNodeNotFound].
func (r RedisDB) Followers(ctx context.Context, node graph.ID) ([]graph.ID, error) {
	return r.members(ctx, followers, node)
}

func (r RedisDB) members(ctx context.Context, key func(graph.ID) string, node graph.ID) ([]graph.ID, error) {
	members, err := r.client.SMembers(ctx, key(node)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %s: %w", key(node), err)
	}

	if len(members) == 0 {
		// check if there are no members because node doesn't exists
		ok, err := r.containsNode(ctx, node)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, fmt.Errorf("failed to fetch %s: %w", key(node), ErrNodeNotFound)
		}
	}

	return toNodes(members), nil
}

// BulkFollows returns the follow-lists of all the provided nodes.
// Do not call on too many nodes (e.g. +100k) to avoid too many recursions.
func (r RedisDB) BulkFollows(ctx context.Context, nodes []graph.ID) ([][]graph.ID, error) {
	return r.bulkMembers(ctx, follows, nodes)
}

func (r RedisDB) bulkMembers(ctx context.Context, key func(graph.ID) string, nodes []graph.ID) ([][]graph.ID, error) {
	switch {
	case len(nodes) == 0:
		return nil, nil

	case len(nodes) < 10000:
		pipe := r.client.Pipeline()
		cmds := make([]*redis.StringSliceCmd, len(nodes))

		for i, node := range nodes {
			cmds[i] = pipe.SMembers(ctx, key(node))
		}

		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("failed to fetch the %s of %d nodes: %w", key(""), len(nodes), err)
		}

		var empty []string
		members := make([][]graph.ID, len(nodes))

		for i, cmd := range cmds {
			m := cmd.Val()
			if len(m) == 0 {
				// empty slice might mean node not found.
				empty = append(empty, node(nodes[i]))
			}

			members[i] = toNodes(m)
		}

		if len(empty) > 0 {
			exists, err := r.client.Exists(ctx, empty...).Result()
			if err != nil {
				return nil, err
			}

			if int(exists) < len(empty) {
				return nil, fmt.Errorf("failed to fetch the %s of these nodes %v: %w", key(""), empty, ErrNodeNotFound)
			}
		}

		return members, nil

	default:
		// too many nodes, split them in two batches
		mid := len(nodes) / 2
		batch1, err := r.bulkMembers(ctx, key, nodes[:mid])
		if err != nil {
			return nil, err
		}

		batch2, err := r.bulkMembers(ctx, key, nodes[mid:])
		if err != nil {
			return nil, err
		}

		return append(batch1, batch2...), nil
	}
}

// FollowCounts returns the number of follows each node has. If a node is not found, it returns 0.
func (r RedisDB) FollowCounts(ctx context.Context, nodes ...graph.ID) ([]int, error) {
	return r.counts(ctx, follows, nodes...)
}

// FollowerCounts returns the number of followers each node has. If a node is not found, it returns 0.
func (r RedisDB) FollowerCounts(ctx context.Context, nodes ...graph.ID) ([]int, error) {
	return r.counts(ctx, followers, nodes...)
}

func (r RedisDB) counts(ctx context.Context, key func(graph.ID) string, nodes ...graph.ID) ([]int, error) {
	if len(nodes) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	cmds := make([]*redis.IntCmd, len(nodes))

	for i, node := range nodes {
		cmds[i] = pipe.SCard(ctx, key(node))
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("failed to count the elements of %d nodes: %w", len(nodes), err)
	}

	counts := make([]int, len(nodes))
	for i, cmd := range cmds {
		counts[i] = int(cmd.Val())
	}

	return counts, nil
}

// Update applies the delta to the graph.
func (r RedisDB) Update(ctx context.Context, delta *graph.Delta) error {
	if delta.Size() == 0 {
		return nil
	}

	ok, err := r.containsNode(ctx, delta.Node)
	if err != nil {
		return fmt.Errorf("failed to update with delta %v: %w", delta, err)
	}

	if !ok {
		return fmt.Errorf("failed to update with delta %v: %w", delta, ErrNodeNotFound)
	}

	switch delta.Kind {
	case nostr.KindFollowList:
		err = r.updateFollows(ctx, delta)

	default:
		err = fmt.Errorf("unsupported kind %d", delta.Kind)
	}

	if err != nil {
		return fmt.Errorf("failed to update with delta %v: %w", delta, err)
	}

	return nil
}

func (r RedisDB) updateFollows(ctx context.Context, delta *graph.Delta) error {
	pipe := r.client.TxPipeline()
	if len(delta.Add) > 0 {
		// add all node --> added
		pipe.SAdd(ctx, follows(delta.Node), toStrings(delta.Add))

		for _, a := range delta.Add {
			pipe.SAdd(ctx, followers(a), delta.Node)
		}
	}

	if len(delta.Remove) > 0 {
		// remove all node --> removed
		pipe.SRem(ctx, follows(delta.Node), toStrings(delta.Remove))

		for _, r := range delta.Remove {
			pipe.SRem(ctx, followers(r), delta.Node)
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("pipeline failed: %w", err)
	}

	return nil
}

// NodeIDs returns a slice of node IDs assosiated with the pubkeys.
// If a pubkey is not found, an empty ID "" is returned
func (r RedisDB) NodeIDs(ctx context.Context, pubkeys ...string) ([]graph.ID, error) {
	if len(pubkeys) == 0 {
		return nil, nil
	}

	IDs, err := r.client.HMGet(ctx, KeyKeyIndex, pubkeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the node IDs of %v: %w", pubkeys, err)
	}

	nodes := make([]graph.ID, len(IDs))
	for i, ID := range IDs {
		switch ID {
		case nil:
			nodes[i] = "" // empty ID means missing pubkey

		default:
			nodes[i] = graph.ID(ID.(string)) // no need to type-assert because everything in redis is a string
		}
	}

	return nodes, nil
}

// Pubkeys returns a slice of pubkeys assosiated with the node IDs.
// If a node ID is not found, an empty pubkey "" is returned
func (r RedisDB) Pubkeys(ctx context.Context, nodes ...graph.ID) ([]string, error) {
	if len(nodes) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(nodes))
	for i, ID := range nodes {
		cmds[i] = pipe.HGet(ctx, node(ID), NodePubkey)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		// deal later with redis.Nil, which means node(s) not found
		return nil, fmt.Errorf("failed to fetch the pubkeys of %v: pipeline failed: %w", nodes, err)
	}

	pubkeys := make([]string, len(nodes))
	for i, cmd := range cmds {
		switch {
		case errors.Is(cmd.Err(), redis.Nil):
			pubkeys[i] = "" // empty pubkey means missing node

		case cmd.Err() != nil:
			return nil, fmt.Errorf("failed to fetch the pubkeys of %v: %w", nodes, cmd.Err())

		default:
			pubkeys[i] = cmd.Val()
		}
	}

	return pubkeys, nil
}

// ScanNodes to return a batch of node IDs of size roughly proportional to limit.
// Limit controls how much "work" is invested in fetching the batch, hence it's not precise.
// Learn more about scan: https://redis.io/docs/latest/commands/scan/
func (r RedisDB) ScanNodes(ctx context.Context, cursor uint64, limit int) ([]graph.ID, uint64, error) {
	match := KeyNodePrefix + "*"
	keys, cursor, err := r.client.Scan(ctx, cursor, match, int64(limit)).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to scan for keys matching %s: %w", match, err)
	}

	nodes := make([]graph.ID, len(keys))
	for i, key := range keys {
		node, found := strings.CutPrefix(key, KeyNodePrefix)
		if !found {
			return nil, 0, fmt.Errorf("failed to scan for keys matching %s: bad match %s", match, node)
		}

		nodes[i] = graph.ID(node)
	}

	return nodes, cursor, nil
}
