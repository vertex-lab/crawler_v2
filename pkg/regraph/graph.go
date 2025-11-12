// The package regraph defines the redis implementation of the database, which stores
// graph relationships (e.g. follows) as well as random walks.
package regraph

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vertex-lab/crawler_v2/pkg/graph"

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

type DB struct {
	Client *redis.Client
}

func New(opt *redis.Options) (DB, error) {
	db := DB{Client: redis.NewClient(opt)}
	if err := db.init(); err != nil {
		return DB{}, err
	}
	return db, nil
}

// Close closes the client, releasing any open resources.
func (db DB) Close() error {
	return db.Client.Close()
}

// Size returns the DBSize of redis, which is the total number of keys
func (db DB) Size(ctx context.Context) (int, error) {
	size, err := db.Client.DBSize(ctx).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to fetch the db size: %w", err)
	}
	return int(size), nil
}

// NodeCount returns the number of nodes stored in redis (in the keyIndex)
func (db DB) NodeCount(ctx context.Context) (int, error) {
	nodes, err := db.Client.HLen(ctx, KeyKeyIndex).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to fetch the node count: %w", err)
	}
	return int(nodes), nil
}

// Nodes fetches a slice of nodes by their IDs.
func (db DB) Nodes(ctx context.Context, IDs ...graph.ID) ([]*graph.Node, error) {
	if len(IDs) == 0 {
		return nil, nil
	}

	pipe := db.Client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(IDs))
	for i, ID := range IDs {
		cmds[i] = pipe.HGetAll(ctx, node(ID))
	}

	var err error
	if _, err = pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("failed to fetch %d nodes: %w", len(IDs), err)
	}

	nodes := make([]*graph.Node, len(IDs))
	for i, cmd := range cmds {
		fields := cmd.Val()
		if len(fields) == 0 {
			return nil, fmt.Errorf("failed to fetch %s: %w", node(IDs[i]), graph.ErrNodeNotFound)
		}

		nodes[i], err = parseNode(fields)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch %s: %w", node(IDs[i]), err)
		}
	}

	return nodes, nil
}

// NodeByID fetches a node by its ID
func (db DB) NodeByID(ctx context.Context, ID graph.ID) (*graph.Node, error) {
	fields, err := db.Client.HGetAll(ctx, node(ID)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %s: %w", node(ID), err)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("failed to fetch %s: %w", node(ID), graph.ErrNodeNotFound)
	}

	return parseNode(fields)
}

// NodeByKey fetches a node by its pubkey
func (db DB) NodeByKey(ctx context.Context, pubkey string) (*graph.Node, error) {
	ID, err := db.Client.HGet(ctx, KeyKeyIndex, pubkey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			err = graph.ErrNodeNotFound
		}
		return nil, fmt.Errorf("failed to fetch ID of node with pubkey %s: %w", pubkey, err)
	}

	fields, err := db.Client.HGetAll(ctx, node(ID)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch node with pubkey %s: %w", pubkey, err)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("failed to fetch node with pubkey %s: %w", pubkey, graph.ErrNodeNotFound)
	}

	return parseNode(fields)
}

// Exists checks for the existance of the pubkey
func (db DB) Exists(ctx context.Context, pubkey string) (bool, error) {
	exists, err := db.Client.HExists(ctx, KeyKeyIndex, pubkey).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check existance of pubkey %s: %w", pubkey, err)
	}
	return exists, nil
}

func (db DB) ensureExists(ctx context.Context, IDs ...graph.ID) error {
	if len(IDs) == 0 {
		return nil
	}

	nodes := make([]string, len(IDs))
	for i, ID := range IDs {
		nodes[i] = node(ID)
	}

	exists, err := db.Client.Exists(ctx, nodes...).Result()
	if err != nil {
		return fmt.Errorf("failed to check for the existence of %d nodes: %w", len(IDs), err)
	}

	if int(exists) < len(IDs) {
		return graph.ErrNodeNotFound
	}

	return nil
}

// AddNode adds a new inactive node to the database and returns its assigned ID
func (db DB) AddNode(ctx context.Context, pubkey string) (graph.ID, error) {
	exists, err := db.Client.HExists(ctx, KeyKeyIndex, pubkey).Result()
	if err != nil {
		return "", fmt.Errorf("failed to check for existence of pubkey %s: %w", pubkey, err)
	}

	if exists {
		return "", fmt.Errorf("failed to add node with pubkey %s: %w", pubkey, graph.ErrNodeAlreadyExists)
	}

	// get the ID outside the transaction, which implies there might be "holes",
	// meaning IDs not associated with any node
	next, err := db.Client.HIncrBy(ctx, KeyDatabase, KeyLastNodeID, 1).Result()
	if err != nil {
		return "", fmt.Errorf("failed to add node with pubkey %s: failed to increment ID", pubkey)
	}
	ID := strconv.FormatInt(next-1, 10)

	pipe := db.Client.TxPipeline()
	pipe.HSetNX(ctx, KeyKeyIndex, pubkey, ID)
	pipe.HSet(ctx, node(ID), NodeID, ID, NodePubkey, pubkey, NodeStatus, graph.StatusInactive, NodeAddedTS, time.Now().Unix())
	if _, err := pipe.Exec(ctx); err != nil {
		return "", fmt.Errorf("failed to add node with pubkey %s: pipeline failed: %w", pubkey, err)
	}

	return graph.ID(ID), nil
}

// Promote changes the node status to active
func (db DB) Promote(ctx context.Context, ID graph.ID) error {
	err := db.Client.HSet(ctx, node(ID), NodeStatus, graph.StatusActive, NodePromotionTS, time.Now().Unix()).Err()
	if err != nil {
		return fmt.Errorf("failed to promote %s: %w", node(ID), err)
	}
	return nil
}

// Demote changes the node status to inactive
func (db DB) Demote(ctx context.Context, ID graph.ID) error {
	err := db.Client.HSet(ctx, node(ID), NodeStatus, graph.StatusInactive, NodeDemotionTS, time.Now().Unix()).Err()
	if err != nil {
		return fmt.Errorf("failed to demote %s: %w", node(ID), err)
	}
	return nil
}

// Follows returns the follow list of node. If node is not found, it returns [graph.ErrNodeNotFound].
func (db DB) Follows(ctx context.Context, node graph.ID) ([]graph.ID, error) {
	return db.members(ctx, follows, node)
}

// Followers returns the list of followers of node. If node is not found, it returns [graph.ErrNodeNotFound].
func (db DB) Followers(ctx context.Context, node graph.ID) ([]graph.ID, error) {
	return db.members(ctx, followers, node)
}

func (db DB) members(ctx context.Context, key func(graph.ID) string, node graph.ID) ([]graph.ID, error) {
	members, err := db.Client.SMembers(ctx, key(node)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %s: %w", key(node), err)
	}

	if len(members) == 0 {
		// check if there are no members because node doesn't exists
		if err := db.ensureExists(ctx, node); err != nil {
			return nil, fmt.Errorf("failed to fetch %s: %w", key(node), err)
		}
	}

	return toNodes(members), nil
}

// BulkFollows returns the follow-lists of all the provided nodes.
// Do not call on too many nodes (e.g. +100k) to avoid too many recursions.
func (db DB) BulkFollows(ctx context.Context, nodes []graph.ID) ([][]graph.ID, error) {
	return db.bulkMembers(ctx, follows, nodes)
}

func (db DB) bulkMembers(ctx context.Context, key func(graph.ID) string, nodes []graph.ID) ([][]graph.ID, error) {
	switch {
	case len(nodes) == 0:
		return nil, nil

	case len(nodes) < 10000:
		pipe := db.Client.Pipeline()
		cmds := make([]*redis.StringSliceCmd, len(nodes))

		for i, node := range nodes {
			cmds[i] = pipe.SMembers(ctx, key(node))
		}

		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("failed to fetch the %s of %d nodes: %w", key(""), len(nodes), err)
		}

		var empty []graph.ID
		members := make([][]graph.ID, len(nodes))

		for i, cmd := range cmds {
			m := cmd.Val()
			if len(m) == 0 {
				// empty slice might mean node not found.
				empty = append(empty, nodes[i])
			}

			members[i] = toNodes(m)
		}

		if len(empty) > 0 {
			err := db.ensureExists(ctx, empty...)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch the %s of these nodes %v: %w", key(""), empty, err)
			}
		}

		return members, nil

	default:
		// too many nodes, split them in two batches
		mid := len(nodes) / 2
		batch1, err := db.bulkMembers(ctx, key, nodes[:mid])
		if err != nil {
			return nil, err
		}

		batch2, err := db.bulkMembers(ctx, key, nodes[mid:])
		if err != nil {
			return nil, err
		}

		return append(batch1, batch2...), nil
	}
}

// FollowCounts returns the number of follows each node has. If a node is not found, it returns 0.
func (db DB) FollowCounts(ctx context.Context, nodes ...graph.ID) ([]int, error) {
	return db.counts(ctx, follows, nodes...)
}

// FollowerCounts returns the number of followers each node has. If a node is not found, it returns 0.
func (db DB) FollowerCounts(ctx context.Context, nodes ...graph.ID) ([]int, error) {
	return db.counts(ctx, followers, nodes...)
}

func (db DB) counts(ctx context.Context, key func(graph.ID) string, nodes ...graph.ID) ([]int, error) {
	if len(nodes) == 0 {
		return nil, nil
	}

	pipe := db.Client.Pipeline()
	cmds := make([]*redis.IntCmd, len(nodes))

	for i, node := range nodes {
		cmds[i] = pipe.SCard(ctx, key(node))
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("failed to count the elements of %d %s of nodes: %w", len(nodes), key(""), err)
	}

	counts := make([]int, len(nodes))
	for i, cmd := range cmds {
		counts[i] = int(cmd.Val())
	}

	return counts, nil
}

// Update applies the delta to the graph.
func (db DB) Update(ctx context.Context, delta graph.Delta) error {
	if delta.Size() == 0 {
		return nil
	}

	err := db.ensureExists(ctx, delta.Node)
	if err != nil {
		return fmt.Errorf("failed to update with delta %v: %w", delta, err)
	}

	switch delta.Kind {
	case nostr.KindFollowList:
		err = db.updateFollows(ctx, delta)

	default:
		err = fmt.Errorf("unsupported kind %d", delta.Kind)
	}

	if err != nil {
		return fmt.Errorf("failed to update with delta %v: %w", delta, err)
	}

	return nil
}

func (db DB) updateFollows(ctx context.Context, delta graph.Delta) error {
	pipe := db.Client.TxPipeline()
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

		for _, db := range delta.Remove {
			pipe.SRem(ctx, followers(db), delta.Node)
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("pipeline failed: %w", err)
	}

	return nil
}

// NodeIDs returns a slice of node IDs assosiated with the pubkeys.
// If a pubkey is not found, an empty ID "" is returned
func (db DB) NodeIDs(ctx context.Context, pubkeys ...string) ([]graph.ID, error) {
	if len(pubkeys) == 0 {
		return nil, nil
	}

	IDs, err := db.Client.HMGet(ctx, KeyKeyIndex, pubkeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the node IDs of %v: %w", pubkeys, err)
	}

	nodes := make([]graph.ID, len(IDs))
	for i, ID := range IDs {
		switch ID {
		case nil:
			// empty ID means missing pubkey
			nodes[i] = ""

		default:
			// direct type convertion because everything in redis is a string
			nodes[i] = graph.ID(ID.(string))
		}
	}

	return nodes, nil
}

// Pubkeys returns a slice of pubkeys assosiated with the node IDs.
// If a node ID is not found, an empty pubkey "" is returned
func (db DB) Pubkeys(ctx context.Context, nodes ...graph.ID) ([]string, error) {
	if len(nodes) == 0 {
		return nil, nil
	}

	pipe := db.Client.Pipeline()
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

type MissingHandler func(ctx context.Context, db DB, pubkey string) (graph.ID, error)

// Ignore pubkeys that are not found
func Ignore(context.Context, DB, string) (graph.ID, error) { return "", nil }

// Return a sentinel value ("-1") as the node ID of pubkeys not found
func Sentinel(context.Context, DB, string) (graph.ID, error) { return "-1", nil }

// AddValid pubkeys to the database if they were not already present
func AddValid(ctx context.Context, db DB, pubkey string) (graph.ID, error) {
	if !nostr.IsValidPublicKey(pubkey) {
		return "", nil
	}
	return db.AddNode(ctx, pubkey)
}

// Resolve pubkeys into node IDs. If a pubkey is missing (ID = ""), it applies the onMissing handler.
func (db DB) Resolve(ctx context.Context, pubkeys []string, onMissing MissingHandler) ([]graph.ID, error) {
	IDs, err := db.NodeIDs(ctx, pubkeys...)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve pubkeys: %w", err)
	}

	j := 0 // write index
	for i, ID := range IDs {
		switch ID {
		case "":
			ID, err = onMissing(ctx, db, pubkeys[i])
			if err != nil {
				return nil, fmt.Errorf("failed to resolve pubkey %q: %w", pubkeys[i], err)
			}

			if ID != "" {
				IDs[j] = ID
				j++
			}

		default:
			if j != i {
				IDs[j] = ID // write only if necessary
			}
			j++
		}
	}

	return IDs[:j], nil
}

// ScanNodes to return a batch of node IDs of size roughly proportional to limit.
// Limit controls how much "work" is invested in fetching the batch, hence it's not precise.
// Learn more about scan: https://redis.io/docs/latest/commands/scan/
func (db DB) ScanNodes(ctx context.Context, cursor uint64, limit int) ([]graph.ID, uint64, error) {
	match := KeyNodePrefix + "*"
	keys, cursor, err := db.Client.Scan(ctx, cursor, match, int64(limit)).Result()
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

// AllNodes returns all the node IDs stored in the database. It's not a blocking operation.
func (db DB) AllNodes(ctx context.Context) ([]graph.ID, error) {
	nodes := make([]graph.ID, 0, 100000)
	var batch []graph.ID
	var cursor uint64
	var err error

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("failed to fetch all nodes: %w", ctx.Err())

		default:
			// proceed with the scan
		}

		batch, cursor, err = db.ScanNodes(ctx, cursor, 10000)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch all nodes: %w", err)
		}

		nodes = append(nodes, batch...)
		if cursor == 0 {
			break
		}
	}

	return nodes, nil
}
