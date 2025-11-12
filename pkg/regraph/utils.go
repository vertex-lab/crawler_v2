package regraph

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/walks"
)

var (
	testAddress = "localhost:6380"

	ErrValueIsNil       = errors.New("value is nil")
	ErrValueIsNotString = errors.New("failed to convert to string")
)

// flushAll deletes all the keys of all existing databases. This command never fails.
func (r RedisDB) flushAll() {
	r.Client.FlushAll(context.Background())
}

func node[ID string | graph.ID](id ID) string {
	return KeyNodePrefix + string(id)
}

func follows[ID string | graph.ID](id ID) string {
	return KeyFollowsPrefix + string(id)
}

func followers[ID string | graph.ID](id ID) string {
	return KeyFollowersPrefix + string(id)
}

func walksVisiting[ID string | graph.ID](id ID) string {
	return KeyWalksVisitingPrefix + string(id)
}

// toNodes converts a slice of strings to node IDs
func toNodes(s []string) []graph.ID {
	IDs := make([]graph.ID, len(s))
	for i, e := range s {
		IDs[i] = graph.ID(e)
	}
	return IDs
}

// toWalks converts a slice of strings to walk IDs
func toWalks(s []string) []walks.ID {
	IDs := make([]walks.ID, len(s))
	for i, e := range s {
		IDs[i] = walks.ID(e)
	}
	return IDs
}

// strings converts graph IDs to a slice of strings
func toStrings[ID graph.ID | walks.ID](ids []ID) []string {
	s := make([]string, len(ids))
	for i, id := range ids {
		s[i] = string(id)
	}
	return s
}

// parseNode() parses the map into a node structure
func parseNode(fields map[string]string) (*graph.Node, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	var node graph.Node
	for key, val := range fields {
		switch key {
		case NodeID:
			node.ID = graph.ID(val)

		case NodePubkey:
			node.Pubkey = val

		case NodeStatus:
			node.Status = val

		case NodeAddedTS:
			ts, err := parseTimestamp(val)
			if err != nil {
				return nil, fmt.Errorf("failed to parse node: %v", err)
			}
			node.Records = append(node.Records, graph.Record{Kind: graph.Addition, Timestamp: ts})

		case NodePromotionTS:
			ts, err := parseTimestamp(val)
			if err != nil {
				return nil, fmt.Errorf("failed to parse node: %v", err)
			}
			node.Records = append(node.Records, graph.Record{Kind: graph.Promotion, Timestamp: ts})

		case NodeDemotionTS:
			ts, err := parseTimestamp(val)
			if err != nil {
				return nil, fmt.Errorf("failed to parse node: %v", err)
			}
			node.Records = append(node.Records, graph.Record{Kind: graph.Demotion, Timestamp: ts})
		}
	}

	return &node, nil
}

// parseTimestamp() parses a unix timestamp string into a time.Time
func parseTimestamp(unix string) (time.Time, error) {
	ts, err := strconv.ParseInt(unix, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse timestamp: %w", err)
	}
	return time.Unix(ts, 0), nil
}

func formatWalk(walk walks.Walk) string {
	nodes := make([]string, walk.Len())
	for i, node := range walk.Path {
		nodes[i] = string(node)
	}
	return strings.Join(nodes, ",")
}

func parseWalk(s string) walks.Walk {
	nodes := strings.Split(s, ",")
	walk := walks.Walk{Path: make([]graph.ID, len(nodes))}
	for i, node := range nodes {
		walk.Path[i] = graph.ID(node)
	}
	return walk
}

func parseString(v any) (string, error) {
	if v == nil {
		return "", ErrValueIsNil
	}

	str, ok := v.(string)
	if !ok {
		return "", ErrValueIsNotString
	}

	return str, nil
}

func parseFloat(v any) (float64, error) {
	str, err := parseString(v)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(str, 64)
}

func parseInt(v any) (int, error) {
	str, err := parseString(v)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(str)
}
