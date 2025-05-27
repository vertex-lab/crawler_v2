package redb

import (
	"context"
	"github/pippellia-btc/crawler/pkg/graph"
	"strconv"
	"time"
)

var (
	testAddress = "localhost:6380"
)

// flushAll deletes all the keys of all existing databases. This command never fails.
func (r RedisDB) flushAll() {
	r.client.FlushAll(context.Background())
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

// ids converts a slice of strings to IDs
func toIDs(s []string) []graph.ID {
	IDs := make([]graph.ID, len(s))
	for i, e := range s {
		IDs[i] = graph.ID(e)
	}
	return IDs
}

// strings converts graph IDs to a slice of strings
func toStrings(ids []graph.ID) []string {
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
				return nil, err
			}
			node.Records = append(node.Records, graph.Record{Kind: graph.Addition, Timestamp: ts})

		case NodePromotionTS:
			ts, err := parseTimestamp(val)
			if err != nil {
				return nil, err
			}
			node.Records = append(node.Records, graph.Record{Kind: graph.Promotion, Timestamp: ts})

		case NodeDemotionTS:
			ts, err := parseTimestamp(val)
			if err != nil {
				return nil, err
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
		return time.Time{}, err
	}
	return time.Unix(ts, 0), nil
}
