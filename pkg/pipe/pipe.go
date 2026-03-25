// Package pipe contains subpackages, each defining a specific entity
// in the crawler pipeline (e.g. Firehose, Engine...).
package pipe

import (
	"context"
	"fmt"
	"slices"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/arbiter"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
)

var (
	ContentKinds = []int{
		nostr.KindTextNote,
		nostr.KindComment,
		nostr.KindArticle,
		20, // Picture Event
		21, // Video Event
		22, // "Tik-Tok" Video Event
	}

	EngagementKinds = []int{
		nostr.KindReaction,
		nostr.KindRepost,
		nostr.KindGenericRepost,
		nostr.KindZap,
		nostr.KindNutZap,
	}

	ProfileKinds = []int{
		nostr.KindProfileMetadata,
		nostr.KindFollowList,
		nostr.KindMuteList,
		nostr.KindRelayListMetadata,
		nostr.KindUserServerList,
	}

	AllKinds = slices.Concat(
		ContentKinds,
		EngagementKinds,
		ProfileKinds,
	)
)

// InitGraph by adding and promoting the provided pubkeys.
func InitGraph(ctx context.Context, db regraph.DB, pubkeys []string) error {
	if len(pubkeys) == 0 {
		return fmt.Errorf("InitGraph: init pubkeys are empty")
	}

	var initNodes = make([]graph.ID, len(pubkeys))
	var err error

	for i, pk := range pubkeys {
		initNodes[i], err = db.AddNode(ctx, pk)
		if err != nil {
			return fmt.Errorf("InitGraph: %v", err)
		}
	}

	for _, node := range initNodes {
		if err := arbiter.Promote(db, node); err != nil {
			return fmt.Errorf("InitGraph: %v", err)
		}
	}
	return nil
}
