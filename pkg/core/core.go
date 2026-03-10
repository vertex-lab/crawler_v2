// Package core contains the core logic for the crawler.
// Each subpackage defines a specific entity in the crawler pipeline (e.g. Firehose, Engine...)
package core

import (
	"fmt"
	"slices"

	"github.com/nbd-wtf/go-nostr"
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

const (
	MaxTags    = 50_000
	MaxContent = 1_000_000
)

// EventTooBig returns an error if the event is too big.
func EventTooBig(e *nostr.Event) error {
	if len(e.Tags) > MaxTags {
		return fmt.Errorf("event with ID %s has too many tags: %d", e.ID, len(e.Tags))
	}
	if len(e.Content) > MaxContent {
		return fmt.Errorf("event with ID %s has too much content: %d", e.ID, len(e.Content))
	}
	return nil
}
