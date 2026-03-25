// Package events provides event utilities and validation functions.
package events

import (
	"fmt"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/slicex"
	"github.com/vertex-lab/crawler_v2/pkg/leaks"
	"github.com/vertex-lab/crawler_v2/pkg/relays"
)

const (
	MaxTags    = 50_000
	MaxContent = 1_000_000
)

// TooBig returns an error if the event is too big.
func TooBig(e *nostr.Event) error {
	if len(e.Tags) > MaxTags {
		return fmt.Errorf("event with ID %s has too many tags: %d", e.ID, len(e.Tags))
	}
	if len(e.Content) > MaxContent {
		return fmt.Errorf("event with ID %s has too much content: %d", e.ID, len(e.Content))
	}
	return nil
}

// ParseTags returns unique values from all tags matching the given key.
// For the sake of efficiency, values are NOT validated.
func ParseTags(key string, tags nostr.Tags) []string {
	if len(tags) == 0 {
		return nil
	}

	size := min(len(tags), MaxTags)
	values := make([]string, 0, size)

	for _, tag := range tags {
		if len(values) > MaxTags {
			break
		}

		if len(tag) < 2 || tag[0] != key {
			continue
		}

		values = append(values, tag[1])
	}
	return slicex.Unique(values)
}

// ParseRelays parses unique and valid relay URLs from the "r" tags.
func ParseRelays(tags nostr.Tags) []string {
	candidates := ParseTags("r", tags)
	if len(candidates) == 0 {
		return nil
	}

	urls := make([]string, 0, len(candidates))
	for _, url := range candidates {
		if err := relays.ValidateURL(url); err != nil {
			continue
		}
		urls = append(urls, url)
	}
	return urls
}

// ContainsLeak returns true if the event contains a leak candidate in either the tags or content.
func ContainsLeak(e *nostr.Event) bool {
	if leaks.Found(e.Content) {
		return true
	}
	for _, tag := range e.Tags {
		for _, v := range tag {
			if leaks.Found(v) {
				return true
			}
		}
	}
	return false
}

// ParseLeaks returns all valid hex secret keys encoded in the message as nip19 "nsec" values.
// Resulting secret keys are deduplicated.
func ParseLeaks(e *nostr.Event) []string {
	keys := leaks.Parse(e.Content)
	for _, tag := range e.Tags {
		for _, v := range tag {
			keys = append(keys, leaks.Parse(v)...)
		}
	}
	return slicex.Unique(keys)
}
