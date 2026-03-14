package engine

import (
	"regexp"
	"strings"

	"github.com/nbd-wtf/go-nostr/nip19"
	"github.com/pippellia-btc/slicex"
)

var nsecRegex = regexp.MustCompile(`(?i)\bnsec1[023456789acdefghjklmnpqrstuvwxyz]{58}\b`)

// ParseNsecs returns all valid (hex) secret keys encoded in the message as nip19 "nsec" values.
func ParseNsecs(message string) []string {
	if len(message) < 63 {
		return nil
	}

	candidates := nsecRegex.FindAllString(message, -1)
	if len(candidates) == 0 {
		return nil
	}

	keys := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		prefix, sk, err := nip19.Decode(strings.ToLower(candidate))
		if err != nil || prefix != "nsec" {
			continue
		}
		keys = append(keys, sk.(string))
	}
	return slicex.Unique(keys)
}
