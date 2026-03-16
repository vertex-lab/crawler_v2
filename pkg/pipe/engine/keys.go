package engine

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	"github.com/pippellia-btc/slicex"
)

const keyLeakedKeys = "leaked_keys"

// storeLeaks derives the pubkey from each secret key and stores them in Redis in the HASH "leaked_keys".
// Invalid secret keys are simply skipped.
func (e *T) storeLeaks(secKeys []string, detectedAt time.Time) error {
	if len(secKeys) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pipe := e.graph.Client.Pipeline()
	for _, sk := range secKeys {
		pk, err := nostr.GetPublicKey(sk)
		if err != nil {
			continue
		}
		pipe.HSetNX(ctx, keyLeakedKeys, pk, keyTime(sk, detectedAt))
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to store leaked keys: %w", err)
	}
	return nil
}

var nsecRegex = regexp.MustCompile(`(?i)\bnsec1[023456789acdefghjklmnpqrstuvwxyz]{58}\b`)

// Pubkeys converts all secret keys to public keys.
// Invalid keys are simply skipped.
func Pubkeys(sks ...string) []string {
	pks := make([]string, 0, len(sks))
	for _, sk := range sks {
		pk, err := nostr.GetPublicKey(sk)
		if err != nil {
			continue
		}
		pks = append(pks, pk)
	}
	return pks
}

// ParseSecrets returns all valid (hex) secret keys encoded in the message as nip19 "nsec" values.
func ParseSecrets(message string) []string {
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

// keyTime returns a colon-separated secret key and unix timestamp.
func keyTime(sk string, t time.Time) string {
	return sk + ":" + strconv.FormatInt(t.Unix(), 10)
}

// parseKeyTime parses a colon-separated secret key and unix timestamp into a secret key and time.Time.
func parseKeyTime(s string) (string, time.Time, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return "", time.Time{}, errors.New("invalid key time format")
	}
	sk := parts[0]
	unix, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", time.Time{}, err
	}
	return sk, time.Unix(unix, 0), nil
}
