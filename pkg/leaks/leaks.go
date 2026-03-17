// Package leaks provides functionality for finding, storing and retrieving leaked secret keys from Redis.
package leaks

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
	"github.com/redis/go-redis/v9"
)

const keyLeakedKeys = "leaked_keys"

// ErrNotFound is returned when no leak record exists for the given pubkey.
var ErrNotFound = errors.New("leak not found")

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

type DB struct {
	client *redis.Client
}

func NewDB(client *redis.Client) *DB {
	return &DB{client: client}
}

// Store derives the pubkey from each secret key (hex) and stores them in Redis in the HASH "leaked_keys".
// Invalid secret keys are simply skipped. StoreLeaks returns the list of pubkeys that were added.
func (db *DB) Store(ctx context.Context, secKeys []string, detectedAt time.Time) ([]string, error) {
	if len(secKeys) == 0 {
		return nil, nil
	}

	pubkeys := make([]string, 0, len(secKeys))
	cmds := make([]*redis.BoolCmd, 0, len(secKeys))
	pipe := db.client.Pipeline()

	for _, sk := range secKeys {
		pk, err := nostr.GetPublicKey(sk)
		if err != nil {
			continue
		}

		pubkeys = append(pubkeys, pk)
		cmds = append(cmds, pipe.HSetNX(ctx, keyLeakedKeys, pk, keyTime(sk, detectedAt)))
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("failed to store leaked keys: %w", err)
	}

	addedPks := make([]string, 0, len(pubkeys))
	for i := range cmds {
		added := cmds[i].Val()
		if added {
			addedPks = append(addedPks, pubkeys[i])
		}
	}
	return addedPks, nil
}

// Read returns the secret key and detection time for the given pubkey.
// If no record is found, it returns ErrNotFound.
func (db *DB) Read(ctx context.Context, pubkey string) (string, time.Time, error) {
	val, err := db.client.HGet(ctx, keyLeakedKeys, pubkey).Result()
	if errors.Is(err, redis.Nil) {
		return "", time.Time{}, ErrNotFound
	}
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to read leaked key: %w", err)
	}

	sk, t, err := parseKeyTime(val)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to parse leaked key: %w", err)
	}
	return sk, t, nil
}

// Record represents a leaked key record.
type Record struct {
	Pubkey     string
	Seckey     string
	DetectedAt time.Time
}

// Validate returns an error if the record is invalid.
// E.g. pubkey is invalid or doesn't match the seckey.
func (r Record) Validate() error {
	if len(r.Pubkey) != 64 || !nostr.IsValidPublicKey(r.Pubkey) {
		return fmt.Errorf("invalid pubkey")
	}
	if len(r.Seckey) != 64 || !nostr.IsValid32ByteHex(r.Seckey) {
		return fmt.Errorf("invalid seckey")
	}
	derivedPk, err := nostr.GetPublicKey(r.Seckey)
	if err != nil {
		return fmt.Errorf("failed to derive pubkey from seckey: %w", err)
	}
	if derivedPk != r.Pubkey {
		return fmt.Errorf("pubkey doesn't match seckey")
	}
	return nil
}

// All returns all leak records from the database. The order or records is non-deterministic.
func (db *DB) All(ctx context.Context) ([]Record, error) {
	result, err := db.client.HGetAll(ctx, keyLeakedKeys).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get all leaked keys: %w", err)
	}

	records := make([]Record, 0, len(result))
	for pk, val := range result {
		sk, t, err := parseKeyTime(val)
		if err != nil {
			return nil, fmt.Errorf("failed to parse leaked key: %w", err)
		}

		records = append(records, Record{
			Pubkey:     pk,
			Seckey:     sk,
			DetectedAt: t,
		})
	}
	return records, nil
}

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
