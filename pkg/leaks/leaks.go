// Package leaks provides functionality for finding, storing and retrieving leaked secret keys from Redis.
package leaks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	"github.com/pippellia-btc/slicex"
	"github.com/redis/go-redis/v9"
)

const (
	// ProofMessage is the message that is signed by the secret key to prove the leak.
	proofMessage = "vertex-proof-leak:"

	// keyLeak is the prefix for all leak keys in Redis.
	keyLeak = "leak:"
)

// ProofMessage returns the message that should be signed by the secret key to prove the leak.
func ProofMessage(pk string) string {
	return proofMessage + pk
}

// Key returns the Redis key for a given pubkey leak record.
// It does not validate the pubkey format.
func Key(pk string) string {
	return keyLeak + pk
}

// Status represents the confidence level of a leaked key detection.
type Status string

const (
	// Confirmed means the leak has been verified with certainty.
	Confirmed Status = "confirmed"
	// Suspected means the leak has been detected but not yet verified.
	Suspected Status = "suspected"
)

// ErrNotFound is returned when no leak record exists for the given pubkey.
var ErrNotFound = errors.New("leak not found")

type DB struct {
	client *redis.Client
}

func NewDB(client *redis.Client) *DB {
	return &DB{client: client}
}

// Store derives the pubkey from each candidate secret key (hex) and stores them in Redis
// as individual hashes "leak:<pubkey>" with fields status, detected_at, proof and secret.
// Invalid secret keys are simply skipped. Store returns the list of pubkeys that were added.
func (db *DB) Store(ctx context.Context, seckeys []string, detectedAt time.Time) ([]string, error) {
	if len(seckeys) == 0 {
		return nil, nil
	}

	type entry struct {
		pk  string
		sk  string
		cmd *redis.BoolCmd
	}

	entries := make([]entry, 0, len(seckeys))
	pipe := db.client.Pipeline()

	for _, sk := range seckeys {
		pk, err := nostr.GetPublicKey(sk)
		if err != nil || !nostr.IsValidPublicKey(pk) {
			continue
		}
		// store the command so we know if "secret" field was already set.
		// In that case we won't update the hash, preferring to keep the existing value.
		cmd := pipe.HSetNX(ctx, Key(pk), "secret", sk)
		entries = append(entries, entry{pk: pk, sk: sk, cmd: cmd})
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("failed to store leaked keys: %w", err)
	}

	addedPks := make([]string, 0, len(entries))
	unix := strconv.FormatInt(detectedAt.Unix(), 10)
	pipe = db.client.Pipeline()

	for _, e := range entries {
		if !e.cmd.Val() {
			// "secret" field was already set, so we won't update the metadata.
			continue
		}

		addedPks = append(addedPks, e.pk)
		proof, err := proof(e.sk, e.pk)
		if err != nil {
			return nil, fmt.Errorf("failed to sign proof for %s: %w", e.pk, err)
		}

		pipe.HSet(ctx, Key(e.pk),
			"status", string(Confirmed),
			"detected_at", unix,
			"proof", proof,
		)
	}

	if len(addedPks) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("failed to store leaked key metadata: %w", err)
		}
	}
	return addedPks, nil
}

// Read returns the leak record for the given pubkey.
// If no record is found, it returns ErrNotFound.
func (db *DB) Read(ctx context.Context, pubkey string) (Record, error) {
	fields, err := db.client.HGetAll(ctx, Key(pubkey)).Result()
	if err != nil {
		return Record{}, fmt.Errorf("failed to read leaked key: %w", err)
	}
	if len(fields) == 0 {
		return Record{}, ErrNotFound
	}

	r, err := parseRecord(pubkey, fields)
	if err != nil {
		return Record{}, fmt.Errorf("failed to parse leak record for %s: %w", pubkey, err)
	}
	return r, nil
}

// Record represents a leaked key record.
// It doesn't include the seckey, only the pubkey and metadata.
type Record struct {
	Pubkey     string
	DetectedAt time.Time
	Status     Status
	Proof      string
}

// Validate returns an error if the record is invalid.
// It checks the pubkey format, the status, and verifies the proof signature.
func (r Record) Validate() error {
	if r.Status == Suspected {
		if len(r.Pubkey) != 64 || !nostr.IsValidPublicKey(r.Pubkey) {
			return fmt.Errorf("invalid pubkey: %s", r.Pubkey)
		}
		return nil
	}

	if r.Status != Confirmed {
		return fmt.Errorf("invalid status: %q", r.Status)
	}

	pkBytes, err := hex.DecodeString(r.Pubkey)
	if err != nil {
		return fmt.Errorf("invalid pubkey hex: %w", err)
	}
	pubKey, err := schnorr.ParsePubKey(pkBytes)
	if err != nil {
		return fmt.Errorf("invalid pubkey: %w", err)
	}
	sigBytes, err := hex.DecodeString(r.Proof)
	if err != nil {
		return fmt.Errorf("invalid proof hex: %w", err)
	}
	sig, err := schnorr.ParseSignature(sigBytes)
	if err != nil {
		return fmt.Errorf("invalid proof signature: %w", err)
	}
	hash := sha256.Sum256([]byte(ProofMessage(r.Pubkey)))
	if !sig.Verify(hash[:], pubKey) {
		return fmt.Errorf("proof verification failed")
	}
	return nil
}

// All returns all leak records from the database. The order of records is non-deterministic.
func (db *DB) All(ctx context.Context) ([]Record, error) {
	var cursor uint64
	var keys []string
	for {
		var batch []string
		var err error
		batch, cursor, err = db.client.Scan(ctx, cursor, keyLeak+"*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan leaked keys: %w", err)
		}
		keys = append(keys, batch...)
		if cursor == 0 {
			break
		}
	}

	records := make([]Record, 0, len(keys))
	for _, key := range keys {
		pk := strings.TrimPrefix(key, keyLeak)
		fields, err := db.client.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to read leaked key %s: %w", key, err)
		}
		if len(fields) == 0 {
			continue
		}
		r, err := parseRecord(pk, fields)
		if err != nil {
			return nil, fmt.Errorf("failed to parse leak record for %s: %w", pk, err)
		}
		records = append(records, r)
	}
	return records, nil
}

// parseRecord builds a Record from a Redis hash fields map.
// status is the only required field; all others default to their zero value if absent.
func parseRecord(pubkey string, fields map[string]string) (Record, error) {
	statusStr, ok := fields["status"]
	if !ok {
		return Record{}, errors.New("missing field: status")
	}
	status := Status(statusStr)
	if status != Confirmed && status != Suspected {
		return Record{}, fmt.Errorf("invalid status value: %q", statusStr)
	}

	var detectedAt time.Time
	if detectedAtStr, ok := fields["detected_at"]; ok {
		unix, err := strconv.ParseInt(detectedAtStr, 10, 64)
		if err != nil {
			return Record{}, fmt.Errorf("invalid detected_at value: %w", err)
		}
		detectedAt = time.Unix(unix, 0)
	}

	return Record{
		Pubkey:     pubkey,
		DetectedAt: detectedAt,
		Status:     status,
		Proof:      fields["proof"],
	}, nil
}

// proof signs the message "[ProofMessage]<pubkey>" with the given secret key
// using Schnorr and returns the hex-encoded signature.
func proof(seckey, pubkey string) (string, error) {
	msg := ProofMessage(pubkey)
	hash := sha256.Sum256([]byte(msg))

	skBytes, err := hex.DecodeString(seckey)
	if err != nil {
		return "", fmt.Errorf("invalid secret key hex: %w", err)
	}
	privKey, _ := btcec.PrivKeyFromBytes(skBytes)
	sig, err := schnorr.Sign(privKey, hash[:], schnorr.FastSign())
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(sig.Serialize()), nil
}

var nsecRegex = regexp.MustCompile(`(?i)\bnsec1[023456789acdefghjklmnpqrstuvwxyz]{58}\b`)

// ParseNsecs returns all valid hex secret keys encoded in the message as nip19 "nsec" values.
// Resulting secret keys are deduplicated.
func ParseNsecs(message string) []string {
	if !Contains(message) {
		return nil
	}

	candidates := nsecRegex.FindAllString(message, -1)
	if len(candidates) == 0 {
		return nil
	}

	keys := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		prefix, v, err := nip19.Decode(strings.ToLower(candidate))
		if err != nil || prefix != "nsec" {
			continue
		}
		// nip19.Decode only checks that the payload is 32 bytes; it does not
		// validate that the scalar is in [1, n-1], so we do it here.
		sk := v.(string)
		pk, err := nostr.GetPublicKey(sk)
		if err != nil || !nostr.IsValidPublicKey(pk) {
			continue
		}

		keys = append(keys, sk)
	}
	return slicex.Unique(keys)
}

// nsecPermutations contains all possible upper/lower case permutations of the "nsec1" prefix.
var nsecPermutations = []string{
	"nsec1", "Nsec1", "NSEC1", "NSec1", "nsEc1", "NsEc1", "nSEc1", "NSEc1",
	"nseC1", "NseC1", "nSeC1", "NSeC1", "nsEC1", "NsEC1", "nSEC1", "nSec1",
}

// Contains reports whether msg contains at least one nsec candidate.
// It's faster than ParseNsecs as it stops at the first match and doesn't perform full validation.
func Contains(msg string) bool {
	if len(msg) < 63 {
		return false
	}

	for _, prefix := range nsecPermutations {
		if strings.Contains(msg, prefix) {
			return true
		}
	}
	return false
}
