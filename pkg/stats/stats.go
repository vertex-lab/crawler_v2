// Package stats provides a database for recording, aggregating and querying statistics about nostr events.
package stats

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
)

// DayFormat is the format used for day keys throughout the stats package.
const DayFormat = "2006-01-02"

type DB struct {
	client       *redis.Client
	creatorKinds []int // kinds whose authors count as "creators"
}

func New(c *redis.Client, creatorKinds []int) *DB {
	return &DB{client: c, creatorKinds: creatorKinds}
}

const (
	separator = ":"
	KeyStats  = "stats"
	KeyKind   = "kind"

	KeyEvents         = "events"
	KeyActivePubkeys  = "active_pubkeys"
	KeyCreatorPubkeys = "creator_pubkeys"
	KeyTotalPubkeys   = "total_pubkeys"

	expiration = 30 * 24 * time.Hour
)

func stats(day string) string          { return KeyStats + separator + day }
func kind(k int) string                { return KeyKind + separator + strconv.Itoa(k) }
func activePubkeys(day string) string  { return KeyActivePubkeys + separator + day }
func creatorPubkeys(day string) string { return KeyCreatorPubkeys + separator + day }
func events(day string, k int) string  { return KeyEvents + separator + day + separator + kind(k) }

// Record records an event in redis, by updating the appropriate HLLs (~1% error):
// - add event.ID to the events:<today>:kind:<event.Kind>
// - add pubkey to the active_pubkeys:<today>
// - add pubkey to the creator_pubkeys:<today> if event is in [core.ContentKinds]
func (db *DB) Record(e *nostr.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	today := time.Now().UTC().Format(DayFormat)
	pipe := db.client.TxPipeline()

	pipe.PFAdd(ctx, events(today, e.Kind), e.ID)
	pipe.ExpireNX(ctx, events(today, e.Kind), expiration)
	pipe.PFAdd(ctx, activePubkeys(today), e.PubKey)
	pipe.ExpireNX(ctx, activePubkeys(today), expiration)

	if slices.Contains(db.creatorKinds, e.Kind) {
		pipe.PFAdd(ctx, creatorPubkeys(today), e.PubKey)
		pipe.ExpireNX(ctx, creatorPubkeys(today), expiration)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to record event with ID %s: pipeline failed: %w", e.ID, err)
	}
	return nil
}

// Aggregate the statistics for a particular day.
// All HLL transient accumulators (e.g. events:<today>..., activePubkeys:<today>, creatorPubkeys:<today>)
// are aggregated into the permanent stats key.
func (db *DB) Aggregate(day string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := KeyEvents + separator + day + separator
	keys, err := db.client.Keys(ctx, prefix+"*").Result()
	if err != nil {
		return fmt.Errorf("failed to fetch all the events keys: %w", err)
	}

	pipe := db.client.Pipeline()
	kinds := make([]string, len(keys))
	kindCounts := make([]*redis.IntCmd, len(keys))

	for i, key := range keys {
		kinds[i] = strings.TrimPrefix(key, prefix)
		kindCounts[i] = pipe.PFCount(ctx, key)
	}

	actives := pipe.PFCount(ctx, activePubkeys(day))
	creators := pipe.PFCount(ctx, creatorPubkeys(day))
	total := pipe.HLen(ctx, regraph.KeyKeyIndex)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("pipeline failed: %w", err)
	}

	statistics := []any{
		KeyActivePubkeys, actives.Val(),
		KeyCreatorPubkeys, creators.Val(),
		KeyTotalPubkeys, total.Val(),
	}

	for i := range kindCounts {
		statistics = append(statistics, kinds[i], kindCounts[i].Val())
	}

	if err = db.client.HSet(ctx, stats(day), statistics...).Err(); err != nil {
		return fmt.Errorf("finalizeStats: failed to write: %w", err)
	}
	return nil
}

// nextMidnight returns a timer that will fire at the next midnight UTC.
func nextMidnight() <-chan time.Time {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	wait := time.Until(midnight)
	return time.After(wait)
}

// today returns the current day in UTC.
func today() string {
	return time.Now().UTC().Format("2006-01-02")
}

// yesterday returns the previous day in UTC.
func yesterday() string {
	return time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
}
