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

// Day holds the aggregated statistics for a single day.
type Day struct {
	ActivePubkeys  int64
	CreatorPubkeys int64
	TotalPubkeys   int64
	EventsByKind   map[int]int64
}

type DB struct {
	client       *redis.Client
	creatorKinds []int // kinds whose authors count as "creators"
}

func NewDB(c *redis.Client, creatorKinds []int) *DB {
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

// Read returns the stats for the given days, keyed by day string (DayFormat).
// Days with no recorded data are omitted from the result.
func (db *DB) Read(ctx context.Context, days []string) (map[string]Day, error) {
	if len(days) == 0 {
		return nil, nil
	}

	pipe := db.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(days))
	for i, day := range days {
		cmds[i] = pipe.HGetAll(ctx, stats(day))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("failed to read stats: %w", err)
	}

	result := make(map[string]Day)
	for i, day := range days {
		fields := cmds[i].Val()
		if len(fields) == 0 {
			continue
		}
		s, err := parseStats(fields)
		if err != nil {
			return nil, fmt.Errorf("failed to parse stats for day %s: %w", day, err)
		}
		result[day] = s
	}
	return result, nil
}

// parseStats parses a raw map[string]string from a stats hash into a Day struct.
// It returns an error if any numeric value cannot be parsed.
func parseStats(fields map[string]string) (Day, error) {
	s := Day{EventsByKind: make(map[int]int64)}
	for field, raw := range fields {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return Day{}, fmt.Errorf("field %q has non-integer value %q: %w", field, raw, err)
		}
		switch field {
		case KeyActivePubkeys:
			s.ActivePubkeys = v
		case KeyCreatorPubkeys:
			s.CreatorPubkeys = v
		case KeyTotalPubkeys:
			s.TotalPubkeys = v
		default:
			// field is "kind:<k>"
			kStr := strings.TrimPrefix(field, KeyKind+separator)
			k, err := strconv.Atoi(kStr)
			if err != nil {
				continue
			}
			s.EventsByKind[k] = v
		}
	}
	return s, nil
}

// DailyRange returns a slice of day strings for every day in the range [start, end),
// truncated to midnight UTC.
func DailyRange(start, end time.Time) []string {
	start = start.UTC().Truncate(24 * time.Hour)
	end = end.UTC().Truncate(24 * time.Hour)

	var days []string
	for d := start; d.Before(end); d = d.AddDate(0, 0, 1) {
		days = append(days, d.Format(DayFormat))
	}
	return days
}
