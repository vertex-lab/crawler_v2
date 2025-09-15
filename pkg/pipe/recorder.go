package pipe

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler_v2/pkg/redb"
)

func Recorder(
	ctx context.Context,
	db redb.RedisDB,
	events <-chan *nostr.Event,
	forward Forward[*nostr.Event],
) {
	log.Println("Recorder: ready")
	defer log.Println("Recorder: shut down")

	timer := midnightTimer()

	for {
		select {
		case <-ctx.Done():
			return

		case event, ok := <-events:
			if !ok {
				return
			}

			err := recordEvent(ctx, db, event)
			if err != nil && ctx.Err() == nil {
				log.Printf("Recorder: %v", err)
			}

			if err := forward(event); err != nil {
				log.Printf("Recorder: %v", err)
			}

		case <-timer:
			// at the beginning of the next day (midnight), finalize
			// the statistics of the previous day
			yesterday := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")

			err := finalizeStats(db, yesterday)
			if err != nil {
				log.Printf("Recorder: %v", err)
			} else {
				log.Printf("Recorder: finalized stats for %s", yesterday)
			}

			timer = midnightTimer()
		}
	}
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

// recordEvent in redis, by updating the appropriate HLLs (~1% error):
// - add event.ID to the events:<today>:kind:<event.Kind>
// - add pubkey to the active_pubkeys:<today>
// - add pubkey to the creator_pubkeys:<today> if event is in [contentKinds]
func recordEvent(ctx context.Context, db redb.RedisDB, event *nostr.Event) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	today := time.Now().UTC().Format("2006-01-02")

	pipe := db.Client.TxPipeline()
	pipe.PFAdd(ctx, events(today, event.Kind), event.ID)
	pipe.ExpireNX(ctx, events(today, event.Kind), expiration)

	pipe.PFAdd(ctx, activePubkeys(today), event.PubKey)
	pipe.ExpireNX(ctx, activePubkeys(today), expiration)

	if slices.Contains(contentKinds, event.Kind) {
		pipe.PFAdd(ctx, creatorPubkeys(today), event.PubKey)
		pipe.ExpireNX(ctx, creatorPubkeys(today), expiration)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to record event with ID %s: pipeline failed: %w", event.ID, err)
	}
	return nil
}

// finalizeStats for a particular day, adding to stats the total and active pubkey counts.
func finalizeStats(db redb.RedisDB, day string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prefix := KeyEvents + separator + day + separator
	eventKeys, err := db.Client.Keys(ctx, prefix+"*").Result()
	if err != nil {
		return fmt.Errorf("finalizeStats: failed to fetch all the events keys: %w", err)
	}

	pipe := db.Client.Pipeline()
	kinds := make([]string, len(eventKeys))
	kindCounts := make([]*redis.IntCmd, len(eventKeys))

	for i, key := range eventKeys {
		kinds[i] = strings.TrimPrefix(key, prefix)
		kindCounts[i] = pipe.PFCount(ctx, key)
	}

	actives := pipe.PFCount(ctx, activePubkeys(day))
	creators := pipe.PFCount(ctx, creatorPubkeys(day))
	total := pipe.HLen(ctx, redb.KeyKeyIndex)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("finalizeStats: pipeline failed: %w", err)
	}

	statistics := []any{
		KeyActivePubkeys, actives.Val(),
		KeyCreatorPubkeys, creators.Val(),
		KeyTotalPubkeys, total.Val(),
	}

	for i := range kindCounts {
		statistics = append(statistics, kinds[i], kindCounts[i].Val())
	}

	if err = db.Client.HSet(ctx, stats(day), statistics...).Err(); err != nil {
		return fmt.Errorf("finalizeStats: failed to write: %w", err)
	}
	return nil
}

func midnightTimer() <-chan time.Time {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	wait := time.Until(midnight)
	return time.After(wait)
}
