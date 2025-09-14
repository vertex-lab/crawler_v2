package pipe

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/nbd-wtf/go-nostr"
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

			if err := recordEvent(db, event); err != nil {
				log.Printf("Recorder: %v", err)
			}

			if err := forward(event); err != nil {
				log.Printf("Recorder: %v", err)
			}

		case <-timer:
			// at the beginning of the next day (midnight), finalize
			// the statistics of the previous day
			yesterday := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")

			if err := finalizeStats(db, yesterday); err != nil {
				log.Printf("Recorder: %v", err)
			}

			log.Printf("Recorder: finalized stats for %s", yesterday)
			timer = midnightTimer()
		}
	}
}

const (
	separator        = ":"
	KeyStats         = "stats"
	KeyKind          = "kind"
	KeyActivePubkeys = "active_pubkeys"
	KeyTotalPubkeys  = "total_pubkeys"
)

func stats(day string) string         { return KeyStats + separator + day }
func kind(kind int) string            { return KeyKind + separator + strconv.Itoa(kind) }
func activePubkeys(day string) string { return KeyActivePubkeys + separator + day }

// recordEvent in redis. In particular:
// - add pubkey to the active_pubkeys HLL (~1% error)
// - increment the count for event.Kind.
//
// The latter assumes that [Recorder] does not receives (many) duplicate events.
// We could have used an HLL for that as well, but it's unclear whether
// it would be more precise given the HLL is a probabilistic method with ~1% error.
func recordEvent(db redb.RedisDB, event *nostr.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	today := time.Now().UTC().Format("2006-01-02")

	pipe := db.Client.TxPipeline()
	pipe.HIncrBy(ctx, stats(today), kind(event.Kind), 1)
	pipe.PFAdd(ctx, activePubkeys(today), event.PubKey)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to record event: pipeline failed: %w", err)
	}
	return nil
}

// finalizeStats for a particular day, adding to stats the total and active pubkey counts.
func finalizeStats(db redb.RedisDB, day string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	totalPubkeys, err := db.NodeCount(ctx)
	if err != nil {
		return fmt.Errorf("finalizeStats: %v", err)
	}

	activePubkeys, err := db.Client.PFCount(ctx, activePubkeys(day)).Result()
	if err != nil {
		return fmt.Errorf("finalizeStats: failed to fetch the active pubkeys count: %w", err)
	}

	err = db.Client.HSet(ctx, stats(day),
		KeyActivePubkeys, activePubkeys,
		KeyTotalPubkeys, totalPubkeys,
	).Err()

	if err != nil {
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
