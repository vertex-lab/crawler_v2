package recorder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
	core "github.com/vertex-lab/crawler_v2/pkg/pipe"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
)

var (
	ErrIsClosing = errors.New("recorder is closing")
	ErrQueueFull = errors.New("recorder queue is full")
)

// T represents the recorder, which records events and creates statistics.
type T struct {
	db        regraph.DB
	events    chan *nostr.Event
	isClosing atomic.Bool
}

func New(c Config, db regraph.DB) *T {
	return &T{
		db:     db,
		events: make(chan *nostr.Event, c.Queue),
	}
}

// Enqueue adds an event to the recorder's queue.
func (r *T) Enqueue(e *nostr.Event) error {
	if e == nil {
		return errors.New("event is nil")
	}
	if r.isClosing.Load() {
		return ErrIsClosing
	}

	select {
	case r.events <- e:
		return nil
	default:
		return ErrQueueFull
	}
}

func (r *T) Run(ctx context.Context, forward func(*nostr.Event) error) {
	slog.Info("Recorder: ready")
	defer slog.Info("Recorder: shut down")

	timer := nextMidnight()

	for {
		select {
		case <-ctx.Done():
			// record all the buffered events and return
			r.isClosing.Store(true)
			for {
				select {
				case e := <-r.events:
					if err := r.record(e); err != nil {
						slog.Error("Recorder: failed to record event", "error", err, "id", e.ID, "kind", e.Kind, "pubkey", e.PubKey)
					}

					if err := forward(e); err != nil {
						slog.Error("Recorder: failed to forward", "error", err)
					}
				default:
					return
				}
			}

		case e := <-r.events:
			if err := r.record(e); err != nil {
				slog.Error("Recorder: failed to record event", "error", err, "id", e.ID, "kind", e.Kind, "pubkey", e.PubKey)
			}

			if err := forward(e); err != nil {
				slog.Error("Recorder: failed to forward", "error", err)
			}

		case <-timer:
			// at the beginning of the next day (midnight), finalize
			// the statistics of the previous day
			timer = nextMidnight()
			day := yesterday()

			if err := r.finalize(day); err != nil {
				slog.Error("Recorder: failed to finalize stats", "error", err)
			} else {
				slog.Info("Recorder: finalized stats", "day", day)
			}
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

// record records an event in redis, by updating the appropriate HLLs (~1% error):
// - add event.ID to the events:<today>:kind:<event.Kind>
// - add pubkey to the active_pubkeys:<today>
// - add pubkey to the creator_pubkeys:<today> if event is in [core.ContentKinds]
func (r *T) record(e *nostr.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	today := today()
	pipe := r.db.Client.TxPipeline()
	pipe.PFAdd(ctx, events(today, e.Kind), e.ID)
	pipe.ExpireNX(ctx, events(today, e.Kind), expiration)

	pipe.PFAdd(ctx, activePubkeys(today), e.PubKey)
	pipe.ExpireNX(ctx, activePubkeys(today), expiration)

	if slices.Contains(core.ContentKinds, e.Kind) {
		pipe.PFAdd(ctx, creatorPubkeys(today), e.PubKey)
		pipe.ExpireNX(ctx, creatorPubkeys(today), expiration)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to record event with ID %s: pipeline failed: %w", e.ID, err)
	}
	return nil
}

// finalize for a particular day, adding to stats the total and active pubkey counts.
func (r *T) finalize(day string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := KeyEvents + separator + day + separator
	keys, err := r.db.Client.Keys(ctx, prefix+"*").Result()
	if err != nil {
		return fmt.Errorf("finalizeStats: failed to fetch all the events keys: %w", err)
	}

	pipe := r.db.Client.Pipeline()
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

	if err = r.db.Client.HSet(ctx, stats(day), statistics...).Err(); err != nil {
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
