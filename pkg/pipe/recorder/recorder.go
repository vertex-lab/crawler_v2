// Package recorder provides a background goroutine that records events and creates statistics.
package recorder

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler_v2/pkg/pipe"
	"github.com/vertex-lab/crawler_v2/pkg/stats"
)

var (
	ErrIsClosing = errors.New("recorder is closing")
	ErrQueueFull = errors.New("recorder queue is full")
)

// T represents the recorder, which records events and creates statistics.
type T struct {
	stats     *stats.DB
	events    chan *nostr.Event
	isClosing atomic.Bool
}

func New(c Config, redis *redis.Client) *T {
	return &T{
		stats:  stats.NewDB(redis, pipe.ContentKinds),
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
					if err := r.stats.Record(e); err != nil {
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
			if err := r.stats.Record(e); err != nil {
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

			if err := r.stats.Aggregate(day); err != nil {
				slog.Error("Recorder: failed to finalize stats", "error", err)
			} else {
				slog.Info("Recorder: finalized stats", "day", day)
			}
		}
	}
}

// nextMidnight returns a timer that will fire at the next midnight UTC.
func nextMidnight() <-chan time.Time {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	wait := time.Until(midnight)
	return time.After(wait)
}

// yesterday returns the previous day in UTC.
func yesterday() string {
	return time.Now().UTC().AddDate(0, 0, -1).Format(stats.DayFormat)
}
