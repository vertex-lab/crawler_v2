package relays

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

func TestPoolStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	l := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	pool, err := NewPool(
		[]string{damus, primal},
		WithLogger(l),
		WithRelayRetry(time.Second),
		WithSubscriptionRetry(time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	t.Logf("pool is up and running")

	since := nostr.Timestamp(time.Now().Add(-time.Minute).Unix())
	filter := nostr.Filter{
		Kinds: []int{1},
		Since: &since,
	}

	stream, err := pool.Stream("test", filter)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	var counter int
	for {
		select {
		case <-ctx.Done():
			// all good!
			t.Logf("[%d] context done", counter)
			return

		case <-stream.Done():
			t.Fatalf("[%d] stream was closed: %v", counter, stream.Err())
			return

		case event := <-stream.Events():
			t.Logf("[%d] received event %s", counter, event.ID)
		}

		counter++
	}
}

func TestPoolQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	l := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	pool, err := NewPool(
		[]string{damus, primal},
		WithLogger(l),
		WithRelayRetry(time.Second),
		WithSubscriptionRetry(time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	t.Logf("pool is up and running")

	since := nostr.Timestamp(time.Now().Add(-time.Minute).Unix())
	filter := nostr.Filter{
		Kinds: []int{1},
		Since: &since,
	}

	events, err := pool.Query(ctx, "test", filter)
	if err != nil {
		t.Fatal(err)
	}

	for i, e := range events {
		t.Logf("[%d] received event %s", i, e.ID)
	}
}
