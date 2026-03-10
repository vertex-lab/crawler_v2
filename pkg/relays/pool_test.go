package relays

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ctx    = context.Background()
	primal = "wss://relay.primal.net"
	damus  = "wss://relay.damus.io"
	pip    = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
)

func TestRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	relay, err := New(ctx, damus)
	if err != nil {
		t.Fatalf("failed to create relay: %v", err)
	}
	defer relay.Close()
	t.Logf("connected to %s", relay.URL())

	filter := nostr.Filter{
		Kinds: []int{1},
		Limit: 10,
	}

	sub, err := relay.Subscribe("test", filter)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer sub.Close()

	var counter int
	eose := sub.EOSE()

	for {
		select {
		case <-ctx.Done():
			// all good!
			t.Logf("[%d] context done", counter)
			return

		case <-sub.Done():
			t.Fatalf("[%d] subscription was closed: %v", counter, sub.Err())
			return

		case <-eose:
			t.Logf("[%d] received eose", counter)
			eose = nil // avoid infinite loop

		case event := <-sub.Events():
			t.Logf("[%d] received event %s", counter, event.ID)
		}

		counter++
	}
}

func TestPoolStream(t *testing.T) {
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
