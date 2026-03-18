package firehose

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/pool"
)

var (
	ctx        = context.Background()
	pip string = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
)

// mockDB implements [DB] for testing.
type mockDB struct {
	exists map[string]bool
	err    error
	calls  int
}

func (m *mockDB) Exists(_ context.Context, pubkey string) (bool, error) {
	m.calls++
	return m.exists[pubkey], m.err
}

// Manually publish an event with the pip's pubkey and see if the events gets printed.
// Works only with `go test`
func TestFirehose(t *testing.T) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()

	pool, err := pool.New(pool.NewConfig())
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	config := NewConfig()
	policy := TrustPolicy(&mockDB{exists: map[string]bool{pip: true}}, 128)

	firehose := New(config, pool, policy)
	firehose.Run(ctx, func(e *nostr.Event) error {
		fmt.Printf("ID: %s, Kind: %d, Pubkey: %s\n", e.ID, e.Kind, e.PubKey)
		return nil
	})
}

func TestTrustPolicy(t *testing.T) {
	pubkey := "abc123"
	event := &nostr.Event{PubKey: pubkey}

	t.Run("allows pubkey present in store", func(t *testing.T) {
		store := &mockDB{exists: map[string]bool{pubkey: true}}
		gate := TrustPolicy(store, 128)
		if !gate.Allow(ctx, event) {
			t.Error("expected Allows to return true for a known pubkey")
		}
	})

	t.Run("denies pubkey absent from store", func(t *testing.T) {
		store := &mockDB{exists: map[string]bool{}}
		gate := TrustPolicy(store, 128)
		if gate.Allow(ctx, event) {
			t.Error("expected Allows to return false for an unknown pubkey")
		}
	})

	t.Run("denies pubkey on store error", func(t *testing.T) {
		store := &mockDB{err: errors.New("redis down")}
		gate := TrustPolicy(store, 128)
		if gate.Allow(ctx, event) {
			t.Error("expected Allows to return false when the store errors")
		}
	})

	t.Run("caches positive lookups", func(t *testing.T) {
		store := &mockDB{exists: map[string]bool{pubkey: true}}
		gate := TrustPolicy(store, 128)

		gate.Allow(ctx, event)
		gate.Allow(ctx, event)
		gate.Allow(ctx, event)

		if store.calls != 1 {
			t.Errorf("expected 1 store call, got %d", store.calls)
		}
	})

	t.Run("does not cache negative lookups", func(t *testing.T) {
		store := &mockDB{exists: map[string]bool{}}
		gate := TrustPolicy(store, 128)

		gate.Allow(ctx, event)
		gate.Allow(ctx, event)

		if store.calls != 2 {
			t.Errorf("expected 2 store calls, got %d", store.calls)
		}
	})
}
