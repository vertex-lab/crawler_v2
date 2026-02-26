package firehose

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ctx        = context.Background()
	pip string = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
)

// mockStore implements NodeStore for testing.
type mockStore struct {
	exists map[string]bool
	err    error
	calls  int
}

func (m *mockStore) Exists(_ context.Context, pubkey string) (bool, error) {
	m.calls++
	return m.exists[pubkey], m.err
}

// Manually change pip's follow list and see if the events gets printed. Works only with `go test`
func TestFirehose(t *testing.T) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()

	config := NewConfig()
	gate := newGate(t, &mockStore{exists: map[string]bool{pip: true}})

	firehose := New(config, gate)
	firehose.Run(ctx, func(e *nostr.Event) error {
		fmt.Printf("ID: %s, Kind: %d, Pubkey: %s\n", e.ID, e.Kind, e.PubKey)
		return nil
	})
}

func TestBuffer(t *testing.T) {
	t.Run("empty buffer contains nothing", func(t *testing.T) {
		b := newRingBuffer(4)
		if b.Contains("id-1") {
			t.Error("empty buffer should not contain any ID")
		}
	})

	t.Run("contains ID after add", func(t *testing.T) {
		b := newRingBuffer(4)
		b.Add("id-1")
		if !b.Contains("id-1") {
			t.Error("buffer should contain id-1 after Add")
		}
	})

	t.Run("does not contain un-added ID", func(t *testing.T) {
		b := newRingBuffer(4)
		b.Add("id-1")
		if b.Contains("id-2") {
			t.Error("buffer should not contain id-2")
		}
	})

	t.Run("ring wrap-around evicts oldest entry", func(t *testing.T) {
		b := newRingBuffer(3)
		b.Add("id-1")
		b.Add("id-2")
		b.Add("id-3")
		// writing id-4 wraps around and overwrites id-1
		b.Add("id-4")
		if b.Contains("id-1") {
			t.Error("id-1 should have been evicted after wrap-around")
		}
		if !b.Contains("id-2") {
			t.Error("id-2 should still be present")
		}
		if !b.Contains("id-3") {
			t.Error("id-3 should still be present")
		}
		if !b.Contains("id-4") {
			t.Error("id-4 should be present")
		}
	})
}

func TestExistenceGate(t *testing.T) {
	const pubkey = "abc123"

	t.Run("allows pubkey present in store", func(t *testing.T) {
		store := &mockStore{exists: map[string]bool{pubkey: true}}
		gate := newGate(t, store)
		if !gate.Allow(ctx, pubkey) {
			t.Error("expected Allows to return true for a known pubkey")
		}
	})

	t.Run("denies pubkey absent from store", func(t *testing.T) {
		store := &mockStore{exists: map[string]bool{}}
		gate := newGate(t, store)
		if gate.Allow(ctx, pubkey) {
			t.Error("expected Allows to return false for an unknown pubkey")
		}
	})

	t.Run("denies pubkey on store error", func(t *testing.T) {
		store := &mockStore{err: errors.New("redis down")}
		gate := newGate(t, store)
		if gate.Allow(ctx, pubkey) {
			t.Error("expected Allows to return false when the store errors")
		}
	})

	t.Run("caches positive lookups", func(t *testing.T) {
		store := &mockStore{exists: map[string]bool{pubkey: true}}
		gate := newGate(t, store)

		gate.Allow(ctx, pubkey)
		gate.Allow(ctx, pubkey)
		gate.Allow(ctx, pubkey)

		if store.calls != 1 {
			t.Errorf("expected 1 store call, got %d", store.calls)
		}
	})

	t.Run("does not cache negative lookups", func(t *testing.T) {
		store := &mockStore{exists: map[string]bool{}}
		gate := newGate(t, store)

		gate.Allow(ctx, pubkey)
		gate.Allow(ctx, pubkey)

		if store.calls != 2 {
			t.Errorf("expected 2 store calls, got %d", store.calls)
		}
	})
}

func newGate(t *testing.T, store *mockStore) *ExistenceGate {
	t.Helper()
	gate, err := NewExistenceGate(store, 128)
	if err != nil {
		t.Fatalf("NewExistenceGate: %v", err)
	}
	return gate
}
