package pipe

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
)

var (
	ctx = context.Background()

	odell string = "04c915daefee38317fa734444acee390a8269fe5810b2241e5e6dd343dfbecc9"
	calle string = "50d94fc2d8580c682b071a542f8b1e31a200b0508bab95a33bef0855df281d63"
	pip   string = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
)

type mockGate struct {
	pubkey string
}

func (g mockGate) Allows(ctx context.Context, pubkey string) bool {
	return pubkey == g.pubkey
}

func print(e *nostr.Event) error {
	fmt.Printf("\nevent ID: %v", e.ID)
	fmt.Printf("\nevent pubkey: %v", e.PubKey)
	fmt.Printf("\nevent kind: %d\n", e.Kind)
	return nil
}

// Manually change pip's follow list and see if the events gets printed. Works only with `go test`
func TestFirehose(t *testing.T) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()

	gate := mockGate{pubkey: pip}
	config := NewFirehoseConfig()
	Firehose(ctx, config, gate, print)
}

func TestFetch(t *testing.T) {
	pubkeys := []string{odell, calle, pip}
	config := NewFetcherConfig()

	events, err := fetch(ctx, config, pubkeys)
	if err != nil {
		t.Fatalf("expected error nil, got %v", err)
	}

	expected := len(pubkeys) * 2
	if len(events) != expected {
		t.Fatalf("expected %d events, got %d", expected, len(events))
	}
}

// Manually check on a test database
func TestFinalizeStats(t *testing.T) {
	db := regraph.New(&redis.Options{
		Addr: "localhost:6379",
	})

	err := finalizeStats(db, "2025-09-15")
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}
