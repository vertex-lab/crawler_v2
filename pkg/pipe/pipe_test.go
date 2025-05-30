package pipe

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ctx = context.Background()

	odell string = "04c915daefee38317fa734444acee390a8269fe5810b2241e5e6dd343dfbecc9"
	calle string = "50d94fc2d8580c682b071a542f8b1e31a200b0508bab95a33bef0855df281d63"
	pip   string = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
)

// Manually change pip's follow list and see if the events gets printed. Works only with `go test`
func TestFirehose(t *testing.T) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()

	checker := mockChecker{pubkey: pip}
	config := NewFirehoseConfig()

	Firehose(ctx, config, checker, print)
}

func TestFetch(t *testing.T) {
	pool := nostr.NewSimplePool(ctx)
	pubkeys := []string{odell, calle, pip}

	events, err := fetch(ctx, pool, defaultRelays, pubkeys)
	if err != nil {
		t.Fatalf("expected error nil, got %v", err)
	}

	expected := len(pubkeys) * len(relevantKinds)
	if len(events) != expected {
		t.Fatalf("expected %d events, got %d", expected, len(events))
	}
}

type mockChecker struct {
	pubkey string
}

func (c mockChecker) Exists(ctx context.Context, pubkey string) (bool, error) {
	return pubkey == c.pubkey, nil
}

func print(e *nostr.Event) error {
	fmt.Printf("\nevent ID: %v", e.ID)
	fmt.Printf("\nevent pubkey: %v", e.PubKey)
	fmt.Printf("\nevent kind: %d\n", e.Kind)
	return nil
}
