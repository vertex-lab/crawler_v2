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

	pip = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
)

// Manually change pip's follow list and see if the events gets printed. Works only with `go test`
func TestFirehose(t *testing.T) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()

	checker := mockChecker{pubkey: pip}
	config := FirehoseConfig{Relays: defaultRelays}
	Firehose(ctx, config, checker, print)
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
