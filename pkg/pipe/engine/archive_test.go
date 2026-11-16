package engine

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler_v2/pkg/pipe"
	"github.com/vertex-lab/crawler_v2/pkg/store"
)

func TestArchiveKindsConfiguration(t *testing.T) {
	c := NewConfig()
	if !slices.Equal(c.ArchiveKinds, pipe.ProfileKinds) {
		t.Fatal("default archival coverage changed")
	}
	c.ArchiveKinds[0] = 10001
	if NewConfig().ArchiveKinds[0] != nostr.KindProfileMetadata {
		t.Fatal("configurations share a mutable default")
	}
	for _, value := range []string{"-1", "65536", "20000", "29999", "invalid"} {
		t.Run(value, func(t *testing.T) {
			c := NewConfig()
			err := env.ParseWithOptions(&c, env.Options{
				Environment: map[string]string{"ENGINE_ARCHIVE_KINDS": value},
			})
			if err == nil {
				err = c.Validate()
			}
			if err == nil {
				t.Fatalf("accepted invalid archive kinds %q", value)
			}
		})
	}
}

func TestConfiguredArchival(t *testing.T) {
	ctx := context.Background()
	sk := nostr.GeneratePrivateKey()
	for _, custom := range []bool{false, true} {
		name := "defaults"
		if custom {
			name = "custom"
		}
		t.Run(name, func(t *testing.T) {
			db, err := store.New(filepath.Join(t.TempDir(), "events.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			c := NewConfig()
			if custom {
				if err := env.ParseWithOptions(&c, env.Options{
					Environment: map[string]string{"ENGINE_ARCHIVE_KINDS": "1,10001,30000"},
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Validate(); err != nil {
				t.Fatal(err)
			}
			e := &T{config: c, store: db}
			ingest := func(kind int, timestamp nostr.Timestamp, d string) nostr.Event {
				t.Helper()
				event := nostr.Event{
					Kind: kind, CreatedAt: timestamp, Content: "{}",
					Tags: nostr.Tags{},
				}
				if d != "" {
					event.Tags = nostr.Tags{{"d", d}}
				}
				if err := event.Sign(sk); err != nil {
					t.Fatal(err)
				}
				e.processIngest(&event)
				return event
			}
			ingest(10001, 100, "")
			latest := ingest(10001, 200, "")
			ingest(10001, 150, "")
			ingest(30000, 100, "one")
			one := ingest(30000, 200, "one")
			two := ingest(30000, 100, "two")
			note1 := ingest(1, 100, "")
			note2 := ingest(1, 200, "")
			ingest(10019, 100, "") // excluded in both configurations
			profile := ingest(0, 100, "")
			// Relay discovery is processed even when omitted from ArchiveKinds.
			relayList := ingest(10002, 100, "")

			got, err := db.Query(ctx, nostr.Filter{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			want := []nostr.Event{profile, relayList}
			if custom {
				want = []nostr.Event{latest, one, two, note1, note2, relayList}
			}
			if len(got) != len(want) {
				t.Fatalf("got %d archived events, want %d: %+v", len(got), len(want), got)
			}
			for _, expected := range want {
				if !slices.ContainsFunc(got, func(event nostr.Event) bool { return event.ID == expected.ID }) {
					t.Errorf("missing kind %d event %s", expected.Kind, expected.ID)
				}
			}
		})
	}
}
