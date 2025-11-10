package store

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ctx = context.Background()

	event = nostr.Event{
		ID:        "f7a73d54e45714f5e3ca97b789dfc7898e7dd31f77981989d71a54030e627ff6",
		Kind:      0,
		PubKey:    "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2",
		CreatedAt: 1739547448,
		Sig:       "51a89ee1e24d83bd8e9209daf6a38245c974b49206ecb66fe156c9d7875c782f653b40cd73582f6bc9de5d1db497b925a13a828d521f8b78982fea359206e4e8",
		Content:   "{\"name\":\"pippellia\",\"nip05\":\"pip@vertexlab.io\",\"about\":\"simplifying social graph analysis so you can focus on building great experiences https://vertexlab.io/\",\"lud16\":\"whitebat1@primal.net\",\"display_name\":\"Pip the social graph guy\",\"picture\":\"https://m.primal.net/IfSZ.jpg\",\"banner\":\"https://m.primal.net/IfSc.png\",\"website\":\"pippellia.com\",\"displayName\":\"Pip the social graph guy\",\"pubkey\":\"f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2\",\"npub\":\"npub176p7sup477k5738qhxx0hk2n0cty2k5je5uvalzvkvwmw4tltmeqw7vgup\",\"created_at\":1738783677}",
	}

	profile = Profile{
		ID:          event.ID,
		Pubkey:      event.PubKey,
		Name:        "pippellia",
		DisplayName: "Pip the social graph guy",
		About:       "simplifying social graph analysis so you can focus on building great experiences https://vertexlab.io/",
		Website:     "pippellia.com",
		Nip05:       "pip@vertexlab.io",
	}
)

func TestSaveProfile(t *testing.T) {
	path := t.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	saved, err := store.Save(ctx, &event)
	if err != nil {
		t.Fatal(err)
	}

	if !saved {
		t.Fatal("expected saved true, got false")
	}

	var p Profile
	row := store.DB.QueryRowContext(ctx, "SELECT * FROM profiles_fts WHERE id = ?", event.ID)
	err = row.Scan(&p.ID, &p.Pubkey, &p.Name, &p.DisplayName, &p.About, &p.Website, &p.Nip05)

	if err != nil {
		t.Fatalf("failed to query for event ID %s in profiles_fts: %v", event.ID, err)
	}

	if !reflect.DeepEqual(p, profile) {
		t.Fatalf("expected profile %v, got %v", profile, p)
	}
}

func Remove(URL string) {
	os.Remove(URL)
	os.Remove(URL + "-shm")
	os.Remove(URL + "-wal")
}
