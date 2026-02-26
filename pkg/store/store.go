package store

import (
	_ "embed"

	sqlite "github.com/vertex-lab/nostr-sqlite"
)

//go:embed schema.sql
var schema string

func New(path string, options ...sqlite.Option) (*sqlite.Store, error) {
	options = append(
		options,
		sqlite.WithAdditionalSchema(schema),
		sqlite.WithCacheSize(256*sqlite.MiB),
	)
	return sqlite.New(path, options...)
}

// Profile represent the internal representation of the content of kind:0s, used for full-text-search.
type Profile struct {
	ID          string
	Pubkey      string
	Name        string
	DisplayName string
	About       string
	Website     string
	Nip05       string
}
