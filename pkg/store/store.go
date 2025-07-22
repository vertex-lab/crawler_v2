package store

import (
	"github.com/pippellia-btc/nastro/sqlite"
)

var (
	profileFTS = `
	CREATE VIRTUAL TABLE IF NOT EXISTS profiles_fts USING fts5(
		id UNINDEXED,
		pubkey UNINDEXED,
		name,
		display_name,
		about,
		website,
		nip05,
		tokenize = 'trigram',
	  );

	CREATE TRIGGER IF NOT EXISTS profiles_ai AFTER INSERT ON events
	  WHEN NEW.kind = 0
	  BEGIN
	  INSERT INTO profiles_fts (id, pubkey, name, display_name, about, website, nip05)
	  VALUES (
	  	NEW.id,
	  	NEW.pubkey,
	  	NEW.content ->> '$.name',
	  	COALESCE( NEW.content ->> '$.display_name', NEW.content ->> '$.displayName'),
	  	NEW.content ->> '$.about',
	  	NEW.content ->> '$.website',
	  	NEW.content ->> '$.nip05'
	  );
	  END;

	CREATE TRIGGER IF NOT EXISTS profiles_ad AFTER DELETE ON events
	WHEN OLD.kind = 0
	BEGIN
	  DELETE FROM profiles_fts WHERE id = OLD.id;
	END;`

	// indexing a-z and A-Z tags of responses for efficient look-up
	responseTagsIndex = `
	CREATE TRIGGER IF NOT EXISTS response_tags_ai AFTER INSERT ON events
	WHEN (NEW.kind BETWEEN 6312 AND 6315 OR NEW.kind = 7000)
	BEGIN
	INSERT INTO event_tags (event_id, key, value)
		SELECT NEW.id, json_extract(value, '$[0]'), json_extract(value, '$[1]')
		FROM json_each(NEW.tags)
		WHERE json_type(value) = 'array'
			AND json_array_length(value) > 1
			AND typeof(json_extract(value, '$[0]')) = 'text'
			AND json_extract(value, '$[0]') GLOB '[a-zA-Z]';
	END;`
)

func New(URL string, opts ...sqlite.Option) (*sqlite.Store, error) {
	store, err := sqlite.New(URL,
		sqlite.WithAdditionalSchema(profileFTS),
		sqlite.WithAdditionalSchema(responseTagsIndex),
		sqlite.WithRetries(2),
	)

	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		if err := opt(store); err != nil {
			return nil, err
		}
	}
	return store, nil
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
