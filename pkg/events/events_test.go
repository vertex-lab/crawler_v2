package events

import (
	"reflect"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

var (
	pk1 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	pk2 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	pk3 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	nsec1 = "nsec1zjlj7zp29dhk92fzn9ettntkag9wdyaxuugmmmn7gtve7xlqtupqwtyx8z"
	key1  = "14bf2f082a2b6f62a9229972b5cd76ea0ae693a6e711bdee7e42d99f1be05f02"
	nsec2 = "nsec1mzq8xppqa2lvy9u95y4wzsfydclnj6g6dpnm4myvf44y3248pzqsdl2rar"
	key2  = "d880730420eabec21785a12ae141246e3f39691a6867baec8c4d6a48aaa70881"
)

func TestParseTags(t *testing.T) {
	tests := []struct {
		name string
		key  string
		tags nostr.Tags
		want []string
	}{
		{
			name: "no tags",
			key:  "p",
			tags: nostr.Tags{},
			want: nil,
		},
		{
			name: "single p tag",
			key:  "p",
			tags: nostr.Tags{{"p", pk1}},
			want: []string{pk1},
		},
		{
			name: "two distinct p tags",
			key:  "p",
			tags: nostr.Tags{{"p", pk1}, {"p", pk2}},
			want: []string{pk1, pk2},
		},
		{
			name: "duplicate p tags are deduplicated",
			key:  "p",
			tags: nostr.Tags{{"p", pk1}, {"p", pk1}},
			want: []string{pk1},
		},
		{
			name: "non-p tags are ignored",
			key:  "p",
			tags: nostr.Tags{{"e", pk1}, {"r", "wss://relay.example.com"}},
			want: nil,
		},
		{
			name: "tag too short is ignored",
			key:  "p",
			tags: nostr.Tags{{"p"}},
			want: nil,
		},
		{
			name: "mixed p and non-p tags",
			key:  "p",
			tags: nostr.Tags{{"p", pk1}, {"e", pk2}, {"p", pk3}},
			want: []string{pk1, pk3},
		},
		{
			name: "e tags",
			key:  "e",
			tags: nostr.Tags{{"e", pk1}, {"e", pk2}, {"p", pk3}},
			want: []string{pk1, pk2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ParseTags(test.key, test.tags)
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("want %v, got %v", test.want, got)
			}
		})
	}
}

func TestParseRelays(t *testing.T) {
	tests := []struct {
		name string
		tags nostr.Tags
		want []string
	}{
		{
			name: "no tags",
			tags: nostr.Tags{},
			want: nil,
		},
		{
			name: "single valid wss relay",
			tags: nostr.Tags{{"r", "wss://relay.example.com"}},
			want: []string{"wss://relay.example.com"},
		},
		{
			name: "single valid ws relay",
			tags: nostr.Tags{{"r", "ws://relay.example.com"}},
			want: []string{"ws://relay.example.com"},
		},
		{
			name: "two distinct relays",
			tags: nostr.Tags{
				{"r", "wss://relay.example.com"},
				{"r", "wss://relay2.example.com"},
			},
			want: []string{"wss://relay.example.com", "wss://relay2.example.com"},
		},
		{
			name: "duplicate relays are deduplicated",
			tags: nostr.Tags{
				{"r", "wss://relay.example.com"},
				{"r", "wss://relay.example.com"},
			},
			want: []string{"wss://relay.example.com"},
		},
		{
			name: "invalid scheme is rejected",
			tags: nostr.Tags{{"r", "https://relay.example.com"}},
			want: []string{},
		},
		{
			name: "empty url is rejected",
			tags: nostr.Tags{{"r", ""}},
			want: []string{},
		},
		{
			name: "tag too short is ignored",
			tags: nostr.Tags{{"r"}},
			want: nil,
		},
		{
			name: "non-r tags are ignored",
			tags: nostr.Tags{{"p", pk1}, {"e", "someeventid"}},
			want: nil,
		},
		{
			name: "mixed r and non-r tags",
			tags: nostr.Tags{
				{"p", pk1},
				{"r", "wss://relay.example.com"},
				{"r", "https://invalid.example.com"},
				{"r", "wss://relay2.example.com"},
			},
			want: []string{"wss://relay.example.com", "wss://relay2.example.com"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ParseRelays(test.tags)
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("want %v, got %v", test.want, got)
			}
		})
	}
}

func TestParseLeaks(t *testing.T) {
	tests := []struct {
		name string
		e    *nostr.Event
		want []string
	}{
		{
			name: "no leaks",
			e: &nostr.Event{
				Content: "just a normal note",
				Tags:    nostr.Tags{{"p", pk1}},
			},
			want: nil,
		},
		{
			name: "leak in content",
			e: &nostr.Event{
				Content: "my key is " + nsec1,
			},
			want: []string{key1},
		},
		{
			name: "leak in a tag",
			e: &nostr.Event{
				Content: "nothing here",
				Tags:    nostr.Tags{{"note", nsec1}},
			},
			want: []string{key1},
		},
		{
			name: "leak in both content and tag",
			e: &nostr.Event{
				Content: "key: " + nsec1,
				Tags:    nostr.Tags{{"note", nsec2}},
			},
			want: []string{key1, key2},
		},
		{
			name: "duplicate across content and tag are deduplicated",
			e: &nostr.Event{
				Content: nsec1,
				Tags:    nostr.Tags{{"note", nsec1}},
			},
			want: []string{key1},
		},
		{
			name: "two leaks in content are deduplicated",
			e: &nostr.Event{
				Content: nsec1 + " " + nsec1,
			},
			want: []string{key1},
		},
		{
			name: "leak spread across multiple tags",
			e: &nostr.Event{
				Tags: nostr.Tags{
					{"a", nsec1},
					{"b", nsec2},
				},
			},
			want: []string{key1, key2},
		},
		{
			name: "invalid nsec in tag is rejected",
			e: &nostr.Event{
				Tags: nostr.Tags{{"note", "nsec1abcd"}},
			},
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ParseLeaks(test.e)
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("want %v, got %v", test.want, got)
			}
		})
	}
}
