package leaks

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestStoreRead(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6380"})
	t.Cleanup(func() { client.Close() })

	db := NewDB(client)

	sk := "14bf2f082a2b6f62a9229972b5cd76ea0ae693a6e711bdee7e42d99f1be05f02"
	detectedAt := time.Unix(time.Now().Unix(), 0) // truncate to seconds

	addedPks, err := db.Store(ctx, []string{sk}, detectedAt)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if len(addedPks) != 1 {
		t.Fatalf("expected 1 added pubkey, got %d", len(addedPks))
	}
	pk := addedPks[0]
	t.Cleanup(func() { client.HDel(ctx, keyLeakedKeys, pk) })

	gotSK, gotTime, err := db.Read(ctx, pk)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if gotSK != sk {
		t.Errorf("secret key: want %q, got %q", sk, gotSK)
	}
	if !gotTime.Equal(detectedAt) {
		t.Errorf("time: want %v, got %v", detectedAt, gotTime)
	}
}

func TestParseNsecs(t *testing.T) {
	key1 := "14bf2f082a2b6f62a9229972b5cd76ea0ae693a6e711bdee7e42d99f1be05f02"
	nsec1 := "nsec1zjlj7zp29dhk92fzn9ettntkag9wdyaxuugmmmn7gtve7xlqtupqwtyx8z"
	key2 := "d880730420eabec21785a12ae141246e3f39691a6867baec8c4d6a48aaa70881"
	nsec2 := "nsec1mzq8xppqa2lvy9u95y4wzsfydclnj6g6dpnm4myvf44y3248pzqsdl2rar"

	tests := []struct {
		name    string
		message string
		keys    []string
	}{
		{
			name:    "empty",
			message: "",
			keys:    nil,
		},
		{
			name:    "too short",
			message: "hello world",
			keys:    nil,
		},
		{
			name:    "single valid nsec",
			message: "my compromised key is " + nsec1,
			keys:    []string{key1},
		},
		{
			name:    "single valid nsec with punctuation",
			message: `do not trust this account: "` + nsec1 + `"`,
			keys:    []string{key1},
		},
		{
			name:    "two valid nsecs",
			message: "first=" + nsec1 + " second=" + nsec2,
			keys:    []string{key1, key2},
		},
		{
			name:    "duplicate nsecs are deduplicated",
			message: nsec1 + " " + nsec1,
			keys:    []string{key1},
		},
		{
			name:    "invalid charset does not match",
			message: "fake nsec1iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",
			keys:    nil,
		},
		{
			name:    "wrong length too short does not match",
			message: "fake nsec1abcd",
			keys:    nil,
		},
		{
			name:    "wrong length too long does not match",
			message: "fake " + nsec1 + "abc",
			keys:    nil,
		},
		{
			name:    "embedded in another word should not match",
			message: "xx" + nsec1 + "yy",
			keys:    nil,
		},
		{
			name:    "mixed content with one valid one invalid",
			message: "bad nsec1abcd good " + nsec2,
			keys:    []string{key2},
		},
		{
			name:    "newline separated",
			message: "compromised\n" + nsec1 + "\nrotate now",
			keys:    []string{key1},
		},
		{
			name:    "tab separated",
			message: "compromised\t" + nsec1 + "\trotate now",
			keys:    []string{key1},
		},
		{
			name:    "carriage return separated",
			message: "compromised\r\n" + nsec1 + "\r\nrotate now",
			keys:    []string{key1},
		},
		{
			name:    "surrounded by parentheses",
			message: "(" + nsec1 + ")",
			keys:    []string{key1},
		},
		{
			name:    "surrounded by brackets",
			message: "[" + nsec1 + "]",
			keys:    []string{key1},
		},
		{
			name:    "surrounded by braces",
			message: "{" + nsec1 + "}",
			keys:    []string{key1},
		},
		{
			name:    "surrounded by angle brackets",
			message: "<" + nsec1 + ">",
			keys:    []string{key1},
		},
		{
			name:    "followed by comma",
			message: nsec1 + ", rotate now",
			keys:    []string{key1},
		},
		{
			name:    "followed by period",
			message: nsec1 + ". rotate now",
			keys:    []string{key1},
		},
		{
			name:    "followed by colon",
			message: nsec1 + ": rotate now",
			keys:    []string{key1},
		},
		{
			name:    "followed by semicolon",
			message: nsec1 + "; rotate now",
			keys:    []string{key1},
		},
		{
			name:    "leading punctuation",
			message: "!!" + nsec1,
			keys:    []string{key1},
		},
		{
			name:    "at start of string",
			message: nsec1 + " leaked",
			keys:    []string{key1},
		},
		{
			name:    "at end of string",
			message: "leaked " + nsec1,
			keys:    []string{key1},
		},
		{
			name:    "uppercase entire nsec should decode",
			message: "leaked " + "NSEC1ZJLJ7ZP29DHK92FZN9ETTNTKAG9WDYAXUUGMMMN7GTVE7XLQTUPQWTYX8Z",
			keys:    []string{key1},
		},
		{
			name:    "mixed case matches regex",
			message: "leaked nSeC1zjlj7zp29dhk92fzn9ettntkag9wdyaxuugmmmn7gtve7xlqtupqwtyx8z",
			keys:    []string{key1},
		},
		{
			name:    "npub should not match",
			message: "wrong prefix npub1zjlj7zp29dhk92fzn9ettntkag9wdyaxuugmmmn7gtve7xlqtupq3tvk8w",
			keys:    nil,
		},
		{
			name:    "note should not match",
			message: "wrong prefix note1zjlj7zp29dhk92fzn9ettntkag9wdyaxuugmmmn7gtve7xlqtupq3tvk8w",
			keys:    nil,
		},
		{
			name:    "nsec with invalid checksum same length should be rejected",
			message: "fake nsec1zjlj7zp29dhk92fzn9ettntkag9wdyaxuugmmmn7gtve7xlqtupqwtyx8y",
			keys:    []string{},
		},
		{
			name:    "two valid nsecs separated by punctuation only",
			message: nsec1 + "," + nsec2,
			keys:    []string{key1, key2},
		},
		{
			name:    "many duplicates with punctuation are deduplicated",
			message: nsec1 + "," + nsec1 + ";" + nsec1 + "." + nsec1,
			keys:    []string{key1},
		},
		{
			name:    "underscore before should not match because not a word boundary",
			message: "foo_" + nsec1,
			keys:    nil,
		},
		{
			name:    "underscore after should not match because not a word boundary",
			message: nsec1 + "_bar",
			keys:    nil,
		},
		{
			name:    "slash before should match",
			message: "/" + nsec1,
			keys:    []string{key1},
		},
		{
			name:    "slash after should match",
			message: nsec1 + "/",
			keys:    []string{key1},
		},
		{
			name:    "URL query style should match",
			message: "https://example.com/?key=" + nsec1 + "&x=1",
			keys:    []string{key1},
		},
		{
			name:    "hash before should match",
			message: "#" + nsec1,
			keys:    []string{key1},
		},
		{
			name:    "apostrophes around key",
			message: "'" + nsec1 + "'",
			keys:    []string{key1},
		},
		{
			name:    "double quotes around key",
			message: `"` + nsec1 + `"`,
			keys:    []string{key1},
		},
		{
			name:    "backticks around key",
			message: "`" + nsec1 + "`",
			keys:    []string{key1},
		},
		{
			name:    "three lines with junk around valid keys",
			message: "junk nsec1abcd\n" + nsec1 + "\nhello\n" + nsec2 + "\nbye",
			keys:    []string{key1, key2},
		},
		{
			name:    "adjacent valid keys without delimiter should not match",
			message: nsec1 + nsec2,
			keys:    nil,
		},
		{
			name:    "valid then letter suffix should not match",
			message: nsec1 + "x",
			keys:    nil,
		},
		{
			name:    "valid then digit suffix should not match",
			message: nsec1 + "2",
			keys:    nil,
		},
		{
			name:    "letter prefix then valid should not match",
			message: "x" + nsec1,
			keys:    nil,
		},
		{
			name:    "digit prefix then valid should not match",
			message: "2" + nsec1,
			keys:    nil,
		},
		{
			name:    "multiple lines duplicates and one distinct",
			message: nsec1 + "\n" + nsec2 + "\n" + nsec1 + "\n" + nsec2 + "\n" + nsec1,
			keys:    []string{key1, key2},
		},
		{
			name:    "garbage with many nsec1 prefixes but no full candidate",
			message: "nsec1 nsec1abc nsec1zz nsec1foo",
			keys:    nil,
		},
		{
			name:    "surrounded by emojis should match",
			message: "🔥" + nsec1 + "🔥",
			keys:    []string{key1},
		},
		{
			name:    "unicode letters around key should not match as simple boundary regex may still accept",
			message: "à" + nsec1 + "è",
			keys:    []string{key1},
		},
		{
			name:    "one valid one uppercase duplicate",
			message: nsec1 + " " + "NSEC1ZJLJ7ZP29DHK92FZN9ETTNTKAG9WDYAXUUGMMMN7GTVE7XLQTUPQWTYX8Z",
			keys:    []string{key1},
		},
		{
			name:    "lots of punctuation noise",
			message: "...(" + nsec1 + ")..." + " !!! [" + nsec2 + "] ???",
			keys:    []string{key1, key2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ParseNsecs(test.message)
			if !reflect.DeepEqual(got, test.keys) {
				t.Fatalf("expected %v, got %v", test.keys, got)
			}
		})
	}
}
