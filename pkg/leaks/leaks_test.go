package leaks

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ctx         = context.Background()
	testAddress = "localhost:6380"

	seckey = "64719d406ec238fb1eb0ae38e1619b7919836d66efdc65353a2f8a68f51dcf6c"
	pubkey = "d45aaa6518d4781febb7e7678a71c8679e6ef425b34ed328814ff7ff0fcb6e51"
)

func TestValidate(t *testing.T) {
	validProof, err := proof(seckey, pubkey)
	if err != nil {
		t.Fatalf("failed to generate proof: %v", err)
	}

	confirmedRecord := Record{
		Pubkey:     pubkey,
		DetectedAt: time.Now(),
		Status:     Confirmed,
		Proof:      validProof,
	}

	suspectedRecord := Record{
		Pubkey: pubkey,
		Status: Suspected,
	}

	tests := []struct {
		name    string
		record  Record
		isValid bool
	}{
		{
			name:    "valid confirmed record",
			record:  confirmedRecord,
			isValid: true,
		},
		{
			name:    "valid suspected record",
			record:  suspectedRecord,
			isValid: true,
		},
		{
			name:    "invalid pk suspected record",
			record:  Record{Pubkey: "invalid", Status: Suspected},
			isValid: false,
		},
		{
			name:    "empty proof fails",
			record:  Record{Pubkey: pubkey, Status: Confirmed, Proof: ""},
			isValid: false,
		},
		{
			name: "tampered proof fails",
			record: Record{
				Pubkey: pubkey,
				Status: Confirmed,
				// flip the last byte of a valid proof
				Proof: validProof[:len(validProof)-2] + "00",
			},
			isValid: false,
		},
		{
			name: "proof for wrong pubkey fails",
			record: Record{
				// swap in a different pubkey that doesn't match the proof
				Pubkey: "b94f6f125c79e46f9100ce6f39e4be8b3e4e2f0bbeacff9e5a5b1bfae3be1f2a",
				Status: Confirmed,
				Proof:  validProof,
			},
			isValid: false,
		},
		{
			name:    "invalid status fails",
			record:  Record{Pubkey: pubkey, Status: "unknown", Proof: validProof},
			isValid: false,
		},
		{
			name:    "invalid pubkey fails",
			record:  Record{Pubkey: "notahex", Status: Confirmed, Proof: validProof},
			isValid: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.record.Validate()
			if test.isValid && err != nil {
				t.Errorf("Validate() expected no error, got %v", err)
			}
			if !test.isValid && err == nil {
				t.Errorf("Validate() expected error, got nil")
			}
		})
	}
}

func TestStoreRead(t *testing.T) {
	redis := redis.NewClient(&redis.Options{Addr: testAddress})
	defer redis.FlushAll(ctx)

	db := NewDB(redis)
	detectedAt := time.Unix(time.Now().Unix(), 0) // truncate to seconds

	addedPks, err := db.Store(ctx, []string{seckey}, detectedAt)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if len(addedPks) != 1 {
		t.Fatalf("expected 1 added pubkey, got %d", len(addedPks))
	}

	pk := addedPks[0]
	if pk != pubkey {
		t.Errorf("expected pubkey %s, got %s", pubkey, pk)
	}

	record, err := db.Read(ctx, pk)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !record.DetectedAt.Equal(detectedAt) {
		t.Errorf("time: want %v, got %v", detectedAt, record.DetectedAt)
	}
	if record.Status != Confirmed {
		t.Errorf("status: want %q, got %q", Confirmed, record.Status)
	}
	if err := record.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestStoreIdempotency(t *testing.T) {
	redis := redis.NewClient(&redis.Options{Addr: testAddress})
	defer redis.FlushAll(ctx)

	db := NewDB(redis)
	detectedAt := time.Unix(time.Now().Unix(), 0)

	// first Store: should add the key
	addedPks, err := db.Store(ctx, []string{seckey}, detectedAt)
	if err != nil {
		t.Fatalf("first Store: %v", err)
	}
	if len(addedPks) != 1 {
		t.Fatalf("first Store: expected 1 added pubkey, got %d", len(addedPks))
	}

	pk := addedPks[0]
	if pk != pubkey {
		t.Errorf("expected pubkey %s, got %s", pubkey, pk)
	}

	first, err := db.Read(ctx, pk)
	if err != nil {
		t.Fatalf("Read after first Store: %v", err)
	}

	// second Store: same key, later time — should be a no-op
	later := detectedAt.Add(time.Hour)
	addedPks, err = db.Store(ctx, []string{seckey}, later)
	if err != nil {
		t.Fatalf("second Store: %v", err)
	}
	if len(addedPks) != 0 {
		t.Fatalf("second Store: expected 0 added pubkeys, got %d", len(addedPks))
	}

	second, err := db.Read(ctx, pk)
	if err != nil {
		t.Fatalf("Read after second Store: %v", err)
	}

	if !reflect.DeepEqual(first, second) {
		t.Errorf("record changed after second Store: want %+v, got %+v", first, second)
	}
}

func TestParseUnique(t *testing.T) {
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
			keys:    nil,
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
		{
			name:    "Mr nsec",
			message: "{\"name\":\"Mr.nsec1zdp9nfa3346g5nnkt74k7tlt6hat4xtvgdt5madyj85wdjvtuakq05rxys\",\"about\":\"Just your average nostr enjoyer\"}",
			keys:    []string{"134259a7b18d748a4e765fab6f2febd5faba996c43574df5a491e8e6c98be76c"},
		},
		{
			// nsec encoding of 32 zero bytes (k=0): valid bech32, invalid secp256k1 scalar.
			// nip19.Decode accepts it, but ParseNsecs rejects it because the derived pubkey is invalid.
			name:    "all-zero scalar: valid bech32, invalid secp256k1 key",
			message: "nsec1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqwkhnav",
			keys:    nil,
		},
		{
			// nsec encoding of the curve order n (k=n): valid bech32, invalid secp256k1 scalar.
			// nip19.Decode accepts it, but ParseNsecs rejects it because the derived pubkey is invalid.
			name:    "k=n scalar: valid bech32, invalid secp256k1 key",
			message: "nsec1lllllllllllllllllllllllll6a2ah8x4ay2qwal6f0ge5pkg9qstu3zum",
			keys:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ParseUnique(test.message)
			if !reflect.DeepEqual(got, test.keys) {
				t.Fatalf("expected %v, got %v", test.keys, got)
			}
		})
	}
}
