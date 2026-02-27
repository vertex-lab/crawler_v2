package auth

import (
	"errors"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

var validSK = nostr.GeneratePrivateKey()

func TestNewHandler(t *testing.T) {
	tests := []struct {
		name string
		url  string
		sk   string
		err  error
	}{
		{
			name: "valid",
			url:  "wss://relay.example.com",
			sk:   validSK,
			err:  nil,
		},
		{
			name: "empty url",
			url:  "",
			sk:   validSK,
			err:  ErrInvalidURL,
		},
		{
			name: "invalid url",
			url:  "not-a-url",
			sk:   validSK,
			err:  ErrInvalidURL,
		},
		{
			name: "htestp instead of wss",
			url:  "htestp://relay.example.com",
			sk:   validSK,
			err:  ErrInvalidURL,
		},
		{
			name: "empty sk",
			url:  "wss://relay.example.com",
			sk:   "",
			err:  ErrInvalidSecret,
		},
		{
			name: "sk not hex",
			url:  "wss://relay.example.com",
			sk:   "not-hex-at-all",
			err:  ErrInvalidSecret,
		},
		{
			name: "sk too short",
			url:  "wss://relay.example.com",
			sk:   "deadbeef",
			err:  ErrInvalidSecret,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewHandler(test.url, test.sk)
			if !errors.Is(err, test.err) {
				t.Errorf("NewHandler(%q, %q): got error %v, want %v", test.url, test.sk, err, test.err)
			}
		})
	}
}

func TestResponse(t *testing.T) {
	const relayURL = "wss://relay.example.com"

	tests := []struct {
		name      string
		challenge string
		err       error
	}{
		{
			name:      "no challenge set",
			challenge: "",
			err:       ErrNoChallenge,
		},
		{
			name:      "challenge set",
			challenge: "test-challenge-string",
			err:       nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h, err := NewHandler(relayURL, validSK)
			if err != nil {
				t.Fatalf("setup: NewHandler: %v", err)
			}

			if test.challenge != "" {
				h.SetChallenge(test.challenge)
			}

			event, err := h.Response()
			if !errors.Is(err, test.err) {
				t.Fatalf("Response(): got error %v, want %v", err, test.err)
			}
			if err != nil {
				return
			}

			if event.Kind != Kind {
				t.Errorf("event.Kind = %d, want %d", event.Kind, Kind)
			}
			if !event.CheckID() {
				t.Error("event.ID is invalid")
			}
			match, err := event.CheckSignature()
			if err != nil {
				t.Errorf("event.Signature is invalid: %v", err)
			}
			if !match {
				t.Errorf("event.Signature is invalid")
			}
			if event.Tags.FindWithValue("relay", relayURL) == nil {
				t.Errorf("missing relay tag %q", relayURL)
			}
			if event.Tags.FindWithValue("challenge", test.challenge) == nil {
				t.Errorf("missing challenge tag %q", test.challenge)
			}
		})
	}
}
