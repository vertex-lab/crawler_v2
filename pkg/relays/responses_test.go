package relays

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

func TestParseLabel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		isValid  bool
	}{
		{
			name:     "EVENT label",
			input:    `["EVENT","sub1",{}]`,
			expected: "EVENT",
			isValid:  true,
		},
		{
			name:     "OK label",
			input:    `["OK","abc123",true,""]`,
			expected: "OK",
			isValid:  true,
		},
		{
			name:     "NOTICE label",
			input:    `["NOTICE","hello"]`,
			expected: "NOTICE",
			isValid:  true,
		},
		{
			name:    "not an array",
			input:   `{"label":"EVENT"}`,
			isValid: false,
		},
		{
			name:    "empty input",
			input:   ``,
			isValid: false,
		},
		{
			name:    "number instead of label string",
			input:   `[42,"sub1"]`,
			isValid: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			label, err := parseLabel(json.NewDecoder(strings.NewReader(test.input)))
			if test.isValid {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if label != test.expected {
					t.Fatalf("expected label %q, got %q", test.expected, label)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error, got label %q", label)
				}
			}
		})
	}
}

func TestParseEvent(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *Event
		isValid  bool
	}{
		{
			name:  "valid event",
			input: `["EVENT","sub1",{"id":"aabbcc","pubkey":"deadbeef","created_at":1700000000,"kind":1,"tags":[],"content":"hello","sig":"cafebabe"}]`,
			expected: &Event{
				SubID: "sub1",
				Event: nostr.Event{
					ID:        "aabbcc",
					PubKey:    "deadbeef",
					CreatedAt: 1700000000,
					Kind:      1,
					Tags:      nostr.Tags{},
					Content:   "hello",
					Sig:       "cafebabe",
				},
			},
			isValid: true,
		},
		{
			name:  "empty subscription ID",
			input: `["EVENT","",{"id":"aabbcc","pubkey":"deadbeef","created_at":1700000000,"kind":1,"tags":[],"content":"","sig":"cafebabe"}]`,
			expected: &Event{
				SubID: "",
				Event: nostr.Event{
					ID:        "aabbcc",
					PubKey:    "deadbeef",
					CreatedAt: 1700000000,
					Kind:      1,
					Tags:      nostr.Tags{},
					Content:   "",
					Sig:       "cafebabe",
				},
			},
			isValid: true,
		},
		{
			name:    "missing event object",
			input:   `["EVENT","sub1"]`,
			isValid: false,
		},
		{
			name:    "missing subscription ID",
			input:   `["EVENT"]`,
			isValid: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := json.NewDecoder(strings.NewReader(test.input))

			label, err := parseLabel(d)
			if err != nil {
				t.Fatalf("parseLabel failed: %v", err)
			}
			if label != "EVENT" {
				t.Fatalf("expected label %q, got %q", "EVENT", label)
			}

			got, err := parseEvent(d)
			if test.isValid {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got.SubID != test.expected.SubID {
					t.Errorf("SubID: expected %q, got %q", test.expected.SubID, got.SubID)
				}
				if got.Event.ID != test.expected.Event.ID {
					t.Errorf("Event.ID: expected %q, got %q", test.expected.Event.ID, got.Event.ID)
				}
				if got.Event.PubKey != test.expected.Event.PubKey {
					t.Errorf("Event.PubKey: expected %q, got %q", test.expected.Event.PubKey, got.Event.PubKey)
				}
				if got.Event.Kind != test.expected.Event.Kind {
					t.Errorf("Event.Kind: expected %d, got %d", test.expected.Event.Kind, got.Event.Kind)
				}
				if got.Event.Content != test.expected.Event.Content {
					t.Errorf("Event.Content: expected %q, got %q", test.expected.Event.Content, got.Event.Content)
				}
				if got.Event.Sig != test.expected.Event.Sig {
					t.Errorf("Event.Sig: expected %q, got %q", test.expected.Event.Sig, got.Event.Sig)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error, got %+v", got)
				}
			}
		})
	}
}

func TestParseClosed(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *Closed
		isValid  bool
	}{
		{
			name:     "valid closed",
			input:    `["CLOSED","sub1","error: too many subscriptions"]`,
			expected: &Closed{ID: "sub1", Message: "error: too many subscriptions"},
			isValid:  true,
		},
		{
			name:     "empty message",
			input:    `["CLOSED","sub42",""]`,
			expected: &Closed{ID: "sub42", Message: ""},
			isValid:  true,
		},
		{
			name:    "missing message",
			input:   `["CLOSED","sub1"]`,
			isValid: false,
		},
		{
			name:    "missing both fields",
			input:   `["CLOSED"]`,
			isValid: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := json.NewDecoder(strings.NewReader(test.input))

			label, err := parseLabel(d)
			if err != nil {
				t.Fatalf("parseLabel failed: %v", err)
			}
			if label != "CLOSED" {
				t.Fatalf("expected label %q, got %q", "CLOSED", label)
			}

			got, err := parseClosed(d)
			if test.isValid {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got.ID != test.expected.ID {
					t.Errorf("ID: expected %q, got %q", test.expected.ID, got.ID)
				}
				if got.Message != test.expected.Message {
					t.Errorf("Message: expected %q, got %q", test.expected.Message, got.Message)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error, got %+v", got)
				}
			}
		})
	}
}

func TestParseAuth(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *AuthChallenge
		isValid  bool
	}{
		{
			name:     "valid auth challenge",
			input:    `["AUTH","challengestring123"]`,
			expected: &AuthChallenge{Challenge: "challengestring123"},
			isValid:  true,
		},
		{
			name:     "empty challenge",
			input:    `["AUTH",""]`,
			expected: &AuthChallenge{Challenge: ""},
			isValid:  true,
		},
		{
			name:    "missing challenge",
			input:   `["AUTH"]`,
			isValid: false,
		},
		{
			name:    "number instead of challenge string",
			input:   `["AUTH",42]`,
			isValid: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := json.NewDecoder(strings.NewReader(test.input))

			label, err := parseLabel(d)
			if err != nil {
				t.Fatalf("parseLabel failed: %v", err)
			}
			if label != "AUTH" {
				t.Fatalf("expected label %q, got %q", "AUTH", label)
			}

			got, err := parseAuth(d)
			if test.isValid {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got.Challenge != test.expected.Challenge {
					t.Errorf("Challenge: expected %q, got %q", test.expected.Challenge, got.Challenge)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error, got %+v", got)
				}
			}
		})
	}
}

func TestParseOK(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *OK
		isValid  bool
	}{
		{
			name:     "accepted with no reason",
			input:    `["OK","aabbccddeeff",true,""]`,
			expected: &OK{ID: "aabbccddeeff", Accepted: true, Reason: ""},
			isValid:  true,
		},
		{
			name:     "rejected with reason",
			input:    `["OK","aabbccddeeff",false,"error: duplicate event"]`,
			expected: &OK{ID: "aabbccddeeff", Accepted: false, Reason: "error: duplicate event"},
			isValid:  true,
		},
		{
			name:     "rejected with pow reason",
			input:    `["OK","aabbccddeeff",false,"pow: difficulty 25 is less than 30"]`,
			expected: &OK{ID: "aabbccddeeff", Accepted: false, Reason: "pow: difficulty 25 is less than 30"},
			isValid:  true,
		},
		{
			name:    "missing reason",
			input:   `["OK","aabbccddeeff",true]`,
			isValid: false,
		},
		{
			name:    "missing accepted and reason",
			input:   `["OK","aabbccddeeff"]`,
			isValid: false,
		},
		{
			name:    "missing all fields",
			input:   `["OK"]`,
			isValid: false,
		},
		{
			name:    "wrong type for accepted",
			input:   `["OK","aabbccddeeff","yes",""]`,
			isValid: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := json.NewDecoder(strings.NewReader(test.input))

			label, err := parseLabel(d)
			if err != nil {
				t.Fatalf("parseLabel failed: %v", err)
			}
			if label != "OK" {
				t.Fatalf("expected label %q, got %q", "OK", label)
			}

			got, err := parseOK(d)
			if test.isValid {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got.ID != test.expected.ID {
					t.Errorf("ID: expected %q, got %q", test.expected.ID, got.ID)
				}
				if got.Accepted != test.expected.Accepted {
					t.Errorf("Accepted: expected %v, got %v", test.expected.Accepted, got.Accepted)
				}
				if got.Reason != test.expected.Reason {
					t.Errorf("Reason: expected %q, got %q", test.expected.Reason, got.Reason)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error, got %+v", got)
				}
			}
		})
	}
}

func TestParseNotice(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *Notice
		isValid  bool
	}{
		{
			name:     "valid notice",
			input:    `["NOTICE","this is a notice from the relay"]`,
			expected: &Notice{Message: "this is a notice from the relay"},
			isValid:  true,
		},
		{
			name:     "empty message",
			input:    `["NOTICE",""]`,
			expected: &Notice{Message: ""},
			isValid:  true,
		},
		{
			name:    "missing message",
			input:   `["NOTICE"]`,
			isValid: false,
		},
		{
			name:    "number instead of message string",
			input:   `["NOTICE",404]`,
			isValid: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := json.NewDecoder(strings.NewReader(test.input))

			label, err := parseLabel(d)
			if err != nil {
				t.Fatalf("parseLabel failed: %v", err)
			}
			if label != "NOTICE" {
				t.Fatalf("expected label %q, got %q", "NOTICE", label)
			}

			got, err := parseNotice(d)
			if test.isValid {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got.Message != test.expected.Message {
					t.Errorf("Message: expected %q, got %q", test.expected.Message, got.Message)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error, got %+v", got)
				}
			}
		})
	}
}
