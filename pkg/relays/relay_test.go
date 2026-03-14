package relays

import (
	"context"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ctx    = context.Background()
	primal = "wss://relay.primal.net"
	damus  = "wss://relay.damus.io"
	pip    = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
)

func TestRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	relay, err := New(ctx, damus)
	if err != nil {
		t.Fatalf("failed to create relay: %v", err)
	}
	defer relay.Close()
	t.Logf("connected to %s", relay.URL())

	filter := nostr.Filter{
		Kinds: []int{1},
		Limit: 10,
	}

	sub, err := relay.Subscribe("test", filter)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer sub.Close()

	var counter int
	eose := sub.EOSE()

	for {
		select {
		case <-ctx.Done():
			// all good!
			t.Logf("[%d] context done", counter)
			return

		case <-sub.Done():
			t.Fatalf("[%d] subscription was closed: %v", counter, sub.Err())
			return

		case <-eose:
			t.Logf("[%d] received eose", counter)
			eose = nil // avoid infinite loop

		case event := <-sub.Events():
			t.Logf("[%d] received event %s", counter, event.ID)
		}

		counter++
	}
}

func TestValidateURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		isValid bool
	}{
		{
			name:    "valid wss url",
			url:     "wss://relay.example.com",
			isValid: true,
		},
		{
			name:    "valid ws url with path",
			url:     "ws://relay.example.com/inbox",
			isValid: true,
		},
		{
			name:    "valid url with port",
			url:     "wss://relay.example.com:4848",
			isValid: true,
		},
		{
			name:    "empty url",
			url:     "",
			isValid: false,
		},
		{
			name:    "missing scheme",
			url:     "relay.example.com",
			isValid: false,
		},
		{
			name:    "invalid scheme htestp",
			url:     "htestp://relay.example.com",
			isValid: false,
		},
		{
			name:    "missing host",
			url:     "wss://",
			isValid: false,
		},
		{
			name:    "userinfo not allowed",
			url:     "wss://user:pass@relay.example.com",
			isValid: false,
		},
		{
			name:    "query string not allowed",
			url:     "wss://relay.example.com?broadcast=true",
			isValid: false,
		},
		{
			name:    "fragment not allowed",
			url:     "wss://relay.example.com#frag",
			isValid: false,
		},
		{
			name:    "invalid port",
			url:     "wss://relay.example.com:abc",
			isValid: false,
		},
		{
			name:    "empty port",
			url:     "wss://relay.example.com:",
			isValid: true,
		},
		{
			name:    "host is dot",
			url:     "wss://.",
			isValid: false,
		},
		{
			name:    "relative path only",
			url:     "/relay",
			isValid: false,
		},
		{
			name:    "onion url",
			url:     "ws://girwot2koy3kvj6fk7oseoqazp5vwbeawocb3m27jcqtah65f2fkl3yd.onion",
			isValid: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateURL(test.url)
			if !test.isValid && err == nil {
				t.Fatalf("ValidateURL(%q) expected error, got nil", test.url)
			}
			if test.isValid && err != nil {
				t.Fatalf("ValidateURL(%q) unexpected error: %v", test.url, err)
			}
		})
	}
}

func TestNormalizeURL(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		result  string
		isValid bool
	}{
		// Basic valid cases
		{name: "clean wss", input: "wss://relay.example.com", result: "wss://relay.example.com", isValid: true},
		{name: "clean ws upgraded to wss", input: "ws://relay.example.com", result: "wss://relay.example.com", isValid: true},

		// Trailing slashes
		{name: "trailing slash removed", input: "wss://relay.example.com/", result: "wss://relay.example.com", isValid: true},
		{name: "multiple trailing slashes removed", input: "wss://relay.example.com///", result: "wss://relay.example.com", isValid: true},
		{name: "path with trailing slash", input: "wss://relay.example.com/nostr/", result: "wss://relay.example.com/nostr", isValid: true},
		{name: "path without trailing slash preserved", input: "wss://relay.example.com/nostr", result: "wss://relay.example.com/nostr", isValid: true},

		// Casing
		{name: "uppercase scheme lowercased", input: "WSS://relay.example.com", result: "wss://relay.example.com", isValid: true},
		{name: "mixed case scheme lowercased", input: "Wss://relay.example.com", result: "wss://relay.example.com", isValid: true},
		{name: "uppercase host lowercased", input: "wss://RELAY.EXAMPLE.COM", result: "wss://relay.example.com", isValid: true},
		{name: "mixed case host lowercased", input: "wss://Relay.Example.Com", result: "wss://relay.example.com", isValid: true},
		{name: "uppercase ws upgraded and lowercased", input: "WS://relay.example.com", result: "wss://relay.example.com", isValid: true},

		// Ports
		{name: "explicit port preserved", input: "wss://relay.example.com:443", result: "wss://relay.example.com:443", isValid: true},
		{name: "non-standard port preserved", input: "wss://relay.example.com:8080", result: "wss://relay.example.com:8080", isValid: true},

		// IP addresses
		{name: "ipv4 address", input: "wss://192.168.1.1", result: "wss://192.168.1.1", isValid: true},
		{name: "ipv4 with port", input: "wss://192.168.1.1:8080", result: "wss://192.168.1.1:8080", isValid: true},
		{name: "ipv6 address", input: "wss://[::1]", result: "wss://[::1]", isValid: true},
		{name: "ipv6 with port", input: "wss://[::1]:8080", result: "wss://[::1]:8080", isValid: true},

		// Invalid: empty
		{name: "empty string", input: ""},

		// Invalid: wrong scheme
		{name: "http scheme", input: "http://relay.example.com"},
		{name: "https scheme", input: "https://relay.example.com"},
		{name: "no scheme", input: "relay.example.com"},
		{name: "ftp scheme", input: "ftp://relay.example.com"},

		// Invalid: missing host
		{name: "missing host", input: "wss://"},
		{name: "slash only path no host", input: "wss:///path"},

		// Invalid: onion
		{name: "onion address", input: "wss://abcdefg.onion"},
		{name: "onion address uppercase", input: "wss://abcdefg.ONION"},

		// Invalid: userinfo
		{name: "userinfo not allowed", input: "wss://user:pass@relay.example.com"},
		{name: "user only not allowed", input: "wss://user@relay.example.com"},

		// Invalid: query string
		{name: "query string not allowed", input: "wss://relay.example.com?foo=bar"},

		// Invalid: fragment
		{name: "fragment not allowed", input: "wss://relay.example.com#section"},

		// Combos a user might realistically type
		{name: "ws with trailing slash", input: "ws://relay.example.com/", result: "wss://relay.example.com", isValid: true},
		{name: "uppercase host with trailing slash", input: "wss://RELAY.EXAMPLE.COM/", result: "wss://relay.example.com", isValid: true},
		{name: "ws uppercase host trailing slash", input: "WS://RELAY.EXAMPLE.COM/", result: "wss://relay.example.com", isValid: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NormalizeURL(test.input)
			if test.isValid && err != nil {
				t.Fatalf("NormalizeURL(%q) error = %v, isValid %v", test.input, err, test.isValid)
			}
			if !test.isValid && err == nil {
				t.Fatalf("NormalizeURL(%q) error = nil, isValid %v", test.input, test.isValid)
			}
			if got != test.result {
				t.Errorf("NormalizeURL(%q) = %q, want %q", test.input, got, test.result)
			}
		})
	}
}
