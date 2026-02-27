package auth

import (
	"errors"
	"fmt"
	"sync"

	"github.com/nbd-wtf/go-nostr"
)

const Kind = 22242

var (
	ErrInvalidURL    = errors.New("invalid relay URL")
	ErrInvalidSecret = errors.New("invalid secret key")
	ErrNoChallenge   = errors.New("no challenge set")
)

// Handler contains the authentication state sent by the relay,
// as well as a secret key for signing AUTH requests.
type Handler struct {
	mu        sync.Mutex
	url       string
	challenge string
	sk        string
}

// NewHandler creates a new auth handler for the relay with the specified URL.
// It returns an error if the URL or secret key are invalid.
func NewHandler(url string, sk string) (*Handler, error) {
	if !nostr.IsValidRelayURL(url) {
		return nil, ErrInvalidURL
	}
	if !nostr.IsValid32ByteHex(sk) {
		return nil, fmt.Errorf("%w: sk is not a valid 32 byte hexadecimal", ErrInvalidSecret)
	}
	if _, err := nostr.GetPublicKey(sk); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSecret, err)
	}
	return &Handler{url: url, sk: sk}, nil
}

// SetChallenge sets the challenge for the handler.
func (h *Handler) SetChallenge(challenge string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.challenge = challenge
}

func (h *Handler) Response() (nostr.Event, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.challenge == "" {
		return nostr.Event{}, fmt.Errorf("Response: %w", ErrNoChallenge)
	}

	event := nostr.Event{
		Kind:      Kind,
		CreatedAt: nostr.Now(),
		Tags: nostr.Tags{
			{"relay", h.url},
			{"challenge", h.challenge},
		},
	}

	if err := event.Sign(h.sk); err != nil {
		return nostr.Event{}, fmt.Errorf("Response: %w", err)
	}
	return event, nil
}
