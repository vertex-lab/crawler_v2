package relays

import (
	"fmt"

	"github.com/goccy/go-json"
	"github.com/nbd-wtf/go-nostr"
)

// EOSE represents a ["EOSE", <subscriptionID>] message received from a relay,
// signalling that all stored events for a subscription have been sent.
type EOSE struct {
	ID string
}

// OKResponse represents a ["OK", <eventID>, <accepted>, <message>] message received from a relay,
// indicating whether a published event was accepted or rejected.
type OK struct {
	ID       string
	Accepted bool
	Reason   string
}

// Closed represents a ["CLOSED", <subscriptionID>, <message>] message received from a relay,
// indicating that a subscription was closed server-side.
type Closed struct {
	ID      string
	Message string
}

// Notice represents a ["NOTICE", <message>] message received from a relay,
// carrying a human-readable string intended for the client.
type Notice struct {
	Message string
}

// AuthChallenge represents a ["AUTH", <challenge>] message received from a relay,
// carrying the challenge string the client must sign to authenticate (NIP-42).
type AuthChallenge struct {
	Challenge string
}

// Event represents a ["EVENT", <subscriptionID>, <event>] message received from a relay,
// carrying an event that matches an open subscription.
type Event struct {
	SubID string
	Event nostr.Event
}

// ParseLabel parses a JSON array containing a label string.
func parseLabel(d *json.Decoder) (string, error) {
	token, err := d.Token()
	if err != nil {
		return "", fmt.Errorf("failed to read next JSON token: %w", err)
	}

	if token != json.Delim('[') {
		return "", fmt.Errorf("expected JSON array start '[' but got %v", token)
	}

	var label string
	if err := d.Decode(&label); err != nil {
		return "", fmt.Errorf("failed to read label: %w", err)
	}
	return label, nil
}

// parseEvent decodes a ["EVENT", <subscriptionID>, <event>] message from the relay.
// parseLabel must be called first to consume the opening bracket and label.
func parseEvent(d *json.Decoder) (Event, error) {
	var e Event
	if err := d.Decode(&e.SubID); err != nil {
		return Event{}, fmt.Errorf("failed to decode subscription ID: %w", err)
	}
	if err := d.Decode(&e.Event); err != nil {
		return Event{}, fmt.Errorf("failed to decode event: %w", err)
	}
	return e, nil
}

// parseClosed decodes a ["CLOSED", <subscriptionID>, <message>] message from the relay.
// parseLabel must be called first to consume the opening bracket and label.
func parseClosed(d *json.Decoder) (Closed, error) {
	var c Closed
	if err := d.Decode(&c.ID); err != nil {
		return Closed{}, fmt.Errorf("failed to decode subscription ID: %w", err)
	}
	if err := d.Decode(&c.Message); err != nil {
		return Closed{}, fmt.Errorf("failed to decode message: %w", err)
	}
	return c, nil
}

// parseAuth decodes a ["AUTH", <challenge>] message from the relay.
// parseLabel must be called first to consume the opening bracket and label.
func parseAuth(d *json.Decoder) (AuthChallenge, error) {
	var a AuthChallenge
	if err := d.Decode(&a.Challenge); err != nil {
		return AuthChallenge{}, fmt.Errorf("failed to decode challenge: %w", err)
	}
	return a, nil
}

// ParseOK decodes a ["OK", <eventID>, <accepted>, <message>] message from the relay.
// parseLabel must be called first to consume the opening bracket and label.
func parseOK(d *json.Decoder) (OK, error) {
	var ok OK
	if err := d.Decode(&ok.ID); err != nil {
		return OK{}, fmt.Errorf("failed to decode event ID: %w", err)
	}
	if err := d.Decode(&ok.Accepted); err != nil {
		return OK{}, fmt.Errorf("failed to decode accepted flag: %w", err)
	}
	if err := d.Decode(&ok.Reason); err != nil {
		return OK{}, fmt.Errorf("failed to decode reason: %w", err)
	}
	return ok, nil
}

// parseNotice decodes a ["NOTICE", <message>] message from the relay.
// parseLabel must be called first to consume the opening bracket and label.
func parseNotice(d *json.Decoder) (Notice, error) {
	var n Notice
	if err := d.Decode(&n.Message); err != nil {
		return Notice{}, fmt.Errorf("failed to decode message: %w", err)
	}
	return n, nil
}
