package relays

import (
	"encoding/json"

	"github.com/nbd-wtf/go-nostr"
)

type Request json.Marshaler

// Req represents a ["REQ", <subscriptionID>, <filter>...] message sent to a relay
// to open a subscription and request events matching the given filters.
type Req struct {
	ID      string
	Filters nostr.Filters
}

func (r Req) MarshalJSON() ([]byte, error) {
	payload := make([]any, 0, 2+len(r.Filters))
	payload = append(payload, "REQ", r.ID)
	for _, f := range r.Filters {
		payload = append(payload, f)
	}
	return json.Marshal(payload)
}

// Close represents a ["CLOSE", <subscriptionID>] message sent to a relay
// to close an open subscription.
type Close struct {
	ID string
}

func (c Close) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{"CLOSE", c.ID})
}

// AuthRequest represents a ["AUTH", <event>] message sent to a relay
// to authenticate via NIP-42.
type AuthRequest struct {
	Event nostr.Event
}

func (m AuthRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{"AUTH", m.Event})
}
