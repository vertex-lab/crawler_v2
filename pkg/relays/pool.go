package relays

import (
	"context"
	"errors"
	"fmt"

	"sync"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/smallset"
)

var ErrNoRelays = fmt.Errorf("no relays in the pool")

// Pool represents a set of relays.
// All relays will have the same options, and will be treated as a single entity.
type Pool struct {
	mu           sync.Mutex
	connected    *smallset.Custom[*Relay]
	disconnected *smallset.Ordered[string] // only the urls
	opts         []Option
}

// NewPool creates a new pool of relays with the provided URLs and options.
// Call connect to establish connections to the relays, Close to disconnect all.
func NewPool(urls []string, opts ...Option) (*Pool, error) {
	disconnected := smallset.From(urls...)
	for _, u := range disconnected.Ascend() {
		if err := ValidateURL(u); err != nil {
			return nil, err
		}
	}

	pool := &Pool{
		connected:    smallset.NewCustom(Compare, disconnected.Size()),
		disconnected: disconnected,
		opts:         opts,
	}
	return pool, nil
}

// Connect to all relays in the pool.
// It returns the number of relays connected and any errors encountered.
// One goroutine is spawned per relay and connections are established concurrently.
func (p *Pool) Connect(ctx context.Context) (int, error) {
	p.mu.Lock()
	toConnect := p.disconnected.Items()
	p.mu.Unlock()

	if len(toConnect) == 0 {
		return 0, nil
	}

	type result struct {
		relay *Relay
		err   error
	}

	results := make(chan result, len(toConnect))
	wg := sync.WaitGroup{}

	for _, url := range toConnect {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			r, err := New(ctx, url, p.opts...)
			results <- result{r, err}
		}(url)
	}

	wg.Wait()
	close(results)

	var errs []error
	var connected int

	for res := range results {
		if res.err != nil {
			errs = append(errs, res.err)
			continue
		}

		p.mu.Lock()
		p.disconnected.Remove(res.relay.url)
		p.connected.Add(res.relay)
		connected++
		p.mu.Unlock()
	}
	return connected, errors.Join(errs...)
}

// Close closes all relay connections.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, r := range p.connected.Ascend() {
		r.Close()
	}
	p.connected.Clear()
}

// Query queries all currently connected relays in the pool concurrently with the given id and filters.
// It returns the combined events from all relays and a joined error of any individual query errors.
// An error from one relay does not prevent events from being returned from other relays.
//
// It is always recommended to use this method with a context timeout (e.g. 10s),
// to avoid bad relays that never send an EOSE (or CLOSED) from blocking indefinitely.
func (p *Pool) Query(ctx context.Context, id string, filters nostr.Filters) ([]nostr.Event, error) {
	p.mu.Lock()
	relays := p.connected.Items()
	p.mu.Unlock()

	type result struct {
		relay  string
		events []nostr.Event
		err    error
	}

	results := make(chan result, len(relays))
	wg := sync.WaitGroup{}

	for _, r := range relays {
		wg.Add(1)
		go func(r *Relay) {
			defer wg.Done()
			events, err := r.Query(ctx, id, filters)
			results <- result{r.url, events, err}
		}(r)
	}

	wg.Wait()
	close(results)

	var events []nostr.Event
	var err error

	for r := range results {
		// TODO: deduplicate events and handle replaceable and addressable events
		events = append(events, r.events...)
		err = errors.Join(err, fmt.Errorf("relay %s: %w", r.relay, r.err))
	}
	return events, err
}
