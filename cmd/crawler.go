package main

import (
	"context"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/pipe"
	"github/pippellia-btc/crawler/pkg/redb"
	"github/pippellia-btc/crawler/pkg/walks"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handleSignals(cancel)

	config, err := LoadConfig()
	if err != nil {
		panic(err)
	}

	events := make(chan *nostr.Event, config.EventsCapacity)
	pubkeys := make(chan string, config.PubkeysCapacity)

	db := redb.New(&redis.Options{Addr: config.RedisAddress})
	count, err := db.NodeCount(ctx)
	if err != nil {
		panic(err)
	}

	if count == 0 {
		log.Println("initializing crawler from empty database")

		nodes := make([]graph.ID, len(config.InitPubkeys))
		for i, pk := range config.InitPubkeys {
			nodes[i], err = db.AddNode(ctx, pk)
			if err != nil {
				panic(err)
			}

			pubkeys <- pk // add to queue
		}

		walks, err := walks.Generate(ctx, db, nodes...)
		if err != nil {
			panic(err)
		}

		if err := db.AddWalks(ctx, walks...); err != nil {
			panic(err)
		}

		log.Printf("correctly added %d init pubkeys", len(config.InitPubkeys))
	}

	_ = events

	// eventStore, err := eventstore.New(config.SQLiteURL)
	// if err != nil {
	// 	panic("failed to connect to the sqlite eventstore: " + err.Error())
	// }

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		pipe.Firehose(ctx, config.Firehose, db, func(event *nostr.Event) error {
			select {
			case events <- event:
			default:
				log.Printf("Firehose: channel is full, dropping event ID %s by %s", event.ID, event.PubKey)
			}
			return nil
		})
	}()

	go func() {
		defer wg.Done()
		pipe.Fetcher(ctx, config.Fetcher, pubkeys, func(event *nostr.Event) error {
			select {
			case events <- event:
			default:
				log.Printf("Fetcher: channel is full, dropping event ID %s by %s", event.ID, event.PubKey)
			}
			return nil
		})
	}()

	go func() {
		defer wg.Done()
		pipe.Arbiter(ctx, config.Arbiter, db, func(pubkey string) error {
			select {
			case pubkeys <- pubkey:
			default:
				log.Printf("Arbiter: channel is full, dropping pubkey %s", pubkey)
			}
			return nil
		})
	}()

	log.Println("ready to process events")
	pipe.Processor(ctx, config.Processor, db, events)
	wg.Wait()
}

// handleSignals listens for OS signals and triggers context cancellation.
func handleSignals(cancel context.CancelFunc) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals

	log.Println(" Signal received. Shutting down...")
	cancel()
}
