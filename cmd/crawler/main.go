package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/vertex-lab/crawler_v2/pkg/config"
	"github.com/vertex-lab/crawler_v2/pkg/graph"
	"github.com/vertex-lab/crawler_v2/pkg/pipe"
	"github.com/vertex-lab/crawler_v2/pkg/redb"
	"github.com/vertex-lab/relay/pkg/store"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
)

/*
This programs assumes syncronization between Redis and the event store, meaning
that the graph in Redis reflects these events.
If that is not the case, go run /cmd/sync/ to syncronize Redis with the event store.
*/

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pipe.HandleSignals(cancel)

	log.Printf("--------- starting up the crawler --------")
	defer log.Printf("-----------------------------------------")

	config, err := config.Load()
	if err != nil {
		panic(err)
	}

	events := make(chan *nostr.Event, config.EventsCapacity)
	pubkeys := make(chan string, config.PubkeysCapacity)

	store, err := store.New(config.SQLiteURL)
	if err != nil {
		panic(err)
	}

	db := redb.New(&redis.Options{Addr: config.RedisAddress})
	count, err := db.NodeCount(ctx)
	if err != nil {
		panic(err)
	}

	if count == 0 {
		if len(config.InitPubkeys) == 0 {
			panic("init pubkeys are empty: impossible to initialize")
		}
		log.Println("initialize from empty database...")

		nodes := make([]graph.ID, len(config.InitPubkeys))
		for i, pk := range config.InitPubkeys {
			nodes[i], err = db.AddNode(ctx, pk)
			if err != nil {
				panic(err)
			}

			pubkeys <- pk // add to queue
		}

		for _, node := range nodes {
			if err := pipe.Promote(db, node); err != nil {
				panic(err)
			}
		}
		log.Printf("correctly added %d init pubkeys", len(config.InitPubkeys))
	}

	if config.PrintStats {
		go printStats(ctx, events, pubkeys)
	}

	var producers sync.WaitGroup
	var consumers sync.WaitGroup

	producers.Add(3)
	go func() {
		defer producers.Done()
		pipe.Firehose(ctx, config.Firehose, db, enqueue(events))
	}()

	go func() {
		defer producers.Done()
		pipe.Fetcher(ctx, config.Fetcher, pubkeys, enqueue(events))
	}()

	go func() {
		defer producers.Done()
		pipe.Arbiter(ctx, config.Arbiter, db, enqueue(pubkeys))
		close(pubkeys) // Arbiter is the only pubkey producer
	}()

	consumers.Add(1)
	go func() {
		defer consumers.Done()
		pipe.Engine(ctx, config.Engine, store, db, events)
	}()

	producers.Wait()
	close(events)

	consumers.Wait()
}

// enqueue things into the specified channel or return an error if full.
func enqueue[T any](queue chan T) func(t T) error {
	return func(t T) error {
		select {
		case queue <- t:
			return nil
		default:
			return fmt.Errorf("channel is full, dropping %v", t)
		}
	}
}

func printStats(ctx context.Context, events chan *nostr.Event, pubkeys chan string) {
	filename := "stats.log"
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(fmt.Errorf("failed to open log file %s: %w", filename, err))
	}

	defer file.Close()
	log := log.New(file, "stats: ", log.LstdFlags)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			goroutines := runtime.NumGoroutine()
			memStats := new(runtime.MemStats)
			runtime.ReadMemStats(memStats)

			log.Println("---------------------------------------")
			log.Printf("events queue: %d/%d\n", len(events), cap(events))
			log.Printf("pubkeys queue: %d/%d\n", len(pubkeys), cap(pubkeys))
			log.Printf("walks tracker: %v\n", pipe.WalksTracker.Load())
			log.Printf("goroutines: %d\n", goroutines)
			log.Printf("memory usage: %.2f MB\n", float64(memStats.Alloc)/(1024*1024))
			log.Println("---------------------------------------")
		}
	}
}
