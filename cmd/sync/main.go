package main

import (
	"context"
	"fmt"
	"github/pippellia-btc/crawler/pkg/config"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/pipe"
	"github/pippellia-btc/crawler/pkg/redb"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/relay/pkg/eventstore"
)

/*
This program syncronize the Redis database to the events already stored in the event store.
If Redis and the eventstore are already in sync, run the executable at /cmd/crawler/.
*/

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handleSignals(cancel)

	config, err := config.Load()
	if err != nil {
		panic(err)
	}

	events := make(chan *nostr.Event, config.EventsCapacity)
	pubkeys := make(chan string, config.PubkeysCapacity)

	store, err := eventstore.New(config.SQLiteURL)
	if err != nil {
		panic(err)
	}

	db := redb.New(&redis.Options{Addr: config.RedisAddress})
	count, err := db.NodeCount(ctx)
	if err != nil {
		panic(err)
	}

	if count != 0 {
		panic("refuse to run sync when redis is not empty")
	}

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

	pipe.Kinds = []int{
		nostr.KindFollowList, // no need to sync other event kinds
	}

	go printStats(ctx, events, pubkeys)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		pipe.FetcherDB(ctx, config.Fetcher, store, pubkeys, enqueue(events))
		close(events) // FetcherDB is the only event producer
	}()

	go func() {
		defer wg.Done()
		pipe.Arbiter(ctx, config.Arbiter, db, enqueue(pubkeys))
		close(pubkeys) // Arbiter is the only pubkey producer
	}()

	go func() {
		defer wg.Done()
		pipe.GraphBuilder(ctx, config.Engine, db, events)
	}()

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
