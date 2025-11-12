package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/vertex-lab/crawler_v2/pkg/config"
	"github.com/vertex-lab/crawler_v2/pkg/pipe"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
	"github.com/vertex-lab/crawler_v2/pkg/store"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
)

/*
This program syncronize the Redis database to the events already stored in the event store.
If Redis and the eventstore are already in sync, go run /cmd/crawl.
*/

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pipe.HandleSignals(cancel)

	log.Printf("--------- starting up the sync process --------")
	defer log.Printf("------------------------------------------------")

	config, err := config.Load()
	if err != nil {
		panic(err)
	}

	config.Fetcher.Kinds = []int{nostr.KindFollowList} // no need to sync other event kinds

	db, err := regraph.New(&redis.Options{Addr: config.RedisAddress})
	if err != nil {
		panic(err)
	}

	defer db.Close()
	slog.Info("redis connected", "address", config.RedisAddress)

	store, err := store.New(config.SQLiteURL)
	if err != nil {
		panic(err)
	}

	defer store.Close()
	slog.Info("sqlite connected", "path", config.SQLiteURL)

	grapherQueue := make(chan *nostr.Event, config.ChannelCapacity)
	fetcherQueue := make(chan string, config.ChannelCapacity)

	nodes, err := db.NodeCount(ctx)
	if err != nil {
		panic(err)
	}

	if nodes != 0 {
		panic("refuse to sync when redis is not empty")
	}

	slog.Info("initialize from empty database...")

	if err := pipe.InitGraph(ctx, db, config.InitPubkeys); err != nil {
		panic(err)
	}

	for _, pk := range config.InitPubkeys {
		fetcherQueue <- pk
	}

	slog.Info("correctly added init pubkeys", "count", len(config.InitPubkeys))

	if config.PrintStats {
		go printStats(ctx, grapherQueue, fetcherQueue)
	}

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		pipe.FetcherDB(ctx, config.Fetcher, fetcherQueue, store, pipe.Send(grapherQueue))
		close(grapherQueue)
	}()

	go func() {
		defer wg.Done()
		pipe.Arbiter(ctx, config.Arbiter, db, pipe.Send(fetcherQueue))
		close(fetcherQueue)
	}()

	go func() {
		defer wg.Done()
		pipe.Grapher(ctx, config.Engine.Grapher, grapherQueue, db)
	}()

	wg.Wait()
}

func printStats(
	ctx context.Context,
	grapherQueue chan *nostr.Event,
	fetcherQueue chan string,
) {
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
			log.Printf("Grapher queue: %d/%d\n", len(grapherQueue), cap(grapherQueue))
			log.Printf("FetcherDB queue: %d/%d\n", len(fetcherQueue), cap(fetcherQueue))
			log.Printf("walks tracker: %v\n", pipe.WalksTracker.Load())
			log.Printf("goroutines: %d\n", goroutines)
			log.Printf("memory usage: %.2f MB\n", float64(memStats.Alloc)/(1024*1024))
			log.Println("---------------------------------------")
		}
	}
}
