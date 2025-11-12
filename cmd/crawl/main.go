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
	sqlite "github.com/vertex-lab/nostr-sqlite"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
)

/*
This programs assumes syncronization between Redis and the event store, meaning
that the graph in Redis reflects these events.
If that is not the case, go run /cmd/sync to syncronize Redis with the event store.
*/

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pipe.HandleSignals(cancel)

	log.Println("--------- starting up the crawler --------")
	defer log.Println("-----------------------------------------")

	config, err := config.Load()
	if err != nil {
		panic(err)
	}

	db, err := regraph.New(
		&redis.Options{Addr: config.RedisAddress},
	)
	if err != nil {
		panic(err)
	}

	defer db.Close()
	slog.Info("redis connected", "address", config.RedisAddress)

	store, err := store.New(
		config.SQLiteURL,
		sqlite.WithEventPolicy(pipe.EventTooBig),
	)
	if err != nil {
		panic(err)
	}

	defer store.Close()
	slog.Info("sqlite connected", "path", config.SQLiteURL)

	recorderQueue := make(chan *nostr.Event, config.ChannelCapacity)
	engineQueue := make(chan *nostr.Event, config.ChannelCapacity)
	fetcherQueue := make(chan string, config.ChannelCapacity)

	nodes, err := db.NodeCount(ctx)
	if err != nil {
		panic(err)
	}

	if nodes == 0 {
		slog.Info("initializing from empty database...")

		if err := pipe.InitGraph(ctx, db, config.InitPubkeys); err != nil {
			panic(err)
		}

		for _, pk := range config.InitPubkeys {
			fetcherQueue <- pk
		}

		slog.Info("correctly added init pubkeys", "count", len(config.InitPubkeys))
	}

	if config.PrintStats {
		go printStats(ctx, recorderQueue, engineQueue, fetcherQueue)
	}

	var producers sync.WaitGroup
	var consumers sync.WaitGroup

	producers.Add(4)
	go func() {
		defer producers.Done()
		gate := pipe.NewExistenceGate(db)
		pipe.Firehose(ctx, config.Firehose, gate, pipe.Send(recorderQueue))
		close(recorderQueue)
	}()

	go func() {
		defer producers.Done()
		pipe.Recorder(ctx, recorderQueue, db, pipe.Send(engineQueue))
	}()

	go func() {
		defer producers.Done()
		pipe.Fetcher(ctx, config.Fetcher, fetcherQueue, pipe.Send(engineQueue))
	}()

	go func() {
		defer producers.Done()
		pipe.Arbiter(ctx, config.Arbiter, db, pipe.Send(fetcherQueue))
		close(fetcherQueue)
	}()

	consumers.Add(1)
	go func() {
		defer consumers.Done()
		pipe.Engine(ctx, config.Engine, engineQueue, store, db)
	}()

	producers.Wait()
	close(engineQueue)
	consumers.Wait()
}

func printStats(
	ctx context.Context,
	recorderQueue, engineQueue chan *nostr.Event,
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
			log.Printf("Recorder queue: %d/%d\n", len(recorderQueue), cap(recorderQueue))
			log.Printf("Engine queue: %d/%d\n", len(engineQueue), cap(engineQueue))
			log.Printf("Fetcher queue: %d/%d\n", len(fetcherQueue), cap(fetcherQueue))
			log.Printf("walks tracker: %v\n", pipe.WalksTracker.Load())
			log.Printf("goroutines: %d\n", goroutines)
			log.Printf("memory usage: %.2f MB\n", float64(memStats.Alloc)/(1024*1024))
			log.Println("---------------------------------------")
		}
	}
}
