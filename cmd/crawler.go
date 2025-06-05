package main

import (
	"context"
	"fmt"
	"github/pippellia-btc/crawler/pkg/graph"
	"github/pippellia-btc/crawler/pkg/pipe"
	"github/pippellia-btc/crawler/pkg/redb"
	"github/pippellia-btc/crawler/pkg/walks"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/redis/go-redis/v9"
)

var events chan *nostr.Event
var pubkeys chan string

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handleSignals(cancel)

	config, err := LoadConfig()
	if err != nil {
		panic(err)
	}

	events = make(chan *nostr.Event, config.EventsCapacity)
	pubkeys = make(chan string, config.PubkeysCapacity)

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

	go printStats(ctx)
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

func printStats(ctx context.Context) {
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
