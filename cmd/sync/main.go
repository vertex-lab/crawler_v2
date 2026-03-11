package main

import (
	"context"
	"log"
	"log/slog"
	"os/signal"
	"sync"
	"syscall"

	"github.com/vertex-lab/crawler_v2/pkg/config"
	"github.com/vertex-lab/crawler_v2/pkg/pipe"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/arbiter"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/engine"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/fetcher"
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
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Println("--------- starting up the sync process --------")
	defer log.Println("------------------------------------------------")

	config, err := config.Load()
	if err != nil {
		panic(err)
	}
	if err := config.Validate(); err != nil {
		panic(err)
	}
	config.Fetcher.Kinds = []int{nostr.KindFollowList} // no need to sync other event kinds

	graph, err := regraph.New(&redis.Options{Addr: config.RedisAddress})
	if err != nil {
		panic(err)
	}
	defer graph.Close()
	slog.Info("redis connected", "address", config.RedisAddress)

	nodes, err := graph.NodeCount(ctx)
	if err != nil {
		panic(err)
	}
	if nodes != 0 {
		panic("refuse to sync when redis is not empty")
	}
	slog.Info("initialize from empty database...")

	store, err := store.New(config.SqlitePath)
	if err != nil {
		panic(err)
	}
	defer store.Close()
	slog.Info("sqlite connected", "path", config.SqlitePath)

	fetcher := fetcher.New(config.Fetcher, store)
	engine := engine.New(config.Engine, store, graph)
	arbiter := arbiter.New(config.Arbiter, graph)

	if err := pipe.InitGraph(ctx, graph, config.InitPubkeys); err != nil {
		panic(err)
	}
	for _, pk := range config.InitPubkeys {
		if err := fetcher.Enqueue(pk); err != nil {
			panic(err)
		}
	}
	slog.Info("correctly added init pubkeys", "count", len(config.InitPubkeys))

	wg := sync.WaitGroup{}
	wg.Add(3)

	go func() {
		defer wg.Done()
		fetcher.Run(ctx, engine.Enqueue)
	}()

	go func() {
		defer wg.Done()
		arbiter.Run(ctx, engine.WalksUpdated, fetcher.Enqueue)
	}()

	go func() {
		defer wg.Done()
		engine.Sync(ctx)
	}()

	wg.Wait()
}
