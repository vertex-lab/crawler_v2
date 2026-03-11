package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/vertex-lab/crawler_v2/pkg/config"
	"github.com/vertex-lab/crawler_v2/pkg/pipe"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/arbiter"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/engine"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/fetcher"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/firehose"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/pool"
	"github.com/vertex-lab/crawler_v2/pkg/pipe/recorder"
	"github.com/vertex-lab/crawler_v2/pkg/regraph"
	"github.com/vertex-lab/crawler_v2/pkg/relays"
	"github.com/vertex-lab/crawler_v2/pkg/store"
	sqlite "github.com/vertex-lab/nostr-sqlite"

	"github.com/redis/go-redis/v9"
)

// This programs assumes syncronization between Redis and the event store, meaning
// that the graph in Redis reflects these events.
// If that is not the case, go run /cmd/sync to syncronize Redis with the event store.

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info("--------- starting up the crawler ---------")
	defer slog.Info("-------------------------------------------------")

	config, err := config.Load()
	if err != nil {
		panic(err)
	}
	if err := config.Validate(); err != nil {
		panic(err)
	}

	graph, err := regraph.New(&redis.Options{Addr: config.RedisAddress})
	if err != nil {
		panic(err)
	}
	defer graph.Close()
	slog.Info("redis connected", "address", config.RedisAddress)

	store, err := store.New(
		config.SqlitePath,
		sqlite.WithEventPolicy(pipe.EventTooBig),
	)
	if err != nil {
		panic(err)
	}
	defer store.Close()
	slog.Info("sqlite connected", "path", config.SqlitePath)

	poolLogger, close, err := NewFileLogger("pool.log", slog.LevelDebug)
	if err != nil {
		panic(err)
	}
	defer close()

	pool, err := pool.New(config.Pool, relays.WithLogger(poolLogger))
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	firehose := firehose.New(config.Firehose, pool, firehose.ExistPolicy(graph, config.Firehose.CacheSize))
	fetcher := fetcher.New(config.Fetcher, fetcher.SourcePool(pool))
	engine := engine.New(config.Engine, store, graph)
	recorder := recorder.New(config.Recorder, graph)
	arbiter := arbiter.New(config.Arbiter, graph)

	nodes, err := graph.NodeCount(ctx)
	if err != nil {
		panic(err)
	}

	if nodes == 0 {
		slog.Info("initializing from empty database...")
		if err := pipe.InitGraph(ctx, graph, config.InitPubkeys); err != nil {
			panic(err)
		}
		for _, pk := range config.InitPubkeys {
			if err := fetcher.Enqueue(pk); err != nil {
				panic(err)
			}
		}
		slog.Info("correctly added init pubkeys", "count", len(config.InitPubkeys))
	}

	wg := sync.WaitGroup{}
	wg.Add(5)

	go func() {
		defer wg.Done()
		firehose.Run(ctx, recorder.Enqueue)
	}()

	go func() {
		defer wg.Done()
		recorder.Run(ctx, engine.Enqueue)
	}()

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
		engine.Ingest(ctx)
	}()

	wg.Wait()
}

func NewFileLogger(path string, level slog.Level) (*slog.Logger, func() error, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}

	l := slog.New(slog.NewTextHandler(f, &slog.HandlerOptions{Level: level}))
	return l, f.Close, nil
}
