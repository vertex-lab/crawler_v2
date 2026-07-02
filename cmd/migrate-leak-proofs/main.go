// migrate rewrites the proof field of all confirmed leak records in Redis
// using the current compromise.MakeProof implementation.
//
// Run once after upgrading to a new proof scheme:
//
//	go run ./cmd/migrate
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Open-Ranking/go-sdk/compromise"
	"github.com/vertex-lab/crawler_v2/pkg/config"
	"github.com/vertex-lab/crawler_v2/pkg/leaks"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	client := redis.NewClient(&redis.Options{Addr: cfg.RedisAddress})
	defer client.Close()

	if err := run(ctx, client); err != nil {
		slog.Error("migration failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, client *redis.Client) error {
	// Scan all leak keys.
	var keys []string
	var cursor uint64
	for {
		batch, next, err := client.Scan(ctx, cursor, leaks.Key("*"), 100).Result()
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}

	slog.Info("found leak records", "count", len(keys))
	if len(keys) == 0 {
		return nil
	}

	// For each record, read the secret and rewrite the proof.
	var updated, skipped int
	pipe := client.Pipeline()

	for _, key := range keys {
		sk, err := client.HGet(ctx, key, "secret").Result()
		if err == redis.Nil {
			// Suspected record — no secret key stored, nothing to prove.
			skipped++
			continue
		}
		if err != nil {
			return fmt.Errorf("hget secret for %s: %w", key, err)
		}

		proof, err := compromise.MakeProof(sk)
		if err != nil {
			return fmt.Errorf("make proof for %s: %w", key, err)
		}

		pipe.HSet(ctx, key, "proof", proof)
		updated++
	}

	if updated > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("writing proofs: %w", err)
		}
	}

	slog.Info("migration complete", "updated", updated, "skipped", skipped)
	return nil
}
