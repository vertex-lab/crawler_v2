# Crawler v2

This repo is a rewrite of the original and discontinued [crawler](https://github.com/vertex-lab/crawler), under active development.

## Goals

The goals of this project are:

- Continuously crawl the Nostr network (24/7/365), searching for `kind:3` follow-lists  and other relevant events among a wide array of relays.

- Relays are seeded from a list and then discovered dynamically using `kind:10002` relay-lists. Subscription closures and relay disconnections are handled gracefully with retries and backoff.

- Efficiently assess whether new events should be added to the database based on the author's rank. Approved events are used to build a custom Redis-backed graph database.

- Generate and maintain random walks for nodes in the graph, updating them as the graph topology evolves.

- Use these random walks to efficiently compute acyclic Monte Carlo Pagerank (personalized and global). Algorithms are inspired by the paper [Fast Incremental and Personalized PageRank](https://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf).

## Apps

`/cmd/crawl/`

The main entry point, which assumes that the Sqlite event store and the Redis graph are syncronized. In case they are empty, the graph will be initialized using the `INIT_PUBKEYS` specified in the environment.

`/cmd/sync/`

This mode builds the Redis graph database from the Sqlite event store. In other words, it syncronizes the Redis graph to reflect the events Sqlite store, starting from the `INIT_PUBKEYS` specified in the environment, and expanding outward.

## How to run

### Step 1. Redis

Make sure you have [Redis](https://redis.io/) version `7.4.1` or higher installed, up and running. Check out the [official documentation](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/) for how to install it.

The Redis database must be empty before the first run.

### Step 2. Sqlite

Make sure you have [Sqlite](https://sqlite.org/index.html) version `3.49.0` or higher installed, up and running. Check out the [official documentation](https://sqlite.org/download.html) for how to install it.
 
Sqlite must be compiled with the [Full Text Search 5 Extention](https://sqlite.org/fts5.html).

### Step 3. Environment

Create a `.env` file to be used by the specific app you want to run.  
You can copy the `.env.example` and modify it to fit your needs. Make sure to place the `.env` in the same directory as the executable(s).

```bash
cp .env.example your/path/.env
```

### Step 4. Build

Build the app you want to use.

```
# build the crawl app
go build --tags "fts5" -o your/path/crawl cmd/crawl/*.go
```

```
# build the sync app
go build --tags "fts5" -o your/path/sync cmd/sync/*.go
```

Then you can run it with `./crawl` or `./sync`.
