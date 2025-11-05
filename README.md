# Crawler v2

This repo is a rewrite of the original and discontinued [crawler](https://github.com/vertex-lab/crawler), under active development.

## Goals

The goals of this project are:

- Continuously crawl the Nostr network (24/7/365), searching for follow lists (`kind:3`) and other relevant events.

- Quickly assess whether new events should be added to the database based on the author's rank. Approved events are used to build a custom Redis-backed graph database.

- Generate and maintain random walks for nodes in the graph, updating them as the graph topology evolves.

- Use these random walks to efficiently compute acyclic Monte Carlo Pagerank (personalized and global). Algorithms are inspired by the paper [Fast Incremental and Personalized PageRank](snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf).

## Apps

`/cmd/crawl/`

The main entry point, which assumes that the SQLite event store and the Redis graph are syncronized. In case they are empty, the graph will be initialized using the `INIT_PUBKEYS` specified in the environment.

`/cmd/sync/`

This mode builds the Redis graph database from the SQLite event store. In other words, it syncronizes the Redis graph to reflect the events SQLite store, starting from the `INIT_PUBKEYS` specified in the environment, and expanding outward.

## How to run

### Step 1. Redis

Make sure you have [Redis](https://redis.io/) version `7.4.1` or higher installed, up and running. Check out the [official documentation](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/) for how to install it.

The Redis database must be empty before the first run.

### Step 2. SQLite

Make sure you have [SQLite](https://sqlite.org/index.html) version `3.49.0` or higher installed, up and running. Check out the [official documentation](https://sqlite.org/download.html) for how to install it.
 
SQLite must be compiled with the [Full Text Search 5 Extention](https://sqlite.org/fts5.html).

### Step 3. Environment

Create a `.env` file to be used by the specific app you want to run. Here is an example file.

```bash
# System

# The host:port where Redis runs.
REDIS_ADDRESS=localhost:6379

# The file path to the sqlite database.
SQLITE_URL=store.sqlite

# The buffer capacity of internal channels.
# Higher capacity implies higher memory consumption, but means lower risk of dropping events / pubkeys due to high load.
CHANNEL_CAPACITY=200000

# The pubkeys that are considered reputable if Redis is empty.
# Must be a comma separated list of hex pubkeys.
INIT_PUBKEYS=3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d

# Firehose
# Firehose is responsible for fetching new events from relays as they are published in real-time.

# The Firehose's filter will have a since = now - offset.
# This allow the Firehose to get events it might have missed during a restart.
# Setting this offset too high might result in rate-limiting by relays.
FIREHOSE_OFFSET=60s

# The event kinds the Firehose queries for.
FIREHOSE_KINDS=0,3,10000,10002,10063

# Fetcher
# Fetcher is responsible for fetching old events of pubkeys promoted by the Arbiter.

# The event kinds the Fetcher queries for.
FETCHER_KINDS=0,3,10000,10002,10063

# Max number of pubkeys per batch when querying for Nostr events.
# Higher values increase data throughput but may result in rate-limiting by relays.
FETCHER_BATCH=100

# Maximum time to wait before fetching.
# If a batch is not full, a fetch query will still run after this interval.
# Lower values reduce latency but may result in rate-limiting by relays.
FETCHER_INTERVAL=60s

# Firehose and Fetcher

# The comma separated list of relays both the Firehose and Fetcher use.
RELAYS=wss://relay.primal.net,wss://relay.damus.io,wss://nos.lol,wss://eden.nostr.land,wss://relay.current.fyi,wss://nostr.wine,wss://relay.nostr.band

# Arbiter
# Arbiter is responsible for scanning through Redis periodically, promoting or demoting pubkeys based on their pagerank.

# How often the Arbiter activates, as the % of random walks that changed since the last activation.  
# Lower values reduce latency but result in higher CPU usage and DB reads.
ARBITER_ACTIVATION=0.01

# How often the Arbiter should check for the % of random walks that changes since the last activation.
ARBITER_PING_WAIT=10s

# The threshold for promoting a pubkey, as a % of the base pagerank.
ARBITER_PROMOTION=0.01

# The threshold for demoting a pubkey, as a % of the base pagerank.
ARBITER_DEMOTION=0.0

# How long should the promotion have to wait.
# Higher values improve sybil-resistance but slow down crawling.
ARBITER_PROMOTION_WAIT=0s

# Whether or not the Arbiter should log statistics on stdout.
ARBITER_PRINT_STATS=true

# Archiver
# Archiver is responsible for saving events to SQLite.

# Event kinds the Archiver will save. Kinds not in the list will be ignored.
ARCHIVER_KINDS=0,3,10000,10002,10063

# How often the Archiver will log progress to stdout (in number of events).
ARCHIVER_PRINT_EVERY=10000

# Grapher
# Grapher is responsible for updating the Redis graph database.

# The maximum size of the Grapher's cache.
# Higher values improve speed but increase RAM usage.
GRAPHER_CACHE_CAPACITY=100000

# How often the Grapher will log progress to stdout (in number of events).
GRAPHER_PRINT_EVERY=5000
```

### Step 4. Build

Build the app you want to use.

```
# build the crawl app
go build --tags "fts5" -o path/for/the/executable/crawl cmd/crawl/*.go
```

```
# build the sync app
go build --tags "fts5" -o path/for/the/executable/sync cmd/sync/*.go
```

Then you can run it with `./crawl` or `./sync`.