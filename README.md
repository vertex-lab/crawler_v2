# Crawler v2

This repo is a rewrite of the original and discontinued [crawler](https://github.com/vertex-lab/crawler), under active developement.

## Goals

The goals of this project are:

- Continuously crawl the Nostr network (24/7/365), searching for follow lists (`kind:3`) and other relevant events.

- Quickly assess whether new events should be added to the database based on the author's rank. Approved events are used to build a custom Redis-backed graph database.

- Generate and maintain random walks for nodes in the graph, updating them as the graph topology evolves.

- Use these random walks to efficiently compute acyclic Monte Carlo Pageranks (personalized and global). Algorithms are inspired by [this paper](http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf)

## Apps

`/cmd/crawl/`

The main entry point, which assumes that the event store and Redis are syncronized. In case they are empty, the graph will be initialized using the `INIT_PUBKEYS` specified in the enviroment.

`/cmd/sync/`

This mode builds the Redis graph database from the event store. In other words, it syncronizes Redis to reflect the events in the event store, starting from the `INIT_PUBKEYS` specified in the enviroment, and expanding outward.

