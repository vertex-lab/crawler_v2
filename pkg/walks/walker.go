package walks

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/vertex-lab/crawler_v2/pkg/graph"
)

type Walker interface {
	// Follows returns the follow-list of the node, used  for generating random walks
	Follows(ctx context.Context, node graph.ID) ([]graph.ID, error)
}

// CachedWalker is a [Walker] with optional fallback that stores follow relationships
// in a compact format (uint32) for reduced memory footprint.
// If its size grows larger than capacity, the least recently used (LRU) key is evicted.
// It is not safe for concurrent use.
type CachedWalker struct {
	lookup map[uint32]*list.Element

	// newest at the front, oldest at the back
	edgeList *list.List
	capacity int

	// for stats
	calls, hits int
	log         *log.Logger

	fallback Walker
}

type Option func(*CachedWalker)

func WithFallback(w Walker) Option { return func(c *CachedWalker) { c.fallback = w } }

func WithCapacity(cap int) Option {
	return func(c *CachedWalker) {
		c.lookup = make(map[uint32]*list.Element, cap)
		c.capacity = cap
	}
}

func WithLogFile(filename string) Option {
	return func(c *CachedWalker) {
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			panic(fmt.Errorf("failed to open log file %s: %w", filename, err))
		}
		c.log = log.New(file, "cache: ", log.LstdFlags)
	}
}

func NewWalker(opts ...Option) *CachedWalker {
	c := &CachedWalker{
		lookup:   make(map[uint32]*list.Element, 10000),
		edgeList: list.New(),
		capacity: 10000,
	}

	for _, opt := range opts {
		opt(c)
	}
	return c
}

type edges struct {
	node    uint32
	follows []uint32
}

// Add compresses node and follows and adds them to the cache.
// It evicts the LRU element if the capacity has been exeeded.
func (c *CachedWalker) Add(node graph.ID, follows []graph.ID) error {
	ID, err := compactID(node)
	if err != nil {
		return fmt.Errorf("failed to compress node %s: %w", node, err)
	}

	IDs, err := compactIDs(follows)
	if err != nil {
		return fmt.Errorf("failed to compress follows of node %s: %w", node, err)
	}

	c.add(ID, IDs)
	return nil
}

// Add node and follows as [edges]. It evicts the LRU element if the capacity has been exeeded.
func (c *CachedWalker) add(node uint32, follows []uint32) {
	if e, ok := c.lookup[node]; ok {
		// node already present, update value
		e.Value = edges{node: node, follows: follows}
		c.edgeList.MoveToFront(e)
		return
	}

	c.lookup[node] = c.edgeList.PushFront(
		edges{node: node, follows: follows},
	)

	if c.Size() > c.capacity {
		oldest := c.edgeList.Back()
		c.edgeList.Remove(oldest)
		delete(c.lookup, oldest.Value.(edges).node)
	}
}

func (c *CachedWalker) Update(ctx context.Context, delta graph.Delta) error {
	if err := c.Add(delta.Node, delta.New()); err != nil {
		return fmt.Errorf("failed to update: %w", err)
	}
	return nil
}

func (c *CachedWalker) Size() int {
	return c.edgeList.Len()
}

func (c *CachedWalker) logStats() {
	if c.log != nil {
		hitRatio := 100 * float64(c.hits) / float64(c.calls)
		c.log.Printf("calls %d, hits %f%%", c.calls, hitRatio)
	}
	c.calls, c.hits = 0, 0
}

func (c *CachedWalker) Follows(ctx context.Context, node graph.ID) ([]graph.ID, error) {
	ID, err := compactID(node)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch follows of %s: %w", node, err)
	}

	c.calls++
	if c.calls >= 1_000_000 {
		defer c.logStats()
	}

	element, hit := c.lookup[ID]
	if hit {
		c.hits++
		c.edgeList.MoveToFront(element)
		return nodes(element.Value.(edges).follows), nil
	}

	if c.fallback == nil {
		return nil, fmt.Errorf("%w: %s", graph.ErrNodeNotFound, node)
	}

	follows, err := c.fallback.Follows(ctx, node)
	if err != nil {
		return nil, err
	}

	IDs, err := compactIDs(follows)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch follows of %s: %w", node, err)
	}

	c.add(ID, IDs)
	return follows, nil
}

func (c *CachedWalker) Load(nodes []graph.ID, follows [][]graph.ID) error {
	if len(nodes) != len(follows) {
		return fmt.Errorf("failed to load: nodes and follows must have the same lenght")
	}

	for i, node := range nodes {
		if err := c.Add(node, follows[i]); err != nil {
			return fmt.Errorf("failed to load: %w", err)
		}
	}
	return nil
}

func compactID(node graph.ID) (uint32, error) {
	ID, err := strconv.ParseUint(string(node), 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(ID), err
}

func compactIDs(nodes []graph.ID) ([]uint32, error) {
	IDs := make([]uint32, len(nodes))
	var err error
	for i, node := range nodes {
		IDs[i], err = compactID(node)
		if err != nil {
			return nil, err
		}
	}

	return IDs, nil
}

func node(ID uint32) graph.ID {
	return graph.ID(strconv.FormatUint(uint64(ID), 10))
}

func nodes(IDs []uint32) []graph.ID {
	nodes := make([]graph.ID, len(IDs))
	for i, ID := range IDs {
		nodes[i] = node(ID)
	}
	return nodes
}

type SimpleWalker struct {
	follows map[graph.ID][]graph.ID
}

func NewSimpleWalker(m map[graph.ID][]graph.ID) *SimpleWalker {
	return &SimpleWalker{follows: m}
}

func (w *SimpleWalker) Follows(ctx context.Context, node graph.ID) ([]graph.ID, error) {
	return w.follows[node], nil
}

func (w *SimpleWalker) Update(ctx context.Context, delta graph.Delta) {
	w.follows[delta.Node] = delta.New()
}

func NewCyclicWalker(n int) *SimpleWalker {
	follows := make(map[graph.ID][]graph.ID, n)
	for i := range n {
		node := graph.ID(strconv.Itoa(i))
		next := graph.ID(strconv.Itoa((i + 1) % n))
		follows[node] = []graph.ID{next}
	}

	return &SimpleWalker{follows: follows}
}
