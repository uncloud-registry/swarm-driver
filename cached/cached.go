package cached

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/uncloud-registry/swarm-driver/lookuper"
	"github.com/uncloud-registry/swarm-driver/publisher"
)

type CachedLookuperPublisher struct {
	lookuper.Lookuper
	publisher.Publisher

	logger  *slog.Logger
	timeout time.Duration
	cached  *lru.Cache[string, cachedResult]
	mtx     sync.RWMutex
}

type cachedResult struct {
	ref swarm.Address
	err error
	ts  int64
}

func New(
	logger *slog.Logger,
	lk lookuper.Lookuper,
	pb publisher.Publisher,
	timeout time.Duration,
) (*CachedLookuperPublisher, error) {
	cache, err := lru.New[string, cachedResult](10000)
	if err != nil {
		return nil, err
	}
	return &CachedLookuperPublisher{
		Lookuper:  lk,
		Publisher: pb,
		logger:    logger,
		timeout:   timeout,
		cached:    cache,
	}, nil
}

func (c *CachedLookuperPublisher) Get(ctx context.Context, id string, version int64) (swarm.Address, error) {
	start := time.Now()
	c.mtx.RLock()
	cRef, found := c.cached.Get(id)
	c.mtx.RUnlock()
	if found {
		c.logger.Debug(
			"returning cached result",
			"id", id,
			"ref", cRef.ref.String(),
			"err", cRef.err,
			"duration", time.Since(start),
		)
		return cRef.ref, cRef.err
	}
	ref, err := c.get(ctx, id, version)
	c.mtx.Lock()
	_ = c.cached.Add(id, cachedResult{ref: ref, err: err, ts: time.Now().Unix()})
	c.mtx.Unlock()
	c.logger.Debug(
		"adding result to cache",
		"id", id,
		"ref", ref.String(),
		"err", err,
		"duration", time.Since(start),
	)
	return ref, err
}

func (c *CachedLookuperPublisher) get(ctx context.Context, id string, version int64) (swarm.Address, error) {
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return c.Lookuper.Get(cctx, id, version)
}

func (c *CachedLookuperPublisher) Put(ctx context.Context, id string, version int64, ref swarm.Address) error {
	start := time.Now()
	err := c.Publisher.Put(ctx, id, version, ref)
	if err == nil {
		c.mtx.Lock()
		_ = c.cached.Add(id, cachedResult{ref: ref, ts: time.Now().Unix()})
		c.mtx.Unlock()
		c.logger.Debug(
			"adding new entry to cache",
			"id", id,
			"ref", ref.String(),
			"duration", time.Since(start),
		)
	}
	return err
}

func (c *CachedLookuperPublisher) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
	return
}
