package cached

import (
	"context"
	"log/slog"
	"os"
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

	timeout time.Duration
	cached  *lru.Cache[string, cachedResult]
	mtx     sync.RWMutex
}

type cachedResult struct {
	ref swarm.Address
	err error
	ts  int64
}

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelDebug,
}))

func New(lk lookuper.Lookuper, pb publisher.Publisher, timeout time.Duration) (*CachedLookuperPublisher, error) {
	cache, err := lru.New[string, cachedResult](10000)
	if err != nil {
		return nil, err
	}
	return &CachedLookuperPublisher{
		Lookuper:  lk,
		Publisher: pb,
		timeout:   timeout,
		cached:    cache,
	}, nil
}

func (c *CachedLookuperPublisher) Get(ctx context.Context, id string, version int64) (swarm.Address, error) {
	c.mtx.RLock()
	cRef, found := c.cached.Get(id)
	c.mtx.RUnlock()
	if found {
		if time.Since(time.Unix(cRef.ts, 0)) > 3*time.Second {
			go func() {
				ref, err := c.get(context.Background(), id, version)
				if err == nil {
					c.mtx.Lock()
					_ = c.cached.Add(id, cachedResult{ref: ref, err: err, ts: time.Now().Unix()})
					c.mtx.Unlock()
				}
			}()
		}
		res := cRef
		// log.Debugf("returning cached result id %s ref %s err %v", id, res.ref.String(), res.err)
		logger.Debug("returning cached result", "id", id, "ref", res.ref.String(), "err", slog.Any("error", res.err))
		return res.ref, res.err
	}
	ref, err := c.get(ctx, id, version)
	c.mtx.Lock()
	_ = c.cached.Add(id, cachedResult{ref: ref, err: err, ts: time.Now().Unix()})
	c.mtx.Unlock()
	logger.Debug("cached result", "id", id, "ref", ref.String(), "err", slog.Any("error", err))
	return ref, err
}

func (c *CachedLookuperPublisher) get(ctx context.Context, id string, version int64) (swarm.Address, error) {
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return c.Lookuper.Get(cctx, id, version)
}

func (c *CachedLookuperPublisher) Put(ctx context.Context, id string, version int64, ref swarm.Address) error {
	logger.Debug("put request", "id", id, "ref", ref.String())
	err := c.Publisher.Put(ctx, id, version, ref)
	if err == nil {
		c.mtx.Lock()
		_ = c.cached.Add(id, cachedResult{ref: ref, ts: time.Now().Unix()})
		c.mtx.Unlock()
		// log.Debugf("adding to cache id %s ref %s", id, ref.String())
	}
	logger.Debug("put result", "id", id, "ref", ref.String(), "err", slog.Any("error", err))
	return err
}

func (c *CachedLookuperPublisher) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
	return
}
