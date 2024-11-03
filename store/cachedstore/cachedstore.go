package cachedstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/uncloud-registry/swarm-driver/store"
)

type cachedStore struct {
	store.PutGetter
	cache *lru.Cache[string, swarm.Chunk]
	mtx   sync.RWMutex
}

func New(st store.PutGetter) (*cachedStore, error) {
	cache, err := lru.New[string, swarm.Chunk](100000)
	if err != nil {
		return nil, fmt.Errorf("failed creating cache %w", err)
	}
	return &cachedStore{PutGetter: st, cache: cache}, nil
}

func (c *cachedStore) Get(ctx context.Context, address swarm.Address) (ch swarm.Chunk, err error) {
	c.mtx.RLock()
	ch, found := c.cache.Get(address.ByteString())
	c.mtx.RUnlock()
	if !found {
		ch, err = c.PutGetter.Get(ctx, address)
		if err == nil {
			c.mtx.Lock()
			_ = c.cache.Add(address.ByteString(), ch)
			c.mtx.Unlock()
			// log.Debugf("adding chunk to cache %s", ch.Address().String())
		}
	}
	return
}

func (c *cachedStore) Put(ctx context.Context, ch swarm.Chunk) (err error) {
	err = c.PutGetter.Put(ctx, ch)
	if err == nil {
		c.mtx.Lock()
		_ = c.cache.Add(ch.Address().ByteString(), ch)
		c.mtx.Unlock()
	}
	return err
}
