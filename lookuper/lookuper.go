package lookuper

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/feeds"
	"github.com/ethersphere/bee/v2/pkg/feeds/factory"
	"github.com/ethersphere/bee/v2/pkg/soc"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/uncloud-registry/swarm-driver/store"
)

type Lookuper interface {
	Get(ctx context.Context, id string, version int64) (swarm.Address, error)
}

type lookuperImpl struct {
	store   store.PutGetter
	owner   common.Address
	hintMap sync.Map
	logger  *slog.Logger
}

func New(logger *slog.Logger, store store.PutGetter, owner common.Address) Lookuper {
	return &lookuperImpl{logger: logger, store: store, owner: owner}
}

func (l *lookuperImpl) Get(ctx context.Context, id string, version int64) (swarm.Address, error) {
	start := time.Now()

	lk, err := factory.New(l.store).NewLookup(feeds.Sequence, feeds.New([]byte(id), l.owner))
	if err != nil {
		return swarm.ZeroAddress, fmt.Errorf("failed creating lookuper %w", err)
	}

	hint := uint64(l.hint(id))
	ch, current, _, err := lk.At(ctx, version, hint)
	if err != nil {
		l.logger.Error("lookup error", "id", id, "version", version, "hint", hint, "error", err)
		return swarm.ZeroAddress, fmt.Errorf("failed looking up key %w", err)
	}
	if ch == nil {
		l.logger.Error("lookup error", "id", id, "version", version, "hint", hint, "error", "invalid chunk")
		return swarm.ZeroAddress, errors.New("invalid chunk lookup")
	}

	ref, ts, err := ParseFeedUpdate(ch)
	if err != nil {
		return swarm.ZeroAddress, fmt.Errorf("failed parsing feed update %w", err)
	}

	l.setHint(id, current)
	l.logger.Debug("lookup complete", "id", id, "version", version, "found", ts, "ref", ref.String(), "duration", time.Since(start))

	return ref, nil
}

func (l *lookuperImpl) hint(id string) int64 {
	h, ok := l.hintMap.Load(id)
	if !ok {
		return 0
	}
	return h.(int64)
}

func (l *lookuperImpl) setHint(id string, index feeds.Index) {
	buf, err := index.MarshalBinary()
	if err == nil {
		hint := binary.BigEndian.Uint64(buf)
		l.hintMap.Store(id, int64(hint))
	}
}

func ParseFeedUpdate(ch swarm.Chunk) (swarm.Address, int64, error) {
	s, err := soc.FromChunk(ch)
	if err != nil {
		return swarm.ZeroAddress, 0, fmt.Errorf("soc unmarshal: %w", err)
	}

	update := s.WrappedChunk().Data()
	// split the timestamp and reference
	// possible values right now:
	// unencrypted ref: span+timestamp+ref => 8+8+32=48
	// encrypted ref: span+timestamp+ref+decryptKey => 8+8+64=80
	if len(update) != 48 && len(update) != 80 {
		return swarm.ZeroAddress, 0, fmt.Errorf("invalid update")
	}
	ts := binary.BigEndian.Uint64(update[8:16])
	ref := swarm.NewAddress(update[16:])
	return ref, int64(ts), nil
}

func Latest(
	store storage.Getter,
	owner common.Address,
) func(ctx context.Context, id string) (feeds.Index, int64, error) {
	return func(ctx context.Context, id string) (feeds.Index, int64, error) {
		lk, err := factory.New(store).NewLookup(feeds.Sequence, feeds.New([]byte(id), owner))
		if err != nil {
			return nil, 0, err
		}

		ch, current, _, err := lk.At(ctx, time.Now().Unix(), 0)
		if err != nil {
			return nil, 0, err
		}

		if ch == nil {
			return nil, 0, errors.New("invalid chunk")
		}

		_, ts, err := ParseFeedUpdate(ch)
		if err != nil {
			return nil, 0, err
		}

		return current, ts, nil
	}
}
