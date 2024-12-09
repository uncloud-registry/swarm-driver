package publisher

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/feeds"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

type Publisher interface {
	Put(ctx context.Context, id string, version int64, ref swarm.Address) error
}

type Loader func(ctx context.Context, id string) (feeds.Index, int64, error)

type pubImpl struct {
	putter     storage.Putter
	signer     crypto.Signer
	loader     Loader
	updaterMap sync.Map
	logger     *slog.Logger
}

type feedState struct {
	currIndex feeds.Index
	ts        int64
}

func New(logger *slog.Logger, putter storage.Putter, signer crypto.Signer, loader Loader) Publisher {
	return &pubImpl{putter: putter, signer: signer, loader: loader, logger: logger}
}

func (p *pubImpl) Put(ctx context.Context, id string, version int64, ref swarm.Address) error {
	p.logger.Debug("publisher: Put called", "id", id, "version", version, "ref", ref.String())
	var nxtIndex feeds.Index

	state, found := p.updaterMap.Load(id)
	if !found {
		p.logger.Debug("publisher: state not found in updaterMap", "id", id)
		currIndex, at, err := p.loader(ctx, id)
		if err == nil {
			p.logger.Debug("publisher: loaded initial version", "version", currIndex, "timestamp", at)
			nxtIndex = currIndex.Next(at, uint64(version))
		} else {
			p.logger.Debug("publisher: loader error, initializing new index", "error", err)
			nxtIndex = new(index)
		}
	} else {
		p.logger.Debug("publisher: state found in updaterMap", "id", id)
		fstate := state.(feedState)
		nxtIndex = fstate.currIndex.Next(fstate.ts, uint64(version))
	}

	p.logger.Debug("publisher: updating index", "id", id, "version", version, "nextIndex", nxtIndex)
	err := p.update(ctx, id, version, nxtIndex, ref.Bytes())
	if err != nil {
		p.logger.Error("publisher: failed to update next index", "id", id, "version", version, "error", err)
		return fmt.Errorf("publisher: failed to update next index: %w", err)
	}
	p.logger.Debug("publisher: updated index", "id", id, "version", version, "nextIndex", nxtIndex)
	p.updaterMap.Store(id, feedState{currIndex: nxtIndex, ts: version})
	p.logger.Debug("publisher: updated", "id", id, "version", version, "ref", ref.String())
	return nil
}

func (p *pubImpl) update(
	ctx context.Context,
	id string,
	version int64,
	idx feeds.Index,
	payload []byte,
) error {
	p.logger.Debug("publisher: update called", "id", id, "version", version, "index", idx.String())

	putter, err := feeds.NewPutter(p.putter, p.signer, []byte(id))
	if err != nil {
		p.logger.Error("publisher: failed to create new putter", "id", id, "version", version, "error", err)
		return err
	}
	p.logger.Debug("publisher: created new putter", "id", id, "version", version)
	err = putter.Put(ctx, idx, version, payload)
	if err != nil {
		p.logger.Error("publisher: failed to put data", "id", id, "version", version, "index", idx.String(), "error", err)
		return err
	}

	p.logger.Debug("publisher: update successful", "id", id, "version", version, "index", idx.String())
	return nil
}

// index replicates the feeds.sequence.Index. This index creation is not exported from
// the package as a result loader doesn't know how to return the first feeds.Index.
// This index will be used to return which will provide a compatible interface to
// feeds.sequence.Index.
type index struct {
	index uint64
}

func (i *index) String() string {
	return strconv.FormatUint(i.index, 10)
}

func (i *index) MarshalBinary() ([]byte, error) {
	indexBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(indexBytes, i.index)
	return indexBytes, nil
}

func (i *index) Next(last int64, at uint64) feeds.Index {
	return &index{i.index + 1}
}
