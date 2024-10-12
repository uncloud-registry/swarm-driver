package store

import (
	"io"

	"github.com/ethersphere/bee/v2/pkg/storage"
)

type PutGetter interface {
	storage.Putter
	storage.Getter
	io.Closer
}
