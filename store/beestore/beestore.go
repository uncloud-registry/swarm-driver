package beestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

var successWsMsg = []byte{}

// BeeStore provies a storage.PutGetter that adds/retrieves chunks to/from swarm through the HTTP chunk API.
type BeeStore struct {
	Client   *http.Client
	baseUrl  string
	batch    string
	readOnly bool
	tag      string
}

// NewBeeStore creates a new APIStore.
func NewBeeStore(host string, port int, tls bool, batch string, readOnly bool, createTag bool) (*BeeStore, error) {
	if host == "" {
		return nil, errors.New("Beestore: NewBeeStore: host cannot be empty")
	}
	if port <= 0 {
		return nil, errors.New("Beestore: NewBeeStore: invalid port number")
	}
	if batch == "" {
		return nil, errors.New("Beestore: NewBeeStore: batch cannot be empty")
	}

	scheme := "http"
	if tls {
		scheme += "s"
	}
	u := &url.URL{
		Host:   fmt.Sprintf("%s:%d", host, port),
		Scheme: scheme,
		Path:   "chunks",
	}
	b := &BeeStore{
		Client:   http.DefaultClient,
		baseUrl:  u.String(),
		batch:    batch,
		readOnly: readOnly,
		tag:      "",
	}

	if !readOnly && createTag {
		tagUrl := &url.URL{
			Host:   fmt.Sprintf("%s:%d", host, port),
			Scheme: scheme,
			Path:   "tags",
		}
		res, err := http.Post(tagUrl.String(), "application/json", nil)
		if err != nil {
			return nil, fmt.Errorf("Beestore: NewBeeStore: failed creating tag %w", err)
		}
		resBody, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, fmt.Errorf("Beestore: NewBeeStore: failed to read tag body %w", err)
		}
		val := make(map[string]interface{})
		err = json.Unmarshal(resBody, &val)
		if err != nil {
			return nil, fmt.Errorf("Beestore: NewBeeStore: error unmarshalling response body %w", err)
		}
		intVal, ok := val["uid"].(float64)
		if !ok {
			return nil, fmt.Errorf("Beestore: NewBeeStore: error converting uid value %v", val["uid"])
		}
		b.tag = fmt.Sprintf("%d", int(intVal))
	}
	return b, nil
}

func (b *BeeStore) Put(ctx context.Context, ch swarm.Chunk) (err error) {
	if b.readOnly {
		return errors.New("Beestore: Put: read-only mode")
	}

	reqHeader := http.Header{}
	reqHeader.Set("Content-Type", "application/octet-stream")
	reqHeader.Set("Swarm-Postage-Batch-Id", b.batch)
	if b.tag != "" {
		reqHeader.Set("Swarm-Tag", b.tag)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", b.baseUrl, bytes.NewReader(ch.Data()))
	if err != nil {
		return fmt.Errorf("Beestore: Put: failed creating http req %w", err)
	}

	req.Header = reqHeader
	res, err := b.Client.Do(req)
	if err != nil {
		return fmt.Errorf("Beestore: Put: failed executing http req %w", err)
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("Beestore: Put: failed putting chunk %s %w", ch.Address().String(), storage.ErrInvalidChunk)
	}

	return nil
}

func (b *BeeStore) Get(ctx context.Context, address swarm.Address) (ch swarm.Chunk, err error) {
	addressHex := address.String()
	url := strings.Join([]string{b.baseUrl, addressHex}, "/")
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("Beestore: Get: failed creating http req %w", err)
	}
	res, err := b.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Beestore: Get: failed executing http req %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Beestore: Get: chunk %s not found %w", addressHex, storage.ErrNotFound)
	}
	chunkData, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("Beestore: Get: failed reading chunk body %w", err)
	}
	ch = swarm.NewChunk(address, chunkData)
	return ch, nil
}

func (b *BeeStore) Close() error {
	return nil
}
