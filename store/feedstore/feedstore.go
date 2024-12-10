package feedstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/ethersphere/bee/v2/pkg/soc"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

type FeedStore struct {
	Client  *http.Client
	getter  storage.Getter
	baseUrl string
	owner   string
	batch   string
	tag     string
}

func NewFeedStore(host string, port int, tls bool, batch, owner string, getter storage.Getter, createTag bool) (*FeedStore, error) {
	scheme := "http"
	if tls {
		scheme += "s"
	}
	u := &url.URL{
		Host:   fmt.Sprintf("%s:%d", host, port),
		Scheme: scheme,
		Path:   "soc",
	}

	f := &FeedStore{
		Client:  http.DefaultClient,
		getter:  getter,
		baseUrl: u.String(),
		owner:   owner,
		batch:   batch,
	}

	if createTag {
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
		f.tag = fmt.Sprintf("%d", int(intVal))
	}

	return f, nil
}

func (f *FeedStore) Get(ctx context.Context, address swarm.Address) (swarm.Chunk, error) {
	return f.getter.Get(ctx, address)
}

func (f *FeedStore) Put(ctx context.Context, ch swarm.Chunk) (err error) {
	if !soc.Valid(ch) {
		return errors.New("FeedStore: Put: chunk not a single owner chunk")
	}
	err = f.putSOCChunk(ctx, ch)
	if err != nil {
		return err
	}

	return nil
}

func (f *FeedStore) Close() error {
	return nil
}

func (f *FeedStore) putSOCChunk(ctx context.Context, ch swarm.Chunk) error {
	chunkData := ch.Data()
	cursor := 0
	id := hex.EncodeToString(chunkData[cursor:swarm.HashSize])
	cursor += swarm.HashSize
	signature := hex.EncodeToString(chunkData[cursor : cursor+swarm.SocSignatureSize])
	cursor += swarm.SocSignatureSize
	chData := chunkData[cursor:]
	qURL, err := url.Parse(strings.Join([]string{f.baseUrl, f.owner, id}, "/"))
	if err != nil {
		return fmt.Errorf("FeedStore: putSOCChunk: failed parsing URL %w", err)
	}
	q := qURL.Query()
	q.Set("sig", signature)
	qURL.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, "POST", qURL.String(), bytes.NewBuffer(chData))
	if err != nil {
		return fmt.Errorf("FeedStore: putSOCChunk: failed creating HTTP req %w", err)
	}
	req.Header.Set("Swarm-Postage-Batch-Id", f.batch)
	if f.tag != "" {
		req.Header.Set("Swarm-Tag", f.tag)
	}
	resp, err := f.Client.Do(req)
	if err != nil {
		return fmt.Errorf("FeedStore: putSOCChunk: failed executing HTTP req %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("FeedStore: putSOCChunk: invalid status code from response %d", resp.StatusCode)
	}
	return nil
}
