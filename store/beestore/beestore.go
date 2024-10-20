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
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/gorilla/websocket"
)

var successWsMsg = []byte{}

// BeeStore provies a storage.PutGetter that adds/retrieves chunks to/from swarm through the HTTP chunk API.
type BeeStore struct {
	Client    *http.Client
	baseUrl   string
	streamUrl string
	batch     string
	cancel    context.CancelFunc
	opChan    chan putOp
	wg        sync.WaitGroup
	readOnly  bool
	tag       string
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
	st := &url.URL{
		Host:   fmt.Sprintf("%s:%d", host, port),
		Scheme: "ws",
		Path:   "chunks/stream",
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &BeeStore{
		Client:    http.DefaultClient,
		baseUrl:   u.String(),
		streamUrl: st.String(),
		batch:     batch,
		cancel:    cancel,
		opChan:    make(chan putOp),
		readOnly:  readOnly,
		tag:       "",
	}

	if !readOnly {
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
			b.tag = fmt.Sprintf("%d", int(intVal))
		}
		err := b.putWorker(ctx)
		if err != nil {
			return nil, fmt.Errorf("Beestore: NewBeeStore: failed to start putWorker %w", err)
		}
	}
	return b, nil
}

type putOp struct {
	ch   swarm.Chunk
	errc chan<- error
}

func (b *BeeStore) putWorker(ctx context.Context) error {
	reqHeader := http.Header{}
	reqHeader.Set("Content-Type", "application/octet-stream")
	reqHeader.Set("Swarm-Postage-Batch-Id", b.batch)
	if b.tag != "" {
		reqHeader.Set("Swarm-Tag", b.tag)
	}
	dialer := websocket.Dialer{
		ReadBufferSize:  swarm.ChunkSize,
		WriteBufferSize: swarm.ChunkSize,
	}
	conn, _, err := dialer.DialContext(ctx, b.streamUrl, reqHeader)
	if err != nil {
		return fmt.Errorf("Beestore: putWorker: failed creating connection %s: %w", b.streamUrl, err)
	}
	var wsErr string
	serverClosed := make(chan struct{})
	conn.SetCloseHandler(func(code int, text string) error {
		wsErr = fmt.Sprintf("Beestore: putWorker: websocket connection closed code:%d msg:%s", code, text)
		close(serverClosed)
		return nil
	})
	b.wg.Add(1)
	go func() {
		defer conn.Close()
		defer b.wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			case op, more := <-b.opChan:
				if !more {
					return
				}
				select {
				case <-serverClosed:
					return
				case <-ctx.Done():
					return
				default:
				}
				err := conn.SetWriteDeadline(time.Now().Add(time.Second * 4))
				if err != nil {
					op.errc <- fmt.Errorf("Beestore: putWorker: failed setting write deadline %w", err)
					continue
				}
				err = conn.WriteMessage(websocket.BinaryMessage, op.ch.Data())
				if err != nil {
					op.errc <- fmt.Errorf("Beestore: putWorker: failed writing message %w", err)
					continue
				}
				err = conn.SetReadDeadline(time.Now().Add(time.Second * 4))
				if err != nil {
					// server sent close message with error
					if wsErr != "" {
						op.errc <- errors.New(wsErr)
						continue
					}
					op.errc <- fmt.Errorf("Beestore: putWorker: failed setting read deadline %w", err)
					continue
				}
				mt, msg, err := conn.ReadMessage()
				if err != nil {
					op.errc <- fmt.Errorf("Beestore: putWorker: failed reading message %w", err)
					continue
				}
				if mt != websocket.BinaryMessage || !bytes.Equal(successWsMsg, msg) {
					op.errc <- errors.New("Beestore: putWorker: invalid msg returned on success")
					continue
				}
				op.errc <- nil
			}
		}
	}()
	return nil
}

func (b *BeeStore) Put(ctx context.Context, ch swarm.Chunk) (err error) {
	if b.readOnly {
		return errors.New("Beestore: Put: read-only mode")
	}

	errc := make(chan error, 1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.opChan <- putOp{ch: ch, errc: errc}:
	}

	select {
	case err := <-errc:
		if err != nil {
			return fmt.Errorf("Beestore: Put: failed putting chunk %w", err)
		}
	case <-ctx.Done():
		return ctx.Err()
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
	b.cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.wg.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("Beestore: Close: closing beestore with ongoing routines")
	}
}
