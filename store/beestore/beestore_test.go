package beestore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/cac"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	testingc "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/gorilla/websocket"
	"github.com/uncloud-registry/swarm-driver/store/beestore"
)

func TestStoreCorrectness(t *testing.T) {

	batchID, err := getNewTestBatchID()
	if err != nil {
		t.Fatal(err)
	}

	bstore, err := beestore.NewBeeStore("localhost", 1633, false, batchID, false, true)
	if err != nil {
		return
	}
	fmt.Println("Beestore: ", bstore)
	srvURLStr := newTestServer(t, bstore)
	srvURL, err := url.Parse(srvURLStr)
	if err != nil {
		t.Fatal(err)
	}

	host := srvURL.Hostname()
	port, err := strconv.Atoi(srvURL.Port())
	if err != nil {
		t.Fatal(err)
	}
	bId := swarm.NewAddress(postagetesting.MustNewID()).String()

	t.Run("read-write", func(t *testing.T) {
		st, err := beestore.NewBeeStore(host, port, false, bId, false, true)
		if err != nil {
			t.Fatal("failed creating new beestore", err)
		}

		t.Cleanup(func() {
			err := st.Close()
			if err != nil {
				t.Fatal(err)
			}
		})

		ctx := context.Background()

		for i := 0; i < 50; i++ {
			ch := testingc.GenerateTestRandomChunk()
			err := st.Put(ctx, ch)
			if err != nil {
				t.Fatal(err)
			}
			chResult, err := st.Get(ctx, ch.Address())
			if err != nil {
				t.Fatal(err)
			}
			if !ch.Equal(chResult) {
				t.Fatal("chunk mismatch")
			}
		}
	})

	t.Run("read-only", func(t *testing.T) {
		st, err := beestore.NewBeeStore(host, port, false, bId, true, true)
		if err != nil {
			t.Fatal("failed creating new beestore")
		}

		t.Cleanup(func() {
			err := st.Close()
			if err != nil {
				t.Fatal(err)
			}
		})

		ch := testingc.GenerateTestRandomChunk()
		err = st.Put(context.TODO(), ch)
		if err == nil {
			t.Fatal("expected error while putting")
		}
	})
}

// newTestServer creates an http server to serve the bee http api endpoints.
func newTestServer(t *testing.T, store *beestore.BeeStore) string {
	t.Helper()
	handler := http.NewServeMux()

	handler.HandleFunc("/chunks/stream", func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}

			if len(msg) < swarm.SpanSize {
				conn.WriteMessage(websocket.TextMessage, []byte(err.Error()))
				return
			}

			ch, err := cac.NewWithDataSpan(msg)
			if err != nil {
				conn.WriteMessage(websocket.TextMessage, []byte(err.Error()))
				return
			}

			if err := store.Put(context.Background(), ch); err != nil {
				conn.WriteMessage(websocket.TextMessage, []byte(err.Error()))
				continue
			}

			conn.WriteMessage(websocket.BinaryMessage, []byte{})
		}
	})

	handler.HandleFunc("/chunks/{address}", func(w http.ResponseWriter, r *http.Request) {
		addrStr := r.PathValue("address")
		addr, err := swarm.ParseHexAddress(addrStr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ch, err := store.Get(context.Background(), addr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "binary/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(int64(len(ch.Data())), 10))
		_, _ = io.Copy(w, bytes.NewReader(ch.Data()))
	})

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server.URL
}

func getNewTestBatchID() (string, error) {
	host := "localhost"
	port := 1633
	scheme := "http"
	stampsURL := &url.URL{
		Host:   fmt.Sprintf("%s:%d", host, port),
		Scheme: scheme,
		Path:   "stamps/10/17",
	}
	res, err := http.Post(stampsURL.String(), "application/json", nil)
	if err != nil {
		return "", fmt.Errorf("failed creating stamp %w", err)
	}
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read stamp body %w", err)
	}
	fmt.Println("Stamp Created: ", string(resBody))

	val := make(map[string]interface{})
	err = json.Unmarshal(resBody, &val)
	if err != nil {
		return "", fmt.Errorf("Error unmarshalling resbody for stamp %w", err)
	}
	batchID, ok := val["batchID"].(string)
	if !ok {
		return "", fmt.Errorf("Error converting val for stamp  %v", val["batchID"])
	}

	return batchID, nil
}
