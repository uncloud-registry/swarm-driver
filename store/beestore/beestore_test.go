package beestore_test

import (
	"bytes"
	"context"
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
	"github.com/uncloud-registry/swarm-driver/store/beestore"
	"github.com/uncloud-registry/swarm-driver/store/teststore"
)

func TestStoreCorrectness(t *testing.T) {
	tstore := teststore.NewSwarmInMemoryStore()
	srvURLStr := newTestServer(t, tstore)

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
		st, err := beestore.NewBeeStore(host, port, false, bId, false, false)
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
func newTestServer(t *testing.T, store *teststore.SwarmInMemoryStore) string {
	t.Helper()
	handler := http.NewServeMux()

	handler.HandleFunc("POST /chunks", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		ch, err := cac.NewWithDataSpan(data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := store.Put(r.Context(), ch); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
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
