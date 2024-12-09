package feedstore_test

import (
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/cac"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	"github.com/ethersphere/bee/v2/pkg/soc"
	soctesting "github.com/ethersphere/bee/v2/pkg/soc/testing"
	testingc "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/uncloud-registry/swarm-driver/store/feedstore"
	"github.com/uncloud-registry/swarm-driver/store/teststore"
)

func TestStoreCorrectness(t *testing.T) {
	store := teststore.NewSwarmInMemoryStore()
	srvURLStr := newTestServer(t, store)
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
	sch := soctesting.GenerateMockSOC(t, []byte("dummy"))

	st, err := feedstore.NewFeedStore(host, port, false, bId, hex.EncodeToString(sch.Owner), store)
	if err != nil {
		t.Fatal("failed creating new beestore")
	}

	t.Run("soc chunk", func(t *testing.T) {
		err = st.Put(context.TODO(), sch.Chunk())
		if err != nil {
			t.Fatal(err)
		}
		chResult, err := st.Get(context.TODO(), sch.Address())
		if err != nil {
			t.Fatal(err)
		}
		if !sch.Chunk().Equal(chResult) {
			t.Fatal("chunk mismatch")
		}
	})

	t.Run("cac chunk fails", func(t *testing.T) {
		ch := testingc.GenerateTestRandomChunk()
		err := st.Put(context.TODO(), ch)
		if err == nil {
			t.Fatal("expected failure for cac")
		}
	})

}

func newTestServer(t *testing.T, store *teststore.SwarmInMemoryStore) string {
	t.Helper()
	handler := http.NewServeMux()

	handler.HandleFunc("/soc/{owner}/{id}", func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		owner, err := hex.DecodeString(r.PathValue("owner"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		id, err := hex.DecodeString(r.PathValue("id"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		queryVals := r.URL.Query()
		sig, err := hex.DecodeString(queryVals.Get("sig"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ch, err := cac.NewWithDataSpan(data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ss, err := soc.NewSigned(id, ch, owner, sig)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sch, err := ss.Chunk()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !soc.Valid(sch) {
			http.Error(w, "invalid soc", http.StatusBadRequest)
			return
		}
		err = store.Put(context.Background(), sch)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server.URL
}
