package beestore_test

import (
	"context"
	"crypto/ecdsa"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/api"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/log"
	mockbatchstore "github.com/ethersphere/bee/v2/pkg/postage/batchstore/mock"
	mockpost "github.com/ethersphere/bee/v2/pkg/postage/mock"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	testingc "github.com/ethersphere/bee/v2/pkg/storage/testing"
	mockstorer "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/tracing"
	"github.com/uncloud-registry/swarm-driver/store/beestore"
)

func TestStoreCorrectness(t *testing.T) {
	srvUrl := newTestServer(t, mockstorer.New())

	host := srvUrl.Hostname()
	port, err := strconv.Atoi(srvUrl.Port())
	if err != nil {
		t.Fatal(err)
	}
	bId := swarm.NewAddress(postagetesting.MustNewID()).String()

	t.Run("read-write", func(t *testing.T) {
		st, err := beestore.NewBeeStore(host, port, false, false, bId, false)
		if err != nil {
			t.Fatal("failed creating new beestore")
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
		st, err := beestore.NewBeeStore(host, port, false, false, bId, true)
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
func newTestServer(t *testing.T, storer mockstorer.mockStorer) *url.URL {
	t.Helper()
	logger := log.NewLogger("test").Build()
	// store := statestore.NewStateStore()
	pk, _ := crypto.GenerateSecp256k1Key()
	signer := crypto.NewDefaultSigner(pk)
	batchStore := mockbatchstore.New(mockbatchstore.WithAcceptAllExistsFunc())

	var (
		dummyKey   ecdsa.PublicKey
		dummyOwner common.Address
	)

	s := api.New(dummyKey, dummyKey, dummyOwner, logger, nil, batchStore, false, api.FullMode, true, true, nil)

	var extraOpts = api.ExtraOptions{
		// Tags:   tags.NewTags(store, logger),
		Storer: storer,
		Post:   mockpost.New(mockpost.WithAcceptAll()),
	}

	noOpTracer, tracerCloser, _ := tracing.NewTracer(&tracing.Options{
		Enabled: false,
	})

	t.Cleanup(func() { _ = tracerCloser.Close() })

	_ = s.Configure(signer, noOpTracer, api.Options{}, extraOpts, 10, nil)

	s.MountAPI()

	ts := httptest.NewServer(s)
	srvUrl, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ts.Close)
	return srvUrl
}
