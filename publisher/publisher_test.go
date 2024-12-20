package publisher_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/feeds"
	"github.com/ethersphere/bee/v2/pkg/feeds/sequence"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/uncloud-registry/swarm-driver/lookuper"
	"github.com/uncloud-registry/swarm-driver/publisher"
	"github.com/uncloud-registry/swarm-driver/store/teststore"
)

func newTestLogger(w io.Writer) *slog.Logger {
	testLogger := slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	return slog.New(testLogger)
}

func TestPublisher(t *testing.T) {
	t.Parallel()
	logger := newTestLogger(os.Stdout)
	store := teststore.NewSwarmInMemoryStore()
	pk, _ := crypto.GenerateSecp256k1Key()
	signer := crypto.NewDefaultSigner(pk)
	owner, err := signer.EthereumAddress()
	if err != nil {
		t.Fatal(err)
	}
	t.Run("new", func(t *testing.T) {
		pub := publisher.New(
			logger,
			store,
			signer,
			func(_ context.Context, _ string) (feeds.Index, int64, error) {
				return nil, 0, errors.New("some error")
			},
		)
		for pfx, id := range []string{"test1", "test2", "test3"} {
			lk := sequence.NewFinder(store, feeds.New([]byte(id), owner))
			var hint uint64
			for idx := 0; idx < 3; idx++ {
				t.Run(fmt.Sprintf("topic=%s/idx=%d", id, idx), func(t *testing.T) {
					ref := swarm.MustParseHexAddress(
						fmt.Sprintf(
							"%d%d00000000000000000000000000000000000000000000000000000000000000",
							pfx, idx,
						),
					)
					err := pub.Put(context.TODO(), id, time.Now().Unix(), ref)
					if err != nil {
						t.Fatal(err)
					}
					time.Sleep(time.Second)
					ch, current, _, err := lk.At(context.TODO(), time.Now().Unix(), hint)
					if err != nil {
						t.Fatal(err)
					}
					ref2, _, err := lookuper.ParseFeedUpdate(ch)
					if err != nil {
						t.Fatal(err)
					}
					if !ref2.Equal(ref) {
						t.Fatalf("incorrect ref in lookup exp %s found %s", ref.String(), ref2.String())
					}
					buf, _ := current.MarshalBinary()
					hint = binary.BigEndian.Uint64(buf)
				})
			}
		}
	})
	t.Run("existing state", func(t *testing.T) {
		pub := publisher.New(
			logger,
			store,
			signer,
			func(ctx context.Context, id string) (feeds.Index, int64, error) {
				lk := sequence.NewFinder(store, feeds.New([]byte(id), owner))
				ch, start, _, err := lk.At(ctx, time.Now().Unix(), 0)
				if err != nil {
					return nil, 0, err
				}
				if ch == nil {
					return nil, 0, errors.New("invalid chunk")
				}
				_, ts, err := lookuper.ParseFeedUpdate(ch)
				if err != nil {
					return nil, 0, err
				}
				return start, ts, nil
			},
		)
		for pfx, id := range []string{"test1", "test2", "test3"} {
			lk := sequence.NewFinder(store, feeds.New([]byte(id), owner))
			var hint uint64
			for idx := 3; idx < 6; idx++ {
				t.Run(fmt.Sprintf("topic=%s/idx=%d", id, idx), func(t *testing.T) {
					ref := swarm.MustParseHexAddress(
						fmt.Sprintf(
							"%d%d00000000000000000000000000000000000000000000000000000000000000",
							pfx, idx,
						),
					)
					err := pub.Put(context.TODO(), id, time.Now().Unix(), ref)
					if err != nil {
						t.Fatal(err)
					}
					time.Sleep(time.Second)
					ch, current, _, err := lk.At(context.TODO(), time.Now().Unix(), hint)
					if err != nil {
						t.Fatal(err)
					}
					ref2, _, err := lookuper.ParseFeedUpdate(ch)
					if err != nil {
						t.Fatal(err)
					}
					if !ref2.Equal(ref) {
						t.Fatalf("incorrect ref in lookup exp %s found %s", ref.String(), ref2.String())
					}
					buf, _ := current.MarshalBinary()
					hint = binary.BigEndian.Uint64(buf)
				})
			}
		}
	})
}
