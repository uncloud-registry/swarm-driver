package lookuper_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/ethersphere/bee/pkg/crypto"
	"github.com/ethersphere/bee/pkg/feeds/sequence"
	"github.com/ethersphere/bee/pkg/swarm"
	logger "github.com/ipfs/go-log/v2"
	"github.com/uncloud-registry/swarm-driver/lookuper"
	"github.com/uncloud-registry/swarm-driver/store/teststore"
)

func TestLookuper(t *testing.T) {
	// set higher level here if required
	_ = logger.SetLogLevel("*", "Error")

	store := teststore.NewSwarmInMemoryStore()
	testID := "test"

	pk, _ := crypto.GenerateSecp256k1Key()
	signer := crypto.NewDefaultSigner(pk)

	upd, err := sequence.NewUpdater(store, signer, []byte(testID))
	if err != nil {
		t.Fatal(err)
	}

	owner, err := signer.EthereumAddress()
	if err != nil {
		t.Fatal(err)
	}

	lkpr := lookuper.New(store, owner)

	var lastUpdate int64

	for i := 1; i <= 3; i++ {
		now := time.Now().Unix()
		ref := swarm.MustParseHexAddress(fmt.Sprintf("%d000000000000000000000000000000000000000000000000000000000000000", i))

		err := upd.Update(context.TODO(), now, ref.Bytes())
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Second * 2)

		t.Run(fmt.Sprintf("lookup_%d", i), func(t *testing.T) {
			ref2, err := lkpr.Get(context.TODO(), testID, now+1)
			if err != nil {
				t.Fatal(err)
			}
			if !ref2.Equal(ref) {
				t.Fatalf("lookup returned invalid ref exp %s found %s", ref.String(), ref2.String())
			}
			lastUpdate = now
		})
	}

	time.Sleep(time.Second)

	t.Run("latest get", func(t *testing.T) {
		ref, err := lkpr.Get(context.TODO(), testID, time.Now().Unix())
		if err != nil {
			t.Fatal(err)
		}
		exp := swarm.MustParseHexAddress("3000000000000000000000000000000000000000000000000000000000000000")
		if !ref.Equal(exp) {
			t.Fatalf("lookup returned invalid ref exp %s found %s", exp.String(), ref.String())
		}
	})

	t.Run("latest", func(t *testing.T) {
		latestFn := lookuper.Latest(store, owner)
		idx, at, err := latestFn(context.TODO(), testID)
		if err != nil {
			t.Fatal(err)
		}
		buf, err := idx.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if binary.BigEndian.Uint64(buf) != 2 {
			t.Fatalf("incorrect index %s exp %d", idx, 2)
		}
		if at != lastUpdate {
			t.Fatalf("incorrect timestamp found %d exp %d", at, lastUpdate)
		}
	})
}
