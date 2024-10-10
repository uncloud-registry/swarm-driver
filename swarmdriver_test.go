package swarmdriver_test

import (
	"encoding/hex"
	"testing"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
	beecrypto "github.com/ethersphere/bee/pkg/crypto"
	"github.com/ethersphere/bee/pkg/file/splitter"
	swarmdriver "github.com/uncloud-registry/swarm-driver"
	"github.com/uncloud-registry/swarm-driver/lookuper"
	"github.com/uncloud-registry/swarm-driver/publisher"
	"github.com/uncloud-registry/swarm-driver/store/teststore"
)

func newSwarmDriverConstructor() (storagedriver.StorageDriver, error) {
	// Generate or retrieve the private key (handle errors as appropriate).
	privateKeyStr := "8e5c893846223894ed783acd26de67ce7d8a614107259dce1c6d6e42da4d24f57a22536487c200c38337afaa3f514ecdd38006657f4e05725baa21e4cf606892054e00dd947d6e737c4be92306d49fa4"
	encrypt := false
	privKeyBytes, _ := hex.DecodeString(privateKeyStr)
	privateKey := beecrypto.Secp256k1PrivateKeyFromBytes(privKeyBytes)
	signer := beecrypto.NewDefaultSigner(privateKey)
	ethAddress, _ := signer.EthereumAddress()
	// bstore, err := beestore.NewBeeStore("localhost", 1633, false, false, "default", false)
	// if err != nil {
	// 	return nil, fmt.Errorf("Create: failed to create BeeStore: %v", err)
	// }
	// fmt.Println("Beestore: ", bstore)

	// // Create a new instance of FeedStore
	// fstore, err := feedstore.NewFeedStore("localhost", 1634, false, false, "default", "default")
	// if err != nil {
	// 	return nil, fmt.Errorf("Create: failed to create feedstore: %v", err)
	// }
	// fmt.Println("Feedstore: ", fstore)

	store := teststore.NewSwarmInMemoryStore()
	lk := lookuper.New(store, ethAddress)
	// Initialize the publisher with the store, signer, and the latest lookuper.
	pb := publisher.New(store, signer, lookuper.Latest(store, ethAddress))
	// Initialize the splitter for splitting files into chunks.
	newSplitter := splitter.NewSimpleSplitter(store)

	return swarmdriver.New(lk, pb, store, newSplitter, encrypt), nil
}

func TestSwarmDriverSuite(t *testing.T) {
	testsuites.Driver(t, newSwarmDriverConstructor)
}

func BenchmarkSwarmDriverSuite(b *testing.B) {
	testsuites.BenchDriver(b, newSwarmDriverConstructor)
}
