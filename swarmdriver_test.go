package swarmdriver_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
	beecrypto "github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/file/splitter"
	swarmdriver "github.com/uncloud-registry/swarm-driver"
	"github.com/uncloud-registry/swarm-driver/lookuper"
	"github.com/uncloud-registry/swarm-driver/publisher"
	"github.com/uncloud-registry/swarm-driver/store/beestore"
	"github.com/uncloud-registry/swarm-driver/store/feedstore"
)

func newSwarmDriverConstructor() (storagedriver.StorageDriver, error) {

	privateKeyStr := "8e5c893846223894ed783acd26de67ce7d8a614107259dce1c6d6e42da4d24f57a22536487c200c38337afaa3f514ecdd38006657f4e05725baa21e4cf606892054e00dd947d6e737c4be92306d49fa4"
	encrypt := false

	signer := getTestSigner(privateKeyStr)
	ethAddress, err := signer.EthereumAddress()
	if err != nil {
		return nil, fmt.Errorf("Test: failed to get ethereum address: %v", err)
	}
	owner := strings.TrimPrefix(ethAddress.String(), "0x")
	batchID, err := getNewTestBatchID()
	if err != nil {
		return nil, fmt.Errorf("Test: failed to get new batchID: %v", err)
	}
	fstore, err := feedstore.NewFeedStore("localhost", 1633, false, true, batchID, owner)
	if err != nil {
		return nil, fmt.Errorf("Test: failed to create feedstore: %v", err)
	}
	bstore, err := beestore.NewBeeStore("localhost", 1633, false, batchID, false, true)
	if err != nil {
		return nil, fmt.Errorf("Test: failed to create BeeStore: %v", err)
	}
	lk := lookuper.New(fstore, ethAddress)
	pb := publisher.New(fstore, signer, lookuper.Latest(fstore, ethAddress))
	newSplitter := splitter.NewSimpleSplitter(bstore)

	return swarmdriver.New(lk, pb, bstore, newSplitter, encrypt), nil
}

func TestSwarmDriverSuite(t *testing.T) {
	testsuites.Driver(t, newSwarmDriverConstructor)
}

func BenchmarkSwarmDriverSuite(b *testing.B) {
	testsuites.BenchDriver(b, newSwarmDriverConstructor)
}

func getTestSigner(privateKeyStr string) beecrypto.Signer {
	privKeyBytes, _ := hex.DecodeString(privateKeyStr)
	privateKey := beecrypto.Secp256k1PrivateKeyFromBytes(privKeyBytes)
	signer := beecrypto.NewDefaultSigner(privateKey)
	return signer
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
