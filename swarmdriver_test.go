package swarmdriver_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
	beecrypto "github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/file/splitter"
	swarmdriver "github.com/uncloud-registry/swarm-driver"
	"github.com/uncloud-registry/swarm-driver/cached"
	"github.com/uncloud-registry/swarm-driver/lookuper"
	"github.com/uncloud-registry/swarm-driver/publisher"
	"github.com/uncloud-registry/swarm-driver/store/beestore"
	"github.com/uncloud-registry/swarm-driver/store/feedstore"
	"github.com/uncloud-registry/swarm-driver/store/teststore"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var logger *slog.Logger

func init() {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	logger = slog.New(handler)
}

func newSwarmDriverInMemoryConstructor() (storagedriver.StorageDriver, error) {
	logger.Debug("Starting Inmemory SwarmDriver")
	privateKeyStr := "8e5c893846223894ed783acd26de67ce7d8a614107259dce1c6d6e42da4d24f57a22536487c200c38337afaa3f514ecdd38006657f4e05725baa21e4cf606892054e00dd947d6e737c4be92306d49fa4"
	encrypt := false

	signer := getTestSigner(privateKeyStr)
	ethAddress, err := signer.EthereumAddress()
	if err != nil {
		return nil, fmt.Errorf("Inmemory Test: failed to get ethereum address: %v", err)
	}

	store := teststore.NewSwarmInMemoryStore()

	clp, err := cached.New(
		lookuper.New(store, ethAddress),
		publisher.New(store, signer, lookuper.Latest(store, ethAddress)),
		120*time.Second,
	)
	if err != nil {
		return nil, fmt.Errorf("Create: failed to create cached lookuper publisher: %v", err)
	}

	newSplitter := splitter.NewSimpleSplitter(store)

	return swarmdriver.New(clp, clp, store, newSplitter, encrypt), nil
}

func TestInMemorySwarmDriverSuite(t *testing.T) {
	testsuites.Driver(t, newSwarmDriverInMemoryConstructor)
}

func BenchmarkInMemorySwarmDriverSuite(b *testing.B) {
	testsuites.BenchDriver(b, newSwarmDriverInMemoryConstructor)
}

func newSwarmDriverBeeConstructor(t *testing.T) (storagedriver.StorageDriver, error) {
	logger.Debug("Starting Bee SwarmDriver")
	ctx := context.Background()
	beeNodeContainer, err := StartBeeNodeContainer(ctx)
	if err != nil {
		return nil, fmt.Errorf("Bee Test: failed to start Bee node container: %v", err)
	}
	t.Cleanup(func() {
		if err := beeNodeContainer.Terminate(ctx); err != nil {
			t.Fatalf("Failed to terminate Bee node container: %v", err)
		}
	})
	privateKeyStr := "8e5c893846223894ed783acd26de67ce7d8a614107259dce1c6d6e42da4d24f57a22536487c200c38337afaa3f514ecdd38006657f4e05725baa21e4cf606892054e00dd947d6e737c4be92306d49fa4"
	encrypt := false
	host := "localhost"
	port := 1633
	signer := getTestSigner(privateKeyStr)
	ethAddress, err := signer.EthereumAddress()
	if err != nil {
		return nil, fmt.Errorf("Bee Test: failed to get ethereum address: %v", err)
	}
	owner := strings.TrimPrefix(ethAddress.String(), "0x")
	batchID, err := getNewTestBatchID()
	if err != nil {
		return nil, fmt.Errorf("Bee Test: failed to get new batchID: %v", err)
	}
	fstore, err := feedstore.NewFeedStore(host, port, false, true, batchID, owner)
	if err != nil {
		return nil, fmt.Errorf("Bee Test: failed to create feedstore: %v", err)
	}
	bstore, err := beestore.NewBeeStore(host, port, false, batchID, false, true)
	if err != nil {
		return nil, fmt.Errorf("Bee Test: failed to create BeeStore: %v", err)
	}
	lk := lookuper.New(fstore, ethAddress)
	pb := publisher.New(fstore, signer, lookuper.Latest(fstore, ethAddress))
	newSplitter := splitter.NewSimpleSplitter(bstore)

	return swarmdriver.New(lk, pb, bstore, newSplitter, encrypt), nil
}

func TestBeeSwarmDriverSuite(t *testing.T) {
	testsuites.Driver(t, func() (storagedriver.StorageDriver, error) {
		return newSwarmDriverBeeConstructor(t)
	})
}

func BenchmarkBeeSwarmDriverSuite(b *testing.B) {
	testsuites.BenchDriver(b, func() (storagedriver.StorageDriver, error) {
		var t *testing.T
		return newSwarmDriverBeeConstructor(t)
	})
}

// StartBeeNodeContainer starts a Bee node container and returns the container instance.
func StartBeeNodeContainer(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "ethersphere/bee:2.2.0",   // Image of the Bee node
		ExposedPorts: []string{"1633:1633/tcp"}, // Expose required ports
		Env: map[string]string{
			"BEE_API_ADDR":             ":1633", // API address for the Bee node
			"BEE_CORS_ALLOWED_ORIGINS": "*",     // Allow all CORS origins
			"BEE_VERBOSITY":            "5",     // Verbosity level
		},
		Cmd:        []string{"dev"},
		WaitingFor: wait.ForListeningPort("1633/tcp").WithStartupTimeout(120 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true, // Automatically start the container after creation
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to start Bee node container: %w", err)
	}

	return container, nil
}

func printBeeNodeContainerLogs(ctx context.Context, container testcontainers.Container) {
	logReader, err := container.Logs(ctx)
	if err != nil {
		logger.Debug("Failed to get logs from Bee node container", "error", err)
		return
	}
	defer logReader.Close()
	logger.Debug("Bee node logs:")
	logReaderBody, err := io.ReadAll(logReader)
	if err != nil {
		logger.Debug("Failed to read logs from Bee node container")
	} else {
		logger.Debug(string(logReaderBody))
	}
}

func testBeeNodeContainer(ctx context.Context, beeNode testcontainers.Container) {
	host, err := beeNode.Host(ctx)
	if err != nil {
		logger.Debug("Failed to get host from Bee node container", "error", err)
		return
	}
	mappedPort, err := beeNode.MappedPort(ctx, "1633/tcp")
	if err != nil {
		logger.Debug("Failed to get mapped port from Bee node container", "error", err)
		return
	}
	logger.Debug("Bee node is accessible at", "url", fmt.Sprintf("http://%s:%s", host, mappedPort.Port()))
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
		return "", fmt.Errorf("Bee Test: getNewTestBatchID: failed creating stamp %w", err)
	}
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("Bee Test: getNewTestBatchID: failed to read stamp body %w", err)
	}

	val := make(map[string]interface{})
	err = json.Unmarshal(resBody, &val)
	if err != nil {
		return "", fmt.Errorf("Bee Test: getNewTestBatchID: Error unmarshalling resbody for stamp %w", err)
	}
	batchID, ok := val["batchID"].(string)
	if !ok {
		return "", fmt.Errorf("Bee Test: getNewTestBatchID: Error converting val for stamp  %v", val["batchID"])
	}

	return batchID, nil
}

func getTestSigner(privateKeyStr string) beecrypto.Signer {
	privKeyBytes, _ := hex.DecodeString(privateKeyStr)
	privateKey := beecrypto.Secp256k1PrivateKeyFromBytes(privKeyBytes)
	signer := beecrypto.NewDefaultSigner(privateKey)
	return signer
}
