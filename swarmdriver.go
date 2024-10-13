package swarmdriver

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
	beecrypto "github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/file"
	"github.com/ethersphere/bee/v2/pkg/file/joiner"
	"github.com/ethersphere/bee/v2/pkg/file/splitter"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/uncloud-registry/swarm-driver/lookuper"
	"github.com/uncloud-registry/swarm-driver/publisher"
	"github.com/uncloud-registry/swarm-driver/store"
	"github.com/uncloud-registry/swarm-driver/store/beestore"
	"github.com/uncloud-registry/swarm-driver/store/feedstore"
	"github.com/uncloud-registry/swarm-driver/store/teststore"
)

const driverName = "swarm"

var logger *slog.Logger

func init() {
	factory.Register(driverName, &swarmDriverFactory{})
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	logger = slog.New(handler)
}

// swarmDriverFactory implements the factory.StorageDriverFactory interface.
type swarmDriverFactory struct{}

// Publisher is an interface for publishing data references.
type Publisher interface {
	// Put publishes a data reference with the given id, version, and reference.
	Put(ctx context.Context, id string, version int64, ref swarm.Address) error
}

// Lookuper is an interface for looking up data references.
type Lookuper interface {
	// Get retrieves a data reference for the given id and version.
	Get(ctx context.Context, id string, version int64) (swarm.Address, error)
}

// swarmDriver is the main struct implementing the storagedriver.StorageDriver interface.
type swarmDriver struct {
	mutex     sync.RWMutex    // Mutex to handle concurrent access.
	synced    bool            // Flag to indicate if the driver is synced.
	store     store.PutGetter // Interface for storing and retrieving data.
	encrypt   bool            // Flag to indicate if encryption is enabled.
	publisher Publisher       // Interface for publishing data references.
	lookuper  Lookuper        // Interface for looking up data references.
	splitter  file.Splitter   // Interface for splitting files into chunks.
}

// metaData represents the metadata for a file or directory.
type metaData struct {
	IsDir    bool     // Flag to indicate if the path is a directory.
	Path     string   // The path of the file or directory.
	ModTime  int64    // The modification time of the file or directory.
	Size     int      // The size of the file.
	Children []string // List of children paths if the path is a directory.
}

var _ storagedriver.StorageDriver = &swarmDriver{}

// Create initializes a new instance of the swarmDriver with the provided parameters.
func (factory *swarmDriverFactory) Create(ctx context.Context, parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	encrypt, ok := parameters["encrypt"].(bool)
	if !ok {
		return nil, fmt.Errorf("Create: missing or invalid 'encrypt' parameter")
	}
	privateKeyStr, ok := parameters["privateKey"].(string)
	if !ok {
		return nil, fmt.Errorf("Create: missing or invalid 'privateKey' parameter")
	}
	inmemory, ok := parameters["inmemory"].(bool)
	if !ok {
		return nil, fmt.Errorf("Create: missing or invalid 'inmemory' parameter")
	}
	privKeyBytes, _ := hex.DecodeString(privateKeyStr)
	privateKey := beecrypto.Secp256k1PrivateKeyFromBytes(privKeyBytes)
	signer := beecrypto.NewDefaultSigner(privateKey)
	ethAddress, _ := signer.EthereumAddress()
	var lk Lookuper
	var pb Publisher
	var store store.PutGetter
	var newSplitter file.Splitter

	if inmemory {
		logger.Debug("Creating New Inmemory Swarm Driver")
		store = teststore.NewSwarmInMemoryStore()
		lk = lookuper.New(store, ethAddress)
		// Initialize the publisher with the store, signer, and the latest lookuper.
		pb = publisher.New(store, signer, lookuper.Latest(store, ethAddress))
		// Initialize the splitter for splitting files into chunks.
		newSplitter = splitter.NewSimpleSplitter(store)
	} else {
		logger.Debug("Creating New Bee Swarm Driver")
		// Create a new instance of BeeStore
		store, err := beestore.NewBeeStore("localhost", 1633, false, "default", false)
		if err != nil {
			return nil, fmt.Errorf("Create: failed to create BeeStore: %v", err)
		}
		// Create a new instance of FeedStore
		fstore, err := feedstore.NewFeedStore("localhost", 1634, false, false, "default", "default")
		if err != nil {
			return nil, fmt.Errorf("Create: failed to create feedstore: %v", err)
		}
		lk = lookuper.New(fstore, ethAddress)
		pb = publisher.New(fstore, signer, lookuper.Latest(fstore, ethAddress))
		newSplitter = splitter.NewSimpleSplitter(store)
	}

	// Pass the signer to the New function instead of generating the key inside.
	return New(lk, pb, store, newSplitter, encrypt), nil
}

// New constructs a new swarmDriver instance.
func New(lk Lookuper, pb Publisher, store store.PutGetter, splitter file.Splitter, encrypt bool) *swarmDriver {
	logger.Debug("Creating New Swarm Driver")

	d := &swarmDriver{
		store:     store,
		encrypt:   encrypt,
		lookuper:  lk,
		publisher: pb,
		splitter:  splitter,
	}
	// Add the root path to the driver.
	if err := d.addPathToRoot(context.Background(), ""); err != nil {
		logger.Error("New: Failed to create root path:")
	}
	logger.Debug("Swarm driver successfully created!")
	return d
}

// Implement the storagedriver.StorageDriver interface.
func (d *swarmDriver) Name() string {
	return driverName
}

// Check if address is a zero address
func isZeroAddress(ref swarm.Address) bool {
	if ref.Equal(swarm.ZeroAddress) {
		return true
	}
	zeroAddr := make([]byte, 32)
	return swarm.NewAddress(zeroAddr).Equal(ref)
}

// isValidPath checks if the provided path is valid according to certain rules.
func isValidPath(path string) error {
	// A path is invalid if it's empty
	if path == "" {
		return fmt.Errorf("isValidPath: Invalid Path: Path should not be empty")
	}
	// A path is invalid if it's the root "/"
	if path == "/" {
		return fmt.Errorf("isValidPath: Invalid Path: Path should not be /")
	}
	// A path is invalid if it does not start with a "/"
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("isValidPath: Invalid Path: Path should contain / at the start")
	}
	// A path is invalid if it ends with a "/"
	if strings.HasSuffix(path, "/") {
		return fmt.Errorf("isValidPath: Invalid Path: Path should not contain / at the end")
	}
	// A path is invalid if it contains any invalid characters
	invalidChars := []string{"*", "?", "<", ">", "|", "\"", ":"}
	for _, char := range invalidChars {
		if strings.Contains(path, char) {
			return fmt.Errorf("isValidPath: Invalid Path: Path should not contain invalid character %q", char)
		}
	}
	// A path is invalid if it contains double slashes
	if strings.Contains(path, "//") {
		return fmt.Errorf("isValidPath: Invalid Path: Path should not contain //")
	}
	// If all checks pass, the path is valid
	return nil
}

func (d *swarmDriver) addPathToRoot(ctx context.Context, path string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	rootPath := "/"
	// Retrieve root metadata
	rootMeta, err := d.getMetadata(ctx, rootPath)
	if err != nil {
		// If root metadata does not exist, initialize it
		rootMeta = metaData{
			IsDir:    true,
			Path:     rootPath,
			ModTime:  time.Now().Unix(),
			Children: []string{},
		}
	}
	if path != "" {
		// Check if the path is already present in the root's children
		found := false
		for _, child := range rootMeta.Children {
			if child == path {
				found = true
				break
			}
		}
		// Add the path to the root's children if it's not already present
		if !found {
			rootMeta.Children = append(rootMeta.Children, path)
			rootMeta.ModTime = time.Now().Unix()
		}
	}
	// Marshal the updated root metadata to JSON
	metaBuf, err := json.Marshal(rootMeta)
	if err != nil {
		return fmt.Errorf("addPathToRoot: failed to marshal metadata: %v", err)
	}
	// Split the metadata and get a reference
	metaRef, err := d.splitter.Split(ctx, io.NopCloser(bytes.NewReader(metaBuf)), int64(len(metaBuf)), d.encrypt)
	if err != nil || isZeroAddress(metaRef) {
		return fmt.Errorf("addPathToRoot: failed to split metadata: %v", err)
	}
	// Publish the metadata
	err = d.publisher.Put(ctx, filepath.Join(rootPath, "mtdt"), time.Now().Unix(), metaRef)
	if err != nil {
		return fmt.Errorf("addPathToRoot: failed to publish metadata: %v", err)
	}
	logger.Debug("addPathToRoot: Success!", slog.String("path", path))
	return nil
}

// fromMetadata reads metadata from an io.Reader and unmarshals it into a metaData struct.
func fromMetadata(reader io.Reader) (metaData, error) {
	md := metaData{}
	// Read all data from the reader into a buffer.
	buf, err := io.ReadAll(reader)
	if err != nil {
		return metaData{}, fmt.Errorf("fromMetadata: failed reading metadata %w", err)
	}
	// Unmarshal the JSON data from the buffer into the metaData struct.
	err = json.Unmarshal(buf, &md)
	if err != nil {
		return metaData{}, fmt.Errorf("fromMetadata: failed unmarshalling metadata %w", err)
	}
	return md, nil
}

// getMetadata retrieves the metadata for the given path.
func (d *swarmDriver) getMetadata(ctx context.Context, path string) (metaData, error) {
	// Normalize the path to use forward slashes.
	path = filepath.ToSlash(path)
	// Lookup the metadata reference for the given path.
	metaRef, err := d.lookuper.Get(ctx, filepath.Join(path, "mtdt"), time.Now().Unix())
	if err != nil {
		return metaData{}, fmt.Errorf("getMetadata: failed to get metadata for path %s %v", path, err)
	}
	// Create a joiner to read the metadata.
	metaJoiner, _, err := joiner.New(ctx, d.store, d.store, metaRef)
	if err != nil {
		return metaData{}, fmt.Errorf("getMetadata: failed to create reader for metadata: %v", err)
	}
	// Read and unmarshal the metadata.
	meta, err := fromMetadata(metaJoiner)
	if err != nil {
		return metaData{}, fmt.Errorf("getMetadata: failed to read metadata: %v", err)
	}
	return meta, nil
}

func (d *swarmDriver) putMetadata(ctx context.Context, path string, meta metaData) error {
	logger.Debug("putMetadata Hit", slog.String("path", path))
	// Marshal the metadata to JSON
	metaBuf, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("putMetadata: failed to marshal metadata: %v", err)
	}
	// Split the metadata and get a reference
	metaRef, err := d.splitter.Split(ctx, io.NopCloser(bytes.NewReader(metaBuf)), int64(len(metaBuf)), d.encrypt)
	if err != nil || isZeroAddress(metaRef) {
		return fmt.Errorf("putMetadata: failed to split metadata: %v", err)
	}
	// Publish the metadata
	err = d.publisher.Put(ctx, filepath.Join(path, "mtdt"), time.Now().Unix(), metaRef)
	if err != nil {
		return fmt.Errorf("putMetadata: failed to publish metadata: %v", err)
	}
	// If the path is the root, no need to update parent directories
	if path == "/" {
		return nil
	}
	// Update metadata for each parent directory up to the root
	for currentPath := filepath.ToSlash(filepath.Dir(path)); ; currentPath = filepath.ToSlash(filepath.Dir(currentPath)) {
		// Retrieve parent metadata
		parentMeta, err := d.getMetadata(ctx, currentPath)
		if err != nil {
			logger.Warn("putMetadata: Metadata not found. Creating new", slog.String("path", currentPath))
			parentMeta = metaData{
				IsDir:    true,
				Path:     currentPath,
				ModTime:  time.Now().Unix(),
				Children: []string{},
			}
		}
		// Check if the current path is already a child of the parent
		childPath := filepath.Base(path)
		found := false
		for _, child := range parentMeta.Children {
			if child == childPath {
				found = true
				break
			}
		}
		// Add the current path to the parent's children if not already present
		if !found {
			parentMeta.Children = append(parentMeta.Children, childPath)
			parentMeta.ModTime = time.Now().Unix()

			// Marshal the updated parent metadata to JSON
			parentMetaBuf, err := json.Marshal(parentMeta)
			if err != nil {
				return fmt.Errorf("putMetadata: failed to marshal parent metadata: %v", err)
			}
			// Split the parent metadata and get a reference
			parentMetaRef, err := d.splitter.Split(ctx, io.NopCloser(bytes.NewReader(parentMetaBuf)), int64(len(parentMetaBuf)), d.encrypt)
			if err != nil || isZeroAddress(parentMetaRef) {
				return fmt.Errorf("putMetadata: failed to split parent metadata: %v", err)
			}
			// Publish the parent metadata
			err = d.publisher.Put(ctx, filepath.Join(currentPath, "mtdt"), time.Now().Unix(), parentMetaRef)
			if err != nil {
				return fmt.Errorf("putMetadata: failed to publish parent metadata: %v", err)
			}
		}
		// Break the loop if we have reached the root
		if currentPath == "/" {
			break
		}
		// Move up to the parent directory
		path = currentPath
	}
	logger.Debug("putMetadata: Success!", slog.String("path", path))
	return nil
}

// getData retrieves the data stored at the given path as a byte slice.
func (d *swarmDriver) getData(ctx context.Context, path string) ([]byte, error) {
	// Lookup the data reference for the given path.
	dataRef, err := d.lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("getData: failed to lookup data: %v", err)
	}
	// Create a joiner to read the data.
	dataJoiner, _, err := joiner.New(ctx, d.store, d.store, dataRef)
	if err != nil {
		return nil, fmt.Errorf("getData: failed to create joiner for data: %v", err)
	}
	// Read all data from the joiner into a byte slice.
	data, err := io.ReadAll(dataJoiner)
	if err != nil {
		return nil, fmt.Errorf("getData: failed to read data: %w", err)
	}
	return data, nil
}

// putData stores the provided data at the specified path.
func (d *swarmDriver) putData(ctx context.Context, path string, data []byte) error {
	logger.Debug("putData Hit", slog.String("path", path))
	// Check if the data is empty.
	if len(data) == 0 {
		logger.Warn("putData: Empty data", slog.String("path", path))
		emptyRef := swarm.ZeroAddress
		// Publish an empty data reference.
		err := d.publisher.Put(ctx, filepath.Join(path, "data"), time.Now().Unix(), emptyRef)
		if err != nil {
			return fmt.Errorf("putData: failed to publish empty data reference: %v", err)
		}
		return nil
	}
	// Split the data into chunks and get a reference.
	dataRef, err := d.splitter.Split(ctx, io.NopCloser(bytes.NewReader(data)), int64(len(data)), d.encrypt)
	if err != nil || isZeroAddress(dataRef) {
		return fmt.Errorf("putData: failed to split data: %v", err)
	}
	// Publish the data reference.
	err = d.publisher.Put(ctx, filepath.Join(path, "data"), time.Now().Unix(), dataRef)
	if err != nil {
		return fmt.Errorf("putData: failed to publish data reference: %v", err)
	}
	return nil
}

// deleteData nullifies the data reference for the given path by publishing a ZeroAddress.
func (d *swarmDriver) deleteData(ctx context.Context, path string) error {
	// Construct the data reference path.
	dataRefPath := filepath.Join(path, "data")
	// Publish a ZeroAddress to nullify the data reference.
	err := d.publisher.Put(ctx, dataRefPath, time.Now().Unix(), swarm.ZeroAddress)
	if err != nil {
		return fmt.Errorf("deleteData: failed to nullify data reference for path %s: %v", path, err)
	}
	return nil
}

// deleteMetadata nullifies the metadata reference for the given path by publishing a ZeroAddress.
func (d *swarmDriver) deleteMetadata(ctx context.Context, path string) error {
	// Construct the metadata reference path.
	metadataRefPath := filepath.Join(path, "mtdt")
	// Publish a ZeroAddress to nullify the metadata reference.
	err := d.publisher.Put(ctx, metadataRefPath, time.Now().Unix(), swarm.ZeroAddress)
	if err != nil {
		return fmt.Errorf("deleteMetadata: failed to nullify metadata for path %s: %v", path, err)
	}
	return nil
}

func (d *swarmDriver) childExists(ctx context.Context, path string) error {
	if path == "/" {
		return nil
	}
	// Traverse up the directory tree to check if each parent contains the child
	for {
		parentPath := filepath.ToSlash(filepath.Dir(path))
		childPath := filepath.Base(path)

		parentMtdt, err := d.getMetadata(ctx, parentPath)
		if err != nil {
			return err
		}
		// Check if the child exists in the parent's children
		found := false
		for _, child := range parentMtdt.Children {
			if child == childPath {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("childExists: child %s not found in parent %s", childPath, parentPath)
		}
		// If we have reached the root, break the loop
		if parentPath == "/" {
			break
		}
		// Move up to the parent directory
		path = parentPath
	}

	return nil
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *swarmDriver) GetContent(ctx context.Context, path string) ([]byte, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	logger.Debug("GetContent Hit", slog.String("path", path))
	if err := isValidPath(path); err != nil {
		logger.Error("GetContent: Invalid path", slog.String("error", err.Error()))
		return nil, storagedriver.InvalidPathError{DriverName: d.Name()}
	}
	if err := d.childExists(ctx, path); err != nil {
		logger.Error("GetContent: Child not found", slog.String("error", err.Error()))
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Fetch metadata using the helper function
	mtdt, err := d.getMetadata(ctx, path)
	if err != nil {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Check if data is a directory
	if mtdt.IsDir {
		return nil, storagedriver.InvalidPathError{DriverName: d.Name()}
	}
	// Fetch data using the helper function
	data, err := d.getData(ctx, path)
	if err != nil {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	logger.Debug("GetContent: Success!", slog.String("path", path))
	return data, nil
}

func (d *swarmDriver) PutContent(ctx context.Context, path string, content []byte) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	logger.Debug("PutContent Hit", slog.String("path", path))
	if err := isValidPath(path); err != nil {
		logger.Error("GetContent: Invalid path", slog.String("error", err.Error()))
		return storagedriver.InvalidPathError{DriverName: d.Name()}
	}
	// Split the content to get a data reference
	if err := d.putData(ctx, path, content); err != nil {
		logger.Error("PutContent: putData Failed!", slog.String("path", path))
		return storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Create and store metadata for the new content
	mtdt := metaData{
		IsDir:   false,
		Path:    path,
		ModTime: time.Now().Unix(),
		Size:    len(content),
	}
	if err := d.putMetadata(ctx, path, mtdt); err != nil {
		logger.Error("PutContent: putMetaData Failed!", slog.String("path", path))
		return storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	logger.Debug("PutContent: Success!", slog.String("path", path))
	return nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *swarmDriver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	logger.Debug("Reader Hit", slog.String("path", path))
	if offset < 0 {
		logger.Error("Reader: Invalid offset", slog.String("path", path), slog.Int64("offset", offset))
		return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset, DriverName: d.Name()}
	}
	if err := d.childExists(ctx, path); err != nil {
		logger.Error("Reader: Child not found", slog.String("error", err.Error()))
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Lookup data reference for the given path
	dataRef, err := d.lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil && !dataRef.Equal(swarm.ZeroAddress) {
		logger.Error("Reader: Failed to lookup data reference", slog.String("path", path), slog.String("error", err.Error()), "dataref", dataRef)
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	} else if dataRef.Equal(swarm.ZeroAddress) {
		logger.Warn("Reader: Data reference is zero", slog.String("path", path), "dataref", dataRef)
		return io.NopCloser(bytes.NewReader([]byte{})), nil
	}
	// Create a joiner to read the data
	dataJoiner, _, err := joiner.New(ctx, d.store, d.store, dataRef)
	if err != nil {
		logger.Error("Reader: Failed to create joiner", slog.String("path", path))
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Seek to the specified offset
	if _, err := dataJoiner.Seek(offset, io.SeekStart); err != nil {
		logger.Error("Reader: Failed to seek to offset", slog.String("path", path), slog.Int64("offset", offset), slog.String("error", err.Error()))
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	logger.Debug("Reader: Success", slog.String("path", path))
	return io.NopCloser(dataJoiner), nil
}

// Stat returns info about the provided path.
func (d *swarmDriver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	logger.Debug("Stat Hit", slog.String("path", path))
	// Fetch metadata using the helper function
	mtdt, err := d.getMetadata(ctx, path)
	if err != nil {
		logger.Info("Stat: Failed to lookup Metadata path", slog.String("path", path))
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Construct FileInfoFields from metadata
	fi := storagedriver.FileInfoFields{
		Path:    path,
		IsDir:   mtdt.IsDir,
		ModTime: time.Unix(mtdt.ModTime, 0),
	}
	// Set the size if it's not a directory
	if !fi.IsDir {
		fi.Size = int64(mtdt.Size)
	}
	logger.Debug("Stat: Success!", slog.String("path", path), slog.Any("fi", fi))
	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *swarmDriver) List(ctx context.Context, path string) ([]string, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	logger.Debug("List Hit", slog.String("path", path))
	if err := d.childExists(ctx, path); err != nil {
		logger.Error("List: Child not found", slog.String("error", err.Error()))
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Fetch metadata using the helper function
	mtdt, err := d.getMetadata(ctx, path)
	if err != nil {
		logger.Error("List: Failed to lookup Metadata path", slog.String("path", path))
		return nil, storagedriver.PathNotFoundError{Path: filepath.ToSlash(path), DriverName: d.Name()}
	}
	// Ensure it's a directory
	if !mtdt.IsDir {
		logger.Error("List: Not a directory", slog.String("path", path))
		return nil, storagedriver.InvalidPathError{Path: filepath.ToSlash(path), DriverName: d.Name()}
	}
	// Ensure children are not nil
	if len(mtdt.Children) == 0 {
		logger.Warn("List: This path has no children", slog.String("path", path))
		return []string{}, nil
	}
	children := []string{}
	for _, child := range mtdt.Children {
		children = append(children, filepath.ToSlash(filepath.Join(path, child)))
	}
	return children, nil
}

func (d *swarmDriver) Delete(ctx context.Context, path string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	logger.Debug("Delete Hit", slog.String("path", path))
	if path != "/" {
		// Remove the path from the parent's children
		parentPath := filepath.ToSlash(filepath.Dir(path))
		childPath := filepath.Base(path)
		parentMeta, err := d.getMetadata(ctx, parentPath)
		if err != nil {
			logger.Error("Delete: Failed to get parent Metadata", slog.String("childPath", parentPath))
			return storagedriver.PathNotFoundError{DriverName: d.Name(), Path: parentPath}
		}
		parentMeta.Children = removeFromSlice(parentMeta.Children, childPath)
		parentMetaBuf, err := json.Marshal(parentMeta)
		if err != nil {
			return storagedriver.PathNotFoundError{DriverName: d.Name(), Path: parentPath}
		}
		parentMetaRef, err := d.splitter.Split(ctx, io.NopCloser(bytes.NewReader(parentMetaBuf)), int64(len(parentMetaBuf)), d.encrypt)
		if err != nil || isZeroAddress(parentMetaRef) {
			return storagedriver.PathNotFoundError{DriverName: d.Name(), Path: parentPath}
		}
		err = d.publisher.Put(ctx, filepath.Join(parentPath, "mtdt"), time.Now().Unix(), parentMetaRef)
		if err != nil {
			return storagedriver.PathNotFoundError{DriverName: d.Name(), Path: parentPath}
		}
	}
	// Delete data and metadata
	if err := d.deleteData(ctx, path); err != nil {
		return storagedriver.PathNotFoundError{DriverName: d.Name(), Path: path}
	}
	if err := d.deleteMetadata(ctx, path); err != nil {
		return storagedriver.PathNotFoundError{DriverName: d.Name(), Path: path}
	}
	logger.Debug("Successfully deleted path", slog.String("path", path))
	return nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
func (d *swarmDriver) Move(ctx context.Context, sourcePath string, destPath string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	logger.Debug("Move Hit", slog.String("sourcePath", sourcePath), slog.String("destPath", destPath))
	// 1. Lookup and read source metadata
	sourceMeta, err := d.getMetadata(ctx, sourcePath)
	if err != nil {
		logger.Error("Move: Failed to lookup source Metadata path", slog.String("path", sourcePath), slog.String("error", err.Error()))
		return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: d.Name()}
	}
	// 2. Remove entry from the source parent
	sourceParentPath := filepath.ToSlash(filepath.Dir(sourcePath))
	sourceParentMeta, err := d.getMetadata(ctx, sourceParentPath)
	if err != nil {
		logger.Error("Move: Failed to get source parent Metadata", slog.String("path", sourcePath), slog.String("error", err.Error()))
		return storagedriver.PathNotFoundError{Path: sourceParentPath, DriverName: d.Name()}
	}
	sourceParentMeta.Children = removeFromSlice(sourceParentMeta.Children, filepath.Base(sourcePath))
	if err := d.putMetadata(ctx, sourceParentPath, sourceParentMeta); err != nil {
		logger.Error("Move: Failed to update source parent Metadata", slog.String("path", sourceParentPath), slog.String("error", err.Error()))
		return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: d.Name()}
	}
	// 3. Add entry to the destination parent
	destParentPath := filepath.ToSlash(filepath.Dir(destPath))
	destParentMeta, err := d.getMetadata(ctx, destParentPath)
	if err != nil {
		logger.Warn("Move: Destination parent not found, creating new metadata", slog.String("path", destParentPath), slog.String("error", err.Error()))
		destParentMeta = metaData{
			IsDir:    true,
			Path:     destParentPath,
			ModTime:  time.Now().Unix(),
			Children: []string{},
		}
	}
	destParentMeta.Children = append(destParentMeta.Children, filepath.Base(destPath))
	if err := d.putMetadata(ctx, destParentPath, destParentMeta); err != nil {
		logger.Error("Move: Failed to update destination parent Metadata", slog.String("path", destParentPath), slog.String("error", err.Error()))
		return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: d.Name()}
	}
	// 4. Update metadata to the new destination path
	sourceMeta.Path = destPath
	if err := d.putMetadata(ctx, destPath, sourceMeta); err != nil {
		logger.Error("Move: Failed to update Metadata to new destination path", slog.String("path", destPath), slog.String("error", err.Error()))
		return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: d.Name()}
	}
	// 5. Move data recursively from source to destination
	err = d.moveDataRecursively(ctx, sourcePath, destPath)
	if err != nil {
		logger.Error("Move: Failed to move data recursively", slog.String("sourcePath", sourcePath), slog.String("destPath", destPath), slog.String("error", err.Error()))
		return storagedriver.PathNotFoundError{Path: destParentPath, DriverName: d.Name()}
	}
	logger.Debug("Move Success", slog.String("sourcePath", sourcePath), slog.String("destPath", destPath))
	return nil
}

func (d *swarmDriver) moveDataRecursively(ctx context.Context, sourcePath, destPath string) error {
	// Get metadata of the source path
	sourceMetadata, err := d.getMetadata(ctx, sourcePath)
	if err != nil {
		return fmt.Errorf("Move: failed to lookup source metadata: %v", err)
	}
	// Update the metadata's path field to reflect the new destination
	sourceMetadata.Path = destPath
	// Move the data reference for the current path
	dataRef, err := d.lookuper.Get(ctx, filepath.Join(sourcePath, "data"), time.Now().Unix())
	if err != nil {
		return fmt.Errorf("Move: failed to get data reference: %v", err)
	}
	// Publish data reference to destination
	err = d.publisher.Put(ctx, filepath.Join(destPath, "data"), time.Now().Unix(), dataRef)
	if err != nil {
		return fmt.Errorf("Move: failed to publish data reference to destination: %v", err)
	}
	metaBuf, err := json.Marshal(sourceMetadata)
	if err != nil {
		return fmt.Errorf("putMetadata: failed to marshal metadata: %v", err)
	}
	// Publish the updated metadata to the destination
	metaRef, err := d.splitter.Split(ctx, io.NopCloser(bytes.NewReader(metaBuf)), int64(len(metaBuf)), d.encrypt)
	if err != nil || isZeroAddress(metaRef) {
		return fmt.Errorf("putMetadata: failed to split metadata: %v", err)
	}
	err = d.publisher.Put(ctx, filepath.Join(destPath, "mtdt"), time.Now().Unix(), metaRef)
	if err != nil {
		return fmt.Errorf("putMetadata: failed to publish metadata: %v", err)
	}
	// Recursively handle children
	for _, child := range sourceMetadata.Children {
		sourceChildPath := filepath.Join(sourcePath, child)
		destChildPath := filepath.Join(destPath, child)
		// Recursively move each child
		err := d.moveDataRecursively(ctx, sourceChildPath, destChildPath)
		if err != nil {
			return fmt.Errorf("Move: failed to move child data from %s to %s: %v", sourceChildPath, destChildPath, err)
		}
	}
	return nil
}

func removeFromSlice(slice []string, item string) []string {
	for i, v := range slice {
		if v == item {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// RedirectURL returns a URL which may be used to retrieve the content stored at the given path.
func (d *swarmDriver) RedirectURL(*http.Request, string) (string, error) {
	logger.Debug("RedirectURL Hit")
	return "", nil
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file and directory
func (d *swarmDriver) Walk(ctx context.Context, path string, f storagedriver.WalkFn, options ...func(*storagedriver.WalkOptions)) error {
	logger.Debug("Walk Hit", slog.String("path", path))
	return storagedriver.WalkFallback(ctx, d, path, f, options...)
}

// swarmFile represents a file in the swarm storage system.
// swarmFile represents a file in the swarm storage system.
type swarmFile struct {
	d         *swarmDriver  // Reference to the swarmDriver instance.
	path      string        // Path of the file in the storage system.
	buffer    *bytes.Buffer // Buffer to hold the file data.
	closed    bool          // Indicates if the file has been closed.
	committed bool          // Indicates if the file has been committed.
	cancelled bool          // Indicates if the file operation has been cancelled.
	offset    int64         // Offset for reading/writing data.
	size      int64         // Size of the file.
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *swarmDriver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	logger.Debug("Writer Hit", slog.String("path", path), slog.Bool("append", append))
	var combinedData bytes.Buffer
	w := &swarmFile{
		d:         d,
		path:      path,
		closed:    false,
		committed: false,
		cancelled: false,
		size:      0,
	}
	if append {
		logger.Debug("Writer: Append True", slog.String("path", path))
		// Lookup existing data at the specified path
		oldDataRef, err := d.lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
		if err != nil && !oldDataRef.Equal(swarm.ZeroAddress) {
			logger.Error("Writer: Append: Failed to fetch data", slog.String("path", path), slog.String("error", err.Error()))
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
		} else if oldDataRef.Equal(swarm.ZeroAddress) {
			logger.Warn("Writer: Append: Data reference is zero", slog.String("path", path))
			w.buffer = &combinedData
			return w, nil
		}
		// Create a joiner to read the existing data
		oldDataJoiner, _, err := joiner.New(ctx, d.store, d.store, oldDataRef)
		if err != nil {
			logger.Error("Writer: Append: Failed to create joiner", slog.String("path", path), slog.String("error", err.Error()))
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
		}
		// Copy existing data into the buffer
		if _, err := io.Copy(&combinedData, oldDataJoiner); err != nil {
			logger.Error("Writer: Append: Failed to copy data", slog.String("path", path), slog.String("error", err.Error()))
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
		}
		logger.Debug("Writer: Append: Successfully appended data", slog.String("path", path))
	}
	// Set the buffer and size in the writer
	w.buffer = &combinedData
	w.size = int64(w.buffer.Len())
	logger.Debug("Writer: Success", slog.String("path", path))
	// Return the FileWriter
	return w, nil
}

// Write writes the provided data to the swarmFile's buffer.
func (w *swarmFile) Write(p []byte) (int, error) {
	w.d.mutex.Lock()
	defer w.d.mutex.Unlock()
	// Check if the file is already closed, committed, or cancelled.
	if w.closed {
		return 0, fmt.Errorf("Write: already closed")
	} else if w.committed {
		return 0, fmt.Errorf("Write: already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("Write: already cancelled")
	}
	// Update the size of the file.
	w.size += int64(len(p))
	// Write the data to the buffer.
	return w.buffer.Write(p)
}

// Size returns the current size of the swarmFile.
func (w *swarmFile) Size() int64 {
	logger.Debug("Size Hit", slog.String("path", w.path), slog.Int64("size", int64(w.size)))
	return int64(w.size)
}

// Close finalizes the swarmFile, ensuring any unwritten data is stored.
func (w *swarmFile) Close() error {
	w.d.mutex.Lock()
	defer w.d.mutex.Unlock()
	logger.Debug("Close Hit", slog.String("path", w.path))
	if w.closed {
		return fmt.Errorf("Close: already closed")
	}
	// Add logic to only publish data ref if not committed and buffer has data
	if !w.committed && w.buffer.Len() > 0 {
		err := w.d.putData(context.Background(), w.path, w.buffer.Bytes())
		if err != nil {
			return fmt.Errorf("Close: failed to publish data reference: %v", err)
		}
	}
	w.closed = true
	return nil
}

// Cancel aborts the swarmFile operation, discarding any unwritten data.
// Cancel aborts the swarmFile operation, discarding any unwritten data.
func (w *swarmFile) Cancel(ctx context.Context) error {
	logger.Info("Cancel Hit", slog.String("path", w.path))
	// Check if the file is already closed or committed.
	if w.closed {
		return fmt.Errorf("Cancel: already closed")
	} else if w.committed {
		return fmt.Errorf("Cancel: already committed")
	}
	// Mark the file as cancelled.
	w.cancelled = true
	// Set the swarmFile instance to nil to discard any unwritten data.
	w = nil
	return nil
}

// Commit finalizes the swarmFile, ensuring all data is stored and metadata is updated.
func (w *swarmFile) Commit(ctx context.Context) error {
	w.d.mutex.Lock()
	defer w.d.mutex.Unlock()
	logger.Debug("Commit Hit", slog.String("path", w.path))
	// Check if the file is already closed, committed, or cancelled.
	if w.closed {
		return fmt.Errorf("Commit: already closed")
	} else if w.committed {
		return fmt.Errorf("Commit: already committed")
	} else if w.cancelled {
		return fmt.Errorf("Commit: already cancelled")
	}
	// Use the helper function to split and store data.
	err := w.d.putData(ctx, w.path, w.buffer.Bytes())
	if err != nil {
		return fmt.Errorf("Commit: failed to publish data reference: %v", err)
	}
	// Create metadata for the committed content.
	meta := metaData{
		IsDir:   false,
		Path:    w.path,
		ModTime: time.Now().Unix(),
		Size:    w.buffer.Len(),
	}
	// Store the metadata using the helper function.
	if err := w.d.putMetadata(ctx, w.path, meta); err != nil {
		return fmt.Errorf("Commit: failed to publish metadata reference: %v", err)
	}
	// Reset the buffer after committing data and metadata.
	w.buffer.Reset()
	// Mark the file as committed.
	w.committed = true
	logger.Debug("Commit: Successfully committed data and metadata", slog.String("path", w.path))
	return nil
}
