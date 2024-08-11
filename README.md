# swarm-driver
This is an implementation of docker storage driver on the swarm
```
type StorageDriver interface {
	// Name returns the human-readable "name" of the driver, useful in error
	// messages and logging. By convention, this will just be the registration
	// name, but drivers may provide other information here.
	Name() string

	// GetContent retrieves the content stored at "path" as a []byte.
	// This should primarily be used for small objects.
	GetContent(ctx context.Context, path string) ([]byte, error)

	// PutContent stores the []byte content at a location designated by "path".
	// This should primarily be used for small objects.
	PutContent(ctx context.Context, path string, content []byte) error

	// Reader retrieves an io.ReadCloser for the content stored at "path"
	// with a given byte offset.
	// May be used to resume reading a stream by providing a nonzero offset.
	Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error)

	// Writer returns a FileWriter which will store the content written to it
	// at the location designated by "path" after the call to Commit.
	// A path may be appended to if it has not been committed, or if the
	// existing committed content is zero length.
	//
	// The behaviour of appending to paths with non-empty committed content is
	// undefined. Specific implementations may document their own behavior.
	Writer(ctx context.Context, path string, append bool) (FileWriter, error)

	// Stat retrieves the FileInfo for the given path, including the current
	// size in bytes and the creation time.
	Stat(ctx context.Context, path string) (FileInfo, error)

	// List returns a list of the objects that are direct descendants of the
	// given path.
	List(ctx context.Context, path string) ([]string, error)

	// Move moves an object stored at sourcePath to destPath, removing the
	// original object.
	// Note: This may be no more efficient than a copy followed by a delete for
	// many implementations.
	Move(ctx context.Context, sourcePath string, destPath string) error

	// Delete recursively deletes all objects stored at "path" and its subpaths.
	Delete(ctx context.Context, path string) error

	// RedirectURL returns a URL which the client of the request r may use
	// to retrieve the content stored at path. Returning the empty string
	// signals that the request may not be redirected.
	RedirectURL(r *http.Request, path string) (string, error)

	// Walk traverses a filesystem defined within driver, starting
	// from the given path, calling f on each file.
	// If the returned error from the WalkFn is ErrSkipDir and fileInfo refers
	// to a directory, the directory will not be entered and Walk
	// will continue the traversal.
	// If the returned error from the WalkFn is ErrFilledBuffer, processing stops.
	Walk(ctx context.Context, path string, f WalkFn, options ...func(*WalkOptions)) error
}
```

```
PutContent(ctx context.Context, path string, content []byte) error
- Add content to swarm using splitter and get reference => r2
- Publish(path + "/data") => r2
- Publish(path + "/mtdt") => new metadata
- Lookup(parent(path))
- Add path to metadata if required
- Put metadata to swarm and get reference => r1
- Publish(parent(path) + "/mtdt") => r1

GetContent(ctx context.Context, path string) ([]byte, error)
- Read mtdt to check if dir or file
    - if directory return error
- Lookup(path + "/data", latest) => ref
- Joiner(store, ref) => data
```

```
Writer(ctx context.Context, path string, append bool) (FileWriter, error)
- if append
    - Lookup(path + "/data") => r1
    - Read r1 and write to FileWriter
    - Return FileWriter
- else
    - Return FileWriter

- FileWriter commit
    - Add all content to swarm => r2
    - Publish(path + "/data") => r2
    - Publish(path + "/mtdt") => new metadata
    - Lookup(parent(path))
    - Add path to metadata if required
    - Put metadata to swarm and get reference => r3
    - Publish(parent(path) + "/mtdt") => r3


Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error)
- Lookup(path + "/mtdt") => r1
- Read mtdt to check if dir or file
    - if directory return error
- Lookup(path + "/data") => r1
- Return ReadCloser
```

```
Stat(ctx context.Context, path string) (FileInfo, error)
- Lookup(path + "/mtdt") => r1
- Read data and populate FileInfo
- Return FileInfo
```
