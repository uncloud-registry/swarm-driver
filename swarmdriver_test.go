package swarmdriver

import (
	"testing"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
)

func newSwarmDriverConstructor() (storagedriver.StorageDriver, error) {
	// Generate or retrieve the private key (handle errors as appropriate).
	privateKey := "8e5c893846223894ed783acd26de67ce7d8a614107259dce1c6d6e42da4d24f57a22536487c200c38337afaa3f514ecdd38006657f4e05725baa21e4cf606892054e00dd947d6e737c4be92306d49fa4"
	encrypt := false
	// store := teststore.NewSwarmInMemoryStore()

	return New(privateKey, encrypt), nil
}

func TestSwarmDriverSuite(t *testing.T) {
	testsuites.Driver(t, newSwarmDriverConstructor)
}

func BenchmarkSwarmDriverSuite(b *testing.B) {
	testsuites.BenchDriver(b, newSwarmDriverConstructor)
}
