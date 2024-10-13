package teststore_test

import (
	"context"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/storage"
	testingc "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/uncloud-registry/swarm-driver/store/teststore"
)

func TestSwarmInMemoryStore(t *testing.T) {
	store := teststore.NewSwarmInMemoryStore()
	defer store.Close()

	ctx := context.Background()

	// Test Put and Get
	chunk := testingc.GenerateTestRandomChunk()
	err := store.Put(ctx, chunk)
	if err != nil {
		t.Fatal(err)
	}

	retrievedChunk, err := store.Get(ctx, chunk.Address())
	if err != nil {
		t.Fatal(err)
	}

	if !chunk.Equal(retrievedChunk) {
		t.Fatal("retrieved chunk does not match the original chunk")
	}

	// Test Get with non-existent address
	nonExistentAddress := swarm.NewAddress([]byte{1, 2, 3})
	_, err = store.Get(ctx, nonExistentAddress)
	if err != storage.ErrNotFound {
		t.Fatal("expected ErrNotFound, got:", err)
	}
}
