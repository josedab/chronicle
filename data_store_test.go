package chronicle

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestFileDataStore(t *testing.T) {
	f, err := os.CreateTemp("", "filedata_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	// Initialize with magic header
	if _, err := f.Write([]byte("CHRDB1")); err != nil {
		t.Fatal(err)
	}

	ds := NewFileDataStore(f)

	// Test WritePartition
	data := []byte("test partition data")
	offset, length, err := ds.WritePartition(context.Background(), 1, data)
	if err != nil {
		t.Fatalf("WritePartition failed: %v", err)
	}
	if offset < 6 { // Should be after header
		t.Errorf("expected offset >= 6, got %d", offset)
	}
	if length != int64(len(data))+blockHeaderSize {
		t.Errorf("expected length %d, got %d", int64(len(data))+blockHeaderSize, length)
	}

	// Test ReadPartitionAt
	readData, err := ds.ReadPartitionAt(context.Background(), offset, length)
	if err != nil {
		t.Fatalf("ReadPartitionAt failed: %v", err)
	}
	if string(readData) != string(data) {
		t.Errorf("expected %q, got %q", data, readData)
	}

	// Test Stat
	size, err := ds.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if size <= 0 {
		t.Errorf("expected positive size, got %d", size)
	}

	// Test Sync
	if err := ds.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

func TestBackendDataStore(t *testing.T) {
	backend := NewMemoryBackend()
	ds := NewBackendDataStore(backend, "")

	ctx := context.Background()

	// Test WritePartition
	data := []byte("test partition data for backend")
	offset, length, err := ds.WritePartition(ctx, 42, data)
	if err != nil {
		t.Fatalf("WritePartition failed: %v", err)
	}
	if offset != 0 { // Backend storage always returns offset 0
		t.Errorf("expected offset 0, got %d", offset)
	}
	if length != int64(len(data)) {
		t.Errorf("expected length %d, got %d", len(data), length)
	}

	// Test ReadPartition
	readData, err := ds.ReadPartition(ctx, 42)
	if err != nil {
		t.Fatalf("ReadPartition failed: %v", err)
	}
	if string(readData) != string(data) {
		t.Errorf("expected %q, got %q", data, readData)
	}

	// ReadPartitionAt should be unsupported for backend storage
	if _, err := ds.ReadPartitionAt(ctx, 0, 10); !errors.Is(err, ErrUnsupportedOperation) {
		t.Errorf("expected ErrUnsupportedOperation, got %v", err)
	}

	// Test Stat
	size, err := ds.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if size != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), size)
	}

	// Test DeletePartition
	if err := ds.DeletePartition(ctx, 42); err != nil {
		t.Fatalf("DeletePartition failed: %v", err)
	}

	// Verify deleted
	_, err = ds.ReadPartition(ctx, 42)
	if err == nil {
		t.Error("expected error reading deleted partition")
	}

	// Test Close
	if err := ds.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestBackendDataStoreWithPrefix(t *testing.T) {
	backend := NewMemoryBackend()
	ds := NewBackendDataStore(backend, "mydb/")

	ctx := context.Background()

	data := []byte("prefixed data")
	_, _, err := ds.WritePartition(ctx, 123, data)
	if err != nil {
		t.Fatalf("WritePartition failed: %v", err)
	}

	// Verify the key uses the prefix
	readData, err := backend.Read(ctx, "mydb/partition/123")
	if err != nil {
		t.Fatalf("Backend.Read failed: %v", err)
	}
	if string(readData) != string(data) {
		t.Errorf("expected %q, got %q", data, readData)
	}
}

func TestFileDataStoreContextCancellation(t *testing.T) {
	f, err := os.CreateTemp("", "filedata_cancel_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	ds := NewFileDataStore(f)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err = ds.ReadPartitionAt(ctx, 0, 10)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	_, _, err = ds.WritePartition(ctx, 1, []byte("data"))
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
