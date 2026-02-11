package chronicle

import (
	"bytes"
	"context"
	"io"
)

// StorageBackend defines the interface for partition storage.
// This allows Chronicle to store data in different backends like
// local filesystem, S3, GCS, or Azure Blob Storage.
type StorageBackend interface {
	// Read reads a partition from storage.
	Read(ctx context.Context, key string) ([]byte, error)

	// Write writes a partition to storage.
	Write(ctx context.Context, key string, data []byte) error

	// Delete removes a partition from storage.
	Delete(ctx context.Context, key string) error

	// List returns all partition keys matching a prefix.
	List(ctx context.Context, prefix string) ([]string, error)

	// Exists checks if a partition exists.
	Exists(ctx context.Context, key string) (bool, error)

	// Close releases any resources.
	Close() error
}

// BufferedWriter wraps a StorageBackend with in-memory write buffering
// to reduce the number of calls to the underlying backend.
type BufferedWriter struct {
	backend StorageBackend
	buffer  *bytes.Buffer
	maxSize int
}

// Ensure interfaces are implemented
var (
	_ StorageBackend = (*FileBackend)(nil)
	_ StorageBackend = (*S3Backend)(nil)
	_ StorageBackend = (*MemoryBackend)(nil)
	_ StorageBackend = (*TieredBackend)(nil)
)

// StorageBackendFromReader creates a simple ReadCloser-based reader.
func StorageBackendFromReader(r io.ReadCloser) ([]byte, error) {
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}
