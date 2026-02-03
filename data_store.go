package chronicle

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"sync"
)

// DataStore abstracts partition storage operations.
// This interface allows the database to use different storage backends
// for partition data, including local files, S3, or other object stores.
type DataStore interface {
	// ReadPartition reads partition data by its ID.
	ReadPartition(ctx context.Context, partitionID uint64) ([]byte, error)

	// WritePartition writes partition data and returns offset/length for index.
	// For file-based storage, this returns actual file offsets.
	// For backend-based storage, offset is 0 and length is data size.
	WritePartition(ctx context.Context, partitionID uint64, data []byte) (offset, length int64, err error)

	// DeletePartition removes partition data.
	DeletePartition(ctx context.Context, partitionID uint64) error

	// Sync ensures all written data is persisted.
	Sync() error

	// Close releases resources.
	Close() error

	// Stat returns storage statistics (size in bytes).
	Stat() (size int64, err error)

	// ReadPartitionAt reads partition data from a specific offset and length.
	// For backend storage, this is unsupported and should return an error.
	ReadPartitionAt(ctx context.Context, offset, length int64) ([]byte, error)
}

// FileDataStore implements DataStore using direct file operations.
// This is the original storage model using a single file with offset-based access.
type FileDataStore struct {
	file *os.File
	mu   sync.Mutex
}

// NewFileDataStore creates a FileDataStore from an open file.
func NewFileDataStore(file *os.File) *FileDataStore {
	return &FileDataStore{file: file}
}

// ReadPartition reads partition data from file at the specified offset.
// Note: For FileDataStore, partitionID is actually the file offset.
func (f *FileDataStore) ReadPartition(ctx context.Context, partitionID uint64) ([]byte, error) {
	return nil, ErrUnsupportedOperation
}

// ReadPartitionAt reads partition data from file at the specified offset and length.
func (f *FileDataStore) ReadPartitionAt(ctx context.Context, offset, length int64) ([]byte, error) {
	if length <= 0 {
		return nil, fmt.Errorf("invalid partition length")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	payload := make([]byte, length)
	if _, err := f.file.ReadAt(payload, offset); err != nil {
		return nil, err
	}

	if len(payload) < blockHeaderSize {
		return nil, fmt.Errorf("partition block too small")
	}

	blockType := payload[0]
	if blockType != blockTypePartition {
		return nil, fmt.Errorf("unexpected block type")
	}

	blockLen := int64(binary.LittleEndian.Uint32(payload[1:5]))
	blockChecksum := binary.LittleEndian.Uint32(payload[5:9])

	if blockLen != length-blockHeaderSize {
		return nil, fmt.Errorf("partition length mismatch")
	}

	data := payload[blockHeaderSize:]
	if crc32.ChecksumIEEE(data) != blockChecksum {
		return nil, fmt.Errorf("partition checksum mismatch")
	}

	return data, nil
}

// WritePartition writes partition data to the file.
func (f *FileDataStore) WritePartition(ctx context.Context, partitionID uint64, data []byte) (offset, length int64, err error) {
	select {
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	default:
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	checksum := crc32.ChecksumIEEE(data)

	if _, err := f.file.Seek(0, io.SeekEnd); err != nil {
		return 0, 0, err
	}

	offset, err = f.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, err
	}

	header := []byte{blockTypePartition}
	header = append(header, make([]byte, 8)...)
	binary.LittleEndian.PutUint32(header[1:], uint32(len(data)))
	binary.LittleEndian.PutUint32(header[5:], checksum)

	if _, err := f.file.Write(header); err != nil {
		return 0, 0, err
	}
	if _, err := f.file.Write(data); err != nil {
		return 0, 0, err
	}

	length = int64(len(data)) + blockHeaderSize
	return offset, length, nil
}

// DeletePartition is a no-op for file-based storage (handled by compaction).
func (f *FileDataStore) DeletePartition(ctx context.Context, partitionID uint64) error {
	// File-based storage handles deletion through compaction
	return nil
}

// Sync flushes data to disk.
func (f *FileDataStore) Sync() error {
	return f.file.Sync()
}

// Close closes the underlying file.
func (f *FileDataStore) Close() error {
	return f.file.Close()
}

// Stat returns the file size.
func (f *FileDataStore) Stat() (int64, error) {
	info, err := f.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// File returns the underlying file for legacy operations.
func (f *FileDataStore) File() *os.File {
	return f.file
}

// BackendDataStore implements DataStore using a StorageBackend.
// Each partition is stored as a separate object with key "partition/{id}".
type BackendDataStore struct {
	backend StorageBackend
	prefix  string
	mu      sync.Mutex
	size    int64 // Approximate total size
}

// NewBackendDataStore creates a DataStore backed by a StorageBackend.
func NewBackendDataStore(backend StorageBackend, prefix string) *BackendDataStore {
	return &BackendDataStore{
		backend: backend,
		prefix:  prefix,
	}
}

func (b *BackendDataStore) partitionKey(partitionID uint64) string {
	return b.prefix + "partition/" + strconv.FormatUint(partitionID, 10)
}

// ReadPartition reads partition data from the backend.
func (b *BackendDataStore) ReadPartition(ctx context.Context, partitionID uint64) ([]byte, error) {
	key := b.partitionKey(partitionID)
	return b.backend.Read(ctx, key)
}

// ReadPartitionAt is unsupported for backend storage.
func (b *BackendDataStore) ReadPartitionAt(ctx context.Context, offset, length int64) ([]byte, error) {
	return nil, ErrUnsupportedOperation
}

// WritePartition writes partition data to the backend.
// For backend storage, offset is always 0 (not meaningful) and length is data size.
func (b *BackendDataStore) WritePartition(ctx context.Context, partitionID uint64, data []byte) (offset, length int64, err error) {
	key := b.partitionKey(partitionID)
	if err := b.backend.Write(ctx, key, data); err != nil {
		return 0, 0, err
	}

	b.mu.Lock()
	b.size += int64(len(data))
	b.mu.Unlock()

	// For backend storage, offset is not meaningful; return 0
	// Length is the data size
	return 0, int64(len(data)), nil
}

// DeletePartition removes partition data from the backend.
func (b *BackendDataStore) DeletePartition(ctx context.Context, partitionID uint64) error {
	key := b.partitionKey(partitionID)
	return b.backend.Delete(ctx, key)
}

// Sync is a no-op for backend storage (writes are immediately persisted).
func (b *BackendDataStore) Sync() error {
	return nil
}

// Close closes the underlying backend.
func (b *BackendDataStore) Close() error {
	return b.backend.Close()
}

// Stat returns approximate storage size.
func (b *BackendDataStore) Stat() (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size, nil
}

// Backend returns the underlying StorageBackend.
func (b *BackendDataStore) Backend() StorageBackend {
	return b.backend
}

// Ensure implementations satisfy the interface
var (
	_ DataStore = (*FileDataStore)(nil)
	_ DataStore = (*BackendDataStore)(nil)
)
