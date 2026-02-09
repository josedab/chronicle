package chronicle

import (
	"os"
	"sync"
	"time"
)

// StorageEngine manages the low-level storage components including
// file operations, write-ahead log, index, and partition management.
type StorageEngine struct {
	path   string
	file   *os.File
	wal    *WAL
	index  *Index
	buffer *WriteBuffer

	mu     sync.RWMutex
	closed bool
}

// StorageEngineConfig holds configuration for the storage engine.
type StorageEngineConfig struct {
	Path              string
	Storage           StorageConfig
	WAL               WALConfig
	PartitionDuration time.Duration

	// Legacy fields.
	BufferSize   int
	SyncInterval time.Duration
	WALMaxSize   int64
	WALRetain    int
}

// NewStorageEngine creates a new storage engine with the given configuration.
func NewStorageEngine(cfg StorageEngineConfig) (*StorageEngine, error) {
	se := &StorageEngine{
		path: cfg.Path,
	}
	if cfg.Storage.BufferSize == 0 && cfg.BufferSize != 0 {
		cfg.Storage.BufferSize = cfg.BufferSize
	}
	if cfg.WAL.SyncInterval == 0 && cfg.SyncInterval != 0 {
		cfg.WAL.SyncInterval = cfg.SyncInterval
	}
	if cfg.WAL.WALMaxSize == 0 && cfg.WALMaxSize != 0 {
		cfg.WAL.WALMaxSize = cfg.WALMaxSize
	}
	if cfg.WAL.WALRetain == 0 && cfg.WALRetain != 0 {
		cfg.WAL.WALRetain = cfg.WALRetain
	}
	if cfg.Storage.BufferSize == 0 {
		cfg.Storage.BufferSize = 10_000
	}
	if cfg.WAL.SyncInterval == 0 {
		cfg.WAL.SyncInterval = time.Second
	}

	file, err := os.OpenFile(cfg.Path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	se.file = file

	if err := initStorage(file); err != nil {
		_ = file.Close()
		return nil, err
	}

	se.wal, err = NewWAL(cfg.Path+".wal", cfg.WAL.SyncInterval, cfg.WAL.WALMaxSize, cfg.WAL.WALRetain)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	se.index, err = loadIndex(file)
	if err != nil {
		_ = file.Close()
		_ = se.wal.Close()
		return nil, err
	}

	se.buffer = NewWriteBuffer(cfg.Storage.BufferSize)

	return se, nil
}

// Close closes the storage engine and releases resources.
func (se *StorageEngine) Close() error {
	se.mu.Lock()
	defer se.mu.Unlock()

	if se.closed {
		return nil
	}
	se.closed = true

	var firstErr error
	if err := se.wal.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := se.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// WAL returns the write-ahead log.
func (se *StorageEngine) WAL() *WAL {
	return se.wal
}

// Index returns the partition index.
func (se *StorageEngine) Index() *Index {
	return se.index
}

// Buffer returns the write buffer.
func (se *StorageEngine) Buffer() *WriteBuffer {
	return se.buffer
}

// File returns the underlying file handle.
func (se *StorageEngine) File() *os.File {
	return se.file
}

// Path returns the storage path.
func (se *StorageEngine) Path() string {
	return se.path
}

// IsClosed returns whether the storage engine is closed.
func (se *StorageEngine) IsClosed() bool {
	se.mu.RLock()
	defer se.mu.RUnlock()
	return se.closed
}
