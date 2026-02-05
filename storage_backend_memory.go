package chronicle

import (
	"context"
	"os"
	"sync"
)

// MemoryBackend implements StorageBackend using in-memory storage.
// Useful for testing and WASM environments.
type MemoryBackend struct {
	data map[string][]byte
	mu   sync.RWMutex
}

// NewMemoryBackend creates a new in-memory storage backend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		data: make(map[string][]byte),
	}
}

func (m *MemoryBackend) Read(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.data[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return data, nil
}

func (m *MemoryBackend) Write(ctx context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = append([]byte(nil), data...)
	return nil
}

func (m *MemoryBackend) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

func (m *MemoryBackend) List(ctx context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for k := range m.data {
		if len(prefix) == 0 || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *MemoryBackend) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.data[key]
	return ok, nil
}

func (m *MemoryBackend) Close() error {
	return nil
}

// Size returns the number of items in memory.
func (m *MemoryBackend) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}
