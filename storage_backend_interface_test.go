package chronicle

import "testing"

func TestStorageBackendInterface(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "memory_implements_storage_backend"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var _ StorageBackend = NewMemoryBackend()
		})
	}
}
