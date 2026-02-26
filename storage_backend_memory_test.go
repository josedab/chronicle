package chronicle

import (
	"context"
	"testing"
)

func TestStorageBackendMemory(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
	}{
		{name: "new_memory_backend"},
		{name: "write_read_delete"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mb := NewMemoryBackend()
			if mb == nil {
				t.Fatal("expected non-nil MemoryBackend")
			}
			defer mb.Close()

			switch tt.name {
			case "new_memory_backend":
				if mb.Size() != 0 {
					t.Errorf("got size %d, want 0", mb.Size())
				}
			case "write_read_delete":
				if err := mb.Write(ctx, "k1", []byte("v1")); err != nil {
					t.Fatalf("Write() error = %v", err)
				}
				data, err := mb.Read(ctx, "k1")
				if err != nil {
					t.Fatalf("Read() error = %v", err)
				}
				if string(data) != "v1" {
					t.Errorf("got %q, want %q", string(data), "v1")
				}
				if err := mb.Delete(ctx, "k1"); err != nil {
					t.Fatalf("Delete() error = %v", err)
				}
				exists, _ := mb.Exists(ctx, "k1")
				if exists {
					t.Error("expected key to be deleted")
				}
			}
		})
	}
}
