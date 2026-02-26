package chronicle

import (
	"context"
	"testing"
)

func TestStorageBackendFile(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	tests := []struct {
		name string
	}{
		{name: "new_file_backend"},
		{name: "write_read_cycle"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fb, err := NewFileBackend(dir + "/" + tt.name)
			if err != nil {
				t.Fatalf("NewFileBackend() error = %v", err)
			}
			defer fb.Close()

			switch tt.name {
			case "new_file_backend":
				// Constructor succeeded.
			case "write_read_cycle":
				if err := fb.Write(ctx, "test-key", []byte("hello")); err != nil {
					t.Fatalf("Write() error = %v", err)
				}
				data, err := fb.Read(ctx, "test-key")
				if err != nil {
					t.Fatalf("Read() error = %v", err)
				}
				if string(data) != "hello" {
					t.Errorf("got %q, want %q", string(data), "hello")
				}
			}
		})
	}
}
