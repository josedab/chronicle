package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestStorageBackendTiered(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
	}{
		{name: "new_tiered_backend"},
		{name: "write_read"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hot := NewMemoryBackend()
			cold := NewMemoryBackend()
			tb := NewTieredBackend(hot, cold, time.Hour)
			if tb == nil {
				t.Fatal("expected non-nil TieredBackend")
			}
			defer tb.Close()

			switch tt.name {
			case "new_tiered_backend":
				// Constructor succeeded.
			case "write_read":
				if err := tb.Write(ctx, "key1", []byte("data")); err != nil {
					t.Fatalf("Write() error = %v", err)
				}
				data, err := tb.Read(ctx, "key1")
				if err != nil {
					t.Fatalf("Read() error = %v", err)
				}
				if string(data) != "data" {
					t.Errorf("got %q, want %q", string(data), "data")
				}
			}
		})
	}
}
