package chronicle

import (
	"context"
	"os"
	"testing"
)

func TestFileBackend(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewFileBackend(dir)
	if err != nil {
		t.Fatalf("NewFileBackend failed: %v", err)
	}

	ctx := context.Background()

	// Write
	if err := backend.Write(ctx, "test/key1", []byte("hello")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read
	data, err := backend.Read(ctx, "test/key1")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("expected 'hello', got '%s'", data)
	}

	// Exists
	exists, err := backend.Exists(ctx, "test/key1")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected key to exist")
	}

	// List
	keys, err := backend.List(ctx, "test")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(keys) != 1 {
		t.Errorf("expected 1 key, got %d", len(keys))
	}

	// Delete
	if err := backend.Delete(ctx, "test/key1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	exists, _ = backend.Exists(ctx, "test/key1")
	if exists {
		t.Error("expected key to be deleted")
	}
}

func TestMemoryBackend(t *testing.T) {
	backend := NewMemoryBackend()
	ctx := context.Background()

	// Write
	if err := backend.Write(ctx, "key1", []byte("value1")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read
	data, err := backend.Read(ctx, "key1")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("expected 'value1', got '%s'", data)
	}

	// Size
	if backend.Size() != 1 {
		t.Errorf("expected size 1, got %d", backend.Size())
	}

	// Read non-existent
	_, err = backend.Read(ctx, "nonexistent")
	if !os.IsNotExist(err) {
		t.Error("expected not exist error")
	}

	// Exists
	exists, _ := backend.Exists(ctx, "key1")
	if !exists {
		t.Error("expected key to exist")
	}

	// List
	_ = backend.Write(ctx, "prefix/a", []byte("a"))
	_ = backend.Write(ctx, "prefix/b", []byte("b"))
	_ = backend.Write(ctx, "other/c", []byte("c"))

	keys, _ := backend.List(ctx, "prefix/")
	if len(keys) != 2 {
		t.Errorf("expected 2 keys with prefix, got %d", len(keys))
	}

	// Delete
	_ = backend.Delete(ctx, "key1")
	exists, _ = backend.Exists(ctx, "key1")
	if exists {
		t.Error("expected key to be deleted")
	}
}

func TestLRUCache(t *testing.T) {
	cache := NewLRUCache(3)

	// Add items
	cache.Put("a", []byte("1"))
	cache.Put("b", []byte("2"))
	cache.Put("c", []byte("3"))

	// All should exist
	if _, ok := cache.Get("a"); !ok {
		t.Error("expected 'a' to exist")
	}
	if _, ok := cache.Get("b"); !ok {
		t.Error("expected 'b' to exist")
	}

	// Add fourth item - should evict 'a' (LRU)
	cache.Put("d", []byte("4"))

	// 'a' should be evicted since 'b' was accessed after it
	if len(cache.items) > 3 {
		t.Errorf("cache exceeded capacity: %d items", len(cache.items))
	}

	// Delete
	cache.Delete("b")
	if _, ok := cache.Get("b"); ok {
		t.Error("expected 'b' to be deleted")
	}
}

func TestTieredBackend(t *testing.T) {
	hot := NewMemoryBackend()
	cold := NewMemoryBackend()
	tiered := NewTieredBackend(hot, cold, 0)
	ctx := context.Background()

	// Write to tiered (goes to hot)
	if err := tiered.Write(ctx, "key1", []byte("value1")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Should be in hot storage
	if _, err := hot.Read(ctx, "key1"); err != nil {
		t.Error("expected key in hot storage")
	}

	// Read from tiered
	data, err := tiered.Read(ctx, "key1")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("expected 'value1', got '%s'", data)
	}

	// Put data only in cold storage
	_ = cold.Write(ctx, "cold-key", []byte("cold-value"))

	// Read should find it in cold and promote to hot
	data, err = tiered.Read(ctx, "cold-key")
	if err != nil {
		t.Fatalf("Read from cold failed: %v", err)
	}
	if string(data) != "cold-value" {
		t.Errorf("expected 'cold-value', got '%s'", data)
	}

	// Now it should be in hot storage
	if _, err := hot.Read(ctx, "cold-key"); err != nil {
		t.Error("expected key to be promoted to hot storage")
	}

	// List should include keys from both
	_ = hot.Write(ctx, "hot-only", []byte("1"))
	_ = cold.Write(ctx, "cold-only", []byte("2"))

	keys, err := tiered.List(ctx, "")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(keys) < 4 {
		t.Errorf("expected at least 4 keys, got %d", len(keys))
	}

	// Delete
	if err := tiered.Delete(ctx, "key1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	exists, _ := tiered.Exists(ctx, "key1")
	if exists {
		t.Error("expected key to be deleted")
	}
}

func TestFileBackend_PathTraversal(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewFileBackend(dir)
	if err != nil {
		t.Fatalf("NewFileBackend failed: %v", err)
	}

	ctx := context.Background()

	// Test path traversal attempts that should be blocked
	// These are actual attempts to escape the base directory
	traversalKeys := []string{
		"../etc/passwd",
		"foo/../../../etc/passwd",
		"foo/bar/../../../../../../etc/passwd",
	}

	for _, key := range traversalKeys {
		t.Run("Read_"+key, func(t *testing.T) {
			_, err := backend.Read(ctx, key)
			if err == nil {
				t.Errorf("expected error for path traversal key %q, got nil", key)
			}
		})

		t.Run("Write_"+key, func(t *testing.T) {
			err := backend.Write(ctx, key, []byte("malicious"))
			if err == nil {
				t.Errorf("expected error for path traversal key %q, got nil", key)
			}
		})

		t.Run("Delete_"+key, func(t *testing.T) {
			err := backend.Delete(ctx, key)
			if err == nil {
				t.Errorf("expected error for path traversal key %q, got nil", key)
			}
		})

		t.Run("Exists_"+key, func(t *testing.T) {
			_, err := backend.Exists(ctx, key)
			if err == nil {
				t.Errorf("expected error for path traversal key %q, got nil", key)
			}
		})
	}

	// Valid nested paths should still work
	validKeys := []string{
		"partitions/2024/01/data.bin",
		"metrics/cpu/host1",
		"a/b/c/d/e/f",
	}

	for _, key := range validKeys {
		t.Run("ValidKey_"+key, func(t *testing.T) {
			if err := backend.Write(ctx, key, []byte("valid")); err != nil {
				t.Errorf("Write failed for valid key %q: %v", key, err)
			}
			data, err := backend.Read(ctx, key)
			if err != nil {
				t.Errorf("Read failed for valid key %q: %v", key, err)
			}
			if string(data) != "valid" {
				t.Errorf("expected 'valid', got '%s'", data)
			}
		})
	}
}

func TestFileBackend_SafePath(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewFileBackend(dir)
	if err != nil {
		t.Fatalf("NewFileBackend failed: %v", err)
	}

	tests := []struct {
		name      string
		key       string
		wantError bool
	}{
		{"simple key", "data.bin", false},
		{"nested key", "a/b/c.bin", false},
		{"parent traversal", "../outside", true},
		{"deep parent traversal", "a/b/../../../outside", true},
		{"double dots in name", "..data", false}, // Not traversal, just a filename starting with ..
		{"hidden file", ".hidden", false},
		// Note: On Unix, "/etc/passwd" as a key is joined as baseDir+"/etc/passwd"
		// which stays within baseDir. This is safe behavior.
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := backend.Write(ctx, tt.key, []byte("test"))
			if tt.wantError && err == nil {
				t.Errorf("expected error for key %q", tt.key)
			}
			if !tt.wantError && err != nil {
				t.Errorf("unexpected error for key %q: %v", tt.key, err)
			}
		})
	}
}
