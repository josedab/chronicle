package chronicle

import "testing"

func TestStorageBackendS3(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "lru_cache_basic"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewLRUCache(10)
			if cache == nil {
				t.Fatal("expected non-nil LRUCache")
			}
			cache.Put("k1", []byte("v1"))
			data, ok := cache.Get("k1")
			if !ok {
				t.Fatal("expected cache hit")
			}
			if string(data) != "v1" {
				t.Errorf("got %q, want %q", string(data), "v1")
			}
			cache.Delete("k1")
			_, ok = cache.Get("k1")
			if ok {
				t.Error("expected cache miss after delete")
			}
		})
	}
}
