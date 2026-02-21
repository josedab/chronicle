//go:build integration

package chronicle

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"
)

// These tests require a running S3-compatible service (MinIO).
//
// Run locally:
//   docker run -d -p 9000:9000 -p 9001:9001 \
//     -e MINIO_ROOT_USER=minioadmin \
//     -e MINIO_ROOT_PASSWORD=minioadmin \
//     minio/minio server /data --console-address ":9001"
//
//   # Create the test bucket:
//   mc alias set local http://localhost:9000 minioadmin minioadmin
//   mc mb local/chronicle-test
//
//   go test -tags integration -run TestS3 -v
//
// Or set S3_TEST_ENDPOINT to a custom endpoint.

func s3TestConfig(t *testing.T) S3BackendConfig {
	t.Helper()

	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	bucket := os.Getenv("S3_TEST_BUCKET")
	if bucket == "" {
		bucket = "chronicle-test"
	}

	return S3BackendConfig{
		Bucket:             bucket,
		Region:             "us-east-1",
		Endpoint:           endpoint,
		AccessKeyID:        "minioadmin",
		SecretAccessKey:    "minioadmin",
		UsePathStyle:       true,
		Prefix:             fmt.Sprintf("test-%d/", time.Now().UnixNano()),
		CacheSize:          10,
		CacheTTL:           500 * time.Millisecond,
		MaxRetries:         3,
		MultipartThreshold: 1024, // 1KB threshold for testing
		MultipartPartSize:  5 * 1024 * 1024,
	}
}

func skipIfNoS3(t *testing.T) *S3Backend {
	t.Helper()
	cfg := s3TestConfig(t)
	backend, err := NewS3Backend(cfg)
	if err != nil {
		t.Skipf("S3 backend unavailable: %v", err)
	}

	// Quick connectivity check
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := backend.Write(ctx, "_healthcheck", []byte("ok")); err != nil {
		t.Skipf("S3 not reachable: %v", err)
	}
	_ = backend.Delete(ctx, "_healthcheck")
	return backend
}

func TestS3Integration_BasicCRUD(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	// Write
	if err := backend.Write(ctx, "test/key1", []byte("hello world")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read
	data, err := backend.Read(ctx, "test/key1")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(data) != "hello world" {
		t.Errorf("Read: got %q, want %q", string(data), "hello world")
	}

	// Exists
	exists, err := backend.Exists(ctx, "test/key1")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Error("Exists: expected true")
	}

	// List
	keys, err := backend.List(ctx, "test/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) < 1 {
		t.Error("List: expected at least 1 key")
	}

	// Delete
	if err := backend.Delete(ctx, "test/key1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	exists, _ = backend.Exists(ctx, "test/key1")
	if exists {
		t.Error("Exists after delete: expected false")
	}
}

func TestS3Integration_MultipartUpload(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	// Create data larger than MultipartThreshold (set to 1KB in test config)
	data := make([]byte, 2048)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	// This should trigger multipart upload
	if err := backend.Write(ctx, "multipart/large", data); err != nil {
		t.Fatalf("Write multipart: %v", err)
	}

	// Read back and verify
	got, err := backend.Read(ctx, "multipart/large")
	if err != nil {
		t.Fatalf("Read multipart: %v", err)
	}
	if len(got) != len(data) {
		t.Errorf("Read multipart: got %d bytes, want %d", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Errorf("Read multipart: mismatch at byte %d", i)
			break
		}
	}

	// Cleanup
	_ = backend.Delete(ctx, "multipart/large")
}

func TestS3Integration_CacheTTLExpiry(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	// Write and read (populates cache)
	_ = backend.Write(ctx, "ttl/key1", []byte("cached"))
	data, ok := backend.cache.Get(backend.config.Prefix + "ttl/key1")
	if !ok {
		t.Fatal("expected cache hit after write")
	}
	if string(data) != "cached" {
		t.Errorf("cache: got %q, want %q", string(data), "cached")
	}

	// Wait for TTL expiry (500ms in test config)
	time.Sleep(600 * time.Millisecond)

	// Cache should be expired
	_, ok = backend.cache.Get(backend.config.Prefix + "ttl/key1")
	if ok {
		t.Error("expected cache miss after TTL expiry")
	}

	// Read should still work (fetches from S3)
	data2, err := backend.Read(ctx, "ttl/key1")
	if err != nil {
		t.Fatalf("Read after TTL: %v", err)
	}
	if string(data2) != "cached" {
		t.Errorf("Read after TTL: got %q", string(data2))
	}

	_ = backend.Delete(ctx, "ttl/key1")
}

func TestS3Integration_ConcurrentOperations(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	// Concurrent writes
	const n = 20
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			key := fmt.Sprintf("concurrent/key%d", i)
			err := backend.Write(ctx, key, []byte(fmt.Sprintf("value%d", i)))
			errs <- err
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent write %d: %v", i, err)
		}
	}

	// Verify all reads
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("concurrent/key%d", i)
		data, err := backend.Read(ctx, key)
		if err != nil {
			t.Errorf("read %s: %v", key, err)
			continue
		}
		want := fmt.Sprintf("value%d", i)
		if string(data) != want {
			t.Errorf("read %s: got %q, want %q", key, string(data), want)
		}
		_ = backend.Delete(ctx, key)
	}
}

func TestS3Integration_NonExistentKey(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	exists, err := backend.Exists(ctx, "nonexistent/key/path")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("expected false for nonexistent key")
	}

	_, err = backend.Read(ctx, "nonexistent/key/path")
	if err == nil {
		t.Error("expected error reading nonexistent key")
	}
}
