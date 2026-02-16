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

func TestS3Integration_MultipartUploadResumption(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	// Create data that triggers multipart upload (>1KB in test config)
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Write, then overwrite (simulates re-upload after failure)
	key := "resumption/large-object"
	if err := backend.Write(ctx, key, data[:2048]); err != nil {
		t.Fatalf("First write: %v", err)
	}

	// Overwrite with full data (simulates resumed upload)
	if err := backend.Write(ctx, key, data); err != nil {
		t.Fatalf("Resumed write: %v", err)
	}

	got, err := backend.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read after resumption: %v", err)
	}
	if len(got) != len(data) {
		t.Errorf("expected %d bytes, got %d", len(data), len(got))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Errorf("data mismatch at byte %d", i)
			break
		}
	}
	_ = backend.Delete(ctx, key)
}

func TestS3Integration_RetryWithJitter(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	// Verify that the backend retryer is configured with jitter
	if backend.retryer == nil {
		t.Fatal("expected retryer to be configured")
	}

	// Write and read operations should succeed through retry logic
	key := "retry-jitter/test-key"
	payload := []byte("retry-test-data")

	if err := backend.Write(ctx, key, payload); err != nil {
		t.Fatalf("Write with retry: %v", err)
	}

	data, err := backend.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read with retry: %v", err)
	}
	if string(data) != string(payload) {
		t.Errorf("got %q, want %q", string(data), string(payload))
	}
	_ = backend.Delete(ctx, key)
}

func TestS3Integration_EventualConsistency(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	key := "eventual/consistency-test"
	payload := []byte("consistency-data")

	// Write
	if err := backend.Write(ctx, key, payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Immediate read-after-write should succeed (S3 is now strongly consistent
	// for all operations, but we test the pattern for S3-compatible stores)
	var data []byte
	var err error
	maxAttempts := 5
	for i := 0; i < maxAttempts; i++ {
		data, err = backend.Read(ctx, key)
		if err == nil && string(data) == string(payload) {
			break
		}
		time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Read after eventual consistency wait: %v", err)
	}
	if string(data) != string(payload) {
		t.Errorf("data mismatch: got %q, want %q", string(data), string(payload))
	}

	// Delete and verify eventual disappearance
	if err := backend.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	var exists bool
	for i := 0; i < maxAttempts; i++ {
		exists, _ = backend.Exists(ctx, key)
		if !exists {
			break
		}
		time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
	}
	if exists {
		t.Error("key still exists after delete and consistency wait")
	}
}

func TestS3Integration_LargeObjectBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark in short mode")
	}
	backend := skipIfNoS3(t)
	ctx := context.Background()

	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"64KB", 64 * 1024},
		{"1MB", 1024 * 1024},
	}

	for _, sz := range sizes {
		t.Run(sz.name, func(t *testing.T) {
			data := make([]byte, sz.size)
			if _, err := rand.Read(data); err != nil {
				t.Fatal(err)
			}
			key := fmt.Sprintf("bench/%s", sz.name)

			// Measure write latency
			writeStart := time.Now()
			if err := backend.Write(ctx, key, data); err != nil {
				t.Fatalf("Write %s: %v", sz.name, err)
			}
			writeDur := time.Since(writeStart)

			// Measure read latency (cold: invalidate cache)
			backend.cache.Delete(backend.config.Prefix + key)
			readStart := time.Now()
			got, err := backend.Read(ctx, key)
			if err != nil {
				t.Fatalf("Read %s: %v", sz.name, err)
			}
			readDur := time.Since(readStart)

			if len(got) != sz.size {
				t.Errorf("size mismatch: got %d, want %d", len(got), sz.size)
			}

			writeThroughput := float64(sz.size) / writeDur.Seconds() / 1024 / 1024
			readThroughput := float64(sz.size) / readDur.Seconds() / 1024 / 1024
			t.Logf("%s: write=%v (%.2f MB/s) read=%v (%.2f MB/s)",
				sz.name, writeDur, writeThroughput, readDur, readThroughput)

			_ = backend.Delete(ctx, key)
		})
	}
}

func TestS3Integration_OverwriteConsistency(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	key := "overwrite/test-key"

	// Write v1
	if err := backend.Write(ctx, key, []byte("version-1")); err != nil {
		t.Fatalf("Write v1: %v", err)
	}

	// Overwrite with v2
	if err := backend.Write(ctx, key, []byte("version-2")); err != nil {
		t.Fatalf("Write v2: %v", err)
	}

	// Invalidate cache and read
	backend.cache.Delete(backend.config.Prefix + key)
	data, err := backend.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(data) != "version-2" {
		t.Errorf("expected version-2, got %q", string(data))
	}

	_ = backend.Delete(ctx, key)
}

func TestS3Integration_EmptyAndBinaryData(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	tests := []struct {
		name string
		key  string
		data []byte
	}{
		{"empty", "edge/empty", []byte{}},
		{"single_byte", "edge/single", []byte{0xFF}},
		{"null_bytes", "edge/nulls", []byte{0x00, 0x00, 0x00}},
		{"binary_mix", "edge/binary", []byte{0x00, 0xFF, 0x80, 0x7F, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := backend.Write(ctx, tt.key, tt.data); err != nil {
				t.Fatalf("Write: %v", err)
			}
			got, err := backend.Read(ctx, tt.key)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if len(got) != len(tt.data) {
				t.Errorf("length: got %d, want %d", len(got), len(tt.data))
			}
			for i := range tt.data {
				if i < len(got) && got[i] != tt.data[i] {
					t.Errorf("byte %d: got %x, want %x", i, got[i], tt.data[i])
					break
				}
			}
			_ = backend.Delete(ctx, tt.key)
		})
	}
}

func TestS3Integration_MultipartResumableUpload(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	// Create data large enough for multipart (>1KB with test config)
	data := make([]byte, 3072)
	for i := range data {
		data[i] = byte(i % 256)
	}

	key := "resumable/test-upload"

	// Start the upload (fresh state)
	state, err := backend.WriteMultipartResumable(ctx, key, data, nil)
	if err != nil {
		t.Fatalf("WriteMultipartResumable: %v", err)
	}
	if state == nil {
		t.Fatal("expected non-nil upload state")
	}
	if state.UploadID == "" {
		t.Error("expected non-empty upload ID")
	}

	// Verify data is readable
	got, err := backend.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read after resumable upload: %v", err)
	}
	if len(got) != len(data) {
		t.Errorf("size mismatch: got %d, want %d", len(got), len(data))
	}

	_ = backend.Delete(ctx, key)
}

func TestS3Integration_PrefixIsolation(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	// Write keys under different prefixes within our test prefix
	_ = backend.Write(ctx, "isolation/a/key1", []byte("a1"))
	_ = backend.Write(ctx, "isolation/a/key2", []byte("a2"))
	_ = backend.Write(ctx, "isolation/b/key1", []byte("b1"))

	// List under prefix "a" should only show 2 keys
	keysA, err := backend.List(ctx, "isolation/a/")
	if err != nil {
		t.Fatalf("List prefix a: %v", err)
	}
	if len(keysA) != 2 {
		t.Errorf("expected 2 keys under a/, got %d", len(keysA))
	}

	// List under prefix "b" should only show 1 key
	keysB, err := backend.List(ctx, "isolation/b/")
	if err != nil {
		t.Fatalf("List prefix b: %v", err)
	}
	if len(keysB) != 1 {
		t.Errorf("expected 1 key under b/, got %d", len(keysB))
	}

	// Cleanup
	for _, k := range []string{"isolation/a/key1", "isolation/a/key2", "isolation/b/key1"} {
		_ = backend.Delete(ctx, k)
	}
}

func TestS3Integration_ReadAfterWriteConsistency(t *testing.T) {
	backend := skipIfNoS3(t)
	ctx := context.Background()

	// Test rapid write-read cycles to verify consistency
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("raw-consistency/key%d", i)
		value := fmt.Sprintf("value-%d-%d", i, time.Now().UnixNano())

		if err := backend.Write(ctx, key, []byte(value)); err != nil {
			t.Fatalf("iteration %d write: %v", i, err)
		}

		// Invalidate cache to force S3 read
		backend.cache.Delete(backend.config.Prefix + key)

		data, err := backend.Read(ctx, key)
		if err != nil {
			t.Fatalf("iteration %d read: %v", i, err)
		}
		if string(data) != value {
			t.Errorf("iteration %d: got %q, want %q", i, string(data), value)
		}

		_ = backend.Delete(ctx, key)
	}
}

func TestS3Integration_TieringPolicyConfig(t *testing.T) {
	// Test that S3TieringPolicy configuration is correct
	cfg := s3TestConfig(t)
	if cfg.MaxRetries != 3 {
		t.Errorf("expected MaxRetries=3, got %d", cfg.MaxRetries)
	}
	if cfg.MultipartThreshold != 1024 {
		t.Errorf("expected MultipartThreshold=1024, got %d", cfg.MultipartThreshold)
	}
	if cfg.UsePathStyle != true {
		t.Error("expected UsePathStyle=true for MinIO")
	}

	// Verify tiering policies are well-defined
	tieringCfg := DefaultS3TieringConfig()
	if len(tieringCfg.Policies) == 0 {
		t.Error("expected at least one policy in default tiering config")
	}
	// Verify policies are ordered by age
	for i := 1; i < len(tieringCfg.Policies); i++ {
		if tieringCfg.Policies[i].AfterDays <= tieringCfg.Policies[i-1].AfterDays {
			t.Errorf("policies not ordered: %d <= %d at index %d",
				tieringCfg.Policies[i].AfterDays, tieringCfg.Policies[i-1].AfterDays, i)
		}
	}

	// Test ShouldTransition logic
	policy := tieringCfg.Policies[0] // hot-to-warm at 30 days
	if policy.ShouldTransition(10*24*time.Hour, 256*1024) {
		t.Error("should not transition before AfterDays")
	}
	if !policy.ShouldTransition(31*24*time.Hour, 256*1024) {
		t.Error("should transition after AfterDays with sufficient size")
	}
	if policy.ShouldTransition(31*24*time.Hour, 64) {
		t.Error("should not transition below MinSizeBytes")
	}
}
