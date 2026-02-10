package chronicle

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestEdgeSyncConfig(t *testing.T) {
	cfg := DefaultEdgeSyncConfig()

	if cfg.Enabled {
		t.Error("Default config should have sync disabled")
	}
	if cfg.SyncInterval != time.Minute {
		t.Errorf("Expected 1 minute sync interval, got %v", cfg.SyncInterval)
	}
	if cfg.BatchSize != 10000 {
		t.Errorf("Expected 10000 batch size, got %d", cfg.BatchSize)
	}
	if cfg.MaxRetries != 5 {
		t.Errorf("Expected 5 max retries, got %d", cfg.MaxRetries)
	}
	if !cfg.CompressionEnabled {
		t.Error("Expected compression enabled by default")
	}
	if cfg.ConflictResolution != ConflictResolutionLastWriteWins {
		t.Errorf("Expected last_write_wins, got %s", cfg.ConflictResolution)
	}
}

func TestSyncQueue(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "queue")

	q, err := newSyncQueue(queuePath, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Enqueue points
	points := []Point{
		{Metric: "cpu", Value: 45.5, Timestamp: time.Now().UnixNano()},
		{Metric: "mem", Value: 78.2, Timestamp: time.Now().UnixNano()},
	}

	if err := q.Enqueue(points); err != nil {
		t.Fatalf("Failed to enqueue: %v", err)
	}

	// Check stats
	count, _ := q.Stats()
	if count != 2 {
		t.Errorf("Expected 2 items, got %d", count)
	}

	// Dequeue
	batch, err := q.Dequeue(1)
	if err != nil {
		t.Fatalf("Failed to dequeue: %v", err)
	}
	if len(batch) != 1 {
		t.Errorf("Expected 1 item, got %d", len(batch))
	}
	if batch[0].Metric != "cpu" {
		t.Errorf("Expected cpu metric, got %s", batch[0].Metric)
	}

	// Check remaining
	count, _ = q.Stats()
	if count != 1 {
		t.Errorf("Expected 1 remaining, got %d", count)
	}

	// Close and reopen
	if err := q.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	q2, err := newSyncQueue(queuePath, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}

	count, _ = q2.Stats()
	if count != 1 {
		t.Errorf("Expected 1 item after reopen, got %d", count)
	}
}

func TestSyncQueueOverflow(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "queue")

	// Small queue
	q, err := newSyncQueue(queuePath, 500)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue many points
	for i := 0; i < 20; i++ {
		points := []Point{
			{Metric: "test", Value: float64(i), Timestamp: time.Now().UnixNano()},
		}
		_ = q.Enqueue(points)
	}

	// Should have dropped old items
	count, size := q.Stats()
	if size > 600 { // Allow some margin
		t.Errorf("Queue exceeded max size: %d", size)
	}
	t.Logf("Queue has %d items, %d bytes", count, size)
}

func TestHTTPAdapter(t *testing.T) {
	var received []Point
	var receivedCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&receivedCount, 1)

		// Check content type
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("Expected application/json content type")
		}

		// Read and decompress if needed
		var data []byte
		var err error

		if r.Header.Get("Content-Encoding") == "gzip" {
			gr, _ := gzip.NewReader(r.Body)
			data, err = io.ReadAll(gr)
			gr.Close()
		} else {
			data, err = io.ReadAll(r.Body)
		}

		if err != nil {
			t.Errorf("Failed to read body: %v", err)
			w.WriteHeader(500)
			return
		}

		var batch syncBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			t.Errorf("Failed to unmarshal: %v", err)
			w.WriteHeader(400)
			return
		}

		received = append(received, batch.Points...)
		w.WriteHeader(200)
	}))
	defer server.Close()

	adapter, err := newHTTPAdapter(EdgeSyncConfig{
		Endpoint:           server.URL,
		CompressionEnabled: true,
	})
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	// Prepare payload
	points := []Point{
		{Metric: "test1", Value: 1.0, Timestamp: time.Now().UnixNano()},
		{Metric: "test2", Value: 2.0, Timestamp: time.Now().UnixNano()},
	}

	batch := syncBatch{
		Version:   1,
		Timestamp: time.Now().UnixNano(),
		Points:    points,
	}
	data, _ := json.Marshal(batch)

	// Compress
	var buf []byte
	gw := gzip.NewWriter((*bytesBuffer)(&buf))
	gw.Write(data)
	gw.Close()

	// Send
	ctx := context.Background()
	if err := adapter.Send(ctx, buf); err != nil {
		t.Fatalf("Failed to send: %v", err)
	}

	if len(received) != 2 {
		t.Errorf("Expected 2 points, received %d", len(received))
	}
}

type bytesBuffer []byte

func (b *bytesBuffer) Write(p []byte) (n int, err error) {
	*b = append(*b, p...)
	return len(p), nil
}

func TestEdgeSyncManagerFiltering(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	config := DefaultEdgeSyncConfig()
	config.Enabled = true
	config.Endpoint = "http://localhost:9999"
	config.MetricFilter = []string{"allowed_metric"}
	config.TagFilter = map[string][]string{
		"env": {"prod", "staging"},
	}

	manager, err := NewEdgeSyncManager(db, config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	points := []Point{
		{Metric: "allowed_metric", Tags: map[string]string{"env": "prod"}, Value: 1.0},
		{Metric: "allowed_metric", Tags: map[string]string{"env": "dev"}, Value: 2.0},  // Wrong env
		{Metric: "blocked_metric", Tags: map[string]string{"env": "prod"}, Value: 3.0}, // Wrong metric
		{Metric: "allowed_metric", Tags: map[string]string{"env": "staging"}, Value: 4.0},
	}

	filtered := manager.filterPoints(points)

	if len(filtered) != 2 {
		t.Errorf("Expected 2 filtered points, got %d", len(filtered))
	}

	for _, p := range filtered {
		if p.Metric != "allowed_metric" {
			t.Errorf("Unexpected metric: %s", p.Metric)
		}
		env := p.Tags["env"]
		if env != "prod" && env != "staging" {
			t.Errorf("Unexpected env: %s", env)
		}
	}
}

func TestEdgeSyncManagerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow edge sync integration test in short mode")
	}
	var syncedPoints []Point
	var mu atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Add(1)

		var data []byte
		var err error

		if r.Header.Get("Content-Encoding") == "gzip" {
			gr, _ := gzip.NewReader(r.Body)
			data, err = io.ReadAll(gr)
			gr.Close()
		} else {
			data, err = io.ReadAll(r.Body)
		}

		if err != nil {
			w.WriteHeader(500)
			return
		}

		var batch syncBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			w.WriteHeader(400)
			return
		}

		syncedPoints = append(syncedPoints, batch.Points...)
		w.WriteHeader(200)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	config := DefaultEdgeSyncConfig()
	config.Enabled = true
	config.Endpoint = server.URL
	config.SyncInterval = 100 * time.Millisecond
	config.BatchSize = 100

	manager, err := NewEdgeSyncManager(db, config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Enqueue points
	for i := 0; i < 50; i++ {
		points := []Point{
			{
				Metric:    "test_metric",
				Tags:      map[string]string{"index": string(rune('0' + i%10))},
				Value:     float64(i),
				Timestamp: time.Now().UnixNano(),
			},
		}
		if err := manager.Enqueue(points); err != nil {
			t.Errorf("Failed to enqueue: %v", err)
		}
	}

	// Check queue stats
	stats := manager.Stats()
	if stats.QueuedPoints < 50 {
		t.Errorf("Expected at least 50 queued points, got %d", stats.QueuedPoints)
	}

	// Force flush
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := manager.Flush(ctx); err != nil {
		t.Errorf("Flush failed: %v", err)
	}

	// Check synced
	if len(syncedPoints) != 50 {
		t.Errorf("Expected 50 synced points, got %d", len(syncedPoints))
	}

	// Check stats
	stats = manager.Stats()
	if stats.PointsSynced != 50 {
		t.Errorf("Expected 50 points synced, got %d", stats.PointsSynced)
	}
}

func TestEdgeSyncStatus(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	config := DefaultEdgeSyncConfig()
	config.Enabled = true
	config.Endpoint = "http://localhost:9999"

	manager, err := NewEdgeSyncManager(db, config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	status := manager.Status()
	if status.Running {
		t.Error("Expected not running initially")
	}

	manager.Start()
	time.Sleep(50 * time.Millisecond)

	status = manager.Status()
	if !status.Running {
		t.Error("Expected running after start")
	}

	manager.Stop()
	time.Sleep(50 * time.Millisecond)

	status = manager.Status()
	if status.Running {
		t.Error("Expected not running after stop")
	}
}

func TestBandwidthLimiter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow bandwidth limiter test in short mode")
	}
	rl := newBandwidthLimiter(1000) // 1000 bytes per second

	ctx := context.Background()

	// Should pass immediately for small request
	start := time.Now()
	if err := rl.Wait(ctx, 100); err != nil {
		t.Errorf("Wait failed: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("Small request took too long: %v", elapsed)
	}

	// Should wait for large request
	start = time.Now()
	if err := rl.Wait(ctx, 2000); err != nil {
		t.Errorf("Wait failed: %v", err)
	}
	elapsed = time.Since(start)
	if elapsed < 500*time.Millisecond {
		t.Errorf("Large request should have waited longer: %v", elapsed)
	}
}

func TestBandwidthLimiterCancellation(t *testing.T) {
	rl := newBandwidthLimiter(100) // Very slow

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := rl.Wait(ctx, 10000) // Would take 100 seconds
	if err == nil {
		t.Error("Expected context deadline error")
	}
}

func TestLineProtocolConversion(t *testing.T) {
	p := Point{
		Metric:    "cpu_usage",
		Tags:      map[string]string{"host": "server1", "region": "us-west"},
		Value:     45.5,
		Timestamp: 1704067200000000000,
	}

	line := pointToLineProtocol(p)

	// Should contain metric name
	if !strings.Contains(line, "cpu_usage") {
		t.Error("Line should contain metric name")
	}

	// Should contain tags (sorted)
	if !strings.Contains(line, "host=server1") {
		t.Error("Line should contain host tag")
	}
	if !strings.Contains(line, "region=us-west") {
		t.Error("Line should contain region tag")
	}

	// Should contain value
	if !strings.Contains(line, "value=45.5") {
		t.Error("Line should contain value")
	}

	// Should contain timestamp
	if !strings.Contains(line, "1704067200000000000") {
		t.Error("Line should contain timestamp")
	}
}

func TestNewEdgeSyncManagerValidation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Test disabled config
	_, err = NewEdgeSyncManager(db, DefaultEdgeSyncConfig())
	if err == nil {
		t.Error("Expected error for disabled config")
	}

	// Test missing endpoint
	config := DefaultEdgeSyncConfig()
	config.Enabled = true
	_, err = NewEdgeSyncManager(db, config)
	if err == nil {
		t.Error("Expected error for missing endpoint")
	}

	// Test valid config
	config.Endpoint = "http://localhost:9999"
	manager, err := NewEdgeSyncManager(db, config)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if manager == nil {
		t.Error("Expected non-nil manager")
	}
}

func TestQueuePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "queue.json")

	// Create and populate queue
	q1, err := newSyncQueue(queuePath, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	points := []Point{
		{Metric: "persistent", Value: 42.0, Timestamp: 1234567890},
	}
	q1.Enqueue(points)
	q1.Close()

	// Verify file exists
	if _, err := os.Stat(queuePath); os.IsNotExist(err) {
		t.Error("Queue file should exist")
	}

	// Reopen and verify
	q2, err := newSyncQueue(queuePath, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}
	defer q2.Close()

	batch, err := q2.Dequeue(10)
	if err != nil {
		t.Fatalf("Failed to dequeue: %v", err)
	}

	if len(batch) != 1 {
		t.Errorf("Expected 1 point, got %d", len(batch))
	}
	if batch[0].Metric != "persistent" {
		t.Errorf("Expected 'persistent' metric, got %s", batch[0].Metric)
	}
	if batch[0].Value != 42.0 {
		t.Errorf("Expected value 42.0, got %f", batch[0].Value)
	}
}
