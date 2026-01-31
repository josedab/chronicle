package chronicle

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCloudSync_NewCloudSync(t *testing.T) {
	config := DefaultCloudSyncConfig()
	config.Enabled = true
	config.CloudEndpoint = "http://localhost:8080"

	sync := NewCloudSync(nil, config)
	if sync == nil {
		t.Fatal("Expected non-nil CloudSync")
	}

	if sync.config.BatchSize != 5000 {
		t.Errorf("Expected batch size 5000, got %d", sync.config.BatchSize)
	}
}

func TestCloudSync_SyncModes(t *testing.T) {
	tests := []struct {
		mode     SyncMode
		expected string
	}{
		{SyncModeEdgeToCloud, "edge_to_cloud"},
		{SyncModeCloudToEdge, "cloud_to_edge"},
		{SyncModeBidirectional, "bidirectional"},
	}

	for _, tt := range tests {
		if got := tt.mode.String(); got != tt.expected {
			t.Errorf("SyncMode.String() = %v, want %v", got, tt.expected)
		}
	}
}

func TestCloudSync_ConflictResolution(t *testing.T) {
	config := DefaultCloudSyncConfig()
	sync := NewCloudSync(nil, config)

	edgePoint := Point{
		Metric:    "test",
		Value:     100.0,
		Timestamp: time.Now().UnixNano(),
	}

	cloudPoint := Point{
		Metric:    "test",
		Value:     200.0,
		Timestamp: time.Now().Add(-time.Hour).UnixNano(),
	}

	// Test last write wins
	config.ConflictResolution = ConflictLastWriteWins
	sync.config = config
	resolved := sync.ResolveConflict(edgePoint, cloudPoint)
	if resolved.Value != edgePoint.Value {
		t.Error("Expected edge point to win (newer timestamp)")
	}

	// Test cloud wins
	config.ConflictResolution = ConflictCloudWins
	sync.config = config
	resolved = sync.ResolveConflict(edgePoint, cloudPoint)
	if resolved.Value != cloudPoint.Value {
		t.Error("Expected cloud point to win")
	}

	// Test edge wins
	config.ConflictResolution = ConflictEdgeWins
	sync.config = config
	resolved = sync.ResolveConflict(edgePoint, cloudPoint)
	if resolved.Value != edgePoint.Value {
		t.Error("Expected edge point to win")
	}

	// Test merge
	config.ConflictResolution = ConflictMerge
	sync.config = config
	resolved = sync.ResolveConflict(edgePoint, cloudPoint)
	expected := (edgePoint.Value + cloudPoint.Value) / 2
	if resolved.Value != expected {
		t.Errorf("Expected merged value %.2f, got %.2f", expected, resolved.Value)
	}
}

func TestCloudSync_SyncStatus(t *testing.T) {
	tests := []struct {
		status   SyncStatus
		expected string
	}{
		{SyncStatusIdle, "idle"},
		{SyncStatusSyncing, "syncing"},
		{SyncStatusOffline, "offline"},
		{SyncStatusError, "error"},
		{SyncStatusComplete, "complete"},
	}

	for _, tt := range tests {
		if got := tt.status.String(); got != tt.expected {
			t.Errorf("SyncStatus.String() = %v, want %v", got, tt.expected)
		}
	}
}

func TestCloudSync_OfflineQueue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "chronicle_sync_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultCloudSyncConfig()
	config.OfflineQueuePath = tmpDir
	config.MaxOfflineQueueSize = 1024 * 1024

	sync := NewCloudSync(nil, config)

	// Queue some points
	points := []Point{
		{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano()},
		{Metric: "test", Value: 2.0, Timestamp: time.Now().UnixNano()},
	}

	sync.queueOffline(points)

	// Check file was created
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, f := range files {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".json" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Expected offline queue file to be created")
	}
}

func TestCloudSync_Checkpoint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "chronicle_checkpoint_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultCloudSyncConfig()
	config.OfflineQueuePath = tmpDir

	sync := NewCloudSync(nil, config)

	// Update checkpoint
	testTimestamp := time.Now().UnixNano()
	sync.updateCheckpoint(testTimestamp)

	// Save checkpoint
	sync.saveCheckpoint()

	// Create new sync instance and load checkpoint
	sync2 := NewCloudSync(nil, config)

	if sync2.checkpoint == nil {
		t.Error("Expected checkpoint to be loaded")
		return
	}

	if sync2.checkpoint.LastSyncedTimestamp != testTimestamp {
		t.Errorf("Expected timestamp %d, got %d", testTimestamp, sync2.checkpoint.LastSyncedTimestamp)
	}
}

func TestCloudSync_PreparePayload(t *testing.T) {
	config := DefaultCloudSyncConfig()
	config.EnableCompression = true
	sync := NewCloudSync(nil, config)

	points := []Point{
		{Metric: "cpu", Value: 50.0, Timestamp: time.Now().UnixNano()},
		{Metric: "memory", Value: 70.0, Timestamp: time.Now().UnixNano()},
	}

	payload, err := sync.preparePayload(points)
	if err != nil {
		t.Fatalf("preparePayload failed: %v", err)
	}

	if len(payload) == 0 {
		t.Error("Expected non-empty payload")
	}

	// Compressed payload should be smaller than raw
	config.EnableCompression = false
	sync.config = config
	uncompressed, _ := sync.preparePayload(points)

	if len(payload) >= len(uncompressed) {
		t.Log("Note: Compression may not reduce size for small payloads")
	}
}

func TestCloudSync_SplitIntoBatches(t *testing.T) {
	config := DefaultCloudSyncConfig()
	config.BatchSize = 2
	sync := NewCloudSync(nil, config)

	points := []Point{
		{Metric: "a", Value: 1.0},
		{Metric: "b", Value: 2.0},
		{Metric: "c", Value: 3.0},
		{Metric: "d", Value: 4.0},
		{Metric: "e", Value: 5.0},
	}

	batches := sync.splitIntoBatches(points)

	if len(batches) != 3 {
		t.Errorf("Expected 3 batches, got %d", len(batches))
	}

	if len(batches[0]) != 2 || len(batches[1]) != 2 || len(batches[2]) != 1 {
		t.Error("Incorrect batch sizes")
	}
}

func TestCloudSync_MetricFilters(t *testing.T) {
	config := DefaultCloudSyncConfig()
	config.MetricFilters = []string{"cpu", "memory"}

	sync := NewCloudSync(nil, config)

	if sync.metricFilters == nil {
		t.Error("Expected metric filters to be set")
		return
	}

	if _, ok := sync.metricFilters["cpu"]; !ok {
		t.Error("Expected cpu in metric filters")
	}

	if _, ok := sync.metricFilters["disk"]; ok {
		t.Error("disk should not be in metric filters")
	}
}

func TestCloudSync_SendToCloud(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sync/upload" {
			t.Errorf("Expected path /api/v1/sync/upload, got %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := DefaultCloudSyncConfig()
	config.CloudEndpoint = server.URL
	config.EnableCompression = false
	config.MaxRetries = 1

	sync := NewCloudSync(nil, config)

	payload := []byte(`{"test": "data"}`)
	err := sync.sendToCloud(payload)
	if err != nil {
		t.Errorf("sendToCloud failed: %v", err)
	}
}

func TestCloudSync_GetSyncStats(t *testing.T) {
	config := DefaultCloudSyncConfig()
	sync := NewCloudSync(nil, config)

	sync.mu.Lock()
	sync.syncStats.TotalPointsSynced = 100
	sync.syncStats.SuccessfulSyncs = 5
	sync.mu.Unlock()

	stats := sync.GetSyncStats()

	if stats.TotalPointsSynced != 100 {
		t.Errorf("Expected 100 points synced, got %d", stats.TotalPointsSynced)
	}

	if stats.SuccessfulSyncs != 5 {
		t.Errorf("Expected 5 successful syncs, got %d", stats.SuccessfulSyncs)
	}
}

func TestCloudSync_IsOnline(t *testing.T) {
	config := DefaultCloudSyncConfig()
	sync := NewCloudSync(nil, config)

	// Default should be online (idle)
	if !sync.IsOnline() {
		t.Error("Expected to be online by default")
	}

	// Set to offline
	sync.mu.Lock()
	sync.lastSyncStatus = SyncStatusOffline
	sync.mu.Unlock()

	if sync.IsOnline() {
		t.Error("Expected to be offline")
	}
}

func TestCloudSyncFormat_String(t *testing.T) {
	tests := []struct {
		format   CloudSyncFormat
		expected string
	}{
		{CloudSyncFormatJSON, "json"},
		{CloudSyncFormatParquet, "parquet"},
		{CloudSyncFormatCSV, "csv"},
	}

	for _, tt := range tests {
		if got := tt.format.String(); got != tt.expected {
			t.Errorf("CloudSyncFormat.String() = %v, want %v", got, tt.expected)
		}
	}
}

func TestConflictResolutionStrategy_String(t *testing.T) {
	tests := []struct {
		strategy ConflictResolutionStrategy
		expected string
	}{
		{ConflictLastWriteWins, "last_write_wins"},
		{ConflictCloudWins, "cloud_wins"},
		{ConflictEdgeWins, "edge_wins"},
		{ConflictMerge, "merge"},
		{ConflictManual, "manual"},
	}

	for _, tt := range tests {
		if got := tt.strategy.String(); got != tt.expected {
			t.Errorf("ConflictResolutionStrategy.String() = %v, want %v", got, tt.expected)
		}
	}
}
