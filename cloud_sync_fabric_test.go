package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestCloudSyncFabricLifecycle(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	csf := NewCloudSyncFabric(db, DefaultCloudSyncFabricConfig())

	if err := csf.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Double start should fail
	if err := csf.Start(); err == nil {
		t.Fatal("expected error for double start")
	}

	stats := csf.Stats()
	if !stats.Running {
		t.Error("expected running=true")
	}

	if err := csf.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	stats = csf.Stats()
	if stats.Running {
		t.Error("expected running=false after stop")
	}
}

func TestCloudSyncFabricConnectors(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	csf := NewCloudSyncFabric(db, DefaultCloudSyncFabricConfig())

	// Register connector
	conn := NewHTTPCloudConnector(CloudConnectorConfig{
		Name:     "test-http",
		Type:     ConnectorHTTP,
		Endpoint: "http://localhost:9999",
		Enabled:  true,
	})

	if err := csf.RegisterConnector(conn); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Duplicate registration
	if err := csf.RegisterConnector(conn); err == nil {
		t.Fatal("expected error for duplicate connector")
	}

	// List connectors
	list := csf.ListConnectors()
	if len(list) != 1 {
		t.Fatalf("expected 1 connector, got %d", len(list))
	}
	if list[0].Name != "test-http" {
		t.Errorf("expected name 'test-http', got %q", list[0].Name)
	}

	// Unregister
	if err := csf.UnregisterConnector("test-http"); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	if len(csf.ListConnectors()) != 0 {
		t.Error("expected 0 connectors after unregister")
	}

	// Unregister nonexistent
	if err := csf.UnregisterConnector("nope"); err == nil {
		t.Fatal("expected error unregistering nonexistent")
	}
}

func TestCloudSyncFabricMaxConnectors(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCloudSyncFabricConfig()
	config.MaxConnectors = 2
	csf := NewCloudSyncFabric(db, config)

	csf.RegisterConnector(NewHTTPCloudConnector(CloudConnectorConfig{Name: "c1", Type: ConnectorHTTP}))
	csf.RegisterConnector(NewHTTPCloudConnector(CloudConnectorConfig{Name: "c2", Type: ConnectorHTTP}))

	err := csf.RegisterConnector(NewHTTPCloudConnector(CloudConnectorConfig{Name: "c3", Type: ConnectorHTTP}))
	if err == nil {
		t.Fatal("expected error exceeding max connectors")
	}
}

func TestCloudSyncFabricBatchQueue(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	csf := NewCloudSyncFabric(db, DefaultCloudSyncFabricConfig())

	// Enqueue batches with different priorities
	csf.EnqueueBatch(&SyncBatch{ID: "low", Priority: 3, Points: []Point{{Metric: "a"}}})
	csf.EnqueueBatch(&SyncBatch{ID: "high", Priority: 1, Points: []Point{{Metric: "b"}}})
	csf.EnqueueBatch(&SyncBatch{ID: "mid", Priority: 2, Points: []Point{{Metric: "c"}}})

	if csf.QueueSize() != 3 {
		t.Fatalf("expected queue size 3, got %d", csf.QueueSize())
	}
}

func TestHTTPCloudConnector(t *testing.T) {
	conn := NewHTTPCloudConnector(CloudConnectorConfig{
		Name:     "test",
		Type:     ConnectorHTTP,
		Endpoint: "http://localhost:8080",
	})

	if conn.Name() != "test" {
		t.Errorf("expected name 'test', got %q", conn.Name())
	}
	if conn.Type() != ConnectorHTTP {
		t.Errorf("expected type HTTP, got %v", conn.Type())
	}
	if conn.IsConnected() {
		t.Error("should not be connected initially")
	}

	// Connect
	if err := conn.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if !conn.IsConnected() {
		t.Error("should be connected after Connect")
	}

	// Push
	batch := &SyncBatch{
		ID:     "b1",
		Points: []Point{{Metric: "cpu", Value: 80}},
	}
	if err := conn.Push(context.Background(), batch); err != nil {
		t.Fatalf("push: %v", err)
	}
	if conn.PushCount() != 1 {
		t.Errorf("expected 1 pushed, got %d", conn.PushCount())
	}

	// Pull
	pulled, err := conn.Pull(context.Background(), time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if pulled == nil {
		t.Fatal("expected non-nil pull result")
	}

	// Manifest
	manifest, err := conn.GetManifest(context.Background())
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if manifest.NodeID != "test" {
		t.Errorf("expected node ID 'test', got %q", manifest.NodeID)
	}

	// Disconnect
	if err := conn.Disconnect(); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if conn.IsConnected() {
		t.Error("should not be connected after Disconnect")
	}

	// Push while disconnected
	if err := conn.Push(context.Background(), batch); err == nil {
		t.Fatal("expected error pushing while disconnected")
	}
}

func TestFabricMerkleTree(t *testing.T) {
	mt := NewFabricMerkleTree(16)

	if mt.LeafCount() != 0 {
		t.Errorf("expected 0 leaves, got %d", mt.LeafCount())
	}

	// Empty root should be deterministic
	root1 := mt.Root()
	if root1 == "" {
		t.Error("expected non-empty root for empty tree")
	}

	// Insert items
	mt.Insert("data1")
	mt.Insert("data2")
	mt.Insert("data3")

	if mt.LeafCount() != 3 {
		t.Errorf("expected 3 leaves, got %d", mt.LeafCount())
	}

	root2 := mt.Root()
	if root2 == root1 {
		t.Error("root should change after inserts")
	}

	// Verify
	if !mt.Verify("data1") {
		t.Error("should verify data1")
	}
	if mt.Verify("nonexistent") {
		t.Error("should not verify nonexistent data")
	}

	// Same data produces same root
	mt2 := NewFabricMerkleTree(16)
	mt2.Insert("data1")
	mt2.Insert("data2")
	mt2.Insert("data3")
	if mt2.Root() != mt.Root() {
		t.Error("same data should produce same root")
	}

	// Different data produces different root
	mt3 := NewFabricMerkleTree(16)
	mt3.Insert("data1")
	mt3.Insert("data2")
	mt3.Insert("data4")
	if mt3.Root() == mt.Root() {
		t.Error("different data should produce different root")
	}
}

func TestFabricMerkleTreeDiff(t *testing.T) {
	mt1 := NewFabricMerkleTree(16)
	mt1.Insert("a")
	mt1.Insert("b")
	mt1.Insert("c")

	mt2 := NewFabricMerkleTree(16)
	mt2.Insert("a")
	mt2.Insert("b")

	diffs := mt1.Diff(mt2)
	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d", len(diffs))
	}
	if diffs[0] != 2 {
		t.Errorf("expected diff at index 2, got %d", diffs[0])
	}
}

func TestFabricMerkleTreeReset(t *testing.T) {
	mt := NewFabricMerkleTree(16)
	mt.Insert("data")
	mt.Reset()

	if mt.LeafCount() != 0 {
		t.Errorf("expected 0 after reset, got %d", mt.LeafCount())
	}
}

func TestConflictResolver(t *testing.T) {
	tests := []struct {
		name     string
		strategy FabricConflictStrategy
		local    *Point
		remote   *Point
		expected float64
	}{
		{
			name:     "last write wins (remote newer)",
			strategy: FabricConflictLWW,
			local:    &Point{Metric: "m", Value: 10, Timestamp: 100},
			remote:   &Point{Metric: "m", Value: 20, Timestamp: 200},
			expected: 20,
		},
		{
			name:     "last write wins (local newer)",
			strategy: FabricConflictLWW,
			local:    &Point{Metric: "m", Value: 10, Timestamp: 200},
			remote:   &Point{Metric: "m", Value: 20, Timestamp: 100},
			expected: 10,
		},
		{
			name:     "highest value",
			strategy: FabricConflictHighest,
			local:    &Point{Metric: "m", Value: 10, Timestamp: 100},
			remote:   &Point{Metric: "m", Value: 20, Timestamp: 100},
			expected: 20,
		},
		{
			name:     "lowest value",
			strategy: FabricConflictLowest,
			local:    &Point{Metric: "m", Value: 10, Timestamp: 100},
			remote:   &Point{Metric: "m", Value: 20, Timestamp: 100},
			expected: 10,
		},
		{
			name:     "merge (average)",
			strategy: FabricConflictMerge,
			local:    &Point{Metric: "m", Value: 10, Timestamp: 100, Tags: map[string]string{"a": "1"}},
			remote:   &Point{Metric: "m", Value: 30, Timestamp: 200, Tags: map[string]string{"b": "2"}},
			expected: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := NewFabricConflictResolver(tt.strategy)
			result := cr.Resolve(tt.local, tt.remote)
			if result.Value != tt.expected {
				t.Errorf("expected value %f, got %f", tt.expected, result.Value)
			}
			stats := cr.Stats()
			if stats.Conflicts != 1 || stats.Resolved != 1 {
				t.Error("stats mismatch")
			}
		})
	}
}

func TestBandwidthEstimator(t *testing.T) {
	be := NewBandwidthEstimator(10)

	// No samples
	if be.EstimatedBandwidth() != 0 {
		t.Error("expected 0 bandwidth with no samples")
	}

	// Record samples
	be.Record(1000000, time.Second) // 1MB/s
	be.Record(2000000, time.Second) // 2MB/s
	be.Record(3000000, time.Second) // 3MB/s

	bw := be.EstimatedBandwidth()
	if bw <= 0 {
		t.Errorf("expected positive bandwidth, got %f", bw)
	}

	if be.SampleCount() != 3 {
		t.Errorf("expected 3 samples, got %d", be.SampleCount())
	}

	// Optimal batch size
	batchSize := be.OptimalBatchSize(1000)
	if batchSize <= 0 {
		t.Errorf("expected positive batch size, got %d", batchSize)
	}

	// Zero duration should be ignored
	be.Record(1000, 0)
	if be.SampleCount() != 3 {
		t.Error("zero duration record should be ignored")
	}
}

func TestBandwidthEstimatorEviction(t *testing.T) {
	be := NewBandwidthEstimator(3)
	be.Record(1000, time.Second)
	be.Record(2000, time.Second)
	be.Record(3000, time.Second)
	be.Record(4000, time.Second)

	if be.SampleCount() != 3 {
		t.Errorf("expected 3 samples after eviction, got %d", be.SampleCount())
	}
}

func TestGeoTopology(t *testing.T) {
	gt := NewGeoTopology()

	gt.AddNode(&GeoNode{ID: "us-east", Region: "us-east-1", Lat: 39.0, Lon: -77.0, Active: true})
	gt.AddNode(&GeoNode{ID: "eu-west", Region: "eu-west-1", Lat: 53.0, Lon: -6.0, Active: true})
	gt.AddNode(&GeoNode{ID: "ap-south", Region: "ap-south-1", Lat: 19.0, Lon: 73.0, Active: true})
	gt.AddNode(&GeoNode{ID: "inactive", Region: "local", Lat: 0, Lon: 0, Active: false})

	if gt.NodeCount() != 4 {
		t.Errorf("expected 4 nodes, got %d", gt.NodeCount())
	}

	// Nearest to US East
	nearest := gt.NearestNodes(39.0, -77.0, 3)
	if len(nearest) != 3 {
		t.Fatalf("expected 3 nearest (active only), got %d", len(nearest))
	}
	if nearest[0].ID != "us-east" {
		t.Errorf("expected nearest to be 'us-east', got %q", nearest[0].ID)
	}

	// Remove node
	gt.RemoveNode("inactive")
	if gt.NodeCount() != 3 {
		t.Errorf("expected 3 nodes after remove, got %d", gt.NodeCount())
	}

	// List nodes
	all := gt.ListNodes()
	if len(all) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(all))
	}
}

func TestCloudSyncFabricMerkle(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	csf := NewCloudSyncFabric(db, DefaultCloudSyncFabricConfig())

	csf.UpdateMerkleTree("cpu", 1000, 80.5)
	csf.UpdateMerkleTree("mem", 1000, 4096.0)

	root := csf.GetMerkleRoot()
	if root == "" {
		t.Error("expected non-empty Merkle root")
	}

	// Same data, no drift
	if csf.DetectDrift(root) {
		t.Error("should not detect drift with same root")
	}

	// Different root = drift
	if !csf.DetectDrift("different-root") {
		t.Error("should detect drift with different root")
	}
}

func TestCloudSyncFabricStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	csf := NewCloudSyncFabric(db, DefaultCloudSyncFabricConfig())
	stats := csf.Stats()

	if stats.ConnectorCount != 0 {
		t.Errorf("expected 0 connectors, got %d", stats.ConnectorCount)
	}
	if stats.QueueSize != 0 {
		t.Errorf("expected 0 queue size, got %d", stats.QueueSize)
	}
	if stats.Running {
		t.Error("should not be running initially")
	}
}
