package chronicle

import (
	"os"
	"testing"
	"time"
)

func TestVectorClock(t *testing.T) {
	t.Run("Increment", func(t *testing.T) {
		vc := NewVectorClock()
		vc.Increment("node1")
		vc.Increment("node1")
		vc.Increment("node2")

		if vc.Get("node1") != 2 {
			t.Errorf("Expected node1=2, got %d", vc.Get("node1"))
		}
		if vc.Get("node2") != 1 {
			t.Errorf("Expected node2=1, got %d", vc.Get("node2"))
		}
	})

	t.Run("Merge", func(t *testing.T) {
		vc1 := NewVectorClock()
		vc1.Increment("node1")
		vc1.Increment("node1")

		vc2 := NewVectorClock()
		vc2.Increment("node1")
		vc2.Increment("node2")
		vc2.Increment("node2")
		vc2.Increment("node2")

		vc1.Merge(vc2)

		if vc1.Get("node1") != 2 {
			t.Errorf("Expected node1=2 after merge, got %d", vc1.Get("node1"))
		}
		if vc1.Get("node2") != 3 {
			t.Errorf("Expected node2=3 after merge, got %d", vc1.Get("node2"))
		}
	})

	t.Run("Compare", func(t *testing.T) {
		vc1 := NewVectorClock()
		vc1.clocks["node1"] = 2
		vc1.clocks["node2"] = 1

		vc2 := NewVectorClock()
		vc2.clocks["node1"] = 3
		vc2.clocks["node2"] = 1

		// vc1 < vc2
		if vc1.Compare(vc2) != -1 {
			t.Error("Expected vc1 < vc2")
		}

		// vc2 > vc1
		if vc2.Compare(vc1) != 1 {
			t.Error("Expected vc2 > vc1")
		}

		// Concurrent
		vc3 := NewVectorClock()
		vc3.clocks["node1"] = 3
		vc3.clocks["node2"] = 0

		vc4 := NewVectorClock()
		vc4.clocks["node1"] = 2
		vc4.clocks["node2"] = 1

		if vc3.Compare(vc4) != 0 {
			t.Error("Expected concurrent (0)")
		}
	})

	t.Run("Clone", func(t *testing.T) {
		vc := NewVectorClock()
		vc.Increment("node1")
		vc.Increment("node2")

		clone := vc.Clone()
		clone.Increment("node1")

		if vc.Get("node1") != 1 {
			t.Error("Original should not be modified")
		}
		if clone.Get("node1") != 2 {
			t.Error("Clone should be incremented")
		}
	})

	t.Run("Serialize", func(t *testing.T) {
		vc := NewVectorClock()
		vc.clocks["node1"] = 5
		vc.clocks["node2"] = 3

		data, err := vc.Serialize()
		if err != nil {
			t.Fatalf("Serialize failed: %v", err)
		}

		vc2 := NewVectorClock()
		if err := vc2.Deserialize(data); err != nil {
			t.Fatalf("Deserialize failed: %v", err)
		}

		if vc2.Get("node1") != 5 || vc2.Get("node2") != 3 {
			t.Error("Deserialized values don't match")
		}
	})
}

func TestDeltaEncoder(t *testing.T) {
	encoder := NewDeltaEncoder()

	// First value is stored as-is
	ts1, val1 := encoder.Encode(1000, 100.0)
	if ts1 != 1000 || val1 != 100.0 {
		t.Errorf("First encoding wrong: got ts=%d, val=%f", ts1, val1)
	}

	// Second value is delta
	ts2, val2 := encoder.Encode(1100, 105.0)
	if ts2 != 100 || val2 != 5.0 {
		t.Errorf("Delta encoding wrong: got ts=%d, val=%f", ts2, val2)
	}

	// Third value
	ts3, val3 := encoder.Encode(1200, 102.0)
	if ts3 != 100 || val3 != -3.0 {
		t.Errorf("Delta encoding wrong: got ts=%d, val=%f", ts3, val3)
	}
}

func TestDeltaDecoder(t *testing.T) {
	decoder := NewDeltaDecoder()

	// First value
	ts1, val1 := decoder.Decode(1000, 100.0)
	if ts1 != 1000 || val1 != 100.0 {
		t.Errorf("First decoding wrong: got ts=%d, val=%f", ts1, val1)
	}

	// Second value (delta)
	ts2, val2 := decoder.Decode(100, 5.0)
	if ts2 != 1100 || val2 != 105.0 {
		t.Errorf("Delta decoding wrong: got ts=%d, val=%f", ts2, val2)
	}

	// Third value (delta)
	ts3, val3 := decoder.Decode(100, -3.0)
	if ts3 != 1200 || val3 != 102.0 {
		t.Errorf("Delta decoding wrong: got ts=%d, val=%f", ts3, val3)
	}
}

func TestVarintEncoder(t *testing.T) {
	encoder := NewVarintEncoder()

	testCases := []int64{0, 1, 127, 128, 255, 1000, 1000000, -1, -128, -1000}

	for _, tc := range testCases {
		encoded := encoder.EncodeInt64(tc)
		decoded, _ := encoder.DecodeInt64(encoded)
		if decoded != tc {
			t.Errorf("Varint encode/decode failed for %d: got %d", tc, decoded)
		}
	}
}

func TestDeltaSyncManager_DeltaEncoding(t *testing.T) {
	config := DefaultDeltaSyncConfig()
	config.Enabled = true
	config.CloudEndpoint = "http://localhost:8086"
	config.DeviceID = "test-device"
	config.DeltaCompressionEnabled = true

	// Create temp DB
	path := "test_delta_sync.db"
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	db, err := Open(path, Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	manager, err := NewDeltaSyncManager(db, config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Test delta encoding
	points := []Point{
		{Metric: "test", Value: 100, Timestamp: 1000, Tags: map[string]string{}},
		{Metric: "test", Value: 105, Timestamp: 1100, Tags: map[string]string{}},
		{Metric: "test", Value: 102, Timestamp: 1200, Tags: map[string]string{}},
	}

	encoded := manager.deltaEncode(points)

	// First point unchanged
	if encoded[0].Value != 100 || encoded[0].Timestamp != 1000 {
		t.Error("First point should be unchanged")
	}

	// Second point is delta
	if encoded[1].Value != 5 || encoded[1].Timestamp != 100 {
		t.Errorf("Second point wrong: value=%f, ts=%d", encoded[1].Value, encoded[1].Timestamp)
	}

	// Third point is delta from second
	if encoded[2].Value != -3 || encoded[2].Timestamp != 100 {
		t.Errorf("Third point wrong: value=%f, ts=%d", encoded[2].Value, encoded[2].Timestamp)
	}

	// Decode and verify
	decoded := manager.deltaDecode(encoded)

	for i := range points {
		if decoded[i].Value != points[i].Value {
			t.Errorf("Value mismatch at %d: expected %f, got %f", i, points[i].Value, decoded[i].Value)
		}
		if decoded[i].Timestamp != points[i].Timestamp {
			t.Errorf("Timestamp mismatch at %d: expected %d, got %d", i, points[i].Timestamp, decoded[i].Timestamp)
		}
	}
}

func TestDeltaSyncManager_Batching(t *testing.T) {
	config := DefaultDeltaSyncConfig()
	config.Enabled = true
	config.CloudEndpoint = "http://localhost:8086"
	config.DeviceID = "test-device"
	config.BatchSize = 100

	path := "test_delta_batch.db"
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	db, err := Open(path, Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	manager, err := NewDeltaSyncManager(db, config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Create 250 points - should result in 3 batches
	points := make([]Point, 250)
	for i := range points {
		points[i] = Point{
			Metric:    "test",
			Value:     float64(i),
			Timestamp: int64(i * 1000),
			Tags:      map[string]string{},
		}
	}

	batches := manager.splitIntoBatches(points)

	if len(batches) != 3 {
		t.Errorf("Expected 3 batches, got %d", len(batches))
	}

	// Verify batch sizes
	if len(batches[0].Points) != 100 {
		t.Errorf("First batch should have 100 points, got %d", len(batches[0].Points))
	}
	if len(batches[1].Points) != 100 {
		t.Errorf("Second batch should have 100 points, got %d", len(batches[1].Points))
	}
	if len(batches[2].Points) != 50 {
		t.Errorf("Third batch should have 50 points, got %d", len(batches[2].Points))
	}

	// Verify each batch has a unique ID
	ids := make(map[string]bool)
	for _, b := range batches {
		if ids[b.ID] {
			t.Error("Duplicate batch ID")
		}
		ids[b.ID] = true
	}
}

func TestDeltaSyncManager_TagFilter(t *testing.T) {
	config := DefaultDeltaSyncConfig()
	config.Enabled = true
	config.CloudEndpoint = "http://localhost:8086"
	config.DeviceID = "test-device"
	config.FilterTags = map[string][]string{
		"env":    {"prod", "staging"},
		"region": {"us-east"},
	}

	path := "test_delta_filter.db"
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	db, err := Open(path, Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	manager, err := NewDeltaSyncManager(db, config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	testCases := []struct {
		point   Point
		matches bool
	}{
		{Point{Tags: map[string]string{"env": "prod", "region": "us-east"}}, true},
		{Point{Tags: map[string]string{"env": "staging", "region": "us-east"}}, true},
		{Point{Tags: map[string]string{"env": "dev", "region": "us-east"}}, false},
		{Point{Tags: map[string]string{"env": "prod", "region": "eu-west"}}, false},
		{Point{Tags: map[string]string{"env": "prod"}}, false}, // Missing region
	}

	for i, tc := range testCases {
		result := manager.matchesTagFilter(tc.point)
		if result != tc.matches {
			t.Errorf("Case %d: expected %v, got %v", i, tc.matches, result)
		}
	}
}

func TestDeltaSyncManager_Checksum(t *testing.T) {
	config := DefaultDeltaSyncConfig()
	config.Enabled = true
	config.CloudEndpoint = "http://localhost:8086"
	config.DeviceID = "test-device"
	config.EnableChecksum = true

	path := "test_delta_checksum.db"
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	db, err := Open(path, Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	manager, err := NewDeltaSyncManager(db, config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	batch := DeltaBatch{
		Points: []Point{
			{Metric: "test", Value: 1.0, Timestamp: 1000, Tags: map[string]string{}},
			{Metric: "test", Value: 2.0, Timestamp: 2000, Tags: map[string]string{}},
		},
	}

	checksum1 := manager.calculateBatchChecksum(batch)
	checksum2 := manager.calculateBatchChecksum(batch)

	if checksum1 != checksum2 {
		t.Error("Checksums should be deterministic")
	}

	// Modify and verify different checksum
	batch.Points[0].Value = 99.0
	checksum3 := manager.calculateBatchChecksum(batch)

	if checksum1 == checksum3 {
		t.Error("Checksum should change when data changes")
	}
}

func TestDeltaSyncStats(t *testing.T) {
	config := DefaultDeltaSyncConfig()
	config.Enabled = true
	config.CloudEndpoint = "http://localhost:8086"
	config.DeviceID = "test-device"

	path := "test_delta_stats.db"
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	db, err := Open(path, Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	manager, err := NewDeltaSyncManager(db, config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	stats := manager.GetStats()

	if stats.SyncAttempts != 0 {
		t.Error("Initial sync attempts should be 0")
	}
	if stats.PointsSynced != 0 {
		t.Error("Initial points synced should be 0")
	}
}
