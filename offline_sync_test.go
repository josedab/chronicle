package chronicle

import (
	"testing"
	"time"
)

func TestVectorClock_IncrementMerge(t *testing.T) {
	vc1 := NewVectorClock()
	vc1.Increment("node-1")
	vc1.Increment("node-1")

	vc2 := NewVectorClock()
	vc2.Increment("node-2")

	vc1.Merge(vc2)

	encoded := vc1.Encode()
	decoded, err := DecodeVectorClock(encoded)
	if err != nil {
		t.Fatalf("DecodeVectorClock: %v", err)
	}
	_ = decoded
}

func TestVectorClock_HappensBefore(t *testing.T) {
	vc1 := NewVectorClock()
	vc1.Increment("node-1")

	vc2 := vc1.Copy()
	vc2.Increment("node-1")

	if !vc1.HappensBefore(vc2) {
		t.Errorf("expected vc1 happens before vc2")
	}
	if vc2.HappensBefore(vc1) {
		t.Errorf("expected vc2 does NOT happen before vc1")
	}

	t.Run("Concurrent", func(t *testing.T) {
		a := NewVectorClock()
		a.Increment("node-A")

		b := NewVectorClock()
		b.Increment("node-B")

		if a.HappensBefore(b) {
			t.Errorf("expected concurrent clocks, not happens-before")
		}
		if b.HappensBefore(a) {
			t.Errorf("expected concurrent clocks, not happens-before")
		}
		if !a.IsConcurrent(b) {
			t.Errorf("expected IsConcurrent=true")
		}
	})
}

func TestGCounter_IncrementMerge(t *testing.T) {
	gc1 := NewGCounter("node-1")
	gc1.Increment("node-1", 5)
	gc1.Increment("node-1", 3)

	gc2 := NewGCounter("node-2")
	gc2.Increment("node-2", 10)

	gc1.Merge(gc2)

	total := gc1.Value()
	if total != 18 {
		t.Errorf("expected total=18, got %d", total)
	}
}

func TestLWWRegister_SetMerge(t *testing.T) {
	r1 := NewLWWRegister()
	r1.Set("hello", 100, "node-1")

	r2 := NewLWWRegister()
	r2.Set("world", 200, "node-2")

	r1.Merge(r2)

	val, ts := r1.Get()
	if val != "world" {
		t.Errorf("expected 'world' (newer), got %v", val)
	}
	if ts != 200 {
		t.Errorf("expected ts=200, got %d", ts)
	}

	t.Run("SameTimestampTiebreak", func(t *testing.T) {
		a := NewLWWRegister()
		a.Set("v1", 100, "A")

		b := NewLWWRegister()
		b.Set("v2", 100, "B")

		a.Merge(b)
		val, _ := a.Get()
		// B > A lexicographically, so b's value wins.
		if val != "v2" {
			t.Errorf("expected 'v2' (higher nodeID), got %v", val)
		}
	})
}

func TestORSet_AddRemoveMerge(t *testing.T) {
	s1 := NewORSet()
	s1.Add("apple", "tag1")
	s1.Add("banana", "tag2")

	if !s1.Contains("apple") {
		t.Errorf("expected set to contain 'apple'")
	}

	s1.Remove("apple")
	if s1.Contains("apple") {
		t.Errorf("expected 'apple' to be removed")
	}

	s2 := NewORSet()
	s2.Add("cherry", "tag3")
	s2.Add("banana", "tag4")

	s1.Merge(s2)
	if !s1.Contains("cherry") {
		t.Errorf("expected 'cherry' after merge")
	}
	if !s1.Contains("banana") {
		t.Errorf("expected 'banana' after merge")
	}

	elements := s1.Elements()
	if len(elements) < 2 {
		t.Errorf("expected at least 2 elements, got %d", len(elements))
	}
}

func TestBloomFilter_AddContains(t *testing.T) {
	bf := NewBloomFilter(1024, 3)

	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))

	if !bf.Contains([]byte("hello")) {
		t.Errorf("expected bloom filter to contain 'hello'")
	}
	if !bf.Contains([]byte("world")) {
		t.Errorf("expected bloom filter to contain 'world'")
	}

	t.Run("SerializeDeserialize", func(t *testing.T) {
		encoded := bf.Encode()
		decoded, err := DecodeBloomFilter(encoded)
		if err != nil {
			t.Fatalf("DecodeBloomFilter: %v", err)
		}
		if !decoded.Contains([]byte("hello")) {
			t.Errorf("expected decoded filter to contain 'hello'")
		}
	})

	t.Run("Merge", func(t *testing.T) {
		bf2 := NewBloomFilter(1024, 3)
		bf2.Add([]byte("foo"))

		bf.Merge(bf2)
		if !bf.Contains([]byte("foo")) {
			t.Errorf("expected merged filter to contain 'foo'")
		}
	})
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	bf := NewBloomFilter(8192, 5)

	// Add 100 items.
	for i := 0; i < 100; i++ {
		bf.Add([]byte{byte(i), byte(i >> 8)})
	}

	// Check 1000 items that were NOT added (starting at 200).
	falsePositives := 0
	for i := 200; i < 1200; i++ {
		if bf.Contains([]byte{byte(i), byte(i >> 8)}) {
			falsePositives++
		}
	}

	rate := float64(falsePositives) / 1000.0
	// With 8192 bits, 5 hashes, 100 items, false positive rate should be very low.
	if rate > 0.1 {
		t.Errorf("false positive rate too high: %.2f%%", rate*100)
	}
}

func TestConflictResolver_Strategies(t *testing.T) {
	now := time.Now().UnixNano()

	local := &DeltaEntry{
		Point:         &Point{Metric: "cpu", Value: 10, Timestamp: now},
		OperationType: DeltaInsert,
		CreatedAt:     now,
	}
	remote := &DeltaEntry{
		Point:         &Point{Metric: "cpu", Value: 20, Timestamp: now},
		OperationType: DeltaInsert,
		CreatedAt:     now + 1,
	}

	t.Run("LastWriterWins", func(t *testing.T) {
		cr := NewConflictResolver(CRDTLastWriterWins)
		result := cr.Resolve(local, remote)
		if result.Point.Value != 20 {
			t.Errorf("expected remote value=20 (newer), got %f", result.Point.Value)
		}
	})

	t.Run("MaxValueWins", func(t *testing.T) {
		cr := NewConflictResolver(CRDTMaxValueWins)
		result := cr.Resolve(local, remote)
		if result.Point.Value != 20 {
			t.Errorf("expected max value=20, got %f", result.Point.Value)
		}
	})

	t.Run("MergeAll", func(t *testing.T) {
		cr := NewConflictResolver(CRDTMergeAll)
		result := cr.Resolve(local, remote)
		if result.Point.Value != 30 {
			t.Errorf("expected merged value=30 (10+20), got %f", result.Point.Value)
		}
	})
}

func TestDeltaState_RecordAndGet(t *testing.T) {
	cfg := DefaultOfflineSyncConfig()
	ds := NewDeltaState("node-1", &cfg)

	pt := &Point{Metric: "cpu", Value: 42, Timestamp: time.Now().UnixNano()}
	ds.RecordChange(pt)

	if !ds.HasChanges() {
		t.Errorf("expected HasChanges=true")
	}

	delta := ds.GetDelta(0)
	if delta == nil {
		t.Fatal("expected non-nil delta")
	}
	if len(delta.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(delta.Entries))
	}
	if delta.Entries[0].Point.Value != 42 {
		t.Errorf("expected value=42, got %f", delta.Entries[0].Point.Value)
	}
	if delta.SourceNode != "node-1" {
		t.Errorf("expected source node 'node-1', got %q", delta.SourceNode)
	}
}

func TestOfflineSyncManager_PrepareReceive(t *testing.T) {
	cfg := DefaultOfflineSyncConfig()
	cfg.NodeID = "node-A"
	mgr := NewOfflineSyncManager(cfg)

	// Record some writes.
	for i := 0; i < 5; i++ {
		mgr.RecordWrite(&Point{
			Metric:    "cpu",
			Value:     float64(i),
			Timestamp: time.Now().UnixNano(),
		})
	}

	delta, err := mgr.PrepareSyncDelta("node-B")
	if err != nil {
		t.Fatalf("PrepareSyncDelta: %v", err)
	}
	if len(delta.Entries) != 5 {
		t.Errorf("expected 5 entries, got %d", len(delta.Entries))
	}

	// Simulate receiving on another manager.
	cfg2 := DefaultOfflineSyncConfig()
	cfg2.NodeID = "node-B"
	mgr2 := NewOfflineSyncManager(cfg2)

	result, err := mgr2.ReceiveSyncDelta(delta)
	if err != nil {
		t.Fatalf("ReceiveSyncDelta: %v", err)
	}
	if result.PointsApplied != 5 {
		t.Errorf("expected 5 points applied, got %d", result.PointsApplied)
	}

	stats := mgr2.Stats()
	if stats.TotalSyncs != 1 {
		t.Errorf("expected 1 total sync, got %d", stats.TotalSyncs)
	}
}

func TestSchemaEvolution_MergeSchema(t *testing.T) {
	sem := NewSchemaEvolutionManager()

	cv := sem.CurrentVersion()
	if cv == nil {
		t.Fatal("expected non-nil current version")
	}
	if cv.Version != 1 {
		t.Errorf("expected initial version=1, got %d", cv.Version)
	}

	// Add a field.
	sv := sem.AddField("host", "string", "node-1")
	if sv.Version != 2 {
		t.Errorf("expected version=2, got %d", sv.Version)
	}
	if sv.Fields["host"] != "string" {
		t.Errorf("expected field 'host' of type 'string'")
	}

	// Merge a remote schema.
	remote := &SchemaVersion{
		Version: 2,
		Fields: map[string]string{
			"value":     "float64",
			"timestamp": "int64",
			"region":    "string",
		},
		NodeID: "node-2",
	}
	merged, err := sem.MergeSchema(remote)
	if err != nil {
		t.Fatalf("MergeSchema: %v", err)
	}
	if merged.Fields["region"] != "string" {
		t.Errorf("expected 'region' field after merge")
	}
	if merged.Fields["host"] != "string" {
		t.Errorf("expected 'host' field preserved after merge")
	}

	if !sem.IsCompatible(1) {
		t.Errorf("expected version 1 to be compatible")
	}
	if sem.IsCompatible(100) {
		t.Errorf("expected version 100 to be incompatible")
	}

	t.Run("ConflictingTypes", func(t *testing.T) {
		bad := &SchemaVersion{
			Version: 3,
			Fields:  map[string]string{"host": "int64"}, // conflicts with "string"
			NodeID:  "node-3",
		}
		_, err := sem.MergeSchema(bad)
		if err == nil {
			t.Errorf("expected error for conflicting field type")
		}
	})
}
