package chronicle

import (
	"testing"
	"time"
)

func TestGCounterIncrement(t *testing.T) {
	gc := NewGCounter("node-1")
	gc.Increment("node-1", 5)
	gc.Increment("node-1", 3)

	if gc.Value() != 8 {
		t.Errorf("Expected 8, got %d", gc.Value())
	}
}

func TestGCounterMerge(t *testing.T) {
	gc1 := NewGCounter("node-1")
	gc1.Increment("node-1", 10)

	gc2 := NewGCounter("node-2")
	gc2.Increment("node-2", 7)

	gc1.Merge(gc2)

	if gc1.Value() != 17 {
		t.Errorf("Expected 17 after merge, got %d", gc1.Value())
	}
}

func TestGCounterMergeTakesMax(t *testing.T) {
	gc1 := NewGCounter("node-1")
	gc1.Increment("node-1", 10)

	gc2 := NewGCounter("node-2")
	gc2.Increment("node-1", 5) // Lower value for same node

	gc1.Merge(gc2)

	// Merge takes max per node, so node-1 should remain 10.
	if gc1.Value() != 10 {
		t.Errorf("Expected 10 (max of 10 and 5), got %d", gc1.Value())
	}
}

func TestLWWRegisterSetGet(t *testing.T) {
	r := NewLWWRegister()
	r.Set("hello", 100, "node-1")

	val, ts := r.Get()
	if val != "hello" {
		t.Errorf("Expected 'hello', got %v", val)
	}
	if ts != 100 {
		t.Errorf("Expected timestamp 100, got %d", ts)
	}
}

func TestLWWRegisterNewerWins(t *testing.T) {
	r := NewLWWRegister()
	r.Set("old", 100, "node-1")
	r.Set("new", 200, "node-2")

	val, _ := r.Get()
	if val != "new" {
		t.Errorf("Expected 'new' (newer timestamp), got %v", val)
	}
}

func TestLWWRegisterOlderIgnored(t *testing.T) {
	r := NewLWWRegister()
	r.Set("latest", 200, "node-1")
	r.Set("stale", 100, "node-2")

	val, _ := r.Get()
	if val != "latest" {
		t.Errorf("Expected 'latest' (not overwritten by older), got %v", val)
	}
}

func TestLWWRegisterMerge(t *testing.T) {
	r1 := NewLWWRegister()
	r1.Set("local", 100, "node-1")

	r2 := NewLWWRegister()
	r2.Set("remote", 200, "node-2")

	r1.Merge(r2)

	val, _ := r1.Get()
	if val != "remote" {
		t.Errorf("Expected 'remote' after merge, got %v", val)
	}
}

func TestORSetAddContains(t *testing.T) {
	s := NewORSet()
	s.Add("apple", "tag1")
	s.Add("banana", "tag2")

	if !s.Contains("apple") {
		t.Error("Expected ORSet to contain 'apple'")
	}
	if !s.Contains("banana") {
		t.Error("Expected ORSet to contain 'banana'")
	}
	if s.Contains("cherry") {
		t.Error("Expected ORSet to not contain 'cherry'")
	}
}

func TestORSetRemove(t *testing.T) {
	s := NewORSet()
	s.Add("apple", "tag1")
	s.Remove("apple")

	if s.Contains("apple") {
		t.Error("Expected ORSet to not contain 'apple' after removal")
	}
}

func TestORSetElements(t *testing.T) {
	s := NewORSet()
	s.Add("banana", "t1")
	s.Add("apple", "t2")

	elements := s.Elements()
	if len(elements) != 2 {
		t.Errorf("Expected 2 elements, got %d", len(elements))
	}
}

func TestORSetMerge(t *testing.T) {
	s1 := NewORSet()
	s1.Add("a", "t1")

	s2 := NewORSet()
	s2.Add("b", "t2")

	s1.Merge(s2)

	if !s1.Contains("a") || !s1.Contains("b") {
		t.Error("Expected merged set to contain both 'a' and 'b'")
	}
}

func TestBloomFilterAddContains(t *testing.T) {
	bf := NewBloomFilter(1024, 3)
	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))

	if !bf.Contains([]byte("hello")) {
		t.Error("Expected bloom filter to contain 'hello'")
	}
	if !bf.Contains([]byte("world")) {
		t.Error("Expected bloom filter to contain 'world'")
	}
}

func TestBloomFilterNoFalseNegatives(t *testing.T) {
	bf := NewBloomFilter(4096, 3)
	items := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, item := range items {
		bf.Add([]byte(item))
	}

	for _, item := range items {
		if !bf.Contains([]byte(item)) {
			t.Errorf("False negative for '%s'", item)
		}
	}
}

func TestBloomFilterEncodeDecode(t *testing.T) {
	bf := NewBloomFilter(1024, 3)
	bf.Add([]byte("test"))

	encoded := bf.Encode()
	decoded, err := DecodeBloomFilter(encoded)
	if err != nil {
		t.Fatalf("DecodeBloomFilter failed: %v", err)
	}

	if !decoded.Contains([]byte("test")) {
		t.Error("Decoded filter should contain 'test'")
	}
}

func TestBloomFilterMerge(t *testing.T) {
	bf1 := NewBloomFilter(1024, 3)
	bf1.Add([]byte("a"))

	bf2 := NewBloomFilter(1024, 3)
	bf2.Add([]byte("b"))

	bf1.Merge(bf2)

	if !bf1.Contains([]byte("a")) || !bf1.Contains([]byte("b")) {
		t.Error("Merged filter should contain both items")
	}
}

func TestDeltaStateRecordAndGetDelta(t *testing.T) {
	cfg := DefaultOfflineSyncConfig()
	config := &cfg
	ds := NewDeltaState("node-1", config)

	now := time.Now()
	p := &Point{
		Metric:    "cpu",
		Value:     42.0,
		Timestamp: now.UnixNano(),
		Tags:      map[string]string{"host": "a"},
	}
	ds.RecordChange(p)

	if !ds.HasChanges() {
		t.Error("Expected HasChanges to be true")
	}

	delta := ds.GetDelta(0)
	if delta == nil {
		t.Fatal("GetDelta returned nil")
	}
	if len(delta.Entries) == 0 {
		t.Error("Expected non-empty delta entries")
	}
}

func TestDeltaStateApplyDelta(t *testing.T) {
	cfg := DefaultOfflineSyncConfig()
	config := &cfg
	ds1 := NewDeltaState("node-1", config)
	ds2 := NewDeltaState("node-2", config)

	p := &Point{
		Metric:    "mem",
		Value:     80.0,
		Timestamp: time.Now().UnixNano(),
	}
	ds1.RecordChange(p)

	delta := ds1.GetDelta(0)
	conflicts, err := ds2.ApplyDelta(delta)
	if err != nil {
		t.Fatalf("ApplyDelta failed: %v", err)
	}
	if len(conflicts) != 0 {
		t.Errorf("Expected no conflicts, got %d", len(conflicts))
	}
}

func TestConflictResolverLastWriterWins(t *testing.T) {
	cr := NewConflictResolver(CRDTLastWriterWins)

	local := &DeltaEntry{Point: &Point{Metric: "cpu", Value: 10}, CreatedAt: 100}
	remote := &DeltaEntry{Point: &Point{Metric: "cpu", Value: 20}, CreatedAt: 200}

	result := cr.Resolve(local, remote)
	if result.Point.Value != 20 {
		t.Errorf("Expected remote value 20 (newer), got %v", result.Point.Value)
	}
}

func TestConflictResolverMaxValueWins(t *testing.T) {
	cr := NewConflictResolver(CRDTMaxValueWins)

	local := &DeltaEntry{Point: &Point{Metric: "cpu", Value: 30}, CreatedAt: 100}
	remote := &DeltaEntry{Point: &Point{Metric: "cpu", Value: 20}, CreatedAt: 200}

	result := cr.Resolve(local, remote)
	if result.Point.Value != 30 {
		t.Errorf("Expected local value 30 (higher), got %v", result.Point.Value)
	}
}

func TestConflictResolverMergeAll(t *testing.T) {
	cr := NewConflictResolver(CRDTMergeAll)

	local := &DeltaEntry{Point: &Point{Metric: "cpu", Value: 10, Timestamp: 100}, CreatedAt: 100}
	remote := &DeltaEntry{Point: &Point{Metric: "cpu", Value: 20, Timestamp: 200}, CreatedAt: 200}

	result := cr.Resolve(local, remote)
	if result.Point.Value != 30 {
		t.Errorf("Expected merged value 30, got %v", result.Point.Value)
	}
}
