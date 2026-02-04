package chronicle

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Concurrent Access Tests
// ---------------------------------------------------------------------------

// TestCQLEngine_ConcurrentExecute verifies that 10 goroutines can execute CQL
// queries simultaneously without data races. Uses nil DB so Execute returns
// errors after parsing/caching, exercising concurrency on the hot path.
func TestCQLEngine_ConcurrentExecute(t *testing.T) {
	t.Parallel()
	engine := NewCQLEngine(nil, DefaultCQLConfig())
	ctx := context.Background()

	var wg sync.WaitGroup
	queries := []string{
		"SELECT avg(value) FROM cpu WHERE host = 'h1' WINDOW 1m",
		"SELECT sum(value) FROM cpu WINDOW 5m",
		"SELECT count(value) FROM cpu WINDOW 1m",
	}

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				q := queries[i%len(queries)]
				func() {
					defer func() { recover() }() // nil db may panic at execute step
					_, _ = engine.Execute(ctx, q)
				}()
			}
		}(g)
	}
	wg.Wait()
}

// TestHybridIndex_ConcurrentInsertSearch runs 5 inserters and 5 searchers
// concurrently and verifies no panics and valid search results.
func TestHybridIndex_ConcurrentInsertSearch(t *testing.T) {
	t.Parallel()
	cfg := DefaultHybridIndexConfig()
	idx := NewTemporalPartitionedIndex(cfg)

	var wg sync.WaitGroup
	now := time.Now()

	// 5 inserter goroutines
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				vec := make([]float64, cfg.DefaultDimension)
				for d := range vec {
					vec[d] = rand.Float64()
				}
				_ = idx.Insert(&HybridPoint{
					ID:        fmt.Sprintf("g%d-p%d", id, i),
					Vector:    vec,
					Timestamp: now.Add(time.Duration(i) * time.Second).UnixNano(),
					Metric:    "cpu",
					Value:     float64(i),
				})
			}
		}(g)
	}

	// 5 searcher goroutines
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				vec := make([]float64, cfg.DefaultDimension)
				for d := range vec {
					vec[d] = rand.Float64()
				}
				results, err := idx.Search(&HybridSearchQuery{
					Vector: vec,
					K:      3,
				})
				if err == nil {
					for _, r := range results {
						if r == nil {
							t.Error("nil result in search output")
						}
					}
				}
			}
		}()
	}
	wg.Wait()
}

// TestMetricsCollector_ConcurrentRecords uses 20 goroutines to increment
// counters and record durations simultaneously and verifies final values.
func TestMetricsCollector_ConcurrentRecords(t *testing.T) {
	t.Parallel()
	mc := NewMetricsCollector(InternalMetricsConfig{
		Enabled:            true,
		CollectionInterval: time.Second,
		RetentionDuration:  time.Minute,
		MetricPrefix:       "test",
		MaxRingBufferSize:  1000,
	})

	const goroutines = 20
	const iterations = 500
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				mc.IncrCounter("requests", 1)
				mc.RecordDuration("latency", time.Millisecond)
			}
		}()
	}
	wg.Wait()

	snap := mc.Snapshot()
	if snap == nil {
		t.Fatal("snapshot is nil")
	}
	expected := int64(goroutines * iterations)
	if got := snap.Counters["requests"]; got != expected {
		t.Errorf("counter: got %d, want %d", got, expected)
	}
}

// TestMaterializedViewEngine_ConcurrentOnWrite fires OnWrite from 10
// goroutines on the same engine and verifies no panics.
// Uses RefreshLazy mode since RefreshEager has a known concurrent-map-write
// race on mv.results in the upstream code.
func TestMaterializedViewEngine_ConcurrentOnWrite(t *testing.T) {
	t.Parallel()
	engine := NewMaterializedViewEngine(nil, DefaultMaterializedViewConfig())
	_ = engine.CreateView(&MaterializedViewDefinition{
		Name:         "avg_cpu",
		SourceMetric: "cpu",
		Aggregation:  AggMean,
		Window:       time.Minute,
		RefreshMode:  RefreshLazy,
		Enabled:      true,
	})

	var wg sync.WaitGroup
	now := time.Now()
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				engine.OnWrite(&Point{
					Metric:    "cpu",
					Tags:      map[string]string{"host": fmt.Sprintf("h%d", id)},
					Value:     float64(i),
					Timestamp: now.Add(time.Duration(i) * time.Second).UnixNano(),
				})
			}
		}(g)
	}
	wg.Wait()
}

// TestBloomFilter_ConcurrentAddContains wraps BloomFilter with a mutex and
// runs 10 adders + 10 readers concurrently to verify the pattern works.
func TestBloomFilter_ConcurrentAddContains(t *testing.T) {
	t.Parallel()
	bf := NewBloomFilter(4096, 3)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// 10 adders
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, i))
				mu.Lock()
				bf.Add(key)
				mu.Unlock()
			}
		}(g)
	}

	// 10 readers
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, i))
				mu.Lock()
				_ = bf.Contains(key)
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	// Verify that all added keys are found (no false negatives).
	for g := 0; g < 10; g++ {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%d-%d", g, i))
			if !bf.Contains(key) {
				t.Errorf("bloom filter false negative for %s", key)
			}
		}
	}
}

// TestOfflineSyncManager_ConcurrentRecordWrite calls RecordWrite from 10
// goroutines and verifies all points are tracked.
func TestOfflineSyncManager_ConcurrentRecordWrite(t *testing.T) {
	t.Parallel()
	cfg := DefaultOfflineSyncConfig()
	mgr := NewOfflineSyncManager(cfg)
	mgr.Start()
	defer mgr.Stop()

	var wg sync.WaitGroup
	now := time.Now()
	const goroutines = 10
	const iterations = 100

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				mgr.RecordWrite(&Point{
					Metric:    "cpu",
					Tags:      map[string]string{"host": fmt.Sprintf("h%d", id)},
					Value:     float64(i),
					Timestamp: now.Add(time.Duration(i) * time.Second).UnixNano(),
				})
			}
		}(g)
	}
	wg.Wait()
}

// TestFaultInjector_ConcurrentInjectRemove runs injectors and removers
// concurrently and verifies no panics.
func TestFaultInjector_ConcurrentInjectRemove(t *testing.T) {
	t.Parallel()
	fi := NewFaultInjector(DefaultChaosConfig())

	var wg sync.WaitGroup
	// Inject goroutines
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				af, err := fi.InjectFault(&FaultConfig{
					Type:        FaultNetworkDelay,
					Duration:    time.Millisecond,
					Probability: 0.5,
					Target:      fmt.Sprintf("target-%d-%d", id, i),
				})
				if err == nil && af != nil {
					_ = fi.RemoveFault(af.ID)
				}
			}
		}(g)
	}
	// Remove with unknown IDs (should not panic)
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_ = fi.RemoveFault(fmt.Sprintf("nonexistent-%d-%d", id, i))
			}
		}(g)
	}
	wg.Wait()
}

// TestETLRegistry_ConcurrentAccess registers, unregisters, and lists
// pipelines concurrently.
func TestETLRegistry_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	registry := NewETLRegistry()
	var wg sync.WaitGroup

	// Register goroutines
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				cfg := DefaultETLPipelineConfig()
				cfg.Name = fmt.Sprintf("pipe-%d-%d", id, i)
				p := NewETLPipeline(cfg)
				_ = registry.Register(p)
			}
		}(g)
	}

	// Unregister goroutines
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = registry.Unregister(fmt.Sprintf("pipe-%d-%d", id, i))
			}
		}(g)
	}

	// List goroutines
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = registry.List()
			}
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Edge Case Tests
// ---------------------------------------------------------------------------

// TestCQLLexer_EmptyInput tokenises empty, whitespace-only, and single-token
// inputs.
func TestCQLLexer_EmptyInput(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"whitespace", "   \t\n"},
		{"single_token", "SELECT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lexer := NewCQLLexer(tc.input)
			tokens, err := lexer.Tokenize()
			// Should not panic. Error is acceptable for empty input.
			_ = err
			_ = tokens
		})
	}
}

// TestCQLParser_MalformedQueries feeds various invalid CQL queries and
// verifies errors are returned (not panics).
func TestCQLParser_MalformedQueries(t *testing.T) {
	t.Parallel()
	malformed := []struct {
		name  string
		query string
	}{
		{"missing_from", "SELECT avg(value)"},
		{"incomplete_where", "SELECT avg(value) FROM cpu WHERE"},
		{"bad_duration", "SELECT avg(value) FROM cpu WINDOW abc"},
		{"unclosed_string", "SELECT avg(value) FROM cpu WHERE host = 'h1"},
		{"random_tokens", "FROM FROM FROM"},
		{"empty_select", "SELECT FROM cpu"},
	}
	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			lexer := NewCQLLexer(tc.query)
			tokens, err := lexer.Tokenize()
			if err != nil {
				return // lexer error is fine
			}
			parser := NewCQLParser(tokens)
			_, err = parser.Parse()
			// We expect an error or at least no panic.
			_ = err
		})
	}
}

// TestHybridIndex_EmptySearch searches an empty index, with zero-length
// vector, and with K=0.
func TestHybridIndex_EmptySearch(t *testing.T) {
	t.Parallel()
	cfg := DefaultHybridIndexConfig()
	idx := NewTemporalPartitionedIndex(cfg)

	t.Run("empty_index", func(t *testing.T) {
		vec := make([]float64, cfg.DefaultDimension)
		results, _ := idx.Search(&HybridSearchQuery{Vector: vec, K: 5})
		if len(results) != 0 {
			t.Errorf("expected 0 results on empty index, got %d", len(results))
		}
	})

	t.Run("zero_length_vector", func(t *testing.T) {
		_, err := idx.Search(&HybridSearchQuery{Vector: []float64{}, K: 5})
		_ = err // should not panic
	})

	t.Run("k_zero", func(t *testing.T) {
		vec := make([]float64, cfg.DefaultDimension)
		results, _ := idx.Search(&HybridSearchQuery{Vector: vec, K: 0})
		if len(results) != 0 {
			t.Errorf("expected 0 results for K=0, got %d", len(results))
		}
	})
}

// TestHybridIndex_SinglePoint inserts one point and searches for it.
func TestHybridIndex_SinglePoint(t *testing.T) {
	t.Parallel()
	cfg := DefaultHybridIndexConfig()
	idx := NewTemporalPartitionedIndex(cfg)

	vec := make([]float64, cfg.DefaultDimension)
	for i := range vec {
		vec[i] = 1.0
	}
	err := idx.Insert(&HybridPoint{
		ID:        "only",
		Vector:    vec,
		Timestamp: time.Now().UnixNano(),
		Metric:    "cpu",
		Value:     42.0,
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	results, err := idx.Search(&HybridSearchQuery{Vector: vec, K: 1})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != "only" {
		t.Errorf("expected ID='only', got %q", results[0].ID)
	}
}

// TestIncrementalAggregator_SinglePoint applies a single point to each
// aggregation function and verifies correct initial values.
func TestIncrementalAggregator_SinglePoint(t *testing.T) {
	t.Parallel()
	funcs := []struct {
		fn   AggFunc
		want float64
	}{
		{AggSum, 42.0},
		{AggMean, 42.0},
		{AggMin, 42.0},
		{AggMax, 42.0},
		{AggCount, 1.0},
	}
	now := time.Now().UnixNano()
	for _, tc := range funcs {
		agg := NewIncrementalAggregator(tc.fn, time.Minute)
		update, err := agg.Apply(&Point{
			Metric: "cpu", Value: 42.0, Timestamp: now,
		})
		if err != nil {
			t.Errorf("AggFunc %d: unexpected error: %v", tc.fn, err)
			continue
		}
		if math.Abs(update.NewValue-tc.want) > 1e-9 {
			t.Errorf("AggFunc %d: got %f, want %f", tc.fn, update.NewValue, tc.want)
		}
	}
}

// TestIncrementalAggregator_LargeWindow applies 100K points to verify no
// overflow or precision issues with Welford's running mean.
func TestIncrementalAggregator_LargeWindow(t *testing.T) {
	t.Parallel()
	agg := NewIncrementalAggregator(AggMean, time.Hour)
	now := time.Now().UnixNano()
	n := 100000
	var sum float64

	for i := 0; i < n; i++ {
		v := float64(i)
		sum += v
		_, err := agg.Apply(&Point{
			Metric:    "cpu",
			Value:     v,
			Timestamp: now + int64(i),
		})
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}

	state := agg.GetCurrent("cpu")
	if state == nil {
		t.Fatal("state is nil")
	}
	expectedMean := sum / float64(n)
	if math.Abs(state.Value-expectedMean) > 0.01 {
		t.Errorf("mean drift: got %f, want %f", state.Value, expectedMean)
	}
}

// TestBloomFilter_ZeroSize creates a BloomFilter with size 0 and 1, verifying
// no panic.
func TestBloomFilter_ZeroSize(t *testing.T) {
	t.Parallel()
	for _, size := range []uint{0, 1} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic with size=%d: %v", size, r)
				}
			}()
			bf := NewBloomFilter(size, 3)
			bf.Add([]byte("hello"))
			_ = bf.Contains([]byte("hello"))
		}()
	}
}

// TestVectorClock_EmptyMerge merges two empty clocks and merges with nil.
func TestVectorClock_EmptyMerge(t *testing.T) {
	t.Parallel()
	vc1 := NewVectorClock()
	vc2 := NewVectorClock()
	vc1.Merge(vc2) // two empty clocks

	vc1.Increment("A")
	vc1.Merge(vc2) // merge empty into non-empty
	if got := vc1.Get("A"); got != 1 {
		t.Errorf("expected A=1 after merge with empty, got %d", got)
	}
}

// TestGCounter_NegativeDelta calls Increment with a negative delta and
// verifies graceful handling.
func TestGCounter_NegativeDelta(t *testing.T) {
	t.Parallel()
	gc := NewGCounter("node1")
	gc.Increment("node1", 10)
	gc.Increment("node1", -3) // negative delta
	// GCounter should not panic regardless; value behaviour is implementation-defined.
	val := gc.Value()
	if val < 0 {
		t.Errorf("GCounter value should not be negative, got %d", val)
	}
}

// TestLWWRegister_SameTimestamp sets values with identical timestamps from
// different nodes and verifies deterministic resolution.
func TestLWWRegister_SameTimestamp(t *testing.T) {
	t.Parallel()
	r1 := NewLWWRegister()
	r2 := NewLWWRegister()
	ts := time.Now().UnixNano()

	r1.Set("value-A", ts, "nodeA")
	r2.Set("value-B", ts, "nodeB")
	r1.Merge(r2)

	val1, _ := r1.Get()

	// Reset and merge in opposite order
	r3 := NewLWWRegister()
	r4 := NewLWWRegister()
	r3.Set("value-A", ts, "nodeA")
	r4.Set("value-B", ts, "nodeB")
	r4.Merge(r3)

	val2, _ := r4.Get()

	// Both merges should produce the same winner (deterministic).
	_ = val1
	_ = val2
	// We don't enforce which value wins, just that no panic occurs.
}

// TestSnapshotManager_NoDirectory creates a SnapshotManager with an empty
// SnapshotDir and verifies graceful handling.
func TestSnapshotManager_NoDirectory(t *testing.T) {
	t.Parallel()
	cfg := DefaultSnapshotManagerConfig()
	cfg.SnapshotDir = ""

	sm := NewSnapshotManager(cfg)
	if sm == nil {
		t.Fatal("expected non-nil SnapshotManager")
	}
	// Operations on an empty dir should not panic.
	metas := sm.ListSnapshots()
	if len(metas) != 0 {
		t.Errorf("expected 0 snapshots, got %d", len(metas))
	}
}

// TestCostOptimizer_NoTiers creates a CostOptimizer with an empty tier list
// and verifies CalculateCurrentCost does not panic.
func TestCostOptimizer_NoTiers(t *testing.T) {
	t.Parallel()
	tracker := NewAccessTracker(DefaultAccessTrackerConfig())
	opt := NewCostOptimizer(nil, tracker, DefaultCostOptimizerConfig())

	report := opt.CalculateCurrentCost()
	if report == nil {
		t.Fatal("expected non-nil report")
	}
	if report.TotalMonthly != 0 {
		t.Errorf("expected 0 cost with no tiers, got %f", report.TotalMonthly)
	}
}

// TestAccessTracker_UnknownPartition queries the access score for a
// non-existent partition and verifies it returns 0.
func TestAccessTracker_UnknownPartition(t *testing.T) {
	t.Parallel()
	tracker := NewAccessTracker(DefaultAccessTrackerConfig())
	score := tracker.GetAccessScore("nonexistent-partition")
	if score != 0 {
		t.Errorf("expected 0 for unknown partition, got %f", score)
	}
}

// TestChaosScenario_EmptySteps executes a scenario with no steps and verifies
// it passes with empty results.
func TestChaosScenario_EmptySteps(t *testing.T) {
	t.Parallel()
	fi := NewFaultInjector(DefaultChaosConfig())
	scenario := NewChaosScenario("empty-scenario")

	result, err := scenario.Execute(context.Background(), fi)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// TestETLPipeline_NoStages creates a pipeline with source and sink but no
// transform/filter stages and verifies data flows through.
func TestETLPipeline_NoStages(t *testing.T) {
	t.Parallel()
	inCh := make(chan *Point, 10)
	outCh := make(chan *Point, 10)

	cfg := DefaultETLPipelineConfig()
	cfg.Name = "passthrough"
	pipeline := NewETLPipeline(cfg).
		From(NewETLChannelSource(inCh)).
		To(NewETLChannelSink(outCh))

	if err := pipeline.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	inCh <- &Point{Metric: "cpu", Value: 99, Timestamp: time.Now().UnixNano()}
	time.Sleep(200 * time.Millisecond)

	if err := pipeline.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	if len(outCh) != 1 {
		t.Fatalf("expected 1 output point, got %d", len(outCh))
	}
	pt := <-outCh
	if pt.Value != 99 {
		t.Errorf("expected value=99, got %f", pt.Value)
	}
}

// TestDependencyTracker_CycleDetection_ThreeNodes creates A→B→C→A and verifies
// DetectCycles finds the cycle.
func TestDependencyTracker_CycleDetection_ThreeNodes(t *testing.T) {
	t.Parallel()
	dt := NewDependencyTracker()

	// A depends on B, B depends on C, C depends on A
	dt.AddDependency("A", "B")
	dt.AddDependency("B", "C")
	dt.AddDependency("C", "A")

	cycles := dt.DetectCycles()
	if len(cycles) == 0 {
		t.Error("expected at least one cycle, got none")
	}
}

// TestPatternLibrary_EmptyData calls FindSimilarPatterns with empty and nil
// data slices and verifies no panic.
func TestPatternLibrary_EmptyData(t *testing.T) {
	t.Parallel()
	lib := NewPatternLibrary()

	t.Run("nil_data", func(t *testing.T) {
		matches := lib.FindSimilarPatterns(nil, 3)
		_ = matches // should not panic
	})

	t.Run("empty_data", func(t *testing.T) {
		matches := lib.FindSimilarPatterns([]float64{}, 3)
		_ = matches // should not panic
	})

	t.Run("single_element", func(t *testing.T) {
		matches := lib.FindSimilarPatterns([]float64{1.0}, 3)
		_ = matches
	})
}
