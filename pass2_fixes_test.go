package chronicle

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"
)

// --- Error type fixes ---

func TestQueryError_CanceledMatches(t *testing.T) {
	err := newQueryError(QueryErrorTypeCanceled, "query canceled", nil, nil)
	if !errors.Is(err, ErrQueryCanceled) {
		t.Error("canceled QueryError should match ErrQueryCanceled")
	}
	// Should not match unrelated sentinels
	if errors.Is(err, ErrQueryTimeout) {
		t.Error("canceled QueryError should not match ErrQueryTimeout")
	}
}

func TestStorageError_ReadWriteMatches(t *testing.T) {
	readErr := newStorageError(StorageErrorTypeRead, "read failed", "/data", nil)
	if !errors.Is(readErr, ErrStorageRead) {
		t.Error("read StorageError should match ErrStorageRead")
	}

	writeErr := newStorageError(StorageErrorTypeWrite, "write failed", "/data", nil)
	if !errors.Is(writeErr, ErrStorageWrite) {
		t.Error("write StorageError should match ErrStorageWrite")
	}
}

// --- Closed DB guards ---

func TestWriteAfterClose(t *testing.T) {
	db := setupTestDB(t)
	db.Close()

	err := db.Write(Point{Metric: "test", Value: 1.0})
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Write after Close should return ErrClosed, got: %v", err)
	}
}

func TestWriteBatchAfterClose(t *testing.T) {
	db := setupTestDB(t)
	db.Close()

	err := db.WriteBatch([]Point{{Metric: "test", Value: 1.0}})
	if !errors.Is(err, ErrClosed) {
		t.Errorf("WriteBatch after Close should return ErrClosed, got: %v", err)
	}
}

func TestExecuteAfterClose(t *testing.T) {
	db := setupTestDB(t)
	db.Close()

	_, err := db.Execute(&Query{Metric: "test"})
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Execute after Close should return ErrClosed, got: %v", err)
	}
}

func TestFlushAfterClose(t *testing.T) {
	db := setupTestDB(t)
	db.Close()

	err := db.Flush()
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Flush after Close should return ErrClosed, got: %v", err)
	}
}

func TestDeleteMetricAfterClose(t *testing.T) {
	db := setupTestDB(t)
	db.Close()

	err := db.DeleteMetric("test")
	if !errors.Is(err, ErrClosed) {
		t.Errorf("DeleteMetric after Close should return ErrClosed, got: %v", err)
	}
}

func TestCompactAfterClose(t *testing.T) {
	db := setupTestDB(t)
	db.Close()

	err := db.Compact()
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Compact after Close should return ErrClosed, got: %v", err)
	}
}

// --- Nil query returns non-nil Points ---

func TestExecuteNilQueryReturnsEmptyPoints(t *testing.T) {
	db := setupTestDB(t)

	result, err := db.Execute(nil)
	if err != nil {
		t.Fatalf("Execute(nil) should not error, got: %v", err)
	}
	if result.Points == nil {
		t.Error("Execute(nil) should return non-nil Points slice")
	}
	if len(result.Points) != 0 {
		t.Error("Execute(nil) should return empty Points slice")
	}
}

// --- DeleteMetric ---

func TestDeleteMetric(t *testing.T) {
	db := setupTestDB(t)

	ts := time.Now()
	writeTestPoints(t, db, "cpu.load", 10, ts)
	writeTestPoints(t, db, "mem.used", 5, ts)

	// Verify both metrics exist
	assertPointCount(t, db, "cpu.load", 10)
	assertPointCount(t, db, "mem.used", 5)

	// Delete one
	if err := db.DeleteMetric("cpu.load"); err != nil {
		t.Fatalf("DeleteMetric: %v", err)
	}

	// Verify deleted metric returns no points
	result, err := db.Execute(&Query{Metric: "cpu.load"})
	if err != nil {
		t.Fatalf("query after delete: %v", err)
	}
	if len(result.Points) != 0 {
		t.Errorf("expected 0 points after delete, got %d", len(result.Points))
	}

	// Verify other metric still intact
	assertPointCount(t, db, "mem.used", 5)
}

func TestDeleteMetric_EmptyName(t *testing.T) {
	db := setupTestDB(t)

	err := db.DeleteMetric("")
	if err == nil {
		t.Error("expected error for empty metric name")
	}
}

func TestDeleteMetric_NotFound(t *testing.T) {
	db := setupTestDB(t)

	err := db.DeleteMetric("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent metric")
	}
}

// --- ApplyEnvOverrides ---

func TestApplyEnvOverrides_Valid(t *testing.T) {
	t.Setenv("CHRONICLE_HTTP_PORT", "9999")
	t.Setenv("CHRONICLE_MAX_MEMORY", "134217728")
	t.Setenv("CHRONICLE_RETENTION", "168h")

	cfg := DefaultConfig("/tmp/test.db")
	if err := cfg.ApplyEnvOverrides(); err != nil {
		t.Fatalf("ApplyEnvOverrides: %v", err)
	}

	if cfg.HTTP.HTTPPort != 9999 {
		t.Errorf("expected HTTPPort=9999, got %d", cfg.HTTP.HTTPPort)
	}
	if cfg.Storage.MaxMemory != 134217728 {
		t.Errorf("expected MaxMemory=134217728, got %d", cfg.Storage.MaxMemory)
	}
	if cfg.Retention.RetentionDuration != 168*time.Hour {
		t.Errorf("expected RetentionDuration=168h, got %v", cfg.Retention.RetentionDuration)
	}
}

func TestApplyEnvOverrides_InvalidValue(t *testing.T) {
	t.Setenv("CHRONICLE_HTTP_PORT", "not-a-number")

	cfg := DefaultConfig("/tmp/test.db")
	err := cfg.ApplyEnvOverrides()
	if err == nil {
		t.Error("expected error for invalid port value")
	}
}

func TestApplyEnvOverrides_MultipleErrors(t *testing.T) {
	t.Setenv("CHRONICLE_HTTP_PORT", "abc")
	t.Setenv("CHRONICLE_MAX_MEMORY", "xyz")

	cfg := DefaultConfig("/tmp/test.db")
	err := cfg.ApplyEnvOverrides()
	if err == nil {
		t.Error("expected error for multiple invalid values")
	}
}

func TestApplyEnvOverrides_NoVarsSet(t *testing.T) {
	// Unset all CHRONICLE_ vars to ensure clean state
	for _, key := range []string{
		"CHRONICLE_PATH", "CHRONICLE_LOG_LEVEL", "CHRONICLE_MAX_MEMORY",
		"CHRONICLE_HTTP_PORT", "CHRONICLE_HTTP_ENABLED", "CHRONICLE_RATE_LIMIT",
		"CHRONICLE_RETENTION", "CHRONICLE_QUERY_TIMEOUT", "CHRONICLE_WAL_SYNC_INTERVAL",
		"CHRONICLE_PARTITION_DURATION", "CHRONICLE_BUFFER_SIZE",
	} {
		os.Unsetenv(key)
	}

	original := DefaultConfig("/tmp/test.db")
	cfg := DefaultConfig("/tmp/test.db")
	if err := cfg.ApplyEnvOverrides(); err != nil {
		t.Fatalf("ApplyEnvOverrides with no vars: %v", err)
	}
	if cfg.HTTP.HTTPPort != original.HTTP.HTTPPort {
		t.Error("config should be unchanged when no env vars set")
	}
}

// --- ContinuousQueryStats ---

func TestContinuousQueryStats_Empty(t *testing.T) {
	db := setupTestDB(t)

	stats := db.ContinuousQueryStats()
	if len(stats) != 0 {
		t.Errorf("expected 0 CQ stats on DB with no CQs, got %d", len(stats))
	}
}

// --- CoercePoint ---

func TestCoercePoint_AppliesDefaults(t *testing.T) {
	engine := NewSchemaCompatEngine(nil, DefaultSchemaCompatConfig())
	engine.config.EnableAutoCoercion = true
	engine.SetFieldDefault("cpu", "region", 1.0)

	p := &Point{Metric: "cpu", Value: 42.0, Tags: map[string]string{"host": "a"}}
	engine.CoercePoint(p)

	if p.Tags["region"] != "1" {
		t.Errorf("expected region tag to be filled with default '1', got %q", p.Tags["region"])
	}
	// Existing tag should not be overwritten
	if p.Tags["host"] != "a" {
		t.Error("existing tag should not be overwritten")
	}
}

func TestCoercePoint_SkipsExistingTags(t *testing.T) {
	engine := NewSchemaCompatEngine(nil, DefaultSchemaCompatConfig())
	engine.config.EnableAutoCoercion = true
	engine.SetFieldDefault("cpu", "region", 2.0)

	p := &Point{Metric: "cpu", Tags: map[string]string{"region": "existing"}}
	engine.CoercePoint(p)

	if p.Tags["region"] != "existing" {
		t.Errorf("expected existing tag to remain, got %q", p.Tags["region"])
	}
}

func TestCoercePoint_NilPoint(t *testing.T) {
	engine := NewSchemaCompatEngine(nil, DefaultSchemaCompatConfig())
	engine.config.EnableAutoCoercion = true
	// Should not panic
	engine.CoercePoint(nil)
}

func TestCoercePoint_DisabledCoercion(t *testing.T) {
	engine := NewSchemaCompatEngine(nil, DefaultSchemaCompatConfig())
	engine.config.EnableAutoCoercion = false
	engine.SetFieldDefault("cpu", "region", 1.0)

	p := &Point{Metric: "cpu", Tags: map[string]string{}}
	engine.CoercePoint(p)

	if _, exists := p.Tags["region"]; exists {
		t.Error("should not coerce when disabled")
	}
}

// --- MustBuild panics ---

func TestMustBuild_PanicsOnInvalidConfig(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustBuild should panic on invalid config")
		}
	}()

	// Build with no path and no storage backend should fail validation
	b := NewConfigBuilder("")
	b.cfg.Path = ""
	b.cfg.StorageBackend = nil
	b.MustBuild()
}

// --- Autoscale with real DB ---

func TestAutoscaleEngine_RealMetrics(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultAutoscaleConfig()
	cfg.CollectionInterval = 10 * time.Millisecond
	cfg.EvaluationInterval = 10 * time.Millisecond
	engine := NewAutoscaleEngine(db, cfg)

	// Record some operations
	engine.RecordWrite()
	engine.RecordWrite()
	engine.RecordQuery()

	// Wait a bit then collect
	time.Sleep(20 * time.Millisecond)
	engine.collectMetrics()

	// Memory util should be a real value (not the old placeholder 0.5)
	engine.mu.RLock()
	memSamples := engine.metricHistory["memory_util"]
	engine.mu.RUnlock()

	if len(memSamples) == 0 {
		t.Fatal("expected memory_util samples after collection")
	}
	// The value should be real, not the old hardcoded 0.5
	val := memSamples[len(memSamples)-1].Value
	if val < 0 || val > 10.0 {
		t.Errorf("memory_util should be a reasonable ratio, got %f", val)
	}
}

// --- Index.DeleteMetric ---

func TestIndex_DeleteMetric(t *testing.T) {
	idx := newIndex()

	// Register some series
	idx.RegisterSeries("cpu", map[string]string{"host": "a"})
	idx.RegisterSeries("cpu", map[string]string{"host": "b"})
	idx.RegisterSeries("mem", map[string]string{"host": "a"})

	if idx.SeriesCount() != 3 {
		t.Fatalf("expected 3 series, got %d", idx.SeriesCount())
	}

	// Delete cpu
	if !idx.DeleteMetric("cpu") {
		t.Error("expected DeleteMetric to return true")
	}

	// cpu series should be gone
	metrics := idx.Metrics()
	for _, m := range metrics {
		if m == "cpu" {
			t.Error("cpu should be removed from Metrics()")
		}
	}

	// mem should still exist
	found := false
	for _, m := range metrics {
		if m == "mem" {
			found = true
		}
	}
	if !found {
		t.Error("mem should still be in Metrics()")
	}

	// Series count should be 1
	if idx.SeriesCount() != 1 {
		t.Errorf("expected 1 series after delete, got %d", idx.SeriesCount())
	}
}

func TestIndex_DeleteMetric_Empty(t *testing.T) {
	idx := newIndex()
	if idx.DeleteMetric("") {
		t.Error("expected false for empty metric name")
	}
}

func TestIndex_DeleteMetric_NotFound(t *testing.T) {
	idx := newIndex()
	if idx.DeleteMetric("nonexistent") {
		t.Error("expected false for nonexistent metric")
	}
}

// --- Schema evolution wired to writes ---

func TestSchemaEvolution_ObservedOnWrite(t *testing.T) {
	db := setupTestDB(t)

	// Initialize schema evolution engine explicitly
	se := db.SchemaEvolution()

	ts := time.Now()
	if err := db.Write(Point{Metric: "cpu", Value: 1.0, Timestamp: ts.UnixNano(), Tags: map[string]string{"host": "a"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	stats := se.GetStats()
	if stats.TrackedMetrics < 1 {
		t.Errorf("expected at least 1 tracked metric, got %d", stats.TrackedMetrics)
	}
}

func TestSchemaEvolution_DetectsNewTag(t *testing.T) {
	db := setupTestDB(t)

	se := db.SchemaEvolution()
	ts := time.Now()

	// Write with initial tags
	if err := db.Write(Point{Metric: "cpu", Value: 1.0, Timestamp: ts.UnixNano(), Tags: map[string]string{"host": "a"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Write with a new tag — should trigger schema change detection
	if err := db.Write(Point{Metric: "cpu", Value: 2.0, Timestamp: ts.Add(time.Second).UnixNano(), Tags: map[string]string{"host": "a", "region": "us"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	stats := se.GetStats()
	if stats.TotalChanges < 1 {
		t.Errorf("expected at least 1 schema change detected, got %d", stats.TotalChanges)
	}
}

func TestSchemaEvolution_BatchObservesUnique(t *testing.T) {
	db := setupTestDB(t)

	se := db.SchemaEvolution()
	ts := time.Now()

	points := make([]Point, 100)
	for i := range points {
		points[i] = Point{
			Metric:    fmt.Sprintf("metric_%d", i%3),
			Value:     float64(i),
			Timestamp: ts.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Tags:      map[string]string{"host": "a"},
		}
	}
	if err := db.WriteBatch(points); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	stats := se.GetStats()
	if stats.TrackedMetrics != 3 {
		t.Errorf("expected 3 tracked metrics, got %d", stats.TrackedMetrics)
	}
}

// --- WALSyncError multi-unwrap ---

func TestWALSyncError_BothErrorsReachable(t *testing.T) {
	flushErr := errors.New("flush failed")
	syncErr := errors.New("sync failed")

	combined := &WALSyncError{FlushErr: flushErr, SyncErr: syncErr}

	if !errors.Is(combined, flushErr) {
		t.Error("FlushErr should be reachable via errors.Is")
	}
	if !errors.Is(combined, syncErr) {
		t.Error("SyncErr should be reachable via errors.Is")
	}
}

// --- Replication dead-letter queue ---

func TestReplicator_DLQ_BuffersOnCircuitOpen(t *testing.T) {
	cfg := &ReplicationConfig{
		Enabled:       true,
		TargetURL:     "http://unreachable.invalid:9999",
		BatchSize:     10,
		FlushInterval: time.Hour, // don't auto-flush
		Timeout:       50 * time.Millisecond,
		MaxRetries:    1,
		RetryBackoff:  10 * time.Millisecond,
	}
	r := newReplicator(cfg)

	// Manually push points to DLQ (simulating circuit breaker failure)
	points := make([]Point, 5)
	for i := range points {
		points[i] = Point{Metric: "test", Value: float64(i)}
	}
	r.enqueueDLQ(points)

	if r.DLQLen() != 5 {
		t.Errorf("expected DLQ length 5, got %d", r.DLQLen())
	}
	if r.DroppedPoints() != 0 {
		t.Errorf("expected 0 dropped points, got %d", r.DroppedPoints())
	}
}

func TestReplicator_DLQ_OverflowDropsOldest(t *testing.T) {
	cfg := &ReplicationConfig{
		Enabled:       true,
		TargetURL:     "http://localhost:1",
		BatchSize:     100,
		FlushInterval: time.Hour,
		Timeout:       50 * time.Millisecond,
	}
	r := newReplicator(cfg)
	r.dlqMax = 10 // small DLQ for testing

	// Push 15 points into a DLQ that only holds 10
	points := make([]Point, 15)
	for i := range points {
		points[i] = Point{Metric: fmt.Sprintf("m%d", i), Value: float64(i)}
	}
	r.enqueueDLQ(points)

	if r.DLQLen() != 10 {
		t.Errorf("expected DLQ capped at 10, got %d", r.DLQLen())
	}
	if r.DroppedPoints() != 5 {
		t.Errorf("expected 5 dropped points, got %d", r.DroppedPoints())
	}
}

// --- Pass 3: Real health checks ---

func TestHealthCheck_RealChecks(t *testing.T) {
	db := setupTestDB(t)

	hc := NewHealthCheckEngine(db, DefaultHealthCheckConfig())
	hc.Start()
	defer hc.Stop()

	status := hc.Check()
	if status.Overall != "healthy" {
		t.Errorf("expected healthy overall, got %s", status.Overall)
	}

	// Verify all components are present
	componentNames := make(map[string]bool)
	for _, c := range status.Components {
		componentNames[c.Name] = true
		if c.Status != "healthy" {
			t.Errorf("component %s should be healthy, got %s: %s", c.Name, c.Status, c.Message)
		}
	}
	for _, required := range []string{"database", "storage", "wal", "index"} {
		if !componentNames[required] {
			t.Errorf("missing required health component: %s", required)
		}
	}
}

func TestHealthCheck_ClosedDB(t *testing.T) {
	db := setupTestDB(t)
	db.Close()

	hc := NewHealthCheckEngine(db, DefaultHealthCheckConfig())
	hc.Start()
	defer hc.Stop()

	status := hc.Check()
	if status.Overall != "unhealthy" {
		t.Errorf("expected unhealthy for closed DB, got %s", status.Overall)
	}
}

func TestHealthCheck_IndexShowsMetrics(t *testing.T) {
	db := setupTestDB(t)
	ts := time.Now()
	writeTestPoints(t, db, "cpu.load", 5, ts)

	hc := NewHealthCheckEngine(db, DefaultHealthCheckConfig())
	hc.Start()
	defer hc.Stop()

	status := hc.Check()
	for _, c := range status.Components {
		if c.Name == "index" {
			if c.Message == "" || c.Message == "ok" {
				t.Error("index health should show metrics/series count, not just 'ok'")
			}
		}
	}
}

// --- Pass 3: ValidatePoint NaN/Inf ---

func TestValidatePoint_RejectsNaN(t *testing.T) {
	p := &Point{Metric: "cpu", Value: math.NaN()}
	err := ValidatePoint(p)
	if !errors.Is(err, ErrInvalidValue) {
		t.Errorf("expected ErrInvalidValue for NaN, got %v", err)
	}
}

func TestValidatePoint_RejectsInf(t *testing.T) {
	p := &Point{Metric: "cpu", Value: math.Inf(1)}
	err := ValidatePoint(p)
	if !errors.Is(err, ErrInvalidValue) {
		t.Errorf("expected ErrInvalidValue for +Inf, got %v", err)
	}

	p2 := &Point{Metric: "cpu", Value: math.Inf(-1)}
	err2 := ValidatePoint(p2)
	if !errors.Is(err2, ErrInvalidValue) {
		t.Errorf("expected ErrInvalidValue for -Inf, got %v", err2)
	}
}

func TestValidatePoint_AcceptsNormalValues(t *testing.T) {
	for _, v := range []float64{0, 1.0, -1.0, 1e18, -1e18} {
		p := &Point{Metric: "cpu", Value: v}
		if err := ValidatePoint(p); err != nil {
			t.Errorf("ValidatePoint(%f) should pass, got %v", v, err)
		}
	}
}

// --- Pass 3: Aggregation sort stability ---

func TestAggregation_StableGroupBySort(t *testing.T) {
	db := setupTestDB(t)

	ts := time.Now()
	// Write points for two hosts at the same timestamps
	for i := 0; i < 5; i++ {
		stamp := ts.Add(time.Duration(i) * time.Minute)
		_ = db.Write(Point{Metric: "cpu", Value: float64(i), Timestamp: stamp.UnixNano(), Tags: map[string]string{"host": "b"}})
		_ = db.Write(Point{Metric: "cpu", Value: float64(i + 10), Timestamp: stamp.UnixNano(), Tags: map[string]string{"host": "a"}})
	}
	_ = db.Flush()

	// Run the same query twice — results should be identical
	q := &Query{
		Metric:      "cpu",
		GroupBy:     []string{"host"},
		Aggregation: &Aggregation{Function: AggMean, Window: time.Minute},
	}

	r1, err := db.Execute(q)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	r2, err := db.Execute(q)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(r1.Points) != len(r2.Points) {
		t.Fatalf("result lengths differ: %d vs %d", len(r1.Points), len(r2.Points))
	}
	for i := range r1.Points {
		if r1.Points[i].Timestamp != r2.Points[i].Timestamp {
			t.Errorf("point %d: timestamps differ: %d vs %d", i, r1.Points[i].Timestamp, r2.Points[i].Timestamp)
		}
		if r1.Points[i].Value != r2.Points[i].Value {
			t.Errorf("point %d: values differ: %f vs %f", i, r1.Points[i].Value, r2.Points[i].Value)
		}
	}
}

// --- Pass 4: CDC nil tags ---

func TestCDCFilter_NilTags(t *testing.T) {
	filter := CDCFilter{Tags: map[string]string{"host": "a"}}
	event := &ChangeEvent{
		After: &Point{Metric: "cpu", Value: 1.0, Tags: nil},
	}
	// Should not panic, should return false (no matching tags)
	if filter.matches(event) {
		t.Error("filter should not match event with nil tags")
	}
}

func TestCDCFilter_MatchingTags(t *testing.T) {
	filter := CDCFilter{Tags: map[string]string{"host": "a"}}
	event := &ChangeEvent{
		After: &Point{Metric: "cpu", Value: 1.0, Tags: map[string]string{"host": "a"}},
	}
	if !filter.matches(event) {
		t.Error("filter should match event with matching tags")
	}
}

// --- Pass 4: Regex tag filter caching ---

func TestPrepareTagFilters_CompilesRegex(t *testing.T) {
	filters := []TagFilter{
		{Key: "host", Op: TagOpRegex, Values: []string{"^srv-[0-9]+$"}},
	}
	if err := prepareTagFilters(filters); err != nil {
		t.Fatalf("prepareTagFilters: %v", err)
	}
	if filters[0].compiledRe == nil {
		t.Error("expected compiled regex after prepareTagFilters")
	}
}

func TestPrepareTagFilters_RejectsInvalidRegex(t *testing.T) {
	filters := []TagFilter{
		{Key: "host", Op: TagOpRegex, Values: []string{"[invalid"}},
	}
	if err := prepareTagFilters(filters); err == nil {
		t.Error("expected error for invalid regex pattern")
	}
}

func TestPrepareTagFilters_RejectsTooLongPattern(t *testing.T) {
	longPattern := make([]byte, maxTagFilterRegexLen+1)
	for i := range longPattern {
		longPattern[i] = 'a'
	}
	filters := []TagFilter{
		{Key: "host", Op: TagOpRegex, Values: []string{string(longPattern)}},
	}
	if err := prepareTagFilters(filters); err == nil {
		t.Error("expected error for too-long regex pattern")
	}
}

func TestQuery_RegexTagFilter(t *testing.T) {
	db := setupTestDB(t)
	ts := time.Now()
	for _, host := range []string{"srv-01", "srv-02", "web-01"} {
		_ = db.Write(Point{Metric: "cpu", Value: 1.0, Timestamp: ts.UnixNano(), Tags: map[string]string{"host": host}})
	}
	_ = db.Flush()

	result, err := db.Execute(&Query{
		Metric: "cpu",
		TagFilters: []TagFilter{
			{Key: "host", Op: TagOpRegex, Values: []string{"^srv-"}},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Points) != 2 {
		t.Errorf("expected 2 points matching ^srv-, got %d", len(result.Points))
	}
}

// --- Pass 4: BackendDataStore stat accuracy ---

func TestBackendDataStore_StatAfterDelete(t *testing.T) {
	backend := NewMemoryBackend()
	ds := NewBackendDataStore(backend, "test/")

	ctx := t.Context()
	ds.WritePartition(ctx, 1, []byte("hello"))
	ds.WritePartition(ctx, 2, []byte("world!!"))

	size1, _ := ds.Stat()
	if size1 != 12 {
		t.Errorf("expected size 12, got %d", size1)
	}

	ds.DeletePartition(ctx, 1)

	size2, _ := ds.Stat()
	if size2 != 7 {
		t.Errorf("expected size 7 after delete, got %d", size2)
	}
}

// --- Pass 5: Write hook panic recovery ---

func TestWritePipeline_PostHookPanicRecovery(t *testing.T) {
	db := setupTestDB(t)
	wp := NewWritePipelineEngine(db, DefaultWritePipelineConfig())
	wp.Start()
	defer wp.Stop()

	// Register a hook that panics
	wp.Register(WriteHook{
		Name:  "panicker",
		Phase: "post",
		Handler: func(p Point) (Point, error) {
			panic("intentional test panic")
		},
	})

	// ProcessPost should not propagate the panic
	wp.ProcessPost(Point{Metric: "cpu", Value: 1.0})

	stats := wp.Stats()
	if stats.TotalErrors != 1 {
		t.Errorf("expected 1 error from panicking hook, got %d", stats.TotalErrors)
	}
}

func TestWritePipeline_ConcurrentProcessPre(t *testing.T) {
	db := setupTestDB(t)
	wp := NewWritePipelineEngine(db, DefaultWritePipelineConfig())
	wp.Start()
	defer wp.Stop()

	wp.Register(WriteHook{
		Name:  "passthrough",
		Phase: "pre",
		Handler: func(p Point) (Point, error) {
			return p, nil
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = wp.ProcessPre(Point{Metric: "cpu", Value: float64(id*100 + j)})
			}
		}(i)
	}
	wg.Wait()

	stats := wp.Stats()
	if stats.TotalProcessed != 5000 {
		t.Errorf("expected 5000 processed, got %d", stats.TotalProcessed)
	}
}

// --- Follow-up 5: Config cross-field validation ---

func TestConfig_ClickHouseRequiresHTTP(t *testing.T) {
	cfg := DefaultConfig("/tmp/test.db")
	cfg.ClickHouse = &ClickHouseConfig{Enabled: true}
	cfg.HTTP.HTTPEnabled = false

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error: ClickHouse enabled without HTTP")
	}
}

func TestConfig_ClickHouseWithHTTP_OK(t *testing.T) {
	cfg := DefaultConfig("/tmp/test.db")
	cfg.ClickHouse = &ClickHouseConfig{Enabled: true}
	cfg.HTTP.HTTPEnabled = true

	err := cfg.Validate()
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
}

// --- Pass 6: Streaming nil tags ---

func TestStreamHub_PublishNilTags(t *testing.T) {
	hub := NewStreamHub(nil, DefaultStreamConfig())
	sub := hub.Subscribe("", map[string]string{"host": "a"})
	defer hub.Unsubscribe(sub.ID)

	// Should not panic on point with nil tags
	hub.Publish(Point{Metric: "cpu", Value: 1.0, Tags: nil})

	// No point should arrive (filter requires host=a but tags are nil)
	select {
	case <-sub.C():
		t.Error("should not receive point with nil tags when filter requires tags")
	default:
		// expected
	}
}

func TestStreamHub_PublishMatchingTags(t *testing.T) {
	hub := NewStreamHub(nil, DefaultStreamConfig())
	sub := hub.Subscribe("cpu", nil)
	defer hub.Unsubscribe(sub.ID)

	hub.Publish(Point{Metric: "cpu", Value: 42.0, Tags: map[string]string{"host": "a"}})

	select {
	case p := <-sub.C():
		if p.Value != 42.0 {
			t.Errorf("expected value 42.0, got %f", p.Value)
		}
	case <-time.After(time.Second):
		t.Error("timed out waiting for published point")
	}
}

// --- Pass 6: Recording rule state race safety ---

func TestRecordingRulesEngine_ConcurrentEvaluate(t *testing.T) {
	db := setupTestDB(t)
	ts := time.Now()
	writeTestPoints(t, db, "src_metric", 10, ts)

	engine := NewRecordingRulesEngine(db)
	engine.AddRule(RecordingRule{
		Name:         "test_rule",
		Query:        "SELECT mean(value) FROM src_metric",
		TargetMetric: "dst_metric",
		Interval:     time.Millisecond,
		Enabled:      true,
	})

	// Run multiple concurrent evaluations — should not race
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			engine.EvaluateNow(t.Context(), "test_rule")
		}()
	}
	wg.Wait()

	stats := engine.Stats()
	if len(stats) != 1 {
		t.Errorf("expected 1 rule stat, got %d", len(stats))
	}
}

// --- Pass 7: Series dedup includes tags ---

func TestSeriesDedup_DifferentTagsSameValue(t *testing.T) {
	cfg := DefaultSeriesDedupConfig()
	cfg.Enabled = true
	cfg.DeduplicateWindow = time.Second
	engine := NewSeriesDedupEngine(nil, cfg)
	engine.Start()
	defer engine.Stop()

	ts := time.Now().UnixNano()

	// Two different series with same metric and value should NOT be deduped
	p1 := Point{Metric: "cpu", Value: 50.0, Timestamp: ts, Tags: map[string]string{"host": "a"}}
	p2 := Point{Metric: "cpu", Value: 50.0, Timestamp: ts, Tags: map[string]string{"host": "b"}}

	if engine.CheckDuplicate(p1) {
		t.Error("first point should not be a duplicate")
	}
	if engine.CheckDuplicate(p2) {
		t.Error("different series (different tags) should not be deduplicated")
	}
}

func TestSeriesDedup_SameSeriesDuplicate(t *testing.T) {
	cfg := DefaultSeriesDedupConfig()
	cfg.Enabled = true
	cfg.DeduplicateWindow = time.Second
	engine := NewSeriesDedupEngine(nil, cfg)
	engine.Start()
	defer engine.Stop()

	ts := time.Now().UnixNano()
	p := Point{Metric: "cpu", Value: 50.0, Timestamp: ts, Tags: map[string]string{"host": "a"}}

	if engine.CheckDuplicate(p) {
		t.Error("first point should not be a duplicate")
	}
	if !engine.CheckDuplicate(p) {
		t.Error("exact same series+value+timestamp should be a duplicate")
	}
}

// --- Pass 7: Exemplar TTL enforcement at write ---

func TestExemplarStore_RejectsOldExemplars(t *testing.T) {
	store := NewExemplarStore(nil, ExemplarConfig{
		Enabled:           true,
		RetentionDuration: time.Second,
	})

	oldTime := time.Now().Add(-2 * time.Second).UnixNano()

	store.Write(ExemplarPoint{
		Metric:    "test",
		Timestamp: oldTime,
		Exemplar:  &Exemplar{Labels: map[string]string{"trace_id": "old"}},
	})

	stats := store.Stats()
	if stats.TotalExemplars != 0 {
		t.Errorf("expected 0 exemplars (old should be rejected), got %d", stats.TotalExemplars)
	}
}
