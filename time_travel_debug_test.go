package chronicle

import (
	"testing"
	"time"
)

func TestDefaultTimeTravelDebugConfig(t *testing.T) {
	cfg := DefaultTimeTravelDebugConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.MaxSnapshots <= 0 {
		t.Errorf("expected MaxSnapshots > 0, got %d", cfg.MaxSnapshots)
	}
	if cfg.DiffTimeout <= 0 {
		t.Errorf("expected DiffTimeout > 0, got %v", cfg.DiffTimeout)
	}
	if cfg.MaxDiffResults <= 0 {
		t.Errorf("expected MaxDiffResults > 0, got %d", cfg.MaxDiffResults)
	}
	if cfg.ReplayBufferSize <= 0 {
		t.Errorf("expected ReplayBufferSize > 0, got %d", cfg.ReplayBufferSize)
	}
}

func setupDebugTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/debug_test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	return db
}

func writeDebugTestPoints(t *testing.T, db *DB, metric string, n int, startTime time.Time) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := db.Write(Point{
			Metric:    metric,
			Value:     float64(i + 1),
			Timestamp: startTime.Add(time.Duration(i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": "test"},
		}); err != nil {
			t.Fatalf("write point %d: %v", i, err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestNewTimeTravelDebugEngine(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	cfg := DefaultTimeTravelDebugConfig()
	engine := NewTimeTravelDebugEngine(db, cfg)

	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.db != db {
		t.Error("engine.db should reference the given DB")
	}
}

func TestQueryAsOf(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	baseTime := time.Now().Add(-10 * time.Minute)
	writeDebugTestPoints(t, db, "cpu.usage", 5, baseTime)

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())

	// Query as-of a time after all points
	asOf := baseTime.Add(10 * time.Second)
	points, err := engine.QueryAsOf("cpu.usage", map[string]string{"host": "test"}, asOf, 0, 0)
	if err != nil {
		t.Fatalf("QueryAsOf failed: %v", err)
	}
	if len(points) != 5 {
		t.Errorf("expected 5 points, got %d", len(points))
	}

	// Query as-of a time between points (after 3rd point, before 4th)
	asOf2 := baseTime.Add(2*time.Second + 500*time.Millisecond)
	points2, err := engine.QueryAsOf("cpu.usage", map[string]string{"host": "test"}, asOf2, 0, 0)
	if err != nil {
		t.Fatalf("QueryAsOf failed: %v", err)
	}
	if len(points2) != 3 {
		t.Errorf("expected 3 points, got %d", len(points2))
	}
}

func TestQueryAsOfDisabled(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	cfg := DefaultTimeTravelDebugConfig()
	cfg.Enabled = false
	engine := NewTimeTravelDebugEngine(db, cfg)

	_, err := engine.QueryAsOf("cpu", nil, time.Now(), 0, 0)
	if err == nil {
		t.Error("expected error when engine is disabled")
	}
}

func TestDiff(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	baseTime := time.Now().Add(-10 * time.Minute)

	// Write first batch of points
	for i := 0; i < 3; i++ {
		if err := db.Write(Point{
			Metric:    "cpu.usage",
			Value:     float64(10 + i),
			Timestamp: baseTime.Add(time.Duration(i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": "test"},
		}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Write second batch at later timestamps
	for i := 0; i < 5; i++ {
		if err := db.Write(Point{
			Metric:    "cpu.usage",
			Value:     float64(20 + i),
			Timestamp: baseTime.Add(time.Duration(5+i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": "test"},
		}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())

	fromTime := baseTime.Add(3 * time.Second).UnixNano()
	toTime := baseTime.Add(10 * time.Second).UnixNano()

	result, err := engine.Diff("cpu.usage", fromTime, toTime)
	if err != nil {
		t.Fatalf("Diff failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil diff result")
	}
	if result.FromTime != fromTime {
		t.Errorf("expected FromTime %d, got %d", fromTime, result.FromTime)
	}
	if result.ToTime != toTime {
		t.Errorf("expected ToTime %d, got %d", toTime, result.ToTime)
	}

	// Second call should hit cache
	result2, err := engine.Diff("cpu.usage", fromTime, toTime)
	if err != nil {
		t.Fatalf("Diff (cached) failed: %v", err)
	}
	if result2.ComputedAt != result.ComputedAt {
		t.Error("expected cached result")
	}

	stats := engine.Stats()
	if stats.CacheHits < 1 {
		t.Errorf("expected at least 1 cache hit, got %d", stats.CacheHits)
	}
}

func TestDiffAll(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	baseTime := time.Now().Add(-10 * time.Minute)

	// Write points for two different metrics
	for i := 0; i < 3; i++ {
		db.Write(Point{
			Metric:    "cpu.usage",
			Value:     float64(i + 1),
			Timestamp: baseTime.Add(time.Duration(i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": "test"},
		})
		db.Write(Point{
			Metric:    "mem.usage",
			Value:     float64(100 + i),
			Timestamp: baseTime.Add(time.Duration(i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": "test"},
		})
	}
	db.Flush()

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())

	fromTime := baseTime.UnixNano()
	toTime := baseTime.Add(5 * time.Second).UnixNano()

	result, err := engine.DiffAll(fromTime, toTime)
	if err != nil {
		t.Fatalf("DiffAll failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Metric != "" {
		t.Errorf("expected empty metric for DiffAll, got %q", result.Metric)
	}
}

func TestCreateReplay(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	baseTime := time.Now().Add(-10 * time.Minute)
	writeDebugTestPoints(t, db, "cpu.usage", 10, baseTime)

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())

	req := ReplayRequest{
		Metric:    "cpu.usage",
		StartTime: baseTime.UnixNano(),
		EndTime:   baseTime.Add(9 * time.Second).UnixNano(),
		StepSize:  3 * time.Second,
		Tags:      map[string]string{"host": "test"},
	}

	session, err := engine.CreateReplay(req)
	if err != nil {
		t.Fatalf("CreateReplay failed: %v", err)
	}
	if session == nil {
		t.Fatal("expected non-nil session")
	}
	if session.State != "created" {
		t.Errorf("expected state 'created', got %q", session.State)
	}
	if len(session.Frames) == 0 {
		t.Error("expected at least one frame")
	}

	// Verify total is set correctly on all frames
	for i, frame := range session.Frames {
		if frame.Total != len(session.Frames) {
			t.Errorf("frame %d: expected Total=%d, got %d", i, len(session.Frames), frame.Total)
		}
	}
}

func TestReplayNavigation(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	baseTime := time.Now().Add(-10 * time.Minute)
	writeDebugTestPoints(t, db, "cpu.usage", 10, baseTime)

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())

	session, err := engine.CreateReplay(ReplayRequest{
		Metric:    "cpu.usage",
		StartTime: baseTime.UnixNano(),
		EndTime:   baseTime.Add(9 * time.Second).UnixNano(),
		StepSize:  3 * time.Second,
	})
	if err != nil {
		t.Fatalf("CreateReplay: %v", err)
	}

	// NextFrame
	frame, err := engine.NextFrame(session.ID)
	if err != nil {
		t.Fatalf("NextFrame: %v", err)
	}
	if frame.Index != 0 {
		t.Errorf("expected first frame index 0, got %d", frame.Index)
	}

	// SeekFrame
	frame2, err := engine.SeekFrame(session.ID, 1)
	if err != nil {
		t.Fatalf("SeekFrame: %v", err)
	}
	if frame2.Index != 1 {
		t.Errorf("expected frame index 1, got %d", frame2.Index)
	}

	// SeekFrame out of range
	_, err = engine.SeekFrame(session.ID, 9999)
	if err == nil {
		t.Error("expected error for out-of-range seek")
	}

	// CloseReplay
	if err := engine.CloseReplay(session.ID); err != nil {
		t.Fatalf("CloseReplay: %v", err)
	}

	// NextFrame on closed session
	_, err = engine.NextFrame(session.ID)
	if err == nil {
		t.Error("expected error for closed session")
	}
}

func TestReplayValidation(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())

	// Missing metric
	_, err := engine.CreateReplay(ReplayRequest{
		StartTime: 1,
		EndTime:   2,
	})
	if err == nil {
		t.Error("expected error for missing metric")
	}

	// Start >= End
	_, err = engine.CreateReplay(ReplayRequest{
		Metric:    "cpu",
		StartTime: 100,
		EndTime:   50,
	})
	if err == nil {
		t.Error("expected error for start >= end")
	}
}

func TestCreateWhatIf(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())

	scenario, err := engine.CreateWhatIf(WhatIfScenario{
		Name:        "scale-cpu",
		Description: "What if CPU doubled",
		Modifications: []WhatIfModification{
			{
				Metric:    "cpu.usage",
				Operation: "scale",
				Factor:    2.0,
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateWhatIf: %v", err)
	}
	if scenario.ID == "" {
		t.Error("expected non-empty scenario ID")
	}
	if scenario.Name != "scale-cpu" {
		t.Errorf("expected name 'scale-cpu', got %q", scenario.Name)
	}
}

func TestCreateWhatIfValidation(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())

	_, err := engine.CreateWhatIf(WhatIfScenario{})
	if err == nil {
		t.Error("expected error for empty scenario name")
	}
}

func TestRunWhatIf(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	baseTime := time.Now().Add(-10 * time.Minute)
	writeDebugTestPoints(t, db, "cpu.usage", 5, baseTime)

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())

	scenario, _ := engine.CreateWhatIf(WhatIfScenario{
		Name: "double-cpu",
		Modifications: []WhatIfModification{
			{
				Metric:    "cpu.usage",
				Operation: "scale",
				Factor:    2.0,
			},
		},
	})

	result, err := engine.RunWhatIf(scenario.ID)
	if err != nil {
		t.Fatalf("RunWhatIf: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.ScenarioID != scenario.ID {
		t.Errorf("expected scenario ID %s, got %s", scenario.ID, result.ScenarioID)
	}

	// The modified avg should be ~2x the original avg
	origAvg := result.OriginalStats["cpu.usage_avg"]
	modAvg := result.ModifiedStats["cpu.usage_avg"]
	if origAvg > 0 && (modAvg < origAvg*1.9 || modAvg > origAvg*2.1) {
		t.Errorf("expected modified avg ~2x original (orig=%.2f, mod=%.2f)", origAvg, modAvg)
	}
}

func TestRunWhatIfNotFound(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())
	_, err := engine.RunWhatIf("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent scenario")
	}
}

func TestGetTimeline(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	baseTime := time.Now().Add(-10 * time.Minute)
	writeDebugTestPoints(t, db, "cpu.usage", 5, baseTime)

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())

	// Use zero start to avoid partition boundary issues
	end := baseTime.Add(10 * time.Second).UnixNano()

	timeline, err := engine.GetTimeline("cpu.usage", 0, end)
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if timeline == nil {
		t.Fatal("expected non-nil timeline")
	}
	if timeline.Metric != "cpu.usage" {
		t.Errorf("expected metric 'cpu.usage', got %q", timeline.Metric)
	}
	if len(timeline.Points) < 1 {
		t.Errorf("expected at least 1 point, got %d", len(timeline.Points))
	}
	if timeline.EndTime != end {
		t.Errorf("expected end %d, got %d", end, timeline.EndTime)
	}
}

func TestListReplaysAndScenarios(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	baseTime := time.Now().Add(-10 * time.Minute)
	writeDebugTestPoints(t, db, "cpu.usage", 5, baseTime)

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())

	// Initially empty
	if len(engine.ListReplays()) != 0 {
		t.Error("expected 0 replays initially")
	}
	if len(engine.ListScenarios()) != 0 {
		t.Error("expected 0 scenarios initially")
	}

	// Create one of each
	engine.CreateReplay(ReplayRequest{
		Metric:    "cpu.usage",
		StartTime: baseTime.UnixNano(),
		EndTime:   baseTime.Add(5 * time.Second).UnixNano(),
		StepSize:  time.Second,
	})
	engine.CreateWhatIf(WhatIfScenario{Name: "test-scenario"})

	if len(engine.ListReplays()) != 1 {
		t.Errorf("expected 1 replay, got %d", len(engine.ListReplays()))
	}
	if len(engine.ListScenarios()) != 1 {
		t.Errorf("expected 1 scenario, got %d", len(engine.ListScenarios()))
	}
}

func TestStatsTracking(t *testing.T) {
	db := setupDebugTestDB(t)
	defer db.Close()

	baseTime := time.Now().Add(-10 * time.Minute)
	writeDebugTestPoints(t, db, "cpu.usage", 5, baseTime)

	engine := NewTimeTravelDebugEngine(db, DefaultTimeTravelDebugConfig())

	// Initial stats should be zero
	stats := engine.Stats()
	if stats.DiffsComputed != 0 {
		t.Errorf("expected 0 diffs, got %d", stats.DiffsComputed)
	}

	// Run a diff
	from := baseTime.UnixNano()
	to := baseTime.Add(5 * time.Second).UnixNano()
	engine.Diff("cpu.usage", from, to)

	stats = engine.Stats()
	if stats.DiffsComputed != 1 {
		t.Errorf("expected 1 diff, got %d", stats.DiffsComputed)
	}
	if stats.CacheMisses < 1 {
		t.Errorf("expected at least 1 cache miss, got %d", stats.CacheMisses)
	}

	// Run again to test cache hit
	engine.Diff("cpu.usage", from, to)
	stats = engine.Stats()
	if stats.CacheHits < 1 {
		t.Errorf("expected at least 1 cache hit, got %d", stats.CacheHits)
	}

	// Create replay
	engine.CreateReplay(ReplayRequest{
		Metric:    "cpu.usage",
		StartTime: from,
		EndTime:   to,
		StepSize:  time.Second,
	})
	stats = engine.Stats()
	if stats.ReplaysCreated != 1 {
		t.Errorf("expected 1 replay, got %d", stats.ReplaysCreated)
	}

	// Run what-if
	scenario, _ := engine.CreateWhatIf(WhatIfScenario{
		Name:          "s1",
		Modifications: []WhatIfModification{{Metric: "cpu.usage", Operation: "scale", Factor: 2}},
	})
	engine.RunWhatIf(scenario.ID)
	stats = engine.Stats()
	if stats.ScenariosRun != 1 {
		t.Errorf("expected 1 scenario, got %d", stats.ScenariosRun)
	}
}

func TestDiffChangeTypeString(t *testing.T) {
	tests := []struct {
		ct   DiffChangeType
		want string
	}{
		{DiffUnchanged, "unchanged"},
		{DiffAdded, "added"},
		{DiffRemoved, "removed"},
		{DiffModified, "modified"},
		{DiffChangeType(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.ct.String(); got != tt.want {
			t.Errorf("DiffChangeType(%d).String() = %q, want %q", tt.ct, got, tt.want)
		}
	}
}

func TestApplyModification(t *testing.T) {
	tests := []struct {
		value float64
		mod   WhatIfModification
		want  float64
	}{
		{10, WhatIfModification{Operation: "scale", Factor: 2}, 20},
		{10, WhatIfModification{Operation: "shift", Value: 5}, 15},
		{10, WhatIfModification{Operation: "replace", Value: 42}, 42},
		{10, WhatIfModification{Operation: "drop"}, 0},
		{10, WhatIfModification{Operation: "unknown"}, 10},
	}
	for _, tt := range tests {
		got := applyModification(tt.value, tt.mod)
		if got != tt.want {
			t.Errorf("applyModification(%.1f, %q) = %.1f, want %.1f", tt.value, tt.mod.Operation, got, tt.want)
		}
	}
}
