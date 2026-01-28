package chronicle

import (
	"testing"
	"time"
)

func TestContinuousQuery_Struct(t *testing.T) {
	cq := ContinuousQuery{
		Name:         "avg_cpu_5m",
		SourceMetric: "cpu",
		Tags:         map[string]string{"env": "prod"},
		Function:     AggMean,
		Window:       5 * time.Minute,
		GroupBy:      []string{"host"},
		Every:        time.Minute,
		TargetMetric: "cpu_5m_avg",
	}

	if cq.Name != "avg_cpu_5m" {
		t.Errorf("Name = %q, want %q", cq.Name, "avg_cpu_5m")
	}
	if cq.Window != 5*time.Minute {
		t.Errorf("Window = %v, want %v", cq.Window, 5*time.Minute)
	}
	if cq.TargetMetric != "cpu_5m_avg" {
		t.Errorf("TargetMetric = %q, want %q", cq.TargetMetric, "cpu_5m_avg")
	}
}

func TestCQRunner_Execute(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        10,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Write some test data
	baseTime := time.Now().Add(-10 * time.Minute)
	for i := 0; i < 10; i++ {
		err := db.Write(Point{
			Metric:    "cpu",
			Tags:      map[string]string{"host": "server1"},
			Value:     float64(i * 10),
			Timestamp: baseTime.Add(time.Duration(i) * time.Minute).UnixNano(),
		})
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	cq := ContinuousQuery{
		Name:         "test_cq",
		SourceMetric: "cpu",
		Function:     AggMean,
		Window:       5 * time.Minute,
		Every:        time.Minute,
		TargetMetric: "cpu_avg",
	}

	runner := &cqRunner{
		db:   db,
		cq:   cq,
		stop: make(chan struct{}),
	}

	// Execute should not error
	err = runner.execute()
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
}

func TestCQRunner_DefaultInterval(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	cq := ContinuousQuery{
		SourceMetric: "cpu",
		Function:     AggSum,
		Window:       time.Minute,
		Every:        0, // Should default to 5 minutes
		TargetMetric: "cpu_sum",
	}

	runner := &cqRunner{
		db:   db,
		cq:   cq,
		stop: make(chan struct{}),
	}

	// Start runner in goroutine
	go runner.run()

	// Give it a moment then stop
	time.Sleep(10 * time.Millisecond)
	close(runner.stop)
}

func TestDB_StartStopContinuousQueries(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
		ContinuousQueries: []ContinuousQuery{
			{
				Name:         "cq1",
				SourceMetric: "metric1",
				Function:     AggMean,
				Window:       time.Minute,
				Every:        time.Second,
				TargetMetric: "metric1_avg",
			},
			{
				Name:         "cq2",
				SourceMetric: "metric2",
				Function:     AggSum,
				Window:       time.Minute,
				Every:        time.Second,
				TargetMetric: "metric2_sum",
			},
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Start continuous queries
	db.startContinuousQueries()

	// Verify runners were created
	db.lifecycle.mu.Lock()
	numRunners := len(db.lifecycle.cqRunners)
	db.lifecycle.mu.Unlock()

	if numRunners != 2 {
		t.Errorf("expected 2 runners, got %d", numRunners)
	}

	// Stop continuous queries
	db.stopContinuousQueries()

	db.lifecycle.mu.Lock()
	afterStop := db.lifecycle.cqRunners
	db.lifecycle.mu.Unlock()

	if afterStop != nil {
		t.Error("runners should be nil after stop")
	}

	db.Close()
}

func TestDB_StartContinuousQueries_NoQueries(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
		ContinuousQueries: nil,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Should not panic or error with no queries
	db.startContinuousQueries()

	db.lifecycle.mu.Lock()
	runners := db.lifecycle.cqRunners
	db.lifecycle.mu.Unlock()

	if runners != nil {
		t.Error("runners should be nil when no CQs configured")
	}
}

func TestDB_StartContinuousQueries_EmptyTarget(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
		ContinuousQueries: []ContinuousQuery{
			{
				Name:         "empty_target",
				SourceMetric: "cpu",
				Function:     AggMean,
				Window:       time.Minute,
				TargetMetric: "", // Empty - should be skipped
			},
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	db.startContinuousQueries()

	db.lifecycle.mu.Lock()
	numRunners := len(db.lifecycle.cqRunners)
	db.lifecycle.mu.Unlock()

	if numRunners != 0 {
		t.Errorf("expected 0 runners (empty target skipped), got %d", numRunners)
	}

	db.stopContinuousQueries()
}

func TestDB_TryMaterialized_NoMatch(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
		ContinuousQueries: []ContinuousQuery{
			{
				SourceMetric: "cpu",
				Function:     AggMean,
				Window:       5 * time.Minute,
				TargetMetric: "cpu_5m",
			},
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Query that doesn't match the CQ
	q := &Query{
		Metric: "memory", // Different metric
		Aggregation: &Aggregation{
			Function: AggMean,
			Window:   5 * time.Minute,
		},
	}

	result := db.tryMaterialized(q)
	if result.Metric != "memory" {
		t.Error("query should not be rewritten when no CQ matches")
	}
}

func TestDB_TryMaterialized_Match(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
		ContinuousQueries: []ContinuousQuery{
			{
				SourceMetric: "cpu",
				Function:     AggMean,
				Window:       5 * time.Minute,
				TargetMetric: "cpu_5m",
			},
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Query that matches the CQ
	q := &Query{
		Metric: "cpu",
		Aggregation: &Aggregation{
			Function: AggMean,
			Window:   5 * time.Minute,
		},
	}

	result := db.tryMaterialized(q)
	if result.Metric != "cpu_5m" {
		t.Errorf("expected metric to be rewritten to cpu_5m, got %s", result.Metric)
	}
	if result.Aggregation != nil {
		t.Error("aggregation should be nil after materialized match")
	}
}

func TestDB_TryMaterialized_NilQuery(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Should handle nil query gracefully
	result := db.tryMaterialized(nil)
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

func TestDB_TryMaterialized_NoAggregation(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	q := &Query{
		Metric: "cpu",
		// No aggregation
	}

	result := db.tryMaterialized(q)
	if result != q {
		t.Error("query without aggregation should be returned as-is")
	}
}

func TestDB_TryMaterialized_GroupByMatch(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
		ContinuousQueries: []ContinuousQuery{
			{
				SourceMetric: "cpu",
				Function:     AggMean,
				Window:       5 * time.Minute,
				GroupBy:      []string{"host", "region"},
				TargetMetric: "cpu_5m_by_host_region",
			},
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Query with matching groupBy
	q := &Query{
		Metric: "cpu",
		Aggregation: &Aggregation{
			Function: AggMean,
			Window:   5 * time.Minute,
		},
		GroupBy: []string{"host", "region"},
	}

	result := db.tryMaterialized(q)
	if result.Metric != "cpu_5m_by_host_region" {
		t.Errorf("expected metric rewrite, got %s", result.Metric)
	}

	// Query with different groupBy should not match
	q2 := &Query{
		Metric: "cpu",
		Aggregation: &Aggregation{
			Function: AggMean,
			Window:   5 * time.Minute,
		},
		GroupBy: []string{"host"}, // Different
	}

	result2 := db.tryMaterialized(q2)
	if result2.Metric != "cpu" {
		t.Error("query with different groupBy should not be rewritten")
	}
}
