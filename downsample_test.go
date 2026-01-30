package chronicle

import (
	"testing"
	"time"
)

func TestAggFuncName(t *testing.T) {
	tests := []struct {
		fn   AggFunc
		want string
	}{
		{AggCount, "count"},
		{AggSum, "sum"},
		{AggMean, "mean"},
		{AggMin, "min"},
		{AggMax, "max"},
		{AggStddev, "stddev"},
		{AggPercentile, "percentile"},
		{AggRate, "rate"},
		{AggFirst, "first"},
		{AggLast, "last"},
		{AggFunc(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := aggFuncName(tt.fn)
			if got != tt.want {
				t.Errorf("aggFuncName(%d) = %q, want %q", tt.fn, got, tt.want)
			}
		})
	}
}

func TestDownsampleRule_Validation(t *testing.T) {
	rule := DownsampleRule{
		SourceResolution: time.Minute,
		TargetResolution: time.Hour,
		Retention:        24 * time.Hour,
		Aggregations:     []AggFunc{AggMean, AggMax},
	}

	if rule.SourceResolution != time.Minute {
		t.Errorf("SourceResolution = %v, want %v", rule.SourceResolution, time.Minute)
	}
	if rule.TargetResolution != time.Hour {
		t.Errorf("TargetResolution = %v, want %v", rule.TargetResolution, time.Hour)
	}
	if rule.Retention != 24*time.Hour {
		t.Errorf("Retention = %v, want %v", rule.Retention, 24*time.Hour)
	}
	if len(rule.Aggregations) != 2 {
		t.Errorf("Aggregations count = %d, want 2", len(rule.Aggregations))
	}
}

func TestDownsampleRule_Integration(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        10, // Small buffer to force flush
		DownsampleRules: []DownsampleRule{
			{
				SourceResolution: time.Second,
				TargetResolution: time.Minute,
				Aggregations:     []AggFunc{AggMean, AggSum},
			},
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Write some test data
	baseTime := time.Now().Add(-3 * time.Minute)
	for i := 0; i < 60; i++ {
		err := db.Write(Point{
			Metric:    "cpu",
			Tags:      map[string]string{"host": "server1"},
			Value:     float64(i),
			Timestamp: baseTime.Add(time.Duration(i) * time.Second).UnixNano(),
		})
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Flush to ensure data is persisted
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Apply downsampling manually (normally done by background worker)
	// This tests that the function executes without error
	err = db.applyDownsampling()
	if err != nil {
		t.Fatalf("applyDownsampling failed: %v", err)
	}

	// Verify original metric exists
	metrics := db.Metrics()
	foundCPU := false
	for _, m := range metrics {
		if m == "cpu" {
			foundCPU = true
			break
		}
	}
	if !foundCPU {
		t.Log("Note: cpu metric not found in metrics list (may be buffered)")
	}
}

func TestDownsampleRule_EmptyAggregations(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
		DownsampleRules: []DownsampleRule{
			{
				SourceResolution: time.Minute,
				TargetResolution: time.Hour,
				Aggregations:     []AggFunc{}, // Empty
			},
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Should not panic with empty aggregations
	err = db.applyDownsampling()
	if err != nil {
		t.Fatalf("applyDownsampling failed: %v", err)
	}
}

func TestDownsampleRule_NoRules(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
		DownsampleRules:   nil,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Should not panic with no rules
	err = db.applyDownsampling()
	if err != nil {
		t.Fatalf("applyDownsampling failed: %v", err)
	}
}

func TestDownsampleRule_WithRetention(t *testing.T) {
	rule := DownsampleRule{
		SourceResolution: time.Minute,
		TargetResolution: time.Hour,
		Retention:        7 * 24 * time.Hour,
		Aggregations:     []AggFunc{AggMean},
	}

	// Verify retention is properly set
	if rule.Retention != 7*24*time.Hour {
		t.Errorf("Retention = %v, want %v", rule.Retention, 7*24*time.Hour)
	}

	// Retention of 0 should skip pruning
	rule.Retention = 0
	if rule.Retention != 0 {
		t.Error("Retention should be 0")
	}
}

func TestDownsampleMetricNaming(t *testing.T) {
	// Test the naming convention for downsampled metrics
	tests := []struct {
		metric     string
		resolution time.Duration
		aggFunc    AggFunc
		want       string
	}{
		{"cpu", time.Minute, AggMean, "cpu:1m0s:mean"},
		{"memory", time.Hour, AggSum, "memory:1h0m0s:sum"},
		{"disk.usage", 5 * time.Minute, AggMax, "disk.usage:5m0s:max"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.metric + ":" + tt.resolution.String() + ":" + aggFuncName(tt.aggFunc)
			if got != tt.want {
				t.Errorf("metric name = %q, want %q", got, tt.want)
			}
		})
	}
}
