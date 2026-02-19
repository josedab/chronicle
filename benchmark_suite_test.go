package chronicle

import (
	"os"
	"strings"
	"testing"
)

func TestBenchmarkSuite_Compression(t *testing.T) {
	suite := NewBenchmarkSuite()
	result, err := suite.RunCompressionBenchmark(10000)
	if err != nil {
		t.Fatal(err)
	}
	if result.Name != "compression_ratio" {
		t.Errorf("name = %q", result.Name)
	}
	if result.Operations != 10000 {
		t.Errorf("ops = %d, want 10000", result.Operations)
	}
	if result.ThroughputMBs <= 0 {
		t.Error("expected positive throughput")
	}
}

func TestBenchmarkSuite_Memory(t *testing.T) {
	suite := NewBenchmarkSuite()
	result := suite.RunMemoryBenchmark(1000)
	if result.Name != "memory_footprint" {
		t.Errorf("name = %q", result.Name)
	}
	if result.MemAllocBytes <= 0 {
		t.Error("expected positive allocation")
	}
}

func TestBenchmarkSuite_ColdStart(t *testing.T) {
	dir, err := os.MkdirTemp("", "bench-cold-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	suite := NewBenchmarkSuite()
	result, err := suite.RunColdStartBenchmark(dir+"/bench.db", 3)
	if err != nil {
		t.Fatal(err)
	}
	if result.Operations != 3 {
		t.Errorf("ops = %d, want 3", result.Operations)
	}
	if result.AvgLatencyUs <= 0 {
		t.Error("expected positive latency")
	}
}

func TestBenchmarkSuite_Results(t *testing.T) {
	suite := NewBenchmarkSuite()
	suite.RunCompressionBenchmark(1000)
	suite.RunMemoryBenchmark(1000)

	results := suite.Results()
	if len(results) != 2 {
		t.Errorf("results = %d, want 2", len(results))
	}
}

func TestBenchmarkSuite_FormatReport(t *testing.T) {
	suite := NewBenchmarkSuite()
	suite.RunCompressionBenchmark(1000)
	suite.RunMemoryBenchmark(1000)

	report := suite.FormatReport()
	if !strings.Contains(report, "Chronicle Benchmark Report") {
		t.Error("missing report header")
	}
	if !strings.Contains(report, "compression_ratio") {
		t.Error("missing compression result")
	}
	if !strings.Contains(report, "memory_footprint") {
		t.Error("missing memory result")
	}
}

func TestBenchmarkSuite_NilDB(t *testing.T) {
	suite := NewBenchmarkSuite()

	_, err := suite.RunWriteBenchmark(nil, 100, 10)
	if err == nil {
		t.Error("expected error for nil DB")
	}

	_, err = suite.RunQueryBenchmark(nil, 10)
	if err == nil {
		t.Error("expected error for nil DB")
	}
}

func TestBenchmarkSuite_ConcurrentWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark in short mode")
	}
	db, cleanup := setupBenchDB(t)
	defer cleanup()

	suite := NewBenchmarkSuite()
	result, err := suite.RunConcurrentWriteBenchmark(db, 4, 500)
	if err != nil {
		t.Fatal(err)
	}
	if result.Operations != 2000 {
		t.Errorf("ops = %d, want 2000", result.Operations)
	}
	if result.OpsPerSec <= 0 {
		t.Error("expected positive ops/sec")
	}
}

func TestBenchmarkSuite_Cardinality(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark in short mode")
	}
	db, cleanup := setupBenchDB(t)
	defer cleanup()

	suite := NewBenchmarkSuite()
	results, err := suite.RunCardinalityBenchmark(db, []int{10, 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestBenchmarkSuite_Aggregation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark in short mode")
	}
	db, cleanup := setupBenchDB(t)
	defer cleanup()

	suite := NewBenchmarkSuite()
	results, err := suite.RunAggregationBenchmark(db, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 6 {
		t.Errorf("expected 6 aggregation results, got %d", len(results))
	}
	for _, r := range results {
		if r.OpsPerSec <= 0 {
			t.Errorf("%s: expected positive ops/sec", r.Name)
		}
	}
}

func TestBenchmarkSuite_Retention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark in short mode")
	}
	db, cleanup := setupBenchDB(t)
	defer cleanup()

	suite := NewBenchmarkSuite()
	result, err := suite.RunRetentionBenchmark(db, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if result.Operations != 1000 {
		t.Errorf("ops = %d, want 1000", result.Operations)
	}
}

func setupBenchDB(t *testing.T) (*DB, func()) {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/bench.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	return db, func() { db.Close() }
}

func TestVarIntSize(t *testing.T) {
	tests := []struct {
		input    int64
		expected int
	}{
		{0, 1},
		{1, 1},
		{63, 1},
		{64, 2},
		{-1, 1},
	}
	for _, tt := range tests {
		got := varIntSize(tt.input)
		if got != tt.expected {
			t.Errorf("varIntSize(%d) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}
