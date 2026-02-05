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
