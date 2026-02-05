package cep

import (
	"testing"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

func TestCEPConfig(t *testing.T) {
	config := DefaultCEPConfig()

	if config.MaxWindowSize == 0 {
		t.Error("MaxWindowSize should be set")
	}
	if config.MaxPendingEvents == 0 {
		t.Error("MaxPendingEvents should be set")
	}
	if config.WatermarkDelay == 0 {
		t.Error("WatermarkDelay should be set")
	}
}

func TestWindowType(t *testing.T) {
	tests := []struct {
		wt       WindowType
		expected string
	}{
		{WindowTumbling, "tumbling"},
		{WindowSliding, "sliding"},
		{WindowSession, "session"},
		{WindowCount, "count"},
	}

	for _, tc := range tests {
		if tc.wt.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.wt.String())
		}
	}
}

func TestCEPResult(t *testing.T) {
	result := CEPResult{
		QueryID:     "query1",
		WindowStart: time.Now().Add(-time.Hour).UnixNano(),
		WindowEnd:   time.Now().UnixNano(),
		Values:      map[string]float64{"avg": 42.5},
		GroupKey:    map[string]string{"host": "server1"},
	}

	if result.QueryID != "query1" {
		t.Errorf("Expected query1, got %s", result.QueryID)
	}
	if result.Values["avg"] != 42.5 {
		t.Errorf("Expected avg 42.5, got %f", result.Values["avg"])
	}
}

func TestCEPWindow(t *testing.T) {
	window := &CEPWindow{
		ID:       "window1",
		Type:     WindowTumbling,
		Size:     time.Minute,
		Metric:   "cpu",
		Tags:     map[string]string{"host": "server1"},
		Function: chronicle.AggMean,
		GroupBy:  []string{"host"},
	}

	if window.Type != WindowTumbling {
		t.Errorf("Expected WindowTumbling, got %v", window.Type)
	}
	if window.Size != time.Minute {
		t.Errorf("Expected 1 minute, got %v", window.Size)
	}
}

func TestWindowConfig(t *testing.T) {
	config := WindowConfig{
		Type:     WindowSliding,
		Size:     5 * time.Minute,
		Slide:    time.Minute,
		Metric:   "memory",
		Function: chronicle.AggSum,
	}

	if config.Type != WindowSliding {
		t.Errorf("Expected WindowSliding, got %v", config.Type)
	}
	if config.Slide != time.Minute {
		t.Errorf("Expected 1 minute slide, got %v", config.Slide)
	}
}

func TestEventPattern(t *testing.T) {
	pattern := EventPattern{
		Name:       "spike_detection",
		WithinTime: time.Minute,
		Sequence: []PatternStep{
			{
				Name:   "step1",
				Metric: "temperature",
			},
		},
	}

	if pattern.Name != "spike_detection" {
		t.Errorf("Expected spike_detection, got %s", pattern.Name)
	}
	if len(pattern.Sequence) != 1 {
		t.Errorf("Expected 1 step, got %d", len(pattern.Sequence))
	}
}

func TestPatternStep(t *testing.T) {
	step := PatternStep{
		Name:   "high_cpu",
		Metric: "cpu",
		Tags:   map[string]string{"host": "server1"},
	}

	if step.Name != "high_cpu" {
		t.Errorf("Expected high_cpu, got %s", step.Name)
	}
	if step.Metric != "cpu" {
		t.Errorf("Expected cpu, got %s", step.Metric)
	}
}

func TestWindowBuilder(t *testing.T) {
	builder := NewWindowBuilder()

	config := builder.
		Tumbling(5*time.Minute).
		OnMetric("cpu").
		WithTags(map[string]string{"host": "server1"}).
		Aggregate(chronicle.AggMean).
		GroupBy("host", "region").
		Build()

	if config.Type != WindowTumbling {
		t.Errorf("Expected WindowTumbling, got %v", config.Type)
	}
	if config.Size != 5*time.Minute {
		t.Errorf("Expected 5 minutes, got %v", config.Size)
	}
	if config.Metric != "cpu" {
		t.Errorf("Expected cpu, got %s", config.Metric)
	}
	if len(config.GroupBy) != 2 {
		t.Errorf("Expected 2 group by fields, got %d", len(config.GroupBy))
	}
}

func TestCEPStats(t *testing.T) {
	stats := CEPStats{
		WindowCount:  5,
		PatternCount: 3,
		QueryCount:   10,
		Watermark:    time.Now().UnixNano(),
	}

	if stats.WindowCount != 5 {
		t.Errorf("Expected 5 windows, got %d", stats.WindowCount)
	}
	if stats.PatternCount != 3 {
		t.Errorf("Expected 3 patterns, got %d", stats.PatternCount)
	}
}

func TestChangeEvent(t *testing.T) {
	event := chronicle.ChangeEvent{
		Operation: "INSERT",
		Timestamp: time.Now().UnixNano(),
		Before:    nil,
		After:     &chronicle.Point{Metric: "cpu", Value: 50},
	}

	if event.Operation != "INSERT" {
		t.Errorf("Expected INSERT, got %s", event.Operation)
	}
	if event.Before != nil {
		t.Error("Before should be nil for INSERT")
	}
	if event.After == nil {
		t.Error("After should not be nil for INSERT")
	}
}
