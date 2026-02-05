package chronicle

import (
	"math"
	"testing"
)

func TestProfileValues_Monotonic(t *testing.T) {
	values := make([]float64, 100)
	for i := range values {
		values[i] = float64(i)
	}
	p := ProfileValues("ts", values)
	if p.Type != ColumnTypeMonotonic {
		t.Errorf("expected monotonic, got %s", p.Type)
	}
	if p.MonotonicScore < 0.95 {
		t.Errorf("monotonic score = %.2f, want >= 0.95", p.MonotonicScore)
	}
}

func TestProfileValues_Constant(t *testing.T) {
	values := make([]float64, 50)
	for i := range values {
		values[i] = 42.0
	}
	p := ProfileValues("const", values)
	if p.Type != ColumnTypeConstant {
		t.Errorf("expected constant, got %s", p.Type)
	}
}

func TestProfileValues_LowCardinality(t *testing.T) {
	values := make([]float64, 1000)
	for i := range values {
		values[i] = float64(i % 5) // 5 unique values
	}
	p := ProfileValues("tags", values)
	if p.Type != ColumnTypeLowCardinality {
		t.Errorf("expected low_cardinality, got %s", p.Type)
	}
	if p.Cardinality != 5 {
		t.Errorf("cardinality = %d, want 5", p.Cardinality)
	}
}

func TestProfileValues_Sparse(t *testing.T) {
	values := make([]float64, 100)
	for i := range values {
		if i%10 == 0 {
			values[i] = float64(i)
		} else {
			values[i] = math.NaN()
		}
	}
	p := ProfileValues("sparse", values)
	if p.Type != ColumnTypeSparse {
		t.Errorf("expected sparse, got %s", p.Type)
	}
	if p.NullFraction < 0.5 {
		t.Errorf("null fraction = %.2f, want >= 0.5", p.NullFraction)
	}
}

func TestProfileValues_Empty(t *testing.T) {
	p := ProfileValues("empty", nil)
	if p.Type != ColumnTypeUnknown {
		t.Errorf("expected unknown, got %s", p.Type)
	}
}

func TestRecommendCodec(t *testing.T) {
	tests := []struct {
		name     string
		colType  ColumnType
		expected CodecType
	}{
		{"constant", ColumnTypeConstant, CodecRLE},
		{"monotonic", ColumnTypeMonotonic, CodecDeltaDelta},
		{"low_card", ColumnTypeLowCardinality, CodecDictionary},
		{"sparse", ColumnTypeSparse, CodecRLE},
		{"gaussian", ColumnTypeGaussian, CodecGorilla},
		{"random", ColumnTypeRandom, CodecSnappy},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := ColumnProfile{Name: tt.name, Type: tt.colType}
			rec := RecommendCodec(p)
			if rec.Codec != tt.expected {
				t.Errorf("codec = %v, want %v", rec.Codec, tt.expected)
			}
			if rec.Reason == "" {
				t.Error("expected non-empty reason")
			}
		})
	}
}

func TestWorkloadLearner_Basic(t *testing.T) {
	wl := NewWorkloadLearner()

	// Initially no data — defaults to Snappy
	rec := wl.BestCodec("cpu")
	if rec.Codec != CodecSnappy {
		t.Errorf("expected Snappy default, got %v", rec.Codec)
	}

	// Record trials
	for i := 0; i < 10; i++ {
		wl.RecordTrial("cpu", CodecGorilla, 5.0, 100.0)
		wl.RecordTrial("cpu", CodecSnappy, 2.0, 200.0)
	}

	rec = wl.BestCodec("cpu")
	if rec.Reason != "learned from workload history" {
		t.Errorf("expected learned reason, got %q", rec.Reason)
	}
}

func TestWorkloadLearner_ProfileFallback(t *testing.T) {
	wl := NewWorkloadLearner()

	p := ColumnProfile{Name: "ts", Type: ColumnTypeMonotonic}
	wl.UpdateProfile(p)

	rec := wl.BestCodec("ts")
	if rec.Codec != CodecDeltaDelta {
		t.Errorf("expected DeltaDelta from profile fallback, got %v", rec.Codec)
	}
}

func TestWorkloadLearner_Columns(t *testing.T) {
	wl := NewWorkloadLearner()
	wl.UpdateProfile(ColumnProfile{Name: "b"})
	wl.UpdateProfile(ColumnProfile{Name: "a"})

	cols := wl.Columns()
	if len(cols) != 2 || cols[0] != "a" || cols[1] != "b" {
		t.Errorf("columns = %v, want [a b]", cols)
	}
}

func TestColumnType_String(t *testing.T) {
	if ColumnTypeMonotonic.String() != "monotonic" {
		t.Error("unexpected string")
	}
	if ColumnType(99).String() != "unknown" {
		t.Error("unexpected string for out-of-range")
	}
}
