package chronicle

import (
	"math"
	"testing"
)

func TestClassifyPattern(t *testing.T) {
	config := DefaultAdaptiveCodecConfig()

	t.Run("constant", func(t *testing.T) {
		values := make([]float64, 100)
		for i := range values {
			values[i] = 42.0
		}
		stats := computeColumnStats(values)
		pattern := classifyPattern(values, stats, config)
		if pattern != PatternConstant {
			t.Errorf("expected constant, got %s", pattern)
		}
	})

	t.Run("monotonic increasing", func(t *testing.T) {
		values := make([]float64, 100)
		for i := range values {
			values[i] = float64(i)
		}
		stats := computeColumnStats(values)
		pattern := classifyPattern(values, stats, config)
		if pattern != PatternMonotonic {
			t.Errorf("expected monotonic, got %s", pattern)
		}
	})

	t.Run("sparse", func(t *testing.T) {
		values := make([]float64, 100)
		values[10] = 1.0
		values[50] = 2.0
		values[90] = 3.0
		stats := computeColumnStats(values)
		pattern := classifyPattern(values, stats, config)
		if pattern != PatternSparse {
			t.Errorf("expected sparse, got %s", pattern)
		}
	})

	t.Run("periodic", func(t *testing.T) {
		values := make([]float64, 100)
		for i := range values {
			values[i] = math.Sin(float64(i) / 5 * math.Pi)
		}
		stats := computeColumnStats(values)
		pattern := classifyPattern(values, stats, config)
		if pattern != PatternPeriodic {
			t.Errorf("expected periodic, got %s", pattern)
		}
	})

	t.Run("random", func(t *testing.T) {
		// Pseudo-random using a simple formula
		values := make([]float64, 100)
		x := 1.0
		for i := range values {
			x = math.Mod(x*1103515245+12345, 2147483648)
			values[i] = x
		}
		stats := computeColumnStats(values)
		pattern := classifyPattern(values, stats, config)
		if pattern != PatternRandom {
			t.Errorf("expected random, got %s", pattern)
		}
	})
}

func TestAdaptiveCodecSelector(t *testing.T) {
	selector := NewAdaptiveCodecSelector(DefaultAdaptiveCodecConfig())

	t.Run("profile monotonic column", func(t *testing.T) {
		values := make([]float64, 200)
		for i := range values {
			values[i] = float64(i)
		}
		profile := selector.ProfileColumn("cpu", "timestamp", values)
		if profile.Pattern != PatternMonotonic {
			t.Errorf("expected monotonic, got %s", profile.Pattern)
		}
		if profile.Codec != CodecDeltaDelta {
			t.Errorf("expected delta-of-delta codec for monotonic data")
		}
	})

	t.Run("profile constant column", func(t *testing.T) {
		values := make([]float64, 200)
		for i := range values {
			values[i] = 1.0
		}
		profile := selector.ProfileColumn("cpu", "constant", values)
		if profile.Pattern != PatternConstant {
			t.Errorf("expected constant, got %s", profile.Pattern)
		}
		if profile.Codec != CodecRLE {
			t.Errorf("expected RLE codec for constant data")
		}
	})

	t.Run("profile sparse column", func(t *testing.T) {
		values := make([]float64, 200)
		values[10] = 1.0
		values[50] = 2.0
		profile := selector.ProfileColumn("net", "errors", values)
		if profile.Pattern != PatternSparse {
			t.Errorf("expected sparse, got %s", profile.Pattern)
		}
		if profile.Codec != CodecBitPacking {
			t.Errorf("expected bit-packing codec for sparse data")
		}
	})

	t.Run("profile too few samples", func(t *testing.T) {
		values := []float64{1.0, 2.0}
		profile := selector.ProfileColumn("cpu", "tiny", values)
		if profile.Pattern != PatternUnknown {
			t.Errorf("expected unknown for small sample, got %s", profile.Pattern)
		}
	})

	t.Run("get profile", func(t *testing.T) {
		p := selector.GetProfile("cpu", "timestamp")
		if p == nil {
			t.Error("expected profile")
		}
	})

	t.Run("get all profiles", func(t *testing.T) {
		all := selector.GetAllProfiles()
		if len(all) == 0 {
			t.Error("expected non-empty profiles")
		}
	})

	t.Run("show compression", func(t *testing.T) {
		diags := selector.ShowCompression()
		if len(diags) == 0 {
			t.Error("expected diagnostics")
		}
	})

	t.Run("codec distribution", func(t *testing.T) {
		dist := selector.GetCodecDistribution()
		if len(dist) == 0 {
			t.Error("expected distribution")
		}
	})

	t.Run("re-evaluate on compaction", func(t *testing.T) {
		p := &Partition{
			series: map[string]*SeriesData{
				"test": {
					Series: Series{Metric: "test_metric"},
					Values: make([]float64, 100),
					Timestamps: make([]int64, 100),
				},
			},
		}
		for i := 0; i < 100; i++ {
			p.series["test"].Values[i] = float64(i)
			p.series["test"].Timestamps[i] = int64(1000 + i)
		}

		entries := selector.ReEvaluateOnCompaction("test_metric", p)
		if len(entries) == 0 {
			t.Error("expected codec entries from compaction")
		}
	})

	t.Run("re-evaluate nil partition", func(t *testing.T) {
		entries := selector.ReEvaluateOnCompaction("test", nil)
		if len(entries) != 0 {
			t.Error("expected nil result for nil partition")
		}
	})
}

func TestSelectCodecForPattern(t *testing.T) {
	tests := []struct {
		pattern DataPattern
		codec   CodecType
	}{
		{PatternMonotonic, CodecDeltaDelta},
		{PatternSparse, CodecBitPacking},
		{PatternConstant, CodecRLE},
		{PatternPeriodic, CodecGorilla},
		{PatternRandom, CodecGorilla},
		{PatternUnknown, CodecGorilla},
	}
	for _, tt := range tests {
		t.Run(tt.pattern.String(), func(t *testing.T) {
			codec := selectCodecForPattern(tt.pattern)
			if codec != tt.codec {
				t.Errorf("expected codec %d for %s, got %d", tt.codec, tt.pattern, codec)
			}
		})
	}
}

func TestDataPatternString(t *testing.T) {
	patterns := []DataPattern{PatternUnknown, PatternMonotonic, PatternSparse, PatternPeriodic, PatternRandom, PatternConstant}
	expected := []string{"unknown", "monotonic", "sparse", "periodic", "random", "constant"}
	for i, p := range patterns {
		if p.String() != expected[i] {
			t.Errorf("expected %s, got %s", expected[i], p.String())
		}
	}
}

func TestComputeColumnStats(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5}
	stats := computeColumnStats(values)
	if stats.Min != 1 || stats.Max != 5 {
		t.Errorf("expected min=1, max=5")
	}
	if stats.Mean != 3 {
		t.Errorf("expected mean=3, got %f", stats.Mean)
	}
	if stats.DistinctCount != 5 {
		t.Errorf("expected 5 distinct, got %d", stats.DistinctCount)
	}

	empty := computeColumnStats(nil)
	if empty.Min != 0 {
		t.Error("expected zero stats for nil")
	}
}
