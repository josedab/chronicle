package compression

import (
	"testing"
	"time"
)

func TestDefaultAdaptiveCompressionConfig(t *testing.T) {
	cfg := DefaultAdaptiveCompressionConfig()
	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.AnalysisWindow != 1000 {
		t.Errorf("expected AnalysisWindow 1000, got %d", cfg.AnalysisWindow)
	}
	if cfg.ReanalysisInterval != time.Hour {
		t.Errorf("expected ReanalysisInterval 1h, got %v", cfg.ReanalysisInterval)
	}
	if cfg.MinCompressionRatio != 1.5 {
		t.Errorf("expected MinCompressionRatio 1.5, got %f", cfg.MinCompressionRatio)
	}
}

func TestCodecTypeString(t *testing.T) {
	tests := []struct {
		codec CodecType
		want  string
	}{
		{CodecNone, "none"},
		{CodecGorilla, "gorilla"},
		{CodecDeltaDelta, "delta-delta"},
		{CodecDictionary, "dictionary"},
		{CodecRLE, "rle"},
		{CodecZSTD, "zstd"},
		{CodecLZ4, "lz4"},
		{CodecSnappy, "snappy"},
		{CodecGzip, "gzip"},
		{CodecBitPacking, "bit-packing"},
		{CodecFloatXOR, "float-xor"},
		{CodecType(999), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.codec.String(); got != tt.want {
			t.Errorf("CodecType(%d).String() = %q, want %q", tt.codec, got, tt.want)
		}
	}
}
