package chronicle

import (
	"testing"
)

func TestAdaptiveCompressionConfig(t *testing.T) {
	config := DefaultAdaptiveCompressionConfig()

	if !config.Enabled {
		t.Error("Adaptive compression should be enabled by default")
	}
	if config.AnalysisWindow == 0 {
		t.Error("AnalysisWindow should be set")
	}
	if config.MinCompressionRatio == 0 {
		t.Error("MinCompressionRatio should be set")
	}
}

func TestCodecType(t *testing.T) {
	tests := []struct {
		codec    CodecType
		expected string
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
	}

	for _, tc := range tests {
		if tc.codec.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.codec.String())
		}
	}
}

func TestDataCharacteristics(t *testing.T) {
	chars := DataCharacteristics{
		DataType:      "float64",
		Entropy:       0.3,
		Cardinality:   100,
		Monotonicity:  0.9,
		MeanDelta:     1.5,
		DeltaVariance: 0.1,
		RepeatRatio:   0.1,
		Range:         100.0,
		BitsRequired:  8,
	}

	if chars.DataType != "float64" {
		t.Errorf("Expected float64, got %s", chars.DataType)
	}
	if chars.Entropy != 0.3 {
		t.Errorf("Expected entropy 0.3, got %f", chars.Entropy)
	}
	if chars.Monotonicity != 0.9 {
		t.Errorf("Expected monotonicity 0.9, got %f", chars.Monotonicity)
	}
}

func TestAdaptiveCompressionEngine(t *testing.T) {
	config := DefaultAdaptiveCompressionConfig()
	engine := NewAdaptiveCompressionEngine(config)

	if engine == nil {
		t.Fatal("Failed to create AdaptiveCompressionEngine")
	}

	// Start engine
	if err := engine.Start(); err != nil {
		t.Errorf("Failed to start engine: %v", err)
	}

	// Stop engine
	if err := engine.Stop(); err != nil {
		t.Errorf("Failed to stop engine: %v", err)
	}
}

func TestCompressDecompressGzip(t *testing.T) {
	config := DefaultAdaptiveCompressionConfig()
	engine := NewAdaptiveCompressionEngine(config)

	data := []byte("Hello, World! This is test data for compression.")

	compressed, err := engine.Compress(data, CodecGzip)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	decompressed, err := engine.Decompress(compressed, CodecGzip)
	if err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}

	if string(decompressed) != string(data) {
		t.Error("Decompressed data doesn't match original")
	}
}

func TestCompressDecompressRLE(t *testing.T) {
	config := DefaultAdaptiveCompressionConfig()
	engine := NewAdaptiveCompressionEngine(config)

	// Data with repeating bytes
	data := []byte{1, 1, 1, 1, 2, 2, 3, 3, 3}

	compressed, err := engine.Compress(data, CodecRLE)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	decompressed, err := engine.Decompress(compressed, CodecRLE)
	if err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}

	if len(decompressed) != len(data) {
		t.Errorf("Expected %d bytes, got %d", len(data), len(decompressed))
	}
}

func TestCompressDecompressDictionary(t *testing.T) {
	config := DefaultAdaptiveCompressionConfig()
	engine := NewAdaptiveCompressionEngine(config)

	data := []byte("abcabcabcabc")

	compressed, err := engine.Compress(data, CodecDictionary)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	decompressed, err := engine.Decompress(compressed, CodecDictionary)
	if err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}

	if string(decompressed) != string(data) {
		t.Error("Decompressed data doesn't match original")
	}
}

func TestCodecSelection(t *testing.T) {
	config := DefaultAdaptiveCompressionConfig()
	engine := NewAdaptiveCompressionEngine(config)

	// Set a codec for a column
	engine.SetCodec("column1", CodecGorilla)

	codec := engine.GetSelectedCodec("column1")
	if codec != CodecGorilla {
		t.Errorf("Expected CodecGorilla, got %v", codec)
	}
}

func TestCompressionStats(t *testing.T) {
	config := DefaultAdaptiveCompressionConfig()
	engine := NewAdaptiveCompressionEngine(config)

	// Set some codecs
	engine.SetCodec("col1", CodecGorilla)
	engine.SetCodec("col2", CodecDeltaDelta)

	stats := engine.Stats()

	if stats.ColumnCount != 2 {
		t.Errorf("Expected 2 columns, got %d", stats.ColumnCount)
	}
}

func TestCodecPerformance(t *testing.T) {
	perf := CodecPerformance{
		Codec:            CodecGzip,
		CompressionRatio: 3.5,
		InputSize:        1000,
		OutputSize:       286,
	}

	if perf.CompressionRatio != 3.5 {
		t.Errorf("Expected ratio 3.5, got %f", perf.CompressionRatio)
	}
	if perf.Codec != CodecGzip {
		t.Errorf("Expected CodecGzip, got %v", perf.Codec)
	}
}

func TestCodecSelectionModel(t *testing.T) {
	model := NewCodecSelectionModel()

	// Test with empty history
	codec := model.PredictBestCodec(nil)
	if codec != CodecSnappy {
		t.Errorf("Expected default CodecSnappy, got %v", codec)
	}

	// Test with history - model uses weighted recency
	// With equal recent data, the one with more recent high performance wins
	history := []CodecPerformance{
		{Codec: CodecSnappy, CompressionRatio: 2.0}, // weight 0.33
		{Codec: CodecSnappy, CompressionRatio: 2.0}, // weight 0.67
		{Codec: CodecGzip, CompressionRatio: 4.0},   // weight 1.0 (most recent)
	}

	codec = model.PredictBestCodec(history)
	// Gzip should be selected due to more recent high performance
	if codec != CodecGzip {
		t.Errorf("Expected CodecGzip, got %v", codec)
	}
}

func TestBitsRequired(t *testing.T) {
	tests := []struct {
		n        uint64
		expected int
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{7, 3},
		{8, 4},
		{255, 8},
		{256, 9},
	}

	for _, tc := range tests {
		result := bitsRequired(tc.n)
		if result != tc.expected {
			t.Errorf("bitsRequired(%d) = %d, expected %d", tc.n, result, tc.expected)
		}
	}
}

func TestAnalyzeFloat64Data(t *testing.T) {
	config := DefaultAdaptiveCompressionConfig()
	engine := NewAdaptiveCompressionEngine(config)

	// Create float64 data
	data := make([]byte, 80) // 10 float64 values
	// Fill with sequential values (would be encoded properly in real usage)

	chars := engine.analyzeData(data, "float64")

	if chars.DataType != "float64" {
		t.Errorf("Expected float64, got %s", chars.DataType)
	}
}

func TestAnalyzeInt64Data(t *testing.T) {
	config := DefaultAdaptiveCompressionConfig()
	engine := NewAdaptiveCompressionEngine(config)

	data := make([]byte, 80) // 10 int64 values

	chars := engine.analyzeData(data, "int64")

	if chars.DataType != "int64" {
		t.Errorf("Expected int64, got %s", chars.DataType)
	}
}

func TestAnalyzeStringData(t *testing.T) {
	config := DefaultAdaptiveCompressionConfig()
	engine := NewAdaptiveCompressionEngine(config)

	data := []byte("hello world hello world")

	chars := engine.analyzeData(data, "string")

	if chars.DataType != "string" {
		t.Errorf("Expected string, got %s", chars.DataType)
	}
	if chars.Cardinality == 0 {
		t.Error("Cardinality should be calculated")
	}
}

func TestSelectCodecFromCharacteristics(t *testing.T) {
	config := DefaultAdaptiveCompressionConfig()
	engine := NewAdaptiveCompressionEngine(config)

	// Float with low delta variance -> Gorilla
	floatChars := DataCharacteristics{
		DataType:      "float64",
		DeltaVariance: 0.05,
	}
	codec := engine.selectCodecFromCharacteristics(floatChars)
	if codec != CodecGorilla {
		t.Errorf("Expected CodecGorilla for low variance floats, got %v", codec)
	}

	// Monotonic int -> DeltaDelta
	intChars := DataCharacteristics{
		DataType:     "int64",
		Monotonicity: 0.95,
	}
	codec = engine.selectCodecFromCharacteristics(intChars)
	if codec != CodecDeltaDelta {
		t.Errorf("Expected CodecDeltaDelta for monotonic ints, got %v", codec)
	}

	// Low cardinality string -> Dictionary
	stringChars := DataCharacteristics{
		DataType:    "string",
		Cardinality: 50,
	}
	codec = engine.selectCodecFromCharacteristics(stringChars)
	if codec != CodecDictionary {
		t.Errorf("Expected CodecDictionary for low cardinality strings, got %v", codec)
	}
}
