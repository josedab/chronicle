package gpucompression

import (
	"encoding/binary"
	"math"
	"testing"

	chronicle "github.com/chronicle-db/chronicle"
)

func newTestGPUCompressor(t *testing.T) *GPUCompressor {
	t.Helper()
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true
	config.AsyncMode = false // Avoid pipeline workers for simple tests
	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("NewGPUCompressor: %v", err)
	}
	return gc
}

// --- Zero-length input ---

func TestCompress_ZeroLength(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	result, err := gc.Compress([]byte{}, chronicle.CodecGorilla)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Errorf("Expected empty result for empty input, got %d bytes", len(result))
	}
}

func TestCompress_NilInput(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	result, err := gc.Compress(nil, chronicle.CodecGorilla)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Errorf("Expected nil result for nil input, got %d bytes", len(result))
	}
}

func TestCompressBatch_EmptyBatches(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	result, err := gc.CompressBatch([][]byte{}, chronicle.CodecGorilla)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Errorf("Expected empty result for empty batch, got %d", len(result))
	}
}

func TestCompressBatch_NilBatches(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	result, err := gc.CompressBatch(nil, chronicle.CodecGorilla)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Errorf("Expected nil result for nil batch")
	}
}

// --- shouldUseGPU ---

func TestShouldUseGPU_Disabled(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.Enabled = false
	config.FallbackToCPU = true
	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatal(err)
	}
	defer gc.Stop()

	if gc.shouldUseGPU(1024 * 1024) {
		t.Error("shouldUseGPU should be false when disabled")
	}
}

func TestShouldUseGPU_BelowMinBatch(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	if gc.shouldUseGPU(100) {
		t.Error("shouldUseGPU should be false for small data")
	}
}

func TestShouldUseGPU_ExceedsMemoryLimit(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true
	config.MemoryLimit = 1024
	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatal(err)
	}
	defer gc.Stop()

	if gc.shouldUseGPU(2048) {
		t.Error("shouldUseGPU should be false when exceeding memory limit")
	}
}

// --- parallelXOR edge cases ---

func TestParallelXOR_SingleValue(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	result := gc.parallelXOR([]uint64{42})
	if result != nil {
		t.Error("Single value should return nil XOR result")
	}
}

func TestParallelXOR_TwoValues(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	result := gc.parallelXOR([]uint64{0xFF, 0x0F})
	if len(result) != 1 {
		t.Fatalf("Expected 1 XOR value, got %d", len(result))
	}
	expected := uint64(0xFF ^ 0x0F)
	if result[0] != expected {
		t.Errorf("XOR = %X, want %X", result[0], expected)
	}
}

func TestParallelXOR_IdenticalValues(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	values := make([]uint64, 100)
	for i := range values {
		values[i] = 42
	}

	result := gc.parallelXOR(values)
	for i, xor := range result {
		if xor != 0 {
			t.Errorf("XOR[%d] = %d, want 0 for identical values", i, xor)
		}
	}
}

func TestParallelXOR_LargeBatch(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	values := make([]uint64, 10000)
	for i := range values {
		values[i] = uint64(i * 1000)
	}

	result := gc.parallelXOR(values)
	if len(result) != 9999 {
		t.Errorf("Expected 9999 XOR values, got %d", len(result))
	}

	for i := range result {
		expected := values[i] ^ values[i+1]
		if result[i] != expected {
			t.Errorf("XOR[%d] = %d, want %d", i, result[i], expected)
			break
		}
	}
}

// --- parallelDelta edge cases ---

func TestParallelDelta_SingleValue(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	result := gc.parallelDelta([]int64{100})
	if result != nil {
		t.Error("Single value should return nil deltas")
	}
}

func TestParallelDelta_TwoValues(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	result := gc.parallelDelta([]int64{100, 150})
	if len(result) != 1 {
		t.Fatalf("Expected 1 delta, got %d", len(result))
	}
	if result[0] != 50 {
		t.Errorf("Delta = %d, want 50", result[0])
	}
}

func TestParallelDelta_MonotonicIncreasing(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	values := make([]int64, 1000)
	for i := range values {
		values[i] = int64(i) * 10
	}

	result := gc.parallelDelta(values)
	for i, d := range result {
		if d != 10 {
			t.Errorf("Delta[%d] = %d, want 10", i, d)
			break
		}
	}
}

func TestParallelDelta_NegativeDeltas(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	result := gc.parallelDelta([]int64{100, 50, 25})
	if result[0] != -50 {
		t.Errorf("Delta[0] = %d, want -50", result[0])
	}
	if result[1] != -25 {
		t.Errorf("Delta[1] = %d, want -25", result[1])
	}
}

// --- parallelDeltaOfDelta ---

func TestParallelDeltaOfDelta_SingleDelta(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	result := gc.parallelDeltaOfDelta([]int64{10})
	if result != nil {
		t.Error("Single delta should return nil DOD")
	}
}

func TestParallelDeltaOfDelta_ConstantDeltas(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	deltas := make([]int64, 100)
	for i := range deltas {
		deltas[i] = 10
	}

	result := gc.parallelDeltaOfDelta(deltas)
	for i, dod := range result {
		if dod != 0 {
			t.Errorf("DOD[%d] = %d, want 0 for constant deltas", i, dod)
			break
		}
	}
}

// --- GPU backend detection ---

func TestDetectGPUBackend_NoGPU(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	backend := gc.detectGPUBackend()
	_ = backend // Ensure no panic
}

func TestIsGPUAvailable(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	_ = gc.IsGPUAvailable()
}

func TestGetBackend(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	backend := gc.GetBackend()
	_ = backend
}

func TestGetDeviceInfo(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	info := gc.GetDeviceInfo()
	if info == nil {
		return
	}
	if info.Name == "" {
		t.Error("Device name should not be empty")
	}
}

// --- NewGPUCompressor with GPU disabled ---

func TestNewGPUCompressor_Disabled(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.Enabled = false
	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatal(err)
	}
	defer gc.Stop()

	if gc.initialized {
		t.Error("GPU should not be initialized when disabled")
	}
}

func TestNewGPUCompressor_NoFallback(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.Enabled = true
	config.FallbackToCPU = false
	_, err := NewGPUCompressor(config)
	if err == nil {
		// GPU might be available on some systems
		return
	}
}

// --- Stats ---

func TestGetStats_Initial(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	stats := gc.GetStats()
	if stats.GPUCompressions != 0 {
		t.Errorf("Initial GPUCompressions = %d, want 0", stats.GPUCompressions)
	}
	if stats.CPUFallbacks != 0 {
		t.Errorf("Initial CPUFallbacks = %d, want 0", stats.CPUFallbacks)
	}
}

func TestGetStats_AfterCompression(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	data := edgeGenerateFloat64Data(100)
	_, err := gc.Compress(data, chronicle.CodecGorilla)
	if err != nil {
		t.Fatal(err)
	}

	stats := gc.GetStats()
	if stats.TotalBytesIn == 0 {
		t.Error("TotalBytesIn should be > 0 after compression")
	}
}

// --- DefaultGPUCompressionConfig ---

func TestDefaultGPUCompressionConfig_Defaults(t *testing.T) {
	config := DefaultGPUCompressionConfig()

	if !config.Enabled {
		t.Error("Should be enabled by default")
	}
	if !config.FallbackToCPU {
		t.Error("FallbackToCPU should be true by default")
	}
	if config.MinBatchSize <= 0 {
		t.Error("MinBatchSize should be > 0")
	}
	if config.MaxBatchSize <= 0 {
		t.Error("MaxBatchSize should be > 0")
	}
	if config.MemoryLimit <= 0 {
		t.Error("MemoryLimit should be > 0")
	}
	if config.PipelineDepth <= 0 {
		t.Error("PipelineDepth should be > 0")
	}
}

// --- CompressAsync in non-async mode ---

func TestCompressAsync_SyncMode(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true
	config.AsyncMode = false
	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatal(err)
	}
	defer gc.Stop()

	data := edgeGenerateFloat64Data(100)
	ch := gc.CompressAsync(data, chronicle.CodecGorilla)

	result := <-ch
	if result.err != nil {
		t.Fatalf("CompressAsync sync fallback: %v", result.err)
	}
	if len(result.data) == 0 {
		t.Error("Expected non-empty compressed data")
	}
}

// --- Compress/Decompress roundtrip ---

func TestCompressDecompress_Roundtrip_Gorilla(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	original := edgeGenerateFloat64Data(500)
	compressed, err := gc.Compress(original, chronicle.CodecGorilla)
	if err != nil {
		t.Fatal(err)
	}

	decompressed, err := gc.Decompress(compressed, chronicle.CodecGorilla)
	if err != nil {
		t.Fatal(err)
	}

	if len(decompressed) != len(original) {
		t.Errorf("Size mismatch: original=%d, decompressed=%d", len(original), len(decompressed))
	}
}

func TestCompressDecompress_Roundtrip_DeltaDelta(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	original := edgeGenerateTimestampData(500)
	compressed, err := gc.Compress(original, chronicle.CodecDeltaDelta)
	if err != nil {
		t.Fatal(err)
	}

	decompressed, err := gc.Decompress(compressed, chronicle.CodecDeltaDelta)
	if err != nil {
		t.Fatal(err)
	}

	if len(decompressed) != len(original) {
		t.Errorf("Size mismatch: original=%d, decompressed=%d", len(original), len(decompressed))
	}
}

// --- BatchCompressor ---

func TestNewBatchCompressor(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	bc := NewBatchCompressor(gc, 10)
	if bc == nil {
		t.Fatal("Expected non-nil BatchCompressor")
	}
	if bc.batchSize != 10 {
		t.Errorf("batchSize = %d, want 10", bc.batchSize)
	}
}

// --- StreamingCompressor ---

func TestNewStreamingCompressor(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	sc := NewStreamingCompressor(gc, chronicle.CodecGorilla, 1024)
	if sc == nil {
		t.Fatal("Expected non-nil StreamingCompressor")
	}
	if sc.window != 1024 {
		t.Errorf("window = %d, want 1024", sc.window)
	}
}

// --- GPUBenchmark ---

func TestNewGPUBenchmark(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	bm := NewGPUBenchmark(gc, chronicle.CodecGorilla)
	if bm == nil {
		t.Fatal("Expected non-nil GPUBenchmark")
	}
}

// --- GPU memory pool ---

func TestGPUMemoryPool_Init(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	if gc.memoryPool == nil {
		t.Fatal("Memory pool should not be nil")
	}
	if gc.memoryPool.maxAlloc != gc.config.MemoryLimit {
		t.Errorf("maxAlloc = %d, want %d", gc.memoryPool.maxAlloc, gc.config.MemoryLimit)
	}
}

// --- Start/Stop lifecycle ---

func TestStartStop_Lifecycle(t *testing.T) {
	gc := newTestGPUCompressor(t)

	if err := gc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := gc.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// --- Helper data generation ---

func edgeGenerateFloat64Data(count int) []byte {
	data := make([]byte, count*8)
	for i := 0; i < count; i++ {
		val := 100.0 + float64(i)*0.1
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(val))
	}
	return data
}

func edgeGenerateTimestampData(count int) []byte {
	data := make([]byte, count*8)
	base := int64(1700000000000000000)
	for i := 0; i < count; i++ {
		ts := base + int64(i)*int64(1e9)
		binary.LittleEndian.PutUint64(data[i*8:], uint64(ts))
	}
	return data
}
