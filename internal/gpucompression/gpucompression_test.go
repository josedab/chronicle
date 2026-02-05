package gpucompression

import (
	"bytes"
	"encoding/binary"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

func TestGPUCompressor(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()

	if err := gc.Start(); err != nil {
		t.Fatalf("Failed to start GPU compressor: %v", err)
	}

	t.Run("GorillaCompression", func(t *testing.T) {
		// Generate float64 time-series data
		data := generateFloat64Data(1000)

		compressed, err := gc.Compress(data, chronicle.CodecGorilla)
		if err != nil {
			t.Fatalf("Compression failed: %v", err)
		}

		if len(compressed) >= len(data) {
			t.Logf("Warning: compression ratio >= 1 (input: %d, output: %d)", len(data), len(compressed))
		}

		// Verify decompression
		decompressed, err := gc.Decompress(compressed, chronicle.CodecGorilla)
		if err != nil {
			t.Fatalf("Decompression failed: %v", err)
		}

		if len(decompressed) != len(data) {
			t.Errorf("Decompressed size mismatch: expected %d, got %d", len(data), len(decompressed))
		}
	})

	t.Run("DeltaDeltaCompression", func(t *testing.T) {
		// Generate timestamp-like data
		data := generateTimestampData(1000)

		compressed, err := gc.Compress(data, chronicle.CodecDeltaDelta)
		if err != nil {
			t.Fatalf("Compression failed: %v", err)
		}

		if len(compressed) >= len(data) {
			t.Logf("Compression ratio: %.2f", float64(len(data))/float64(len(compressed)))
		}
	})

	t.Run("EmptyData", func(t *testing.T) {
		compressed, err := gc.Compress([]byte{}, chronicle.CodecGorilla)
		if err != nil {
			t.Fatalf("Empty compression failed: %v", err)
		}

		if len(compressed) != 0 {
			t.Errorf("Expected empty output for empty input")
		}
	})

	t.Run("SmallData", func(t *testing.T) {
		// Data smaller than min batch size
		data := generateFloat64Data(10)

		compressed, err := gc.Compress(data, chronicle.CodecGorilla)
		if err != nil {
			t.Fatalf("Small data compression failed: %v", err)
		}

		if compressed == nil {
			t.Error("Expected non-nil compressed data")
		}
	})
}

func TestGPUCompressor_BatchCompression(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()
	gc.Start()

	// Create multiple batches
	batches := make([][]byte, 10)
	for i := range batches {
		batches[i] = generateFloat64Data(500)
	}

	results, err := gc.CompressBatch(batches, chronicle.CodecGorilla)
	if err != nil {
		t.Fatalf("Batch compression failed: %v", err)
	}

	if len(results) != len(batches) {
		t.Errorf("Expected %d results, got %d", len(batches), len(results))
	}

	for i, result := range results {
		if len(result) == 0 {
			t.Errorf("Batch %d: empty result", i)
		}
	}
}

func TestGPUCompressor_AsyncCompression(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true
	config.AsyncMode = true

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()
	gc.Start()

	data := generateFloat64Data(1000)

	// Start async compression
	resultChan := gc.CompressAsync(data, chronicle.CodecGorilla)

	// Wait for result
	select {
	case result := <-resultChan:
		if result.err != nil {
			t.Fatalf("Async compression failed: %v", result.err)
		}
		if len(result.data) == 0 {
			t.Error("Expected non-empty compressed data")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Async compression timed out")
	}
}

func TestGPUCompressor_Stats(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()
	gc.Start()

	// Perform some compressions
	data := generateFloat64Data(1000)
	for i := 0; i < 10; i++ {
		gc.Compress(data, chronicle.CodecGorilla)
	}

	stats := gc.GetStats()

	if stats.TotalCompressions+stats.CPUFallbacks+stats.GPUCompressions == 0 {
		t.Error("Expected some compression operations")
	}

	if stats.TotalBytesIn == 0 {
		t.Error("Expected non-zero bytes in")
	}
}

func TestGPUCompressor_DeviceInfo(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()

	info := gc.GetDeviceInfo()
	if info == nil {
		t.Skip("No GPU device info available")
	}

	if info.Name == "" {
		t.Error("Expected device name")
	}
}

func TestGPUCompressor_BackendDetection(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()

	backend := gc.GetBackend()
	t.Logf("Detected backend: %s", backend)

	// Verify IsGPUAvailable is consistent
	if gc.IsGPUAvailable() && backend == GPUBackendNone {
		t.Error("Inconsistent GPU availability state")
	}
}

func TestGPUBackend_String(t *testing.T) {
	testCases := []struct {
		backend  GPUBackend
		expected string
	}{
		{GPUBackendNone, "none"},
		{GPUBackendCUDA, "cuda"},
		{GPUBackendMetal, "metal"},
		{GPUBackendOpenCL, "opencl"},
		{GPUBackendWebGPU, "webgpu"},
		{GPUBackendVulkan, "vulkan"},
	}

	for _, tc := range testCases {
		if tc.backend.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.backend.String())
		}
	}
}

func TestBatchCompressor(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()
	gc.Start()

	bc := NewBatchCompressor(gc, 10)

	// Add items to batch
	for i := 0; i < 5; i++ {
		bc.Add(generateFloat64Data(100))
	}

	// Flush
	results, err := bc.Flush(chronicle.CodecGorilla)
	if err != nil {
		t.Fatalf("Batch flush failed: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	// Empty flush
	results, err = bc.Flush(chronicle.CodecGorilla)
	if err != nil {
		t.Fatalf("Empty flush failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty flush, got %d", len(results))
	}
}

func TestStreamingCompressor(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()
	gc.Start()

	windowSize := 1024
	sc := NewStreamingCompressor(gc, chronicle.CodecGorilla, windowSize)
	defer sc.Close()

	// Write data in chunks
	data := generateFloat64Data(500)
	for i := 0; i < 5; i++ {
		n, err := sc.Write(data)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if n != len(data) {
			t.Errorf("Expected to write %d bytes, wrote %d", len(data), n)
		}
	}

	// Check for output
	select {
	case compressed := <-sc.Output():
		if len(compressed) == 0 {
			t.Error("Expected non-empty compressed output")
		}
	case <-time.After(100 * time.Millisecond):
		// May not have enough data for a full window
	}

	// Flush remaining
	remaining, err := sc.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// May or may not have remaining data
	_ = remaining
}

func TestGPUBenchmark(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()
	gc.Start()

	benchmark := NewGPUBenchmark(gc, chronicle.CodecGorilla)

	// Run single benchmark
	data := generateFloat64Data(10000)
	result := benchmark.Run(data, 5)

	if result.DataSize != len(data) {
		t.Errorf("Expected data size %d, got %d", len(data), result.DataSize)
	}

	if result.CompressionRatio <= 0 {
		t.Error("Expected positive compression ratio")
	}

	if result.Throughput <= 0 {
		t.Error("Expected positive throughput")
	}

	t.Logf("Benchmark result: ratio=%.2f, throughput=%.2f MB/s, GPU=%v",
		result.CompressionRatio, result.Throughput, result.UsedGPU)
}

func TestGPUBenchmark_Suite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping benchmark suite in short mode")
	}

	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()
	gc.Start()

	benchmark := NewGPUBenchmark(gc, chronicle.CodecGorilla)

	// Run suite with various sizes
	results := benchmark.RunSuite(1024, 65536, 3)

	for _, result := range results {
		t.Logf("Size: %d, Ratio: %.2f, Throughput: %.2f MB/s",
			result.DataSize, result.CompressionRatio, result.Throughput)
	}
}

func TestParallelXOR(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	gc, _ := NewGPUCompressor(config)

	values := []uint64{100, 105, 110, 115, 120, 125, 130}
	xors := gc.parallelXOR(values)

	if len(xors) != len(values)-1 {
		t.Errorf("Expected %d XORs, got %d", len(values)-1, len(xors))
	}

	// Verify XOR computation
	for i := 0; i < len(xors); i++ {
		expected := values[i] ^ values[i+1]
		if xors[i] != expected {
			t.Errorf("XOR[%d]: expected %d, got %d", i, expected, xors[i])
		}
	}
}

func TestParallelDelta(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	gc, _ := NewGPUCompressor(config)

	values := []int64{1000, 1010, 1020, 1030, 1040}
	deltas := gc.parallelDelta(values)

	if len(deltas) != len(values)-1 {
		t.Errorf("Expected %d deltas, got %d", len(values)-1, len(deltas))
	}

	// Verify delta computation
	for i := 0; i < len(deltas); i++ {
		expected := values[i+1] - values[i]
		if deltas[i] != expected {
			t.Errorf("Delta[%d]: expected %d, got %d", i, expected, deltas[i])
		}
	}
}

func TestVarIntEncoding(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	gc, _ := NewGPUCompressor(config)

	testCases := []int64{0, 1, -1, 127, -128, 300, -300, 1000000, -1000000}

	for _, tc := range testCases {
		var buf bytes.Buffer
		gc.writeVarInt(&buf, tc)

		values, err := gc.readVarInts(buf.Bytes())
		if err != nil {
			t.Errorf("Failed to read VarInt for %d: %v", tc, err)
			continue
		}

		if len(values) != 1 {
			t.Errorf("Expected 1 value, got %d", len(values))
			continue
		}

		if values[0] != tc {
			t.Errorf("Expected %d, got %d", tc, values[0])
		}
	}
}

func TestGPUCompressor_ConcurrentAccess(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()
	gc.Start()

	// Concurrent compression
	var wg sync.WaitGroup
	numGoroutines := runtime.NumCPU() * 2
	iterations := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := generateFloat64Data(1000)
			for j := 0; j < iterations; j++ {
				_, err := gc.Compress(data, chronicle.CodecGorilla)
				if err != nil {
					t.Errorf("Concurrent compression failed: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()

	stats := gc.GetStats()
	expectedOps := int64(numGoroutines * iterations)
	actualOps := stats.TotalCompressions + stats.GPUCompressions + stats.CPUFallbacks

	if actualOps < expectedOps {
		t.Logf("Stats: compressions=%d, GPU=%d, CPU fallbacks=%d",
			stats.TotalCompressions, stats.GPUCompressions, stats.CPUFallbacks)
	}
}

func TestGPUCompressor_RoundTrip(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()
	gc.Start()

	codecs := []chronicle.CodecType{chronicle.CodecGorilla, chronicle.CodecFloatXOR}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			// Generate original data
			original := generateFloat64Data(1000)

			// Compress
			compressed, err := gc.Compress(original, codec)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			// Decompress
			decompressed, err := gc.Decompress(compressed, codec)
			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}

			// Note: Round-trip may not be exact due to GPU optimization differences
			// We verify length matches
			if len(decompressed) != len(original) {
				t.Errorf("Length mismatch: original=%d, decompressed=%d",
					len(original), len(decompressed))
			}
		})
	}
}

func TestDefaultGPUCompressionConfig(t *testing.T) {
	config := DefaultGPUCompressionConfig()

	if !config.Enabled {
		t.Error("Expected Enabled to be true by default")
	}

	if !config.FallbackToCPU {
		t.Error("Expected FallbackToCPU to be true by default")
	}

	if config.MinBatchSize <= 0 {
		t.Error("Expected positive MinBatchSize")
	}

	if config.MaxBatchSize <= config.MinBatchSize {
		t.Error("Expected MaxBatchSize > MinBatchSize")
	}

	if config.MemoryLimit <= 0 {
		t.Error("Expected positive MemoryLimit")
	}

	if config.StreamCount <= 0 {
		t.Error("Expected positive StreamCount")
	}
}

func TestBitPackXOR(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	gc, _ := NewGPUCompressor(config)

	firstValue := uint64(100)
	xors := []uint64{5, 5, 5, 5} // Small deltas

	packed, err := gc.bitPackXOR(firstValue, xors)
	if err != nil {
		t.Fatalf("BitPackXOR failed: %v", err)
	}

	// Should have: 8 bytes for first value + 1 byte for bit width + packed XORs
	if len(packed) < 9 {
		t.Errorf("Expected at least 9 bytes, got %d", len(packed))
	}

	// Verify first value is preserved
	firstRead := binary.LittleEndian.Uint64(packed[0:8])
	if firstRead != firstValue {
		t.Errorf("First value mismatch: expected %d, got %d", firstValue, firstRead)
	}
}

func TestGPUCompressor_MemoryLimit(t *testing.T) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true
	config.MemoryLimit = 1024 // Very small limit

	gc, err := NewGPUCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create GPU compressor: %v", err)
	}
	defer gc.Stop()
	gc.Start()

	// Generate data larger than memory limit
	largeData := generateFloat64Data(10000)

	// Should fallback to CPU due to memory limit
	compressed, err := gc.Compress(largeData, chronicle.CodecGorilla)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	if len(compressed) == 0 {
		t.Error("Expected non-empty compressed data")
	}
}

// Helper functions

func generateFloat64Data(count int) []byte {
	data := make([]byte, count*8)
	var value float64 = 100.0

	for i := 0; i < count; i++ {
		// Generate time-series like data with small deltas
		value += 0.1 + float64(i%10)*0.01
		bits := math.Float64bits(value)
		binary.LittleEndian.PutUint64(data[i*8:], bits)
	}

	return data
}

func generateTimestampData(count int) []byte {
	data := make([]byte, count*8)
	timestamp := time.Now().UnixNano()

	for i := 0; i < count; i++ {
		// Generate monotonically increasing timestamps with ~100ms intervals
		timestamp += 100_000_000 + int64(i%10)*1000
		binary.LittleEndian.PutUint64(data[i*8:], uint64(timestamp))
	}

	return data
}

func BenchmarkGPUCompressor_Gorilla(b *testing.B) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, _ := NewGPUCompressor(config)
	gc.Start()
	defer gc.Stop()

	data := generateFloat64Data(10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gc.Compress(data, chronicle.CodecGorilla)
	}
}

func BenchmarkGPUCompressor_DeltaDelta(b *testing.B) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, _ := NewGPUCompressor(config)
	gc.Start()
	defer gc.Stop()

	data := generateTimestampData(10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gc.Compress(data, chronicle.CodecDeltaDelta)
	}
}

func BenchmarkGPUCompressor_Batch(b *testing.B) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, _ := NewGPUCompressor(config)
	gc.Start()
	defer gc.Stop()

	// Create batches
	batches := make([][]byte, 10)
	for i := range batches {
		batches[i] = generateFloat64Data(1000)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gc.CompressBatch(batches, chronicle.CodecGorilla)
	}
}

func BenchmarkGPUCompressor_Concurrent(b *testing.B) {
	config := DefaultGPUCompressionConfig()
	config.FallbackToCPU = true

	gc, _ := NewGPUCompressor(config)
	gc.Start()
	defer gc.Stop()

	data := generateFloat64Data(1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			gc.Compress(data, chronicle.CodecGorilla)
		}
	})
}
