package gpucompression

import (
	"encoding/binary"
	"math"
	"testing"
)

// --- decompressGorillaGPU ---

func TestDecompressGorillaGPU_SmallData(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	// Less than 9 bytes → falls through to decompressGorilla
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(42.0))
	result, err := gc.decompressGorillaGPU(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Error("Expected non-empty result")
	}
}

func TestDecompressGorillaGPU_Roundtrip(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	// Generate some float64 data
	original := make([]uint64, 50)
	for i := range original {
		original[i] = math.Float64bits(100.0 + float64(i)*0.5)
	}
	rawData := make([]byte, len(original)*8)
	for i, v := range original {
		binary.LittleEndian.PutUint64(rawData[i*8:], v)
	}

	// Compress with GPU gorilla
	compressed, err := gc.compressGorillaGPU(rawData)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}

	// Decompress
	decompressed, err := gc.decompressGorillaGPU(compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}

	// Verify values match
	if len(decompressed) != len(rawData) {
		t.Fatalf("Size mismatch: original=%d, decompressed=%d", len(rawData), len(decompressed))
	}
	for i := 0; i < len(original); i++ {
		got := binary.LittleEndian.Uint64(decompressed[i*8:])
		if got != original[i] {
			t.Errorf("Value[%d]: got %d, want %d", i, got, original[i])
			break
		}
	}
}

func TestDecompressGorillaGPU_SingleValue(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	// Compress a single value
	rawData := make([]byte, 8)
	binary.LittleEndian.PutUint64(rawData, math.Float64bits(99.9))

	compressed, err := gc.compressGorillaGPU(rawData)
	if err != nil {
		t.Fatal(err)
	}

	decompressed, err := gc.decompressGorillaGPU(compressed)
	if err != nil {
		t.Fatal(err)
	}

	val := math.Float64frombits(binary.LittleEndian.Uint64(decompressed))
	if math.Abs(val-99.9) > 0.001 {
		t.Errorf("Decompressed value = %f, want 99.9", val)
	}
}

// --- decompressDeltaDeltaGPU ---

func TestDecompressDeltaDeltaGPU_SmallData(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	// Less than 16 bytes → falls through to decompressDeltaDelta
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 1000)
	result, err := gc.decompressDeltaDeltaGPU(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Error("Expected non-empty result")
	}
}

func TestDecompressDeltaDeltaGPU_Roundtrip(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	// Generate monotonic timestamp data
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(1000 + i*10) // constant delta=10
	}
	rawData := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(rawData[i*8:], uint64(v))
	}

	// Compress
	compressed, err := gc.compressDeltaDeltaGPU(rawData)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}

	// Decompress
	decompressed, err := gc.decompressDeltaDeltaGPU(compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}

	// Verify
	if len(decompressed) != len(rawData) {
		t.Fatalf("Size mismatch: original=%d, decompressed=%d", len(rawData), len(decompressed))
	}
	for i := 0; i < len(values); i++ {
		got := int64(binary.LittleEndian.Uint64(decompressed[i*8:]))
		if got != values[i] {
			t.Errorf("Value[%d]: got %d, want %d", i, got, values[i])
			break
		}
	}
}

// --- parallelPrefixXOR ---

func TestParallelPrefixXOR(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	first := uint64(100)
	xors := []uint64{0x0F, 0x03, 0x01}

	result := gc.parallelPrefixXOR(first, xors)
	if len(result) != 4 {
		t.Fatalf("Expected 4 values, got %d", len(result))
	}
	if result[0] != 100 {
		t.Errorf("result[0] = %d, want 100", result[0])
	}
	if result[1] != 100^0x0F {
		t.Errorf("result[1] = %d, want %d", result[1], 100^0x0F)
	}
}

func TestParallelPrefixXOR_Empty(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	result := gc.parallelPrefixXOR(42, nil)
	if len(result) != 1 || result[0] != 42 {
		t.Errorf("Expected [42], got %v", result)
	}
}

// --- decompressGorilla helper ---

func TestDecompressGorilla_SmallInput(t *testing.T) {
	data := []byte{1, 2, 3}
	result, err := decompressGorilla(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Errorf("Small input should pass through, got %d bytes", len(result))
	}
}

func TestDecompressGorilla_SingleValue(t *testing.T) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(42.0))

	result, err := decompressGorilla(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 8 {
		t.Errorf("Expected 8 bytes, got %d", len(result))
	}
}

// --- decompressDeltaDelta helper ---

func TestDecompressDeltaDelta_SmallInput(t *testing.T) {
	data := []byte{1, 2, 3}
	result, err := decompressDeltaDelta(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Errorf("Small input should pass through, got %d bytes", len(result))
	}
}

func TestDecompressDeltaDelta_SingleValue(t *testing.T) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(1000))

	result, err := decompressDeltaDelta(data)
	if err != nil {
		t.Fatal(err)
	}
	val := int64(binary.LittleEndian.Uint64(result))
	if val != 1000 {
		t.Errorf("Value = %d, want 1000", val)
	}
}

func TestDecompressDeltaDelta_TwoValues(t *testing.T) {
	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:], uint64(1000))
	binary.LittleEndian.PutUint64(data[8:], uint64(10)) // delta

	result, err := decompressDeltaDelta(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 16 {
		t.Fatalf("Expected 16 bytes, got %d", len(result))
	}
	v0 := int64(binary.LittleEndian.Uint64(result[0:]))
	v1 := int64(binary.LittleEndian.Uint64(result[8:]))
	if v0 != 1000 {
		t.Errorf("v0 = %d, want 1000", v0)
	}
	if v1 != 1010 {
		t.Errorf("v1 = %d, want 1010", v1)
	}
}

func TestDecompressDeltaDelta_ThreeValues(t *testing.T) {
	data := make([]byte, 24)
	binary.LittleEndian.PutUint64(data[0:], uint64(1000))  // first value
	binary.LittleEndian.PutUint64(data[8:], uint64(10))    // delta
	binary.LittleEndian.PutUint64(data[16:], uint64(0))    // delta-of-delta=0 → constant delta

	result, err := decompressDeltaDelta(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 24 {
		t.Fatalf("Expected 24 bytes, got %d", len(result))
	}
	v0 := int64(binary.LittleEndian.Uint64(result[0:]))
	v1 := int64(binary.LittleEndian.Uint64(result[8:]))
	v2 := int64(binary.LittleEndian.Uint64(result[16:]))
	if v0 != 1000 || v1 != 1010 || v2 != 1020 {
		t.Errorf("Values: %d, %d, %d — want 1000, 1010, 1020", v0, v1, v2)
	}
}

// --- decompressGPU routing ---

func TestDecompressGPU_RoutesCorrectly(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	// Simulate initialized GPU to test routing
	gc.initialized = true
	gc.config.Enabled = true
	gc.config.MinBatchSize = 1 // allow any size

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(42.0))

	// Gorilla codec
	_, err := gc.decompressGPU(data, 0) // CodecGorilla = 0
	if err != nil {
		t.Fatalf("decompressGPU gorilla: %v", err)
	}

	// DeltaDelta codec
	_, err = gc.decompressGPU(data, 1) // CodecDeltaDelta = 1
	if err != nil {
		t.Fatalf("decompressGPU deltadelta: %v", err)
	}
}

// --- compressBatchGPU ---

func TestCompressBatchGPU(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	gc.initialized = true
	gc.config.Enabled = true
	gc.config.MinBatchSize = 1

	batches := [][]byte{
		generateFloat64Data(50),
		generateFloat64Data(100),
	}

	results, err := gc.compressBatchGPU(batches, 0) // CodecGorilla
	if err != nil {
		t.Fatalf("compressBatchGPU: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

// --- gpuGenerateBenchmarkData ---

func TestGPUGenerateBenchmarkData_SmallSize(t *testing.T) {
	data := gpuGenerateBenchmarkData(16)
	if len(data) != 16 {
		t.Errorf("Expected 16 bytes, got %d", len(data))
	}
}

func TestGPUGenerateBenchmarkData_ZeroSize(t *testing.T) {
	data := gpuGenerateBenchmarkData(0)
	if len(data) != 0 {
		t.Errorf("Expected 0 bytes, got %d", len(data))
	}
}

// --- readVarInts ---

func TestReadVarInts(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	// Encode some zigzag varint values
	var encoded []byte
	// zigzag encode: v -> (v << 1) ^ (v >> 63)
	for _, v := range []int64{0, 1, -1, 100, -100} {
		uv := uint64((v << 1) ^ (v >> 63))
		for uv >= 0x80 {
			encoded = append(encoded, byte(uv)|0x80)
			uv >>= 7
		}
		encoded = append(encoded, byte(uv))
	}

	values, err := gc.readVarInts(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 5 {
		t.Fatalf("Expected 5 values, got %d", len(values))
	}
	expected := []int64{0, 1, -1, 100, -100}
	for i, v := range expected {
		if values[i] != v {
			t.Errorf("values[%d] = %d, want %d", i, values[i], v)
		}
	}
}

func TestReadVarInts_Empty(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	values, err := gc.readVarInts(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 0 {
		t.Errorf("Expected 0 values, got %d", len(values))
	}
}
