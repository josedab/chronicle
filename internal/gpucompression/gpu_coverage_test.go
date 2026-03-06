package gpucompression

import (
	"encoding/binary"
	"math"
	"testing"

	chronicle "github.com/chronicle-db/chronicle"
)

// --- GPU backend check stubs ---

func TestCheckCUDAAvailable(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()
	if gc.checkCUDAAvailable() {
		t.Error("CUDA stub should return false")
	}
}

func TestCheckOpenCLAvailable(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()
	if gc.checkOpenCLAvailable() {
		t.Error("OpenCL stub should return false")
	}
}

func TestCheckVulkanAvailable(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()
	if gc.checkVulkanAvailable() {
		t.Error("Vulkan stub should return false")
	}
}

// --- getDeviceInfo ---

func TestGetDeviceInfo_Simulated(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()
	info := gc.getDeviceInfo()
	if info == nil {
		t.Fatal("Expected non-nil device info")
	}
	if info.Name != "Simulated GPU" {
		t.Errorf("Name = %q, want 'Simulated GPU'", info.Name)
	}
	if info.TotalMemory <= 0 {
		t.Error("TotalMemory should be > 0")
	}
}

// --- compressInternal codec routing ---

func TestCompressInternal_Gorilla(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	data := generateFloat64Data(100)
	result, err := gc.compressInternal(data, chronicle.CodecGorilla)
	if err != nil {
		t.Fatalf("compressInternal Gorilla: %v", err)
	}
	if len(result) == 0 {
		t.Error("Expected non-empty result")
	}
}

func TestCompressInternal_DeltaDelta(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	data := generateTimestampData(100)
	result, err := gc.compressInternal(data, chronicle.CodecDeltaDelta)
	if err != nil {
		t.Fatalf("compressInternal DeltaDelta: %v", err)
	}
	if len(result) == 0 {
		t.Error("Expected non-empty result")
	}
}

func TestCompressInternal_FloatXOR(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	data := generateFloat64Data(100)
	result, err := gc.compressInternal(data, chronicle.CodecFloatXOR)
	if err != nil {
		t.Fatalf("compressInternal FloatXOR: %v", err)
	}
	if len(result) == 0 {
		t.Error("Expected non-empty result")
	}
}

// --- compressGorillaGPU edge cases ---

func TestCompressGorillaGPU_SmallData(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	// Less than 8 bytes
	result, err := gc.compressGorillaGPU([]byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Errorf("Small data should pass through, got %d bytes", len(result))
	}
}

func TestCompressGorillaGPU_SingleValue(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(42.0))
	result, err := gc.compressGorillaGPU(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Error("Expected non-empty result for single value")
	}
}

// --- compressDeltaDeltaGPU edge cases ---

func TestCompressDeltaDeltaGPU_SmallData(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	// Less than 16 bytes — falls back to compressDeltaDelta
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 1000)
	result, err := gc.compressDeltaDeltaGPU(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Error("Expected non-empty result")
	}
}

func TestCompressDeltaDeltaGPU_TwoValues(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:], 1000)
	binary.LittleEndian.PutUint64(data[8:], 2000)
	result, err := gc.compressDeltaDeltaGPU(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Error("Expected non-empty result")
	}
}

func TestCompressDeltaDeltaGPU_LargeData(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	data := generateTimestampData(500)
	result, err := gc.compressDeltaDeltaGPU(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) >= len(data) {
		t.Logf("Warning: delta-delta compression ratio >= 1")
	}
}

// --- encodeDeltaDelta ---

func TestEncodeDeltaDelta(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	result, err := gc.encodeDeltaDelta(1000, 10, []int64{0, 0, 0, 1, -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Error("Expected non-empty encoded data")
	}
}

// --- gpuGenerateBenchmarkData ---

func TestGpuGenerateBenchmarkData(t *testing.T) {
	data := gpuGenerateBenchmarkData(800) // 100 floats
	if len(data) != 800 {
		t.Errorf("Expected 800 bytes, got %d", len(data))
	}

	// Verify data is valid float64s
	for i := 0; i < len(data)/8; i++ {
		val := math.Float64frombits(binary.LittleEndian.Uint64(data[i*8:]))
		if math.IsNaN(val) || math.IsInf(val, 0) {
			t.Errorf("Invalid float at index %d: %v", i, val)
		}
	}
}

func TestGpuGenerateBenchmarkData_Large(t *testing.T) {
	data := gpuGenerateBenchmarkData(8000)
	if len(data) != 8000 {
		t.Errorf("Expected 8000 bytes, got %d", len(data))
	}
}

// --- Compress with different codecs ---

func TestCompress_AllCodecs(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	data := generateFloat64Data(200)

	codecs := []chronicle.CodecType{
		chronicle.CodecGorilla,
		chronicle.CodecDeltaDelta,
		chronicle.CodecFloatXOR,
	}

	for _, codec := range codecs {
		result, err := gc.Compress(data, codec)
		if err != nil {
			t.Fatalf("Compress with codec %d: %v", codec, err)
		}
		if len(result) == 0 {
			t.Errorf("Empty result for codec %d", codec)
		}
	}
}

// --- CompressBatch with actual data ---

func TestCompressBatch_MultipleItems(t *testing.T) {
	gc := newTestGPUCompressor(t)
	defer gc.Stop()

	batches := [][]byte{
		generateFloat64Data(100),
		generateFloat64Data(200),
		generateFloat64Data(50),
	}

	results, err := gc.CompressBatch(batches, chronicle.CodecGorilla)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if len(r) == 0 {
			t.Errorf("Result %d is empty", i)
		}
	}
}

// --- GPUBackend String ---

func TestGPUBackend_AllStrings(t *testing.T) {
	tests := []struct {
		backend GPUBackend
		expect  string
	}{
		{GPUBackendNone, "none"},
		{GPUBackendCUDA, "cuda"},
		{GPUBackendMetal, "metal"},
		{GPUBackendOpenCL, "opencl"},
		{GPUBackendWebGPU, "webgpu"},
		{GPUBackendVulkan, "vulkan"},
	}
	for _, tc := range tests {
		if tc.backend.String() != tc.expect {
			t.Errorf("GPUBackend(%d).String() = %q, want %q", tc.backend, tc.backend.String(), tc.expect)
		}
	}
}
