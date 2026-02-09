package chronicle

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// GPUBackend represents the type of GPU backend available.
type GPUBackend int

const (
	// GPUBackendNone indicates no GPU acceleration available.
	GPUBackendNone GPUBackend = iota
	// GPUBackendCUDA uses NVIDIA CUDA for acceleration.
	GPUBackendCUDA
	// GPUBackendMetal uses Apple Metal for acceleration.
	GPUBackendMetal
	// GPUBackendOpenCL uses OpenCL for acceleration.
	GPUBackendOpenCL
	// GPUBackendWebGPU uses WebGPU for acceleration.
	GPUBackendWebGPU
	// GPUBackendVulkan uses Vulkan Compute for acceleration.
	GPUBackendVulkan
)

func (b GPUBackend) String() string {
	switch b {
	case GPUBackendCUDA:
		return "cuda"
	case GPUBackendMetal:
		return "metal"
	case GPUBackendOpenCL:
		return "opencl"
	case GPUBackendWebGPU:
		return "webgpu"
	case GPUBackendVulkan:
		return "vulkan"
	default:
		return "none"
	}
}

// GPUCompressionConfig configures GPU-accelerated compression.
type GPUCompressionConfig struct {
	// Enabled enables GPU acceleration when available.
	Enabled bool

	// PreferredBackend specifies the preferred GPU backend.
	PreferredBackend GPUBackend

	// FallbackToCPU enables automatic fallback to CPU when GPU unavailable.
	FallbackToCPU bool

	// MinBatchSize is the minimum number of values to trigger GPU processing.
	MinBatchSize int

	// MaxBatchSize is the maximum batch size for GPU processing.
	MaxBatchSize int

	// DeviceID specifies which GPU device to use (for multi-GPU systems).
	DeviceID int

	// MemoryLimit limits GPU memory usage in bytes.
	MemoryLimit int64

	// AsyncMode enables asynchronous GPU operations.
	AsyncMode bool

	// StreamCount is the number of CUDA streams for concurrent execution.
	StreamCount int

	// EnableProfiling enables GPU performance profiling.
	EnableProfiling bool

	// PipelineDepth is the depth of the compression pipeline.
	PipelineDepth int

	// UseUnifiedMemory enables CUDA unified memory (managed memory).
	UseUnifiedMemory bool
}

// DefaultGPUCompressionConfig returns default GPU compression configuration.
func DefaultGPUCompressionConfig() GPUCompressionConfig {
	return GPUCompressionConfig{
		Enabled:          true,
		PreferredBackend: GPUBackendNone, // Auto-detect
		FallbackToCPU:    true,
		MinBatchSize:     1024,
		MaxBatchSize:     1024 * 1024,
		DeviceID:         0,
		MemoryLimit:      512 * 1024 * 1024, // 512MB
		AsyncMode:        true,
		StreamCount:      4,
		EnableProfiling:  false,
		PipelineDepth:    3,
		UseUnifiedMemory: false,
	}
}

// GPUCompressor provides GPU-accelerated compression for time-series data.
type GPUCompressor struct {
	config GPUCompressionConfig

	// Backend state
	backend       GPUBackend
	deviceInfo    *GPUDeviceInfo
	initialized   bool
	initMu        sync.Mutex

	// Memory management
	memoryPool    *GPUMemoryPool
	allocatedMem  int64

	// Stream management (for CUDA)
	streams       []GPUStream
	streamIdx     uint32

	// Pipeline
	pipeline      *CompressionPipeline
	pipelineInput chan *compressionJob
	pipelineWg    sync.WaitGroup

	// Statistics
	stats         GPUCompressionStats
	statsMu       sync.RWMutex

	// CPU fallback
	cpuEncoder    *AdaptiveCompressionEngine

	// Lifecycle
	ctx           context.Context
	cancel        context.CancelFunc
}

// GPUDeviceInfo contains GPU device information.
type GPUDeviceInfo struct {
	Name              string
	ComputeCapability string
	TotalMemory       int64
	FreeMemory        int64
	MultiProcessors   int
	MaxThreadsPerMP   int
	ClockRate         int // MHz
	MemoryClockRate   int // MHz
	MemoryBusWidth    int // bits
	L2CacheSize       int
	Warp              int
}

// GPUStream represents a GPU execution stream.
type GPUStream struct {
	ID       int
	Handle   unsafe.Pointer
	InUse    int32
	LastUsed time.Time
}

// GPUMemoryPool manages GPU memory allocation.
type GPUMemoryPool struct {
	mu           sync.Mutex
	blocks       map[int64]*GPUMemoryBlock
	totalAlloc   int64
	maxAlloc     int64
	hitCount     int64
	missCount    int64
}

// GPUMemoryBlock represents an allocated GPU memory block.
type GPUMemoryBlock struct {
	Ptr       unsafe.Pointer
	Size      int64
	InUse     bool
	Timestamp time.Time
}

// GPUCompressionStats contains GPU compression statistics.
type GPUCompressionStats struct {
	// Operation counts
	TotalCompressions   int64
	TotalDecompressions int64
	GPUCompressions     int64
	CPUFallbacks        int64

	// Performance metrics
	TotalBytesIn        int64
	TotalBytesOut       int64
	TotalGPUTime        time.Duration
	TotalCPUTime        time.Duration

	// Memory stats
	PeakGPUMemory       int64
	CurrentGPUMemory    int64
	MemoryPoolHits      int64
	MemoryPoolMisses    int64

	// Throughput
	LastThroughput      float64 // MB/s
	AverageThroughput   float64 // MB/s

	// Errors
	GPUErrors           int64
	RecoveredErrors     int64
}

// CompressionPipeline manages the GPU compression pipeline.
type CompressionPipeline struct {
	stages    []pipelineStage
	depth     int
	batchSize int
}

type pipelineStage struct {
	name    string
	process func(data []byte) ([]byte, error)
}

type compressionJob struct {
	data     []byte
	codec    CodecType
	result   chan compressionResult
}

type compressionResult struct {
	data []byte
	err  error
}

// NewGPUCompressor creates a new GPU-accelerated compressor.
func NewGPUCompressor(config GPUCompressionConfig) (*GPUCompressor, error) {
	ctx, cancel := context.WithCancel(context.Background())

	gc := &GPUCompressor{
		config:        config,
		backend:       GPUBackendNone,
		pipelineInput: make(chan *compressionJob, config.PipelineDepth*10),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Initialize CPU fallback
	gc.cpuEncoder = NewAdaptiveCompressionEngine(DefaultAdaptiveCompressionConfig())

	// Initialize memory pool
	gc.memoryPool = &GPUMemoryPool{
		blocks:   make(map[int64]*GPUMemoryBlock),
		maxAlloc: config.MemoryLimit,
	}

	// Detect and initialize GPU
	if config.Enabled {
		if err := gc.initializeGPU(); err != nil {
			if !config.FallbackToCPU {
				cancel()
				return nil, err
			}
			// Continue with CPU fallback
		}
	}

	return gc, nil
}

// initializeGPU initializes the GPU backend.
func (gc *GPUCompressor) initializeGPU() error {
	gc.initMu.Lock()
	defer gc.initMu.Unlock()

	if gc.initialized {
		return nil
	}

	// Detect available GPU backend
	backend := gc.detectGPUBackend()
	if backend == GPUBackendNone {
		return errors.New("no GPU backend available")
	}

	gc.backend = backend
	gc.deviceInfo = gc.getDeviceInfo()

	// Initialize streams for async operations
	if gc.config.AsyncMode && gc.config.StreamCount > 0 {
		gc.streams = make([]GPUStream, gc.config.StreamCount)
		for i := range gc.streams {
			gc.streams[i] = GPUStream{
				ID:       i,
				LastUsed: time.Now(),
			}
		}
	}

	// Initialize pipeline
	gc.pipeline = &CompressionPipeline{
		depth:     gc.config.PipelineDepth,
		batchSize: gc.config.MaxBatchSize,
	}

	gc.initialized = true
	return nil
}

// detectGPUBackend detects the available GPU backend.
func (gc *GPUCompressor) detectGPUBackend() GPUBackend {
	// Check preferred backend first
	if gc.config.PreferredBackend != GPUBackendNone {
		if gc.isBackendAvailable(gc.config.PreferredBackend) {
			return gc.config.PreferredBackend
		}
	}

	// Auto-detect based on platform
	switch runtime.GOOS {
	case "darwin":
		// Check Metal first on macOS
		if gc.isBackendAvailable(GPUBackendMetal) {
			return GPUBackendMetal
		}
	case "linux", "windows":
		// Check CUDA first on Linux/Windows
		if gc.isBackendAvailable(GPUBackendCUDA) {
			return GPUBackendCUDA
		}
		if gc.isBackendAvailable(GPUBackendOpenCL) {
			return GPUBackendOpenCL
		}
		if gc.isBackendAvailable(GPUBackendVulkan) {
			return GPUBackendVulkan
		}
	}

	// Check WebGPU as fallback
	if gc.isBackendAvailable(GPUBackendWebGPU) {
		return GPUBackendWebGPU
	}

	return GPUBackendNone
}

// isBackendAvailable checks if a GPU backend is available.
func (gc *GPUCompressor) isBackendAvailable(backend GPUBackend) bool {
	// In a real implementation, this would check for actual GPU availability
	// For now, return false to trigger CPU fallback in pure Go
	switch backend {
	case GPUBackendCUDA:
		return gc.checkCUDAAvailable()
	case GPUBackendMetal:
		return gc.checkMetalAvailable()
	case GPUBackendOpenCL:
		return gc.checkOpenCLAvailable()
	case GPUBackendWebGPU:
		return gc.checkWebGPUAvailable()
	case GPUBackendVulkan:
		return gc.checkVulkanAvailable()
	default:
		return false
	}
}

// GPU availability checks (stubs for pure Go implementation)
func (gc *GPUCompressor) checkCUDAAvailable() bool   { return false }
func (gc *GPUCompressor) checkMetalAvailable() bool  { return false }
func (gc *GPUCompressor) checkOpenCLAvailable() bool { return false }
func (gc *GPUCompressor) checkWebGPUAvailable() bool { return false }
func (gc *GPUCompressor) checkVulkanAvailable() bool { return false }

// getDeviceInfo returns GPU device information.
func (gc *GPUCompressor) getDeviceInfo() *GPUDeviceInfo {
	// Return simulated device info for pure Go implementation
	return &GPUDeviceInfo{
		Name:              "Simulated GPU",
		ComputeCapability: "8.6",
		TotalMemory:       8 * 1024 * 1024 * 1024, // 8GB
		FreeMemory:        6 * 1024 * 1024 * 1024, // 6GB
		MultiProcessors:   80,
		MaxThreadsPerMP:   1536,
		ClockRate:         1800, // MHz
		MemoryClockRate:   9500, // MHz
		MemoryBusWidth:    256,
		L2CacheSize:       6 * 1024 * 1024, // 6MB
		Warp:              32,
	}
}

// Start starts the GPU compressor.
func (gc *GPUCompressor) Start() error {
	// Start pipeline workers
	if gc.config.AsyncMode {
		for i := 0; i < gc.config.PipelineDepth; i++ {
			gc.pipelineWg.Add(1)
			go gc.pipelineWorker()
		}
	}
	return nil
}

// Stop stops the GPU compressor.
func (gc *GPUCompressor) Stop() error {
	gc.cancel()
	close(gc.pipelineInput)
	gc.pipelineWg.Wait()

	// Clean up GPU resources
	gc.cleanupGPU()
	return nil
}

func (gc *GPUCompressor) pipelineWorker() {
	defer gc.pipelineWg.Done()

	for {
		select {
		case <-gc.ctx.Done():
			return
		case job, ok := <-gc.pipelineInput:
			if !ok {
				return
			}
			result, err := gc.compressInternal(job.data, job.codec)
			job.result <- compressionResult{data: result, err: err}
		}
	}
}

// cleanupGPU releases GPU resources.
func (gc *GPUCompressor) cleanupGPU() {
	gc.initMu.Lock()
	defer gc.initMu.Unlock()

	// Release memory pool
	if gc.memoryPool != nil {
		gc.memoryPool.mu.Lock()
		gc.memoryPool.blocks = nil
		gc.memoryPool.mu.Unlock()
	}

	gc.initialized = false
}

// Compress compresses data using GPU acceleration.
func (gc *GPUCompressor) Compress(data []byte, codec CodecType) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	// Check if GPU should be used
	if gc.shouldUseGPU(len(data)) {
		return gc.compressGPU(data, codec)
	}

	// Fall back to CPU
	return gc.compressCPU(data, codec)
}

// CompressAsync compresses data asynchronously.
func (gc *GPUCompressor) CompressAsync(data []byte, codec CodecType) <-chan compressionResult {
	resultChan := make(chan compressionResult, 1)

	if !gc.config.AsyncMode {
		// Synchronous fallback
		result, err := gc.Compress(data, codec)
		resultChan <- compressionResult{data: result, err: err}
		close(resultChan)
		return resultChan
	}

	job := &compressionJob{
		data:   data,
		codec:  codec,
		result: resultChan,
	}

	select {
	case gc.pipelineInput <- job:
	default:
		// Pipeline full, process synchronously
		result, err := gc.Compress(data, codec)
		resultChan <- compressionResult{data: result, err: err}
	}

	return resultChan
}

// CompressBatch compresses multiple data blocks in parallel.
func (gc *GPUCompressor) CompressBatch(batches [][]byte, codec CodecType) ([][]byte, error) {
	if len(batches) == 0 {
		return batches, nil
	}

	results := make([][]byte, len(batches))
	errors := make([]error, len(batches))

	// Calculate total size
	totalSize := 0
	for _, b := range batches {
		totalSize += len(b)
	}

	// Use GPU for large batches
	if gc.shouldUseGPU(totalSize) && gc.initialized {
		return gc.compressBatchGPU(batches, codec)
	}

	// Process in parallel on CPU
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, runtime.NumCPU())

	for i, batch := range batches {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(idx int, data []byte) {
			defer wg.Done()
			defer func() { <-semaphore }()
			results[idx], errors[idx] = gc.compressCPU(data, codec)
		}(i, batch)
	}

	wg.Wait()

	// Check for errors
	for _, err := range errors {
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// shouldUseGPU determines if GPU should be used for the given data size.
func (gc *GPUCompressor) shouldUseGPU(dataSize int) bool {
	if !gc.config.Enabled || !gc.initialized {
		return false
	}

	if dataSize < gc.config.MinBatchSize {
		return false
	}

	if int64(dataSize) > gc.config.MemoryLimit {
		return false
	}

	return true
}

// compressGPU compresses data using GPU.
func (gc *GPUCompressor) compressGPU(data []byte, codec CodecType) ([]byte, error) {
	start := time.Now()
	atomic.AddInt64(&gc.stats.GPUCompressions, 1)
	atomic.AddInt64(&gc.stats.TotalBytesIn, int64(len(data)))

	// In a real implementation, this would dispatch to GPU
	// For now, use optimized SIMD-friendly CPU implementation
	result, err := gc.compressInternal(data, codec)
	if err != nil {
		atomic.AddInt64(&gc.stats.GPUErrors, 1)
		if gc.config.FallbackToCPU {
			atomic.AddInt64(&gc.stats.RecoveredErrors, 1)
			return gc.compressCPU(data, codec)
		}
		return nil, err
	}

	gc.statsMu.Lock()
	gc.stats.TotalGPUTime += time.Since(start)
	gc.stats.TotalBytesOut += int64(len(result))
	gc.statsMu.Unlock()

	return result, nil
}

// compressCPU compresses data using CPU.
func (gc *GPUCompressor) compressCPU(data []byte, codec CodecType) ([]byte, error) {
	start := time.Now()
	atomic.AddInt64(&gc.stats.CPUFallbacks, 1)
	atomic.AddInt64(&gc.stats.TotalBytesIn, int64(len(data)))

	result, err := gc.cpuEncoder.Compress(data, codec)
	if err != nil {
		return nil, err
	}

	gc.statsMu.Lock()
	gc.stats.TotalCPUTime += time.Since(start)
	gc.stats.TotalBytesOut += int64(len(result))
	gc.statsMu.Unlock()

	return result, nil
}

// compressInternal is the internal compression implementation.
func (gc *GPUCompressor) compressInternal(data []byte, codec CodecType) ([]byte, error) {
	switch codec {
	case CodecGorilla:
		return gc.compressGorillaGPU(data)
	case CodecDeltaDelta:
		return gc.compressDeltaDeltaGPU(data)
	case CodecFloatXOR:
		return gc.compressFloatXORGPU(data)
	default:
		return gc.cpuEncoder.Compress(data, codec)
	}
}

// compressGorillaGPU compresses float64 data using GPU-optimized Gorilla.
func (gc *GPUCompressor) compressGorillaGPU(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return data, nil
	}

	numValues := len(data) / 8
	values := make([]uint64, numValues)
	for i := 0; i < numValues; i++ {
		values[i] = binary.LittleEndian.Uint64(data[i*8:])
	}

	// GPU-friendly parallel XOR computation
	// In real implementation, this would be a GPU kernel
	xors := gc.parallelXOR(values)

	// Bit-pack the XOR values
	return gc.bitPackXOR(values[0], xors)
}

// parallelXOR computes XOR values in parallel (GPU-optimized pattern).
func (gc *GPUCompressor) parallelXOR(values []uint64) []uint64 {
	if len(values) < 2 {
		return nil
	}

	xors := make([]uint64, len(values)-1)

	// Process in chunks for better cache utilization (simulating GPU blocks)
	blockSize := 256 // Simulate GPU thread block size
	numBlocks := (len(xors) + blockSize - 1) / blockSize

	var wg sync.WaitGroup
	for block := 0; block < numBlocks; block++ {
		wg.Add(1)
		go func(blockIdx int) {
			defer wg.Done()
			start := blockIdx * blockSize
			end := start + blockSize
			if end > len(xors) {
				end = len(xors)
			}
			for i := start; i < end; i++ {
				xors[i] = values[i] ^ values[i+1]
			}
		}(block)
	}
	wg.Wait()

	return xors
}

// bitPackXOR bit-packs XOR values efficiently.
func (gc *GPUCompressor) bitPackXOR(firstValue uint64, xors []uint64) ([]byte, error) {
	var buf bytes.Buffer

	// Write header: first value
	binary.Write(&buf, binary.LittleEndian, firstValue)

	if len(xors) == 0 {
		return buf.Bytes(), nil
	}

	// Analyze XOR distribution for optimal bit packing
	maxBits := 0
	for _, xor := range xors {
		bits := bitsRequired(xor)
		if bits > maxBits {
			maxBits = bits
		}
	}

	// Write bit width
	buf.WriteByte(byte(maxBits))

	// Pack values
	// For simplicity, use byte-aligned storage
	bytesNeeded := (maxBits + 7) / 8
	for _, xor := range xors {
		for b := 0; b < bytesNeeded; b++ {
			buf.WriteByte(byte(xor >> (b * 8)))
		}
	}

	return buf.Bytes(), nil
}

// compressDeltaDeltaGPU compresses timestamps using GPU-optimized delta-delta.
func (gc *GPUCompressor) compressDeltaDeltaGPU(data []byte) ([]byte, error) {
	if len(data) < 16 {
		return compressDeltaDelta(data)
	}

	numValues := len(data) / 8
	values := make([]int64, numValues)
	for i := 0; i < numValues; i++ {
		values[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
	}

	// Parallel delta computation
	deltas := gc.parallelDelta(values)
	if len(deltas) == 0 {
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, values[0])
		return buf.Bytes(), nil
	}

	// Parallel delta-of-delta computation
	dods := gc.parallelDeltaOfDelta(deltas)

	// Encode
	return gc.encodeDeltaDelta(values[0], deltas[0], dods)
}

// parallelDelta computes deltas in parallel.
func (gc *GPUCompressor) parallelDelta(values []int64) []int64 {
	if len(values) < 2 {
		return nil
	}

	deltas := make([]int64, len(values)-1)

	blockSize := 256
	numBlocks := (len(deltas) + blockSize - 1) / blockSize

	var wg sync.WaitGroup
	for block := 0; block < numBlocks; block++ {
		wg.Add(1)
		go func(blockIdx int) {
			defer wg.Done()
			start := blockIdx * blockSize
			end := start + blockSize
			if end > len(deltas) {
				end = len(deltas)
			}
			for i := start; i < end; i++ {
				deltas[i] = values[i+1] - values[i]
			}
		}(block)
	}
	wg.Wait()

	return deltas
}

// parallelDeltaOfDelta computes delta-of-deltas in parallel.
func (gc *GPUCompressor) parallelDeltaOfDelta(deltas []int64) []int64 {
	if len(deltas) < 2 {
		return nil
	}

	dods := make([]int64, len(deltas)-1)

	blockSize := 256
	numBlocks := (len(dods) + blockSize - 1) / blockSize

	var wg sync.WaitGroup
	for block := 0; block < numBlocks; block++ {
		wg.Add(1)
		go func(blockIdx int) {
			defer wg.Done()
			start := blockIdx * blockSize
			end := start + blockSize
			if end > len(dods) {
				end = len(dods)
			}
			for i := start; i < end; i++ {
				dods[i] = deltas[i+1] - deltas[i]
			}
		}(block)
	}
	wg.Wait()

	return dods
}

// encodeDeltaDelta encodes delta-delta compressed data.
func (gc *GPUCompressor) encodeDeltaDelta(firstValue int64, firstDelta int64, dods []int64) ([]byte, error) {
	var buf bytes.Buffer

	// Write first value and delta
	binary.Write(&buf, binary.LittleEndian, firstValue)
	binary.Write(&buf, binary.LittleEndian, firstDelta)

	// Variable-length encode delta-of-deltas
	for _, dod := range dods {
		gc.writeVarInt(&buf, dod)
	}

	return buf.Bytes(), nil
}

// writeVarInt writes a variable-length integer.
func (gc *GPUCompressor) writeVarInt(buf *bytes.Buffer, v int64) {
	// ZigZag encoding for signed integers
	uv := uint64((v << 1) ^ (v >> 63))

	for uv >= 0x80 {
		buf.WriteByte(byte(uv) | 0x80)
		uv >>= 7
	}
	buf.WriteByte(byte(uv))
}

// compressFloatXORGPU compresses floats using GPU-optimized XOR.
func (gc *GPUCompressor) compressFloatXORGPU(data []byte) ([]byte, error) {
	return gc.compressGorillaGPU(data)
}

// compressBatchGPU compresses multiple batches on GPU.
func (gc *GPUCompressor) compressBatchGPU(batches [][]byte, codec CodecType) ([][]byte, error) {
	// In a real implementation, this would:
	// 1. Allocate GPU memory for all batches
	// 2. Copy all batches to GPU in a single transfer
	// 3. Launch parallel kernels for each batch
	// 4. Copy results back in a single transfer

	// For pure Go, use parallel CPU processing
	results := make([][]byte, len(batches))
	errs := make([]error, len(batches))

	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.NumCPU())

	for i, batch := range batches {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, data []byte) {
			defer wg.Done()
			defer func() { <-sem }()
			results[idx], errs[idx] = gc.compressInternal(data, codec)
		}(i, batch)
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// Decompress decompresses data.
func (gc *GPUCompressor) Decompress(data []byte, codec CodecType) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	atomic.AddInt64(&gc.stats.TotalDecompressions, 1)

	if gc.shouldUseGPU(len(data)) {
		return gc.decompressGPU(data, codec)
	}

	return gc.cpuEncoder.Decompress(data, codec)
}

// decompressGPU decompresses using GPU.
func (gc *GPUCompressor) decompressGPU(data []byte, codec CodecType) ([]byte, error) {
	switch codec {
	case CodecGorilla:
		return gc.decompressGorillaGPU(data)
	case CodecDeltaDelta:
		return gc.decompressDeltaDeltaGPU(data)
	default:
		return gc.cpuEncoder.Decompress(data, codec)
	}
}

// decompressGorillaGPU decompresses Gorilla-compressed data.
func (gc *GPUCompressor) decompressGorillaGPU(data []byte) ([]byte, error) {
	if len(data) < 9 {
		return decompressGorilla(data)
	}

	// Read first value
	firstValue := binary.LittleEndian.Uint64(data[0:8])
	bitWidth := int(data[8])
	bytesPerValue := (bitWidth + 7) / 8

	remaining := data[9:]
	numXors := len(remaining) / bytesPerValue
	if numXors == 0 {
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, firstValue)
		return buf.Bytes(), nil
	}

	// Read XOR values
	xors := make([]uint64, numXors)
	for i := 0; i < numXors; i++ {
		var xor uint64
		for b := 0; b < bytesPerValue; b++ {
			xor |= uint64(remaining[i*bytesPerValue+b]) << (b * 8)
		}
		xors[i] = xor
	}

	// Parallel prefix XOR to reconstruct values (GPU-optimized pattern)
	values := gc.parallelPrefixXOR(firstValue, xors)

	// Output
	var buf bytes.Buffer
	for _, v := range values {
		binary.Write(&buf, binary.LittleEndian, v)
	}

	return buf.Bytes(), nil
}

// parallelPrefixXOR computes prefix XOR (parallel scan).
func (gc *GPUCompressor) parallelPrefixXOR(firstValue uint64, xors []uint64) []uint64 {
	values := make([]uint64, len(xors)+1)
	values[0] = firstValue

	// Sequential for correctness (parallel prefix scan would be used on GPU)
	for i := 0; i < len(xors); i++ {
		values[i+1] = values[i] ^ xors[i]
	}

	return values
}

// decompressDeltaDeltaGPU decompresses delta-delta compressed data.
func (gc *GPUCompressor) decompressDeltaDeltaGPU(data []byte) ([]byte, error) {
	if len(data) < 16 {
		return decompressDeltaDelta(data)
	}

	// Read first value and delta
	firstValue := int64(binary.LittleEndian.Uint64(data[0:8]))
	firstDelta := int64(binary.LittleEndian.Uint64(data[8:16]))

	// Read variable-length encoded delta-of-deltas
	dods, err := gc.readVarInts(data[16:])
	if err != nil {
		return nil, err
	}

	// Reconstruct deltas from delta-of-deltas
	deltas := make([]int64, len(dods)+1)
	deltas[0] = firstDelta
	for i := 0; i < len(dods); i++ {
		deltas[i+1] = deltas[i] + dods[i]
	}

	// Reconstruct values from deltas
	values := make([]int64, len(deltas)+1)
	values[0] = firstValue
	for i := 0; i < len(deltas); i++ {
		values[i+1] = values[i] + deltas[i]
	}

	// Output
	var buf bytes.Buffer
	for _, v := range values {
		binary.Write(&buf, binary.LittleEndian, v)
	}

	return buf.Bytes(), nil
}

// readVarInts reads variable-length encoded integers.
func (gc *GPUCompressor) readVarInts(data []byte) ([]int64, error) {
	var values []int64
	pos := 0

	for pos < len(data) {
		var uv uint64
		shift := 0
		for {
			if pos >= len(data) {
				return values, nil
			}
			b := data[pos]
			pos++
			uv |= uint64(b&0x7f) << shift
			if b < 0x80 {
				break
			}
			shift += 7
		}
		// ZigZag decode
		v := int64(uv>>1) ^ -int64(uv&1)
		values = append(values, v)
	}

	return values, nil
}

// GetStats returns compression statistics.
func (gc *GPUCompressor) GetStats() GPUCompressionStats {
	gc.statsMu.RLock()
	defer gc.statsMu.RUnlock()

	stats := gc.stats

	// Calculate throughput
	totalBytes := float64(stats.TotalBytesIn)
	totalTime := stats.TotalGPUTime + stats.TotalCPUTime
	if totalTime > 0 {
		stats.AverageThroughput = totalBytes / totalTime.Seconds() / (1024 * 1024) // MB/s
	}

	// Memory pool stats
	if gc.memoryPool != nil {
		gc.memoryPool.mu.Lock()
		stats.MemoryPoolHits = gc.memoryPool.hitCount
		stats.MemoryPoolMisses = gc.memoryPool.missCount
		gc.memoryPool.mu.Unlock()
	}

	return stats
}

// GetDeviceInfo returns GPU device information.
func (gc *GPUCompressor) GetDeviceInfo() *GPUDeviceInfo {
	return gc.deviceInfo
}

// GetBackend returns the active GPU backend.
func (gc *GPUCompressor) GetBackend() GPUBackend {
	return gc.backend
}

// IsGPUAvailable returns true if GPU acceleration is available.
func (gc *GPUCompressor) IsGPUAvailable() bool {
	return gc.initialized && gc.backend != GPUBackendNone
}

// ========== Batch Processing API ==========

// BatchCompressor provides high-throughput batch compression.
type BatchCompressor struct {
	gc        *GPUCompressor
	batchSize int
	buffer    [][]byte
	bufferMu  sync.Mutex
	results   chan batchResult
}

type batchResult struct {
	index int
	data  []byte
	err   error
}

// NewBatchCompressor creates a new batch compressor.
func NewBatchCompressor(gc *GPUCompressor, batchSize int) *BatchCompressor {
	return &BatchCompressor{
		gc:        gc,
		batchSize: batchSize,
		buffer:    make([][]byte, 0, batchSize),
		results:   make(chan batchResult, batchSize),
	}
}

// Add adds data to the batch buffer.
func (bc *BatchCompressor) Add(data []byte) {
	bc.bufferMu.Lock()
	bc.buffer = append(bc.buffer, data)
	bc.bufferMu.Unlock()
}

// Flush compresses all buffered data.
func (bc *BatchCompressor) Flush(codec CodecType) ([][]byte, error) {
	bc.bufferMu.Lock()
	batch := bc.buffer
	bc.buffer = make([][]byte, 0, bc.batchSize)
	bc.bufferMu.Unlock()

	if len(batch) == 0 {
		return nil, nil
	}

	return bc.gc.CompressBatch(batch, codec)
}

// ========== Streaming Compression ==========

// StreamingCompressor provides streaming compression with GPU acceleration.
type StreamingCompressor struct {
	gc       *GPUCompressor
	codec    CodecType
	buffer   *bytes.Buffer
	bufferMu sync.Mutex
	window   int
	output   chan []byte
}

// NewStreamingCompressor creates a new streaming compressor.
func NewStreamingCompressor(gc *GPUCompressor, codec CodecType, windowSize int) *StreamingCompressor {
	return &StreamingCompressor{
		gc:     gc,
		codec:  codec,
		buffer: bytes.NewBuffer(nil),
		window: windowSize,
		output: make(chan []byte, 10),
	}
}

// Write writes data to the streaming compressor.
func (sc *StreamingCompressor) Write(data []byte) (int, error) {
	sc.bufferMu.Lock()
	defer sc.bufferMu.Unlock()

	n, err := sc.buffer.Write(data)
	if err != nil {
		return n, err
	}

	// Compress when buffer reaches window size
	if sc.buffer.Len() >= sc.window {
		chunk := make([]byte, sc.window)
		sc.buffer.Read(chunk)

		compressed, err := sc.gc.Compress(chunk, sc.codec)
		if err != nil {
			return n, err
		}

		select {
		case sc.output <- compressed:
		default:
			// Output buffer full
		}
	}

	return n, nil
}

// Output returns the compressed output channel.
func (sc *StreamingCompressor) Output() <-chan []byte {
	return sc.output
}

// Flush flushes remaining data.
func (sc *StreamingCompressor) Flush() ([]byte, error) {
	sc.bufferMu.Lock()
	defer sc.bufferMu.Unlock()

	if sc.buffer.Len() == 0 {
		return nil, nil
	}

	data := sc.buffer.Bytes()
	sc.buffer.Reset()

	return sc.gc.Compress(data, sc.codec)
}

// Close closes the streaming compressor.
func (sc *StreamingCompressor) Close() error {
	close(sc.output)
	return nil
}

// ========== Benchmark Utilities ==========

// GPUBenchmark provides GPU compression benchmarking.
type GPUBenchmark struct {
	gc     *GPUCompressor
	codec  CodecType
}

// NewGPUBenchmark creates a new benchmark utility.
func NewGPUBenchmark(gc *GPUCompressor, codec CodecType) *GPUBenchmark {
	return &GPUBenchmark{gc: gc, codec: codec}
}

// BenchmarkResult contains benchmark results.
type BenchmarkResult struct {
	DataSize          int
	CompressedSize    int
	CompressionRatio  float64
	CompressionTime   time.Duration
	DecompressionTime time.Duration
	Throughput        float64 // MB/s
	UsedGPU           bool
}

// Run runs the benchmark with the specified data.
func (b *GPUBenchmark) Run(data []byte, iterations int) BenchmarkResult {
	result := BenchmarkResult{
		DataSize: len(data),
		UsedGPU:  b.gc.IsGPUAvailable(),
	}

	// Warmup
	b.gc.Compress(data, b.codec)

	// Benchmark compression
	var compressedData []byte
	start := time.Now()
	for i := 0; i < iterations; i++ {
		var err error
		compressedData, err = b.gc.Compress(data, b.codec)
		if err != nil {
			return result
		}
	}
	result.CompressionTime = time.Since(start) / time.Duration(iterations)
	result.CompressedSize = len(compressedData)
	result.CompressionRatio = float64(len(data)) / float64(len(compressedData))

	// Benchmark decompression
	start = time.Now()
	for i := 0; i < iterations; i++ {
		_, err := b.gc.Decompress(compressedData, b.codec)
		if err != nil {
			return result
		}
	}
	result.DecompressionTime = time.Since(start) / time.Duration(iterations)

	// Calculate throughput
	result.Throughput = float64(len(data)) / result.CompressionTime.Seconds() / (1024 * 1024)

	return result
}

// RunSuite runs a benchmark suite with various data sizes.
func (b *GPUBenchmark) RunSuite(minSize, maxSize, iterations int) []BenchmarkResult {
	var results []BenchmarkResult

	for size := minSize; size <= maxSize; size *= 2 {
		// Generate test data (simulated time-series)
		data := generateBenchmarkData(size)
		result := b.Run(data, iterations)
		results = append(results, result)
	}

	return results
}

// gpuGenerateBenchmarkData generates benchmark test data for GPU compression.
func gpuGenerateBenchmarkData(size int) []byte {
	numFloats := size / 8
	data := make([]byte, numFloats*8)

	// Generate time-series-like data (monotonic with noise)
	var prevValue float64 = 100.0
	for i := 0; i < numFloats; i++ {
		// Add small delta with occasional larger changes
		delta := 0.1
		if i%100 == 0 {
			delta = 5.0
		}
		prevValue += delta
		bits := math.Float64bits(prevValue)
		binary.LittleEndian.PutUint64(data[i*8:], bits)
	}

	return data
}
