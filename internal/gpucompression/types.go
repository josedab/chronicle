package gpucompression

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"sync"
	"time"
	"unsafe"

	chronicle "github.com/chronicle-db/chronicle"
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
		PreferredBackend: GPUBackendNone,
		FallbackToCPU:    true,
		MinBatchSize:     1024,
		MaxBatchSize:     1024 * 1024,
		DeviceID:         0,
		MemoryLimit:      512 * 1024 * 1024,
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
	backend     GPUBackend
	deviceInfo  *GPUDeviceInfo
	initialized bool
	initMu      sync.Mutex

	// Memory management
	memoryPool   *GPUMemoryPool
	allocatedMem int64

	// Stream management (for CUDA)
	streams   []GPUStream
	streamIdx uint32

	// Pipeline
	pipeline      *CompressionPipeline
	pipelineInput chan *compressionJob
	pipelineWg    sync.WaitGroup

	// Statistics
	stats   GPUCompressionStats
	statsMu sync.RWMutex

	// CPU fallback
	cpuEncoder *chronicle.AdaptiveCompressionEngine

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
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
	mu         sync.Mutex
	blocks     map[int64]*GPUMemoryBlock
	totalAlloc int64
	maxAlloc   int64
	hitCount   int64
	missCount  int64
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
	TotalBytesIn  int64
	TotalBytesOut int64
	TotalGPUTime  time.Duration
	TotalCPUTime  time.Duration

	// Memory stats
	PeakGPUMemory    int64
	CurrentGPUMemory int64
	MemoryPoolHits   int64
	MemoryPoolMisses int64

	// Throughput
	LastThroughput    float64 // MB/s
	AverageThroughput float64 // MB/s

	// Errors
	GPUErrors       int64
	RecoveredErrors int64
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
	data   []byte
	codec  chronicle.CodecType
	result chan compressionResult
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

	gc.cpuEncoder = chronicle.NewAdaptiveCompressionEngine(chronicle.DefaultAdaptiveCompressionConfig())

	gc.memoryPool = &GPUMemoryPool{
		blocks:   make(map[int64]*GPUMemoryBlock),
		maxAlloc: config.MemoryLimit,
	}

	if config.Enabled {
		if err := gc.initializeGPU(); err != nil {
			if !config.FallbackToCPU {
				cancel()
				return nil, err
			}

		}
	}

	return gc, nil
}

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

// StreamingCompressor provides streaming compression with GPU acceleration.
type StreamingCompressor struct {
	gc       *GPUCompressor
	codec    chronicle.CodecType
	buffer   *bytes.Buffer
	bufferMu sync.Mutex
	window   int
	output   chan []byte
}

// NewStreamingCompressor creates a new streaming compressor.
func NewStreamingCompressor(gc *GPUCompressor, codec chronicle.CodecType, windowSize int) *StreamingCompressor {
	return &StreamingCompressor{
		gc:     gc,
		codec:  codec,
		buffer: bytes.NewBuffer(nil),
		window: windowSize,
		output: make(chan []byte, 10),
	}
}

// GPUBenchmark provides GPU compression benchmarking.
type GPUBenchmark struct {
	gc    *GPUCompressor
	codec chronicle.CodecType
}

// NewGPUBenchmark creates a new benchmark utility.
func NewGPUBenchmark(gc *GPUCompressor, codec chronicle.CodecType) *GPUBenchmark {
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

// gpuGenerateBenchmarkData generates benchmark test data for GPU compression.
func gpuGenerateBenchmarkData(size int) []byte {
	numFloats := size / 8
	data := make([]byte, numFloats*8)

	// Generate time-series-like data (monotonic with noise)
	var prevValue float64 = 100.0
	for i := 0; i < numFloats; i++ {

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
