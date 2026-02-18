package hardwareaccel

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

// HardwareAccelConfig configures the hardware acceleration SDK.
type HardwareAccelConfig struct {
	// Enable GPU acceleration
	EnableGPU bool

	// Enable FPGA acceleration
	EnableFPGA bool

	// Enable SIMD optimizations
	EnableSIMD bool

	// GPU device index (-1 for auto)
	GPUDeviceIndex int

	// FPGA device path
	FPGADevicePath string

	// Memory pool size for accelerators (bytes)
	AcceleratorMemoryPool int64

	// Batch size for GPU operations
	GPUBatchSize int

	// Enable async operations
	EnableAsync bool

	// Compression acceleration
	CompressionAcceleration bool

	// Aggregation acceleration
	AggregationAcceleration bool

	// Query acceleration
	QueryAcceleration bool

	// Fallback to CPU on errors
	FallbackToCPU bool

	// Memory alignment for SIMD (bytes)
	SIMDAlignment int
}

// DefaultHardwareAccelConfig returns default configuration.
func DefaultHardwareAccelConfig() *HardwareAccelConfig {
	return &HardwareAccelConfig{
		EnableGPU:               true,
		EnableFPGA:              false,
		EnableSIMD:              true,
		GPUDeviceIndex:          -1,
		FPGADevicePath:          "/dev/fpga0",
		AcceleratorMemoryPool:   1 << 30, // 1GB
		GPUBatchSize:            10000,
		EnableAsync:             true,
		CompressionAcceleration: true,
		AggregationAcceleration: true,
		QueryAcceleration:       true,
		FallbackToCPU:           true,
		SIMDAlignment:           32,
	}
}

// AcceleratorType represents a type of hardware accelerator.
type AcceleratorType int

const (
	AcceleratorCPU AcceleratorType = iota
	AcceleratorSIMD
	AcceleratorGPU
	AcceleratorFPGA
)

func (t AcceleratorType) String() string {
	switch t {
	case AcceleratorCPU:
		return "CPU"
	case AcceleratorSIMD:
		return "SIMD"
	case AcceleratorGPU:
		return "GPU"
	case AcceleratorFPGA:
		return "FPGA"
	default:
		return "Unknown"
	}
}

// AcceleratorCapability represents a capability of an accelerator.
type AcceleratorCapability int

const (
	CapabilityCompression AcceleratorCapability = 1 << iota
	CapabilityDecompression
	CapabilityAggregation
	CapabilityFiltering
	CapabilitySort
	CapabilityJoin
	CapabilityVectorMath
	CapabilityFFT
)

// AcceleratorDevice represents a hardware accelerator device.
type AcceleratorDevice interface {
	// Type returns the accelerator type
	Type() AcceleratorType

	// Name returns device name
	Name() string

	// Capabilities returns supported capabilities
	Capabilities() AcceleratorCapability

	// MemoryTotal returns total memory in bytes
	MemoryTotal() int64

	// MemoryAvailable returns available memory in bytes
	MemoryAvailable() int64

	// Allocate allocates device memory
	Allocate(size int64) (DeviceBuffer, error)

	// Execute runs a kernel on the device
	Execute(kernel AcceleratorKernel, args ...any) error

	// Sync waits for all pending operations
	Sync() error

	// Close releases device resources
	Close() error
}

// DeviceBuffer represents memory allocated on an accelerator.
type DeviceBuffer interface {
	// Size returns buffer size in bytes
	Size() int64

	// CopyToDevice copies data from host to device
	CopyToDevice(data []byte) error

	// CopyToHost copies data from device to host
	CopyToHost() ([]byte, error)

	// Free releases the buffer
	Free() error
}

// AcceleratorKernel represents a computational kernel for accelerators.
type AcceleratorKernel interface {
	// Name returns kernel name
	Name() string

	// Compile compiles the kernel for a device type
	Compile(deviceType AcceleratorType) error
}

// HardwareAccelSDK provides hardware acceleration for time-series operations.
type HardwareAccelSDK struct {
	db     *chronicle.DB
	config *HardwareAccelConfig

	mu      sync.RWMutex
	devices map[AcceleratorType]AcceleratorDevice

	// Memory pools
	gpuPool  *AcceleratorMemoryPool
	fpgaPool *AcceleratorMemoryPool

	// Kernels
	compressionKernel AcceleratorKernel
	aggregationKernel AcceleratorKernel
	filterKernel      AcceleratorKernel

	// Stats
	stats HardwareAccelStats

	// Async operation tracking
	pendingOps sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

// HardwareAccelStats contains acceleration statistics.
type HardwareAccelStats struct {
	// Operations accelerated
	GPUOperations  int64
	FPGAOperations int64
	SIMDOperations int64
	CPUFallbacks   int64

	// Data processed (bytes)
	GPUBytesProcessed  int64
	FPGABytesProcessed int64
	SIMDBytesProcessed int64

	// Timing (nanoseconds)
	TotalGPUTimeNs  int64
	TotalFPGATimeNs int64
	TotalSIMDTimeNs int64

	// Memory usage
	GPUMemoryUsed  int64
	FPGAMemoryUsed int64

	// Errors
	GPUErrors  int64
	FPGAErrors int64
}

// NewHardwareAccelSDK creates a new hardware acceleration SDK.
func NewHardwareAccelSDK(db *chronicle.DB, config *HardwareAccelConfig) (*HardwareAccelSDK, error) {
	if config == nil {
		config = DefaultHardwareAccelConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	sdk := &HardwareAccelSDK{
		db:      db,
		config:  config,
		devices: make(map[AcceleratorType]AcceleratorDevice),
		ctx:     ctx,
		cancel:  cancel,
	}

	// Initialize devices
	if err := sdk.initDevices(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize devices: %w", err)
	}

	// Initialize memory pools
	if err := sdk.initMemoryPools(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize memory pools: %w", err)
	}

	// Initialize kernels
	sdk.initKernels()

	return sdk, nil
}

// initDevices initializes available hardware devices.
func (sdk *HardwareAccelSDK) initDevices() error {
	// Always add CPU device
	sdk.devices[AcceleratorCPU] = &CPUDevice{}

	// Add SIMD device if enabled
	if sdk.config.EnableSIMD && hasSIMDSupport() {
		sdk.devices[AcceleratorSIMD] = &SIMDDevice{
			alignment: sdk.config.SIMDAlignment,
		}
	}

	// Add GPU device if enabled
	if sdk.config.EnableGPU {
		gpu, err := NewGPUDevice(sdk.config.GPUDeviceIndex)
		if err != nil {
			if !sdk.config.FallbackToCPU {
				return fmt.Errorf("GPU initialization failed: %w", err)
			}
			// Log warning but continue
		} else {
			sdk.devices[AcceleratorGPU] = gpu
		}
	}

	// Add FPGA device if enabled
	if sdk.config.EnableFPGA {
		fpga, err := NewFPGADevice(sdk.config.FPGADevicePath)
		if err != nil {
			if !sdk.config.FallbackToCPU {
				return fmt.Errorf("FPGA initialization failed: %w", err)
			}
		} else {
			sdk.devices[AcceleratorFPGA] = fpga
		}
	}

	return nil
}

// initMemoryPools initializes memory pools for accelerators.
func (sdk *HardwareAccelSDK) initMemoryPools() error {
	poolSize := sdk.config.AcceleratorMemoryPool

	if gpu, ok := sdk.devices[AcceleratorGPU]; ok {
		sdk.gpuPool = NewAcceleratorMemoryPool(gpu, poolSize/2)
	}

	if fpga, ok := sdk.devices[AcceleratorFPGA]; ok {
		sdk.fpgaPool = NewAcceleratorMemoryPool(fpga, poolSize/2)
	}

	return nil
}

// initKernels initializes computational kernels.
func (sdk *HardwareAccelSDK) initKernels() {
	sdk.compressionKernel = &CompressionKernel{}
	sdk.aggregationKernel = &AggregationKernel{}
	sdk.filterKernel = &FilterKernel{}
}

// Close shuts down the SDK and releases resources.
func (sdk *HardwareAccelSDK) Close() error {
	sdk.cancel()
	sdk.pendingOps.Wait()

	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	var errs []error
	for _, device := range sdk.devices {
		if err := device.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing devices: %v", errs)
	}
	return nil
}

// GetAvailableDevices returns list of available accelerator devices.
func (sdk *HardwareAccelSDK) GetAvailableDevices() []AcceleratorDevice {
	sdk.mu.RLock()
	defer sdk.mu.RUnlock()

	devices := make([]AcceleratorDevice, 0, len(sdk.devices))
	for _, d := range sdk.devices {
		devices = append(devices, d)
	}
	return devices
}

// SelectDevice selects the best device for a capability.
func (sdk *HardwareAccelSDK) SelectDevice(cap AcceleratorCapability) AcceleratorDevice {
	sdk.mu.RLock()
	defer sdk.mu.RUnlock()

	// Priority: FPGA > GPU > SIMD > CPU
	priorities := []AcceleratorType{AcceleratorFPGA, AcceleratorGPU, AcceleratorSIMD, AcceleratorCPU}

	for _, t := range priorities {
		if d, ok := sdk.devices[t]; ok {
			if d.Capabilities()&cap != 0 {
				return d
			}
		}
	}

	// Fallback to CPU
	return sdk.devices[AcceleratorCPU]
}

// CompressAccelerated performs hardware-accelerated compression.
func (sdk *HardwareAccelSDK) CompressAccelerated(data []float64) ([]byte, error) {
	if !sdk.config.CompressionAcceleration {
		return sdk.compressCPU(data)
	}

	device := sdk.SelectDevice(CapabilityCompression)
	start := time.Now()

	var result []byte
	var err error

	switch device.Type() {
	case AcceleratorGPU:
		result, err = sdk.compressGPU(data)
		if err == nil {
			atomic.AddInt64(&sdk.stats.GPUOperations, 1)
			atomic.AddInt64(&sdk.stats.GPUBytesProcessed, int64(len(data)*8))
			atomic.AddInt64(&sdk.stats.TotalGPUTimeNs, time.Since(start).Nanoseconds())
		} else {
			atomic.AddInt64(&sdk.stats.GPUErrors, 1)
			if sdk.config.FallbackToCPU {
				result, err = sdk.compressCPU(data)
				atomic.AddInt64(&sdk.stats.CPUFallbacks, 1)
			}
		}
	case AcceleratorFPGA:
		result, err = sdk.compressFPGA(data)
		if err == nil {
			atomic.AddInt64(&sdk.stats.FPGAOperations, 1)
			atomic.AddInt64(&sdk.stats.FPGABytesProcessed, int64(len(data)*8))
			atomic.AddInt64(&sdk.stats.TotalFPGATimeNs, time.Since(start).Nanoseconds())
		} else {
			atomic.AddInt64(&sdk.stats.FPGAErrors, 1)
			if sdk.config.FallbackToCPU {
				result, err = sdk.compressCPU(data)
				atomic.AddInt64(&sdk.stats.CPUFallbacks, 1)
			}
		}
	case AcceleratorSIMD:
		result, err = sdk.compressSIMD(data)
		if err == nil {
			atomic.AddInt64(&sdk.stats.SIMDOperations, 1)
			atomic.AddInt64(&sdk.stats.SIMDBytesProcessed, int64(len(data)*8))
			atomic.AddInt64(&sdk.stats.TotalSIMDTimeNs, time.Since(start).Nanoseconds())
		}
	default:
		result, err = sdk.compressCPU(data)
	}

	return result, err
}

// DecompressAccelerated performs hardware-accelerated decompression.
func (sdk *HardwareAccelSDK) DecompressAccelerated(data []byte, count int) ([]float64, error) {
	if !sdk.config.CompressionAcceleration {
		return sdk.decompressCPU(data, count)
	}

	device := sdk.SelectDevice(CapabilityDecompression)
	start := time.Now()

	var result []float64
	var err error

	switch device.Type() {
	case AcceleratorGPU:
		result, err = sdk.decompressGPU(data, count)
		if err == nil {
			atomic.AddInt64(&sdk.stats.GPUOperations, 1)
			atomic.AddInt64(&sdk.stats.TotalGPUTimeNs, time.Since(start).Nanoseconds())
		} else if sdk.config.FallbackToCPU {
			result, err = sdk.decompressCPU(data, count)
			atomic.AddInt64(&sdk.stats.CPUFallbacks, 1)
		}
	case AcceleratorSIMD:
		result, err = sdk.decompressSIMD(data, count)
		if err == nil {
			atomic.AddInt64(&sdk.stats.SIMDOperations, 1)
			atomic.AddInt64(&sdk.stats.TotalSIMDTimeNs, time.Since(start).Nanoseconds())
		}
	default:
		result, err = sdk.decompressCPU(data, count)
	}

	return result, err
}

// AggregateAccelerated performs hardware-accelerated aggregation.
func (sdk *HardwareAccelSDK) AggregateAccelerated(op AggregateOp, data []float64) (float64, error) {
	if !sdk.config.AggregationAcceleration || len(data) < sdk.config.GPUBatchSize {
		return sdk.aggregateCPU(op, data)
	}

	device := sdk.SelectDevice(CapabilityAggregation)
	start := time.Now()

	var result float64
	var err error

	switch device.Type() {
	case AcceleratorGPU:
		result, err = sdk.aggregateGPU(op, data)
		if err == nil {
			atomic.AddInt64(&sdk.stats.GPUOperations, 1)
			atomic.AddInt64(&sdk.stats.GPUBytesProcessed, int64(len(data)*8))
			atomic.AddInt64(&sdk.stats.TotalGPUTimeNs, time.Since(start).Nanoseconds())
		} else if sdk.config.FallbackToCPU {
			result, err = sdk.aggregateCPU(op, data)
			atomic.AddInt64(&sdk.stats.CPUFallbacks, 1)
		}
	case AcceleratorSIMD:
		result, err = sdk.aggregateSIMD(op, data)
		if err == nil {
			atomic.AddInt64(&sdk.stats.SIMDOperations, 1)
			atomic.AddInt64(&sdk.stats.SIMDBytesProcessed, int64(len(data)*8))
			atomic.AddInt64(&sdk.stats.TotalSIMDTimeNs, time.Since(start).Nanoseconds())
		}
	default:
		result, err = sdk.aggregateCPU(op, data)
	}

	return result, err
}

// AggregateOp represents an aggregation operation.
type AggregateOp int

const (
	AggregateSum AggregateOp = iota
	AggregateMin
	AggregateMax
	AggregateMean
	AggregateCount
	AggregateStdDev
	AggregateVariance
)

// FilterAccelerated performs hardware-accelerated filtering.
func (sdk *HardwareAccelSDK) FilterAccelerated(data []float64, predicate func(float64) bool) ([]float64, error) {
	if !sdk.config.QueryAcceleration {
		return sdk.filterCPU(data, predicate)
	}

	// For predicates we need CPU evaluation, but can parallelize with SIMD
	device := sdk.SelectDevice(CapabilityFiltering)

	switch device.Type() {
	case AcceleratorSIMD:
		return sdk.filterSIMD(data, predicate)
	default:
		return sdk.filterCPU(data, predicate)
	}
}

// SortAccelerated performs hardware-accelerated sorting.
func (sdk *HardwareAccelSDK) SortAccelerated(data []float64) error {
	if !sdk.config.QueryAcceleration || len(data) < sdk.config.GPUBatchSize {
		return sdk.sortCPU(data)
	}

	device := sdk.SelectDevice(CapabilitySort)

	switch device.Type() {
	case AcceleratorGPU:
		return sdk.sortGPU(data)
	default:
		return sdk.sortCPU(data)
	}
}

// VectorMathAccelerated performs hardware-accelerated vector math.
func (sdk *HardwareAccelSDK) VectorMathAccelerated(op VectorOp, a, b []float64) ([]float64, error) {
	if len(a) != len(b) {
		return nil, fmt.Errorf("vector length mismatch: %d vs %d", len(a), len(b))
	}
	device := sdk.SelectDevice(CapabilityVectorMath)

	switch device.Type() {
	case AcceleratorGPU:
		return sdk.vectorMathGPU(op, a, b)
	case AcceleratorSIMD:
		return sdk.vectorMathSIMD(op, a, b)
	default:
		return sdk.vectorMathCPU(op, a, b)
	}
}

// VectorOp represents a vector operation.
type VectorOp int

const (
	VectorAdd VectorOp = iota
	VectorSub
	VectorMul
	VectorDiv
	VectorDot
)

// FFTAccelerated performs hardware-accelerated FFT.
func (sdk *HardwareAccelSDK) FFTAccelerated(data []float64) ([]complex128, error) {
	device := sdk.SelectDevice(CapabilityFFT)

	switch device.Type() {
	case AcceleratorGPU:
		return sdk.fftGPU(data)
	default:
		return sdk.fftCPU(data)
	}
}

// Stats returns acceleration statistics.
func (sdk *HardwareAccelSDK) Stats() HardwareAccelStats {
	return HardwareAccelStats{
		GPUOperations:      atomic.LoadInt64(&sdk.stats.GPUOperations),
		FPGAOperations:     atomic.LoadInt64(&sdk.stats.FPGAOperations),
		SIMDOperations:     atomic.LoadInt64(&sdk.stats.SIMDOperations),
		CPUFallbacks:       atomic.LoadInt64(&sdk.stats.CPUFallbacks),
		GPUBytesProcessed:  atomic.LoadInt64(&sdk.stats.GPUBytesProcessed),
		FPGABytesProcessed: atomic.LoadInt64(&sdk.stats.FPGABytesProcessed),
		SIMDBytesProcessed: atomic.LoadInt64(&sdk.stats.SIMDBytesProcessed),
		TotalGPUTimeNs:     atomic.LoadInt64(&sdk.stats.TotalGPUTimeNs),
		TotalFPGATimeNs:    atomic.LoadInt64(&sdk.stats.TotalFPGATimeNs),
		TotalSIMDTimeNs:    atomic.LoadInt64(&sdk.stats.TotalSIMDTimeNs),
		GPUMemoryUsed:      atomic.LoadInt64(&sdk.stats.GPUMemoryUsed),
		FPGAMemoryUsed:     atomic.LoadInt64(&sdk.stats.FPGAMemoryUsed),
		GPUErrors:          atomic.LoadInt64(&sdk.stats.GPUErrors),
		FPGAErrors:         atomic.LoadInt64(&sdk.stats.FPGAErrors),
	}
}

// CPU implementations

func (sdk *HardwareAccelSDK) compressCPU(data []float64) ([]byte, error) {
	// Simple delta encoding + variable length encoding
	if len(data) == 0 {
		return nil, nil
	}

	result := make([]byte, 0, len(data)*8)

	// Write first value as full float64
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(data[0]))
	result = append(result, buf...)

	// Delta encode remaining values
	for i := 1; i < len(data); i++ {
		delta := data[i] - data[i-1]
		binary.LittleEndian.PutUint64(buf, math.Float64bits(delta))
		result = append(result, buf...)
	}

	return result, nil
}

func (sdk *HardwareAccelSDK) decompressCPU(data []byte, count int) ([]float64, error) {
	if len(data) < 8 || count == 0 {
		return nil, nil
	}

	result := make([]float64, count)

	// Read first value
	result[0] = math.Float64frombits(binary.LittleEndian.Uint64(data[0:8]))

	// Delta decode remaining values
	for i := 1; i < count && (i*8+8) <= len(data); i++ {
		delta := math.Float64frombits(binary.LittleEndian.Uint64(data[i*8 : i*8+8]))
		result[i] = result[i-1] + delta
	}

	return result, nil
}

func (sdk *HardwareAccelSDK) aggregateCPU(op AggregateOp, data []float64) (float64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	switch op {
	case AggregateSum:
		sum := 0.0
		for _, v := range data {
			sum += v
		}
		return sum, nil

	case AggregateMin:
		min := data[0]
		for _, v := range data[1:] {
			if v < min {
				min = v
			}
		}
		return min, nil

	case AggregateMax:
		max := data[0]
		for _, v := range data[1:] {
			if v > max {
				max = v
			}
		}
		return max, nil

	case AggregateMean:
		sum := 0.0
		for _, v := range data {
			sum += v
		}
		return sum / float64(len(data)), nil

	case AggregateCount:
		return float64(len(data)), nil

	case AggregateStdDev:
		mean := 0.0
		for _, v := range data {
			mean += v
		}
		mean /= float64(len(data))

		variance := 0.0
		for _, v := range data {
			diff := v - mean
			variance += diff * diff
		}
		variance /= float64(len(data))
		return math.Sqrt(variance), nil

	case AggregateVariance:
		mean := 0.0
		for _, v := range data {
			mean += v
		}
		mean /= float64(len(data))

		variance := 0.0
		for _, v := range data {
			diff := v - mean
			variance += diff * diff
		}
		return variance / float64(len(data)), nil

	default:
		return 0, fmt.Errorf("unknown aggregate op: %d", op)
	}
}

func (sdk *HardwareAccelSDK) filterCPU(data []float64, predicate func(float64) bool) ([]float64, error) {
	result := make([]float64, 0, len(data)/4)
	for _, v := range data {
		if predicate(v) {
			result = append(result, v)
		}
	}
	return result, nil
}

func (sdk *HardwareAccelSDK) sortCPU(data []float64) error {
	// Simple quicksort
	quicksort(data, 0, len(data)-1)
	return nil
}

func quicksort(data []float64, low, high int) {
	if low < high {
		p := partition(data, low, high)
		quicksort(data, low, p-1)
		quicksort(data, p+1, high)
	}
}

func partition(data []float64, low, high int) int {
	pivot := data[high]
	i := low - 1
	for j := low; j < high; j++ {
		if data[j] <= pivot {
			i++
			data[i], data[j] = data[j], data[i]
		}
	}
	data[i+1], data[high] = data[high], data[i+1]
	return i + 1
}

func (sdk *HardwareAccelSDK) vectorMathCPU(op VectorOp, a, b []float64) ([]float64, error) {
	if len(a) != len(b) {
		return nil, fmt.Errorf("vector length mismatch: %d vs %d", len(a), len(b))
	}

	result := make([]float64, len(a))

	switch op {
	case VectorAdd:
		for i := range a {
			result[i] = a[i] + b[i]
		}
	case VectorSub:
		for i := range a {
			result[i] = a[i] - b[i]
		}
	case VectorMul:
		for i := range a {
			result[i] = a[i] * b[i]
		}
	case VectorDiv:
		for i := range a {
			if b[i] == 0 {
				result[i] = math.NaN()
			} else {
				result[i] = a[i] / b[i]
			}
		}
	case VectorDot:
		dot := 0.0
		for i := range a {
			dot += a[i] * b[i]
		}
		return []float64{dot}, nil
	default:
		return nil, fmt.Errorf("unknown vector op: %d", op)
	}

	return result, nil
}

func (sdk *HardwareAccelSDK) fftCPU(data []float64) ([]complex128, error) {
	// Cooley-Tukey FFT (simplified, requires power-of-2 length)
	n := len(data)
	if n == 0 {
		return nil, nil
	}

	// Pad to power of 2
	m := 1
	for m < n {
		m *= 2
	}
	padded := make([]complex128, m)
	for i := 0; i < n; i++ {
		padded[i] = complex(data[i], 0)
	}

	// Bit-reversal permutation
	for i := 1; i < m; i++ {
		j := bitReverse(i, intLog2(m))
		if j > i {
			padded[i], padded[j] = padded[j], padded[i]
		}
	}

	// Iterative FFT
	for s := 1; s <= intLog2(m); s++ {
		mh := 1 << s
		mh2 := mh / 2
		theta := -2 * math.Pi / float64(mh)
		wm := complex(math.Cos(theta), math.Sin(theta))

		for k := 0; k < m; k += mh {
			w := complex(1, 0)
			for j := 0; j < mh2; j++ {
				t := w * padded[k+j+mh2]
				u := padded[k+j]
				padded[k+j] = u + t
				padded[k+j+mh2] = u - t
				w *= wm
			}
		}
	}

	return padded[:n], nil
}

func bitReverse(n, bits int) int {
	result := 0
	for i := 0; i < bits; i++ {
		result = (result << 1) | (n & 1)
		n >>= 1
	}
	return result
}

func intLog2(n int) int {
	result := 0
	for n > 1 {
		n >>= 1
		result++
	}
	return result
}

// SIMD implementations

func (sdk *HardwareAccelSDK) compressSIMD(data []float64) ([]byte, error) {
	// SIMD-optimized compression with parallel delta encoding
	return sdk.compressCPU(data) // Placeholder - real impl would use SIMD intrinsics
}

func (sdk *HardwareAccelSDK) decompressSIMD(data []byte, count int) ([]float64, error) {
	return sdk.decompressCPU(data, count)
}

func (sdk *HardwareAccelSDK) aggregateSIMD(op AggregateOp, data []float64) (float64, error) {
	// Process in SIMD lanes
	const laneWidth = 4

	if len(data) < laneWidth {
		return sdk.aggregateCPU(op, data)
	}

	switch op {
	case AggregateSum:
		// Vectorized sum
		lanes := make([]float64, laneWidth)
		for i := 0; i+laneWidth <= len(data); i += laneWidth {
			for j := 0; j < laneWidth; j++ {
				lanes[j] += data[i+j]
			}
		}
		sum := 0.0
		for _, v := range lanes {
			sum += v
		}
		// Handle remainder
		for i := (len(data) / laneWidth) * laneWidth; i < len(data); i++ {
			sum += data[i]
		}
		return sum, nil

	case AggregateMin:
		lanes := make([]float64, laneWidth)
		for j := 0; j < laneWidth; j++ {
			lanes[j] = math.MaxFloat64
		}
		for i := 0; i+laneWidth <= len(data); i += laneWidth {
			for j := 0; j < laneWidth; j++ {
				if data[i+j] < lanes[j] {
					lanes[j] = data[i+j]
				}
			}
		}
		min := lanes[0]
		for _, v := range lanes[1:] {
			if v < min {
				min = v
			}
		}
		for i := (len(data) / laneWidth) * laneWidth; i < len(data); i++ {
			if data[i] < min {
				min = data[i]
			}
		}
		return min, nil

	case AggregateMax:
		lanes := make([]float64, laneWidth)
		for j := 0; j < laneWidth; j++ {
			lanes[j] = -math.MaxFloat64
		}
		for i := 0; i+laneWidth <= len(data); i += laneWidth {
			for j := 0; j < laneWidth; j++ {
				if data[i+j] > lanes[j] {
					lanes[j] = data[i+j]
				}
			}
		}
		max := lanes[0]
		for _, v := range lanes[1:] {
			if v > max {
				max = v
			}
		}
		for i := (len(data) / laneWidth) * laneWidth; i < len(data); i++ {
			if data[i] > max {
				max = data[i]
			}
		}
		return max, nil

	default:
		return sdk.aggregateCPU(op, data)
	}
}

func (sdk *HardwareAccelSDK) filterSIMD(data []float64, predicate func(float64) bool) ([]float64, error) {
	// Parallel predicate evaluation
	numWorkers := runtime.NumCPU()
	chunkSize := (len(data) + numWorkers - 1) / numWorkers

	results := make([][]float64, numWorkers)
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			start := workerID * chunkSize
			end := start + chunkSize
			if end > len(data) {
				end = len(data)
			}

			local := make([]float64, 0)
			for i := start; i < end; i++ {
				if predicate(data[i]) {
					local = append(local, data[i])
				}
			}
			results[workerID] = local
		}(w)
	}

	wg.Wait()

	// Combine results
	total := 0
	for _, r := range results {
		total += len(r)
	}
	combined := make([]float64, 0, total)
	for _, r := range results {
		combined = append(combined, r...)
	}

	return combined, nil
}

func (sdk *HardwareAccelSDK) vectorMathSIMD(op VectorOp, a, b []float64) ([]float64, error) {
	if len(a) != len(b) {
		return nil, fmt.Errorf("vector length mismatch")
	}

	const laneWidth = 4
	result := make([]float64, len(a))

	// Process in SIMD lanes
	n := len(a)
	aligned := (n / laneWidth) * laneWidth

	switch op {
	case VectorAdd:
		for i := 0; i < aligned; i += laneWidth {
			result[i] = a[i] + b[i]
			result[i+1] = a[i+1] + b[i+1]
			result[i+2] = a[i+2] + b[i+2]
			result[i+3] = a[i+3] + b[i+3]
		}
		for i := aligned; i < n; i++ {
			result[i] = a[i] + b[i]
		}

	case VectorSub:
		for i := 0; i < aligned; i += laneWidth {
			result[i] = a[i] - b[i]
			result[i+1] = a[i+1] - b[i+1]
			result[i+2] = a[i+2] - b[i+2]
			result[i+3] = a[i+3] - b[i+3]
		}
		for i := aligned; i < n; i++ {
			result[i] = a[i] - b[i]
		}

	case VectorMul:
		for i := 0; i < aligned; i += laneWidth {
			result[i] = a[i] * b[i]
			result[i+1] = a[i+1] * b[i+1]
			result[i+2] = a[i+2] * b[i+2]
			result[i+3] = a[i+3] * b[i+3]
		}
		for i := aligned; i < n; i++ {
			result[i] = a[i] * b[i]
		}

	default:
		return sdk.vectorMathCPU(op, a, b)
	}

	return result, nil
}

// GPU implementations (stubs - real impl would use CUDA/OpenCL/Metal)

func (sdk *HardwareAccelSDK) compressGPU(data []float64) ([]byte, error) {
	gpu, ok := sdk.devices[AcceleratorGPU]
	if !ok {
		return nil, fmt.Errorf("GPU device not available")
	}

	// Allocate device memory
	buf, err := gpu.Allocate(int64(len(data) * 8))
	if err != nil {
		return nil, fmt.Errorf("GPU allocation failed: %w", err)
	}
	defer buf.Free()

	// Copy data to device
	hostData := make([]byte, len(data)*8)
	for i, v := range data {
		binary.LittleEndian.PutUint64(hostData[i*8:], math.Float64bits(v))
	}
	if err := buf.CopyToDevice(hostData); err != nil {
		return nil, err
	}

	// Execute compression kernel
	if err := gpu.Execute(sdk.compressionKernel, buf); err != nil {
		return nil, err
	}

	// Copy result back
	return buf.CopyToHost()
}

func (sdk *HardwareAccelSDK) decompressGPU(data []byte, count int) ([]float64, error) {
	gpu, ok := sdk.devices[AcceleratorGPU]
	if !ok {
		return nil, fmt.Errorf("GPU device not available")
	}

	buf, err := gpu.Allocate(int64(len(data)))
	if err != nil {
		return nil, err
	}
	defer buf.Free()

	if err := buf.CopyToDevice(data); err != nil {
		return nil, err
	}

	if err := gpu.Execute(sdk.compressionKernel, buf, "decompress"); err != nil {
		return nil, err
	}

	resultBytes, err := buf.CopyToHost()
	if err != nil {
		return nil, err
	}

	result := make([]float64, count)
	for i := 0; i < count && i*8+8 <= len(resultBytes); i++ {
		result[i] = math.Float64frombits(binary.LittleEndian.Uint64(resultBytes[i*8:]))
	}

	return result, nil
}

func (sdk *HardwareAccelSDK) aggregateGPU(op AggregateOp, data []float64) (float64, error) {
	gpu, ok := sdk.devices[AcceleratorGPU]
	if !ok {
		return 0, fmt.Errorf("GPU device not available")
	}

	buf, err := gpu.Allocate(int64(len(data) * 8))
	if err != nil {
		return 0, err
	}
	defer buf.Free()

	hostData := make([]byte, len(data)*8)
	for i, v := range data {
		binary.LittleEndian.PutUint64(hostData[i*8:], math.Float64bits(v))
	}
	if err := buf.CopyToDevice(hostData); err != nil {
		return 0, err
	}

	if err := gpu.Execute(sdk.aggregationKernel, buf, op); err != nil {
		return 0, err
	}

	resultBytes, err := buf.CopyToHost()
	if err != nil {
		return 0, err
	}

	if len(resultBytes) >= 8 {
		return math.Float64frombits(binary.LittleEndian.Uint64(resultBytes[:8])), nil
	}
	return 0, nil
}

func (sdk *HardwareAccelSDK) sortGPU(data []float64) error {
	gpu, ok := sdk.devices[AcceleratorGPU]
	if !ok {
		return fmt.Errorf("GPU device not available")
	}

	buf, err := gpu.Allocate(int64(len(data) * 8))
	if err != nil {
		return err
	}
	defer buf.Free()

	hostData := make([]byte, len(data)*8)
	for i, v := range data {
		binary.LittleEndian.PutUint64(hostData[i*8:], math.Float64bits(v))
	}
	if err := buf.CopyToDevice(hostData); err != nil {
		return err
	}

	// Execute bitonic sort kernel
	if err := gpu.Execute(nil, buf, "sort"); err != nil {
		return err
	}

	resultBytes, err := buf.CopyToHost()
	if err != nil {
		return err
	}

	for i := range data {
		data[i] = math.Float64frombits(binary.LittleEndian.Uint64(resultBytes[i*8:]))
	}

	return nil
}

func (sdk *HardwareAccelSDK) vectorMathGPU(op VectorOp, a, b []float64) ([]float64, error) {
	gpu, ok := sdk.devices[AcceleratorGPU]
	if !ok {
		return nil, fmt.Errorf("GPU device not available")
	}

	n := len(a)
	bufA, err := gpu.Allocate(int64(n * 8))
	if err != nil {
		return nil, err
	}
	defer bufA.Free()

	bufB, err := gpu.Allocate(int64(n * 8))
	if err != nil {
		return nil, err
	}
	defer bufB.Free()

	// Copy vectors to device
	hostA := make([]byte, n*8)
	hostB := make([]byte, n*8)
	for i := range a {
		binary.LittleEndian.PutUint64(hostA[i*8:], math.Float64bits(a[i]))
		binary.LittleEndian.PutUint64(hostB[i*8:], math.Float64bits(b[i]))
	}
	if err := bufA.CopyToDevice(hostA); err != nil {
		return nil, err
	}
	if err := bufB.CopyToDevice(hostB); err != nil {
		return nil, err
	}

	// Execute vector kernel
	if err := gpu.Execute(nil, bufA, bufB, op); err != nil {
		return nil, err
	}

	resultBytes, err := bufA.CopyToHost()
	if err != nil {
		return nil, err
	}

	result := make([]float64, n)
	for i := range result {
		result[i] = math.Float64frombits(binary.LittleEndian.Uint64(resultBytes[i*8:]))
	}

	return result, nil
}

func (sdk *HardwareAccelSDK) fftGPU(data []float64) ([]complex128, error) {
	// GPU FFT would use cuFFT, clFFT, or similar
	return sdk.fftCPU(data)
}

// FPGA implementation (stub)

func (sdk *HardwareAccelSDK) compressFPGA(data []float64) ([]byte, error) {
	fpga, ok := sdk.devices[AcceleratorFPGA]
	if !ok {
		return nil, fmt.Errorf("FPGA device not available")
	}

	buf, err := fpga.Allocate(int64(len(data) * 8))
	if err != nil {
		return nil, err
	}
	defer buf.Free()

	hostData := make([]byte, len(data)*8)
	for i, v := range data {
		binary.LittleEndian.PutUint64(hostData[i*8:], math.Float64bits(v))
	}
	if err := buf.CopyToDevice(hostData); err != nil {
		return nil, err
	}

	if err := fpga.Execute(sdk.compressionKernel, buf); err != nil {
		return nil, err
	}

	return buf.CopyToHost()
}

// Device implementations

// CPUDevice implements AcceleratorDevice for CPU.
type CPUDevice struct{}

func (d *CPUDevice) Type() AcceleratorType { return AcceleratorCPU }
func (d *CPUDevice) Name() string          { return "CPU" }
func (d *CPUDevice) Capabilities() AcceleratorCapability {
	return CapabilityCompression | CapabilityDecompression | CapabilityAggregation |
		CapabilityFiltering | CapabilitySort | CapabilityVectorMath | CapabilityFFT
}
func (d *CPUDevice) MemoryTotal() int64     { return 0 }
func (d *CPUDevice) MemoryAvailable() int64 { return 0 }
func (d *CPUDevice) Allocate(size int64) (DeviceBuffer, error) {
	return &CPUBuffer{data: make([]byte, size)}, nil
}
func (d *CPUDevice) Execute(kernel AcceleratorKernel, args ...any) error {
	return nil // CPU execution is direct
}
func (d *CPUDevice) Sync() error  { return nil }
func (d *CPUDevice) Close() error { return nil }

// CPUBuffer implements DeviceBuffer for CPU.
type CPUBuffer struct {
	data []byte
}

func (b *CPUBuffer) Size() int64                    { return int64(len(b.data)) }
func (b *CPUBuffer) CopyToDevice(data []byte) error { copy(b.data, data); return nil }
func (b *CPUBuffer) CopyToHost() ([]byte, error)    { return b.data, nil }
func (b *CPUBuffer) Free() error                    { b.data = nil; return nil }

// SIMDDevice implements AcceleratorDevice for SIMD operations.
type SIMDDevice struct {
	alignment int
}

func (d *SIMDDevice) Type() AcceleratorType { return AcceleratorSIMD }
func (d *SIMDDevice) Name() string          { return "SIMD" }
func (d *SIMDDevice) Capabilities() AcceleratorCapability {
	return CapabilityCompression | CapabilityDecompression | CapabilityAggregation |
		CapabilityFiltering | CapabilityVectorMath
}
func (d *SIMDDevice) MemoryTotal() int64     { return 0 }
func (d *SIMDDevice) MemoryAvailable() int64 { return 0 }
func (d *SIMDDevice) Allocate(size int64) (DeviceBuffer, error) {
	// Allocate aligned memory
	aligned := (size + int64(d.alignment-1)) &^ int64(d.alignment-1)
	return &CPUBuffer{data: make([]byte, aligned)}, nil
}
func (d *SIMDDevice) Execute(kernel AcceleratorKernel, args ...any) error {
	return nil
}
func (d *SIMDDevice) Sync() error  { return nil }
func (d *SIMDDevice) Close() error { return nil }

// GPUDevice implements AcceleratorDevice for GPU.
type GPUDevice struct {
	deviceIndex int
	name        string
	memoryTotal int64
	memoryUsed  int64
	mu          sync.Mutex
}

// NewGPUDevice creates a new GPU device.
func NewGPUDevice(deviceIndex int) (*GPUDevice, error) {
	// In real impl, would use CUDA/OpenCL/Metal runtime to detect GPU
	// For now, return a simulated device
	return &GPUDevice{
		deviceIndex: deviceIndex,
		name:        "Simulated GPU",
		memoryTotal: 8 << 30, // 8GB
	}, nil
}

func (d *GPUDevice) Type() AcceleratorType { return AcceleratorGPU }
func (d *GPUDevice) Name() string          { return d.name }
func (d *GPUDevice) Capabilities() AcceleratorCapability {
	return CapabilityCompression | CapabilityDecompression | CapabilityAggregation |
		CapabilitySort | CapabilityVectorMath | CapabilityFFT
}
func (d *GPUDevice) MemoryTotal() int64 { return d.memoryTotal }
func (d *GPUDevice) MemoryAvailable() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.memoryTotal - d.memoryUsed
}

func (d *GPUDevice) Allocate(size int64) (DeviceBuffer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.memoryUsed+size > d.memoryTotal {
		return nil, fmt.Errorf("GPU out of memory: need %d, have %d", size, d.memoryTotal-d.memoryUsed)
	}

	d.memoryUsed += size
	return &GPUBuffer{
		device: d,
		size:   size,
		data:   make([]byte, size), // Simulated - real impl would allocate on GPU
	}, nil
}

func (d *GPUDevice) Execute(kernel AcceleratorKernel, args ...any) error {
	// Simulated execution - real impl would launch GPU kernel
	return nil
}

func (d *GPUDevice) Sync() error {
	// Wait for all GPU operations to complete
	return nil
}

func (d *GPUDevice) Close() error {
	return nil
}

// GPUBuffer implements DeviceBuffer for GPU.
type GPUBuffer struct {
	device *GPUDevice
	size   int64
	data   []byte // Simulated - real impl would be GPU memory pointer
}

func (b *GPUBuffer) Size() int64 { return b.size }

func (b *GPUBuffer) CopyToDevice(data []byte) error {
	// Simulated copy - real impl would use cudaMemcpy or similar
	copy(b.data, data)
	return nil
}

func (b *GPUBuffer) CopyToHost() ([]byte, error) {
	result := make([]byte, len(b.data))
	copy(result, b.data)
	return result, nil
}

func (b *GPUBuffer) Free() error {
	b.device.mu.Lock()
	b.device.memoryUsed -= b.size
	b.device.mu.Unlock()
	b.data = nil
	return nil
}

// FPGADevice implements AcceleratorDevice for FPGA.
type FPGADevice struct {
	devicePath  string
	memoryTotal int64
	memoryUsed  int64
	mu          sync.Mutex
}

// NewFPGADevice creates a new FPGA device.
func NewFPGADevice(devicePath string) (*FPGADevice, error) {
	// In real impl, would open FPGA device file and configure bitstream
	return &FPGADevice{
		devicePath:  devicePath,
		memoryTotal: 4 << 30, // 4GB
	}, nil
}

func (d *FPGADevice) Type() AcceleratorType { return AcceleratorFPGA }
func (d *FPGADevice) Name() string          { return "FPGA: " + d.devicePath }
func (d *FPGADevice) Capabilities() AcceleratorCapability {
	return CapabilityCompression | CapabilityDecompression | CapabilityAggregation
}
func (d *FPGADevice) MemoryTotal() int64 { return d.memoryTotal }
func (d *FPGADevice) MemoryAvailable() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.memoryTotal - d.memoryUsed
}

func (d *FPGADevice) Allocate(size int64) (DeviceBuffer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.memoryUsed+size > d.memoryTotal {
		return nil, fmt.Errorf("FPGA out of memory")
	}

	d.memoryUsed += size
	return &FPGABuffer{
		device: d,
		size:   size,
		data:   make([]byte, size),
	}, nil
}

func (d *FPGADevice) Execute(kernel AcceleratorKernel, args ...any) error {
	// Execute on FPGA - real impl would interact with FPGA driver
	return nil
}

func (d *FPGADevice) Sync() error  { return nil }
func (d *FPGADevice) Close() error { return nil }

// FPGABuffer implements DeviceBuffer for FPGA.
type FPGABuffer struct {
	device *FPGADevice
	size   int64
	data   []byte
}

func (b *FPGABuffer) Size() int64                    { return b.size }
func (b *FPGABuffer) CopyToDevice(data []byte) error { copy(b.data, data); return nil }
func (b *FPGABuffer) CopyToHost() ([]byte, error) {
	result := make([]byte, len(b.data))
	copy(result, b.data)
	return result, nil
}
func (b *FPGABuffer) Free() error {
	b.device.mu.Lock()
	b.device.memoryUsed -= b.size
	b.device.mu.Unlock()
	b.data = nil
	return nil
}

// Kernel implementations

// CompressionKernel implements compression/decompression kernels.
type CompressionKernel struct{}

func (k *CompressionKernel) Name() string                             { return "compression" }
func (k *CompressionKernel) Compile(deviceType AcceleratorType) error { return nil }

// AggregationKernel implements aggregation kernels.
type AggregationKernel struct{}

func (k *AggregationKernel) Name() string                             { return "aggregation" }
func (k *AggregationKernel) Compile(deviceType AcceleratorType) error { return nil }

// FilterKernel implements filtering kernels.
type FilterKernel struct{}

func (k *FilterKernel) Name() string                             { return "filter" }
func (k *FilterKernel) Compile(deviceType AcceleratorType) error { return nil }

// AcceleratorMemoryPool manages memory allocations for an accelerator.
type AcceleratorMemoryPool struct {
	device   AcceleratorDevice
	maxSize  int64
	usedSize int64
	buffers  []*pooledBuffer
	mu       sync.Mutex
}

type pooledBuffer struct {
	buf   DeviceBuffer
	inUse bool
	size  int64
}

// NewAcceleratorMemoryPool creates a new memory pool.
func NewAcceleratorMemoryPool(device AcceleratorDevice, maxSize int64) *AcceleratorMemoryPool {
	return &AcceleratorMemoryPool{
		device:  device,
		maxSize: maxSize,
	}
}

// Allocate gets a buffer from the pool or allocates a new one.
func (p *AcceleratorMemoryPool) Allocate(size int64) (DeviceBuffer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Try to reuse existing buffer
	for _, pb := range p.buffers {
		if !pb.inUse && pb.size >= size {
			pb.inUse = true
			return &pooledBufferWrapper{pool: p, pb: pb}, nil
		}
	}

	// Allocate new buffer
	if p.usedSize+size > p.maxSize {
		return nil, fmt.Errorf("memory pool exhausted")
	}

	buf, err := p.device.Allocate(size)
	if err != nil {
		return nil, err
	}

	pb := &pooledBuffer{buf: buf, inUse: true, size: size}
	p.buffers = append(p.buffers, pb)
	p.usedSize += size

	return &pooledBufferWrapper{pool: p, pb: pb}, nil
}

// Release returns a buffer to the pool.
func (p *AcceleratorMemoryPool) Release(pb *pooledBuffer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pb.inUse = false
}

type pooledBufferWrapper struct {
	pool *AcceleratorMemoryPool
	pb   *pooledBuffer
}

func (w *pooledBufferWrapper) Size() int64                    { return w.pb.buf.Size() }
func (w *pooledBufferWrapper) CopyToDevice(data []byte) error { return w.pb.buf.CopyToDevice(data) }
func (w *pooledBufferWrapper) CopyToHost() ([]byte, error)    { return w.pb.buf.CopyToHost() }
func (w *pooledBufferWrapper) Free() error {
	w.pool.Release(w.pb)
	return nil
}

// Helper functions

func hasSIMDSupport() bool {
	// Check for SIMD support (SSE4, AVX, etc.)
	// In real impl, would check CPU features
	return true
}

// BatchProcessor provides batched processing with hardware acceleration.
type BatchProcessor struct {
	sdk       *HardwareAccelSDK
	batchSize int
}

// NewBatchProcessor creates a new batch processor.
func NewBatchProcessor(sdk *HardwareAccelSDK, batchSize int) *BatchProcessor {
	return &BatchProcessor{
		sdk:       sdk,
		batchSize: batchSize,
	}
}

// ProcessBatch processes a batch of points with acceleration.
func (p *BatchProcessor) ProcessBatch(points []chronicle.Point) ([]chronicle.Point, error) {
	if len(points) == 0 {
		return points, nil
	}

	// Extract values
	values := make([]float64, len(points))
	for i, pt := range points {
		values[i] = pt.Value
	}

	// Compress
	_, err := p.sdk.CompressAccelerated(values)
	if err != nil {
		return nil, err
	}

	return points, nil
}

// AsyncOperation represents an asynchronous accelerated operation.
type AsyncOperation struct {
	id     string
	done   chan struct{}
	result any
	err    error
}

// Wait waits for the operation to complete.
func (op *AsyncOperation) Wait() (any, error) {
	<-op.done
	return op.result, op.err
}

// AsyncCompressAccelerated performs async hardware-accelerated compression.
func (sdk *HardwareAccelSDK) AsyncCompressAccelerated(data []float64) *AsyncOperation {
	op := &AsyncOperation{
		id:   fmt.Sprintf("compress-%d", time.Now().UnixNano()),
		done: make(chan struct{}),
	}

	sdk.pendingOps.Add(1)
	go func() {
		defer sdk.pendingOps.Done()
		defer close(op.done)

		result, err := sdk.CompressAccelerated(data)
		op.result = result
		op.err = err
	}()

	return op
}

// AsyncAggregateAccelerated performs async hardware-accelerated aggregation.
func (sdk *HardwareAccelSDK) AsyncAggregateAccelerated(op AggregateOp, data []float64) *AsyncOperation {
	asyncOp := &AsyncOperation{
		id:   fmt.Sprintf("aggregate-%d", time.Now().UnixNano()),
		done: make(chan struct{}),
	}

	sdk.pendingOps.Add(1)
	go func() {
		defer sdk.pendingOps.Done()
		defer close(asyncOp.done)

		result, err := sdk.AggregateAccelerated(op, data)
		asyncOp.result = result
		asyncOp.err = err
	}()

	return asyncOp
}
