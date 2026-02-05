package gpucompression

import (
	"bytes"
	"encoding/binary"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

// initializeGPU initializes the GPU backend.
func (gc *GPUCompressor) initializeGPU() error {
	gc.initMu.Lock()
	defer gc.initMu.Unlock()

	if gc.initialized {
		return nil
	}

	backend := gc.detectGPUBackend()
	if backend == GPUBackendNone {
		return errors.New("no GPU backend available")
	}

	gc.backend = backend
	gc.deviceInfo = gc.getDeviceInfo()

	if gc.config.AsyncMode && gc.config.StreamCount > 0 {
		gc.streams = make([]GPUStream, gc.config.StreamCount)
		for i := range gc.streams {
			gc.streams[i] = GPUStream{
				ID:       i,
				LastUsed: time.Now(),
			}
		}
	}

	gc.pipeline = &CompressionPipeline{
		depth:     gc.config.PipelineDepth,
		batchSize: gc.config.MaxBatchSize,
	}

	gc.initialized = true
	return nil
}

// detectGPUBackend detects the available GPU backend.
func (gc *GPUCompressor) detectGPUBackend() GPUBackend {

	if gc.config.PreferredBackend != GPUBackendNone {
		if gc.isBackendAvailable(gc.config.PreferredBackend) {
			return gc.config.PreferredBackend
		}
	}

	switch runtime.GOOS {
	case "darwin":

		if gc.isBackendAvailable(GPUBackendMetal) {
			return GPUBackendMetal
		}
	case "linux", "windows":

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

	if gc.isBackendAvailable(GPUBackendWebGPU) {
		return GPUBackendWebGPU
	}

	return GPUBackendNone
}

// isBackendAvailable checks if a GPU backend is available.
func (gc *GPUCompressor) isBackendAvailable(backend GPUBackend) bool {

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
func (gc *GPUCompressor) checkCUDAAvailable() bool { return false }

func (gc *GPUCompressor) checkMetalAvailable() bool { return false }

func (gc *GPUCompressor) checkOpenCLAvailable() bool { return false }

func (gc *GPUCompressor) checkWebGPUAvailable() bool { return false }

func (gc *GPUCompressor) checkVulkanAvailable() bool { return false }

// getDeviceInfo returns GPU device information.
func (gc *GPUCompressor) getDeviceInfo() *GPUDeviceInfo {

	return &GPUDeviceInfo{
		Name:              "Simulated GPU",
		ComputeCapability: "8.6",
		TotalMemory:       8 * 1024 * 1024 * 1024,
		FreeMemory:        6 * 1024 * 1024 * 1024,
		MultiProcessors:   80,
		MaxThreadsPerMP:   1536,
		ClockRate:         1800,
		MemoryClockRate:   9500,
		MemoryBusWidth:    256,
		L2CacheSize:       6 * 1024 * 1024,
		Warp:              32,
	}
}

// Start starts the GPU compressor.
func (gc *GPUCompressor) Start() error {

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

	if gc.memoryPool != nil {
		gc.memoryPool.mu.Lock()
		gc.memoryPool.blocks = nil
		gc.memoryPool.mu.Unlock()
	}

	gc.initialized = false
}

// Compress compresses data using GPU acceleration.
func (gc *GPUCompressor) Compress(data []byte, codec chronicle.CodecType) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	if gc.shouldUseGPU(len(data)) {
		return gc.compressGPU(data, codec)
	}

	return gc.compressCPU(data, codec)
}

// CompressAsync compresses data asynchronously.
func (gc *GPUCompressor) CompressAsync(data []byte, codec chronicle.CodecType) <-chan compressionResult {
	resultChan := make(chan compressionResult, 1)

	if !gc.config.AsyncMode {

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

		result, err := gc.Compress(data, codec)
		resultChan <- compressionResult{data: result, err: err}
	}

	return resultChan
}

// CompressBatch compresses multiple data blocks in parallel.
func (gc *GPUCompressor) CompressBatch(batches [][]byte, codec chronicle.CodecType) ([][]byte, error) {
	if len(batches) == 0 {
		return batches, nil
	}

	results := make([][]byte, len(batches))
	errors := make([]error, len(batches))

	totalSize := 0
	for _, b := range batches {
		totalSize += len(b)
	}

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
func (gc *GPUCompressor) compressGPU(data []byte, codec chronicle.CodecType) ([]byte, error) {
	start := time.Now()
	atomic.AddInt64(&gc.stats.GPUCompressions, 1)
	atomic.AddInt64(&gc.stats.TotalBytesIn, int64(len(data)))

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
func (gc *GPUCompressor) compressCPU(data []byte, codec chronicle.CodecType) ([]byte, error) {
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
func (gc *GPUCompressor) compressInternal(data []byte, codec chronicle.CodecType) ([]byte, error) {
	switch codec {
	case chronicle.CodecGorilla:
		return gc.compressGorillaGPU(data)
	case chronicle.CodecDeltaDelta:
		return gc.compressDeltaDeltaGPU(data)
	case chronicle.CodecFloatXOR:
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

	xors := gc.parallelXOR(values)

	return gc.bitPackXOR(values[0], xors)
}

// parallelXOR computes XOR values in parallel (GPU-optimized pattern).
func (gc *GPUCompressor) parallelXOR(values []uint64) []uint64 {
	if len(values) < 2 {
		return nil
	}

	xors := make([]uint64, len(values)-1)

	blockSize := 256
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

	binary.Write(&buf, binary.LittleEndian, firstValue)

	if len(xors) == 0 {
		return buf.Bytes(), nil
	}

	maxBits := 0
	for _, xor := range xors {
		bits := bitsRequired(xor)
		if bits > maxBits {
			maxBits = bits
		}
	}

	buf.WriteByte(byte(maxBits))

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

	deltas := gc.parallelDelta(values)
	if len(deltas) == 0 {
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, values[0])
		return buf.Bytes(), nil
	}

	dods := gc.parallelDeltaOfDelta(deltas)

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

	binary.Write(&buf, binary.LittleEndian, firstValue)
	binary.Write(&buf, binary.LittleEndian, firstDelta)

	for _, dod := range dods {
		gc.writeVarInt(&buf, dod)
	}

	return buf.Bytes(), nil
}

// writeVarInt writes a variable-length integer.
func (gc *GPUCompressor) writeVarInt(buf *bytes.Buffer, v int64) {

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
func (gc *GPUCompressor) compressBatchGPU(batches [][]byte, codec chronicle.CodecType) ([][]byte, error) {

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
func (gc *GPUCompressor) Decompress(data []byte, codec chronicle.CodecType) ([]byte, error) {
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
func (gc *GPUCompressor) decompressGPU(data []byte, codec chronicle.CodecType) ([]byte, error) {
	switch codec {
	case chronicle.CodecGorilla:
		return gc.decompressGorillaGPU(data)
	case chronicle.CodecDeltaDelta:
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

	xors := make([]uint64, numXors)
	for i := 0; i < numXors; i++ {
		var xor uint64
		for b := 0; b < bytesPerValue; b++ {
			xor |= uint64(remaining[i*bytesPerValue+b]) << (b * 8)
		}
		xors[i] = xor
	}

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

	firstValue := int64(binary.LittleEndian.Uint64(data[0:8]))
	firstDelta := int64(binary.LittleEndian.Uint64(data[8:16]))

	dods, err := gc.readVarInts(data[16:])
	if err != nil {
		return nil, err
	}

	deltas := make([]int64, len(dods)+1)
	deltas[0] = firstDelta
	for i := 0; i < len(dods); i++ {
		deltas[i+1] = deltas[i] + dods[i]
	}

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

	totalBytes := float64(stats.TotalBytesIn)
	totalTime := stats.TotalGPUTime + stats.TotalCPUTime
	if totalTime > 0 {
		stats.AverageThroughput = totalBytes / totalTime.Seconds() / (1024 * 1024)
	}

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
