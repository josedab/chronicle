package gpucompression

import "time"

// Run runs the benchmark with the specified data.
func (b *GPUBenchmark) Run(data []byte, iterations int) BenchmarkResult {
	result := BenchmarkResult{
		DataSize: len(data),
		UsedGPU:  b.gc.IsGPUAvailable(),
	}

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

	start = time.Now()
	for i := 0; i < iterations; i++ {
		_, err := b.gc.Decompress(compressedData, b.codec)
		if err != nil {
			return result
		}
	}
	result.DecompressionTime = time.Since(start) / time.Duration(iterations)

	result.Throughput = float64(len(data)) / result.CompressionTime.Seconds() / (1024 * 1024)

	return result
}

// RunSuite runs a benchmark suite with various data sizes.
func (b *GPUBenchmark) RunSuite(minSize, maxSize, iterations int) []BenchmarkResult {
	var results []BenchmarkResult

	for size := minSize; size <= maxSize; size *= 2 {

		data := generateBenchmarkData(size)
		result := b.Run(data, iterations)
		results = append(results, result)
	}

	return results
}
