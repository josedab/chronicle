package hardwareaccel

import (
"encoding/binary"
"fmt"
"math"
"runtime"
"sync"
)

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
