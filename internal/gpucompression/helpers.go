package gpucompression

import (
	"bytes"
	"encoding/binary"
)

func bitsRequired(n uint64) int {
	if n == 0 {
		return 1
	}
	bits := 0
	for n > 0 {
		bits++
		n >>= 1
	}
	return bits
}

func generateBenchmarkData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
		if i%10 == 0 {
			data[i] = byte(i / 100)
		}
	}
	return data
}

func compressDeltaDelta(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return data, nil
	}

	values := make([]int64, len(data)/8)
	for i := 0; i < len(values); i++ {
		values[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
	}

	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, values[0])

	if len(values) < 2 {
		return buf.Bytes(), nil
	}

	firstDelta := values[1] - values[0]
	binary.Write(&buf, binary.LittleEndian, firstDelta)

	prevDelta := firstDelta
	for i := 2; i < len(values); i++ {
		delta := values[i] - values[i-1]
		dod := delta - prevDelta
		binary.Write(&buf, binary.LittleEndian, dod)
		prevDelta = delta
	}

	return buf.Bytes(), nil
}

func decompressGorilla(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return data, nil
	}

	var buf bytes.Buffer
	values := make([]uint64, len(data)/8)

	values[0] = binary.LittleEndian.Uint64(data[0:8])

	for i := 1; i < len(values); i++ {
		xor := binary.LittleEndian.Uint64(data[i*8:])
		values[i] = values[i-1] ^ xor
	}

	for _, v := range values {
		binary.Write(&buf, binary.LittleEndian, v)
	}

	return buf.Bytes(), nil
}

func decompressDeltaDelta(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return data, nil
	}

	numValues := len(data) / 8
	var buf bytes.Buffer

	if numValues == 0 {
		return buf.Bytes(), nil
	}

	value := int64(binary.LittleEndian.Uint64(data[0:8]))
	binary.Write(&buf, binary.LittleEndian, value)

	if numValues == 1 {
		return buf.Bytes(), nil
	}

	delta := int64(binary.LittleEndian.Uint64(data[8:16]))
	value += delta
	binary.Write(&buf, binary.LittleEndian, value)

	for i := 2; i < numValues; i++ {
		dod := int64(binary.LittleEndian.Uint64(data[i*8:]))
		delta += dod
		value += delta
		binary.Write(&buf, binary.LittleEndian, value)
	}

	return buf.Bytes(), nil
}
