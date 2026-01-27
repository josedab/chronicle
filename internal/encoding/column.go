package encoding

import (
	"bytes"
	"encoding/binary"
)

// EncodeRawInt64 encodes int64 values without compression.
func EncodeRawInt64(values []int64) []byte {
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(values)))
	for _, v := range values {
		_ = binary.Write(buf, binary.LittleEndian, v)
	}
	return buf.Bytes()
}

// DecodeRawInt64 decodes raw-encoded int64 values.
func DecodeRawInt64(data []byte) ([]int64, error) {
	reader := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	out := make([]int64, 0, count)
	for i := uint32(0); i < count; i++ {
		var v int64
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// EncodeRawFloat64 encodes float64 values without compression.
func EncodeRawFloat64(values []float64) []byte {
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(values)))
	for _, v := range values {
		_ = binary.Write(buf, binary.LittleEndian, v)
	}
	return buf.Bytes()
}

// DecodeRawFloat64 decodes raw-encoded float64 values.
func DecodeRawFloat64(data []byte) ([]float64, error) {
	reader := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	out := make([]float64, 0, count)
	for i := uint32(0); i < count; i++ {
		var v float64
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// EncodeRLEBool encodes boolean values using run-length encoding.
func EncodeRLEBool(values []bool) []byte {
	if len(values) == 0 {
		return []byte{0, 0, 0, 0}
	}
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(values)))

	runVal := values[0]
	runLen := uint32(1)
	for i := 1; i < len(values); i++ {
		if values[i] == runVal {
			runLen++
			continue
		}
		_ = binary.Write(buf, binary.LittleEndian, boolToByte(runVal))
		_ = binary.Write(buf, binary.LittleEndian, runLen)
		runVal = values[i]
		runLen = 1
	}
	_ = binary.Write(buf, binary.LittleEndian, boolToByte(runVal))
	_ = binary.Write(buf, binary.LittleEndian, runLen)
	return buf.Bytes()
}

func boolToByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

// MinMaxInt64 returns the minimum and maximum values from a slice.
func MinMaxInt64(values []int64) (minVal, maxVal int64) {
	if len(values) == 0 {
		return 0, 0
	}
	minVal = values[0]
	maxVal = values[0]
	for _, v := range values[1:] {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	return minVal, maxVal
}

// EncodeIndexInt64 encodes min/max index for int64 values.
func EncodeIndexInt64(values []int64) []byte {
	minVal, maxVal := MinMaxInt64(values)
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.LittleEndian, minVal)
	_ = binary.Write(buf, binary.LittleEndian, maxVal)
	return buf.Bytes()
}

// EncodeIndexFloat64 encodes min/max index for float64 values.
func EncodeIndexFloat64(values []float64) []byte {
	if len(values) == 0 {
		return nil
	}
	minVal := values[0]
	maxVal := values[0]
	for _, v := range values[1:] {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.LittleEndian, minVal)
	_ = binary.Write(buf, binary.LittleEndian, maxVal)
	return buf.Bytes()
}

// EncodeIndexBool encodes true/false counts for boolean values.
func EncodeIndexBool(values []bool) []byte {
	var trueCount uint32
	for _, v := range values {
		if v {
			trueCount++
		}
	}
	falseCount := uint32(len(values)) - trueCount
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.LittleEndian, trueCount)
	_ = binary.Write(buf, binary.LittleEndian, falseCount)
	return buf.Bytes()
}
