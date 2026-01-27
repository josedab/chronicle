package chronicle

import (
	"github.com/chronicle-db/chronicle/internal/encoding"
)

func encodeRawInt64(values []int64) []byte {
	return encoding.EncodeRawInt64(values)
}

func decodeRawInt64(data []byte) ([]int64, error) {
	return encoding.DecodeRawInt64(data)
}

func encodeRawFloat64(values []float64) []byte {
	return encoding.EncodeRawFloat64(values)
}

func decodeRawFloat64(data []byte) ([]float64, error) {
	return encoding.DecodeRawFloat64(data)
}

func encodeRLEBool(values []bool) []byte {
	return encoding.EncodeRLEBool(values)
}

func boolToByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func encodeIndexInt64(values []int64) []byte {
	return encoding.EncodeIndexInt64(values)
}

func encodeIndexFloat64(values []float64) []byte {
	return encoding.EncodeIndexFloat64(values)
}

func encodeIndexBool(values []bool) []byte {
	return encoding.EncodeIndexBool(values)
}
