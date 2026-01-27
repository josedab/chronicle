package chronicle

import (
	"github.com/chronicle-db/chronicle/internal/encoding"
)

// deltaEncoder wraps internal encoding.DeltaEncoder.
type deltaEncoder = encoding.DeltaEncoder

// deltaDecoder wraps internal encoding.DeltaDecoder.
type deltaDecoder = encoding.DeltaDecoder

// encodeDelta compresses int64 values.
func encodeDelta(values []int64) []byte {
	return encoding.EncodeDelta(values)
}

// decodeDelta decompresses int64 values.
func decodeDelta(data []byte) ([]int64, error) {
	return encoding.DecodeDelta(data)
}
