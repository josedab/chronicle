package chronicle

import (
	"github.com/chronicle-db/chronicle/internal/encoding"
)

// gorillaEncoder wraps internal encoding.GorillaEncoder.
type gorillaEncoder = encoding.GorillaEncoder

// gorillaDecoder wraps internal encoding.GorillaDecoder.
type gorillaDecoder = encoding.GorillaDecoder

// newGorillaEncoder creates a new Gorilla encoder.
func newGorillaEncoder() *gorillaEncoder {
	return encoding.NewGorillaEncoder()
}

// encodeGorilla compresses float64 values.
func encodeGorilla(values []float64) []byte {
	return encoding.EncodeGorilla(values)
}

// decodeGorilla decompresses float64 values.
func decodeGorilla(data []byte) ([]float64, error) {
	return encoding.DecodeGorilla(data)
}
