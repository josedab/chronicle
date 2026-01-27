package encoding

import (
	"encoding/binary"
	"errors"
	"math"
	stdbits "math/bits"

	"github.com/chronicle-db/chronicle/internal/bits"
)

// GorillaEncoder compresses float64 values using XOR-based encoding.
type GorillaEncoder struct {
	bw     *bits.Writer
	prev   uint64
	prevLZ uint8
	prevTZ uint8
	count  int
}

// NewGorillaEncoder creates a new Gorilla encoder.
func NewGorillaEncoder() *GorillaEncoder {
	return &GorillaEncoder{bw: bits.NewWriter()}
}

// Encode adds a value to the compressed stream.
func (e *GorillaEncoder) Encode(value float64) {
	bitsVal := math.Float64bits(value)
	if e.count == 0 {
		e.bw.WriteBits(bitsVal, 64)
		e.prev = bitsVal
		e.count++
		return
	}

	xor := bitsVal ^ e.prev
	if xor == 0 {
		e.bw.WriteBit(0)
	} else {
		e.bw.WriteBit(1)
		lz := uint8(stdbits.LeadingZeros64(xor))
		tz := uint8(stdbits.TrailingZeros64(xor))

		if lz >= e.prevLZ && tz >= e.prevTZ {
			e.bw.WriteBit(0)
			meaningfulBits := 64 - int(e.prevLZ) - int(e.prevTZ)
			e.bw.WriteBits(xor>>e.prevTZ, meaningfulBits)
		} else {
			e.bw.WriteBit(1)
			e.bw.WriteBits(uint64(lz), 5)
			meaningfulBits := 64 - int(lz) - int(tz)
			e.bw.WriteBits(uint64(meaningfulBits), 6)
			e.bw.WriteBits(xor>>tz, meaningfulBits)
			e.prevLZ = lz
			e.prevTZ = tz
		}
	}

	e.prev = bitsVal
	e.count++
}

// Bytes returns the compressed data.
func (e *GorillaEncoder) Bytes() []byte {
	buf := e.bw.Bytes()
	out := make([]byte, 4+len(buf))
	binary.LittleEndian.PutUint32(out, uint32(e.count))
	copy(out[4:], buf)
	return out
}

// EncodeGorilla compresses a slice of float64 values.
func EncodeGorilla(values []float64) []byte {
	enc := NewGorillaEncoder()
	for _, v := range values {
		enc.Encode(v)
	}
	return enc.Bytes()
}

// GorillaDecoder decompresses Gorilla-encoded float64 values.
type GorillaDecoder struct {
	br     *bits.Reader
	prev   uint64
	prevLZ uint8
	prevTZ uint8
	count  int
	index  int
}

// DecodeGorilla decompresses Gorilla-encoded data.
func DecodeGorilla(data []byte) ([]float64, error) {
	if len(data) < 4 {
		return nil, errors.New("gorilla: data too short")
	}
	count := int(binary.LittleEndian.Uint32(data))
	dec := &GorillaDecoder{
		br:    bits.NewReader(data[4:]),
		count: count,
	}

	out := make([]float64, 0, count)
	for i := 0; i < count; i++ {
		val, err := dec.Next()
		if err != nil {
			return nil, err
		}
		out = append(out, val)
	}
	return out, nil
}

// Next returns the next decompressed value.
func (d *GorillaDecoder) Next() (float64, error) {
	if d.index == 0 {
		first, err := d.br.ReadBits(64)
		if err != nil {
			return 0, err
		}
		d.prev = first
		d.index++
		return math.Float64frombits(first), nil
	}

	bit, err := d.br.ReadBit()
	if err != nil {
		return 0, err
	}
	if bit == 0 {
		return math.Float64frombits(d.prev), nil
	}

	control, err := d.br.ReadBit()
	if err != nil {
		return 0, err
	}

	var lz, tz uint8
	var meaningfulBits int

	if control == 0 {
		lz = d.prevLZ
		tz = d.prevTZ
		meaningfulBits = 64 - int(lz) - int(tz)
	} else {
		lzBits, err := d.br.ReadBits(5)
		if err != nil {
			return 0, err
		}
		lz = uint8(lzBits)

		mbits, err := d.br.ReadBits(6)
		if err != nil {
			return 0, err
		}
		meaningfulBits = int(mbits)
		tz = uint8(64 - meaningfulBits - int(lz))
		d.prevLZ = lz
		d.prevTZ = tz
	}

	xorBits, err := d.br.ReadBits(meaningfulBits)
	if err != nil {
		return 0, err
	}

	value := d.prev ^ (xorBits << tz)
	d.prev = value
	d.index++
	return math.Float64frombits(value), nil
}
