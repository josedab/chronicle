package encoding

import (
	"encoding/binary"
	"errors"

	"github.com/chronicle-db/chronicle/internal/bits"
)

// DeltaEncoder compresses int64 values using delta-of-delta encoding.
type DeltaEncoder struct {
	bw        *bits.Writer
	prevValue int64
	prevDelta int64
	count     int
}

// NewDeltaEncoder creates a new delta encoder.
func NewDeltaEncoder() *DeltaEncoder {
	return &DeltaEncoder{bw: bits.NewWriter()}
}

// Encode adds a value to the compressed stream.
func (e *DeltaEncoder) Encode(value int64) {
	if e.count == 0 {
		e.bw.WriteBits(uint64(value), 64)
		e.prevValue = value
		e.count++
		return
	}

	delta := value - e.prevValue
	if e.count == 1 {
		e.writeVarint(delta)
		e.prevDelta = delta
		e.prevValue = value
		e.count++
		return
	}

	dod := delta - e.prevDelta
	switch {
	case dod == 0:
		e.bw.WriteBit(0)
	case dod >= -63 && dod <= 64:
		e.bw.WriteBits(0b10, 2)
		e.bw.WriteBits(uint64(dod+63), 7)
	case dod >= -255 && dod <= 256:
		e.bw.WriteBits(0b110, 3)
		e.bw.WriteBits(uint64(dod+255), 9)
	case dod >= -2047 && dod <= 2048:
		e.bw.WriteBits(0b1110, 4)
		e.bw.WriteBits(uint64(dod+2047), 12)
	default:
		e.bw.WriteBits(0b1111, 4)
		e.bw.WriteBits(uint64(dod), 64)
	}

	e.prevDelta = delta
	e.prevValue = value
	e.count++
}

func (e *DeltaEncoder) writeVarint(value int64) {
	zigzag := uint64((value << 1) ^ (value >> 63))
	for {
		b := zigzag & 0x7f
		zigzag >>= 7
		if zigzag != 0 {
			e.bw.WriteBits(b|0x80, 8)
		} else {
			e.bw.WriteBits(b, 8)
			break
		}
	}
}

// Bytes returns the compressed data.
func (e *DeltaEncoder) Bytes() []byte {
	buf := e.bw.Bytes()
	out := make([]byte, 4+len(buf))
	binary.LittleEndian.PutUint32(out, uint32(e.count))
	copy(out[4:], buf)
	return out
}

// EncodeDelta compresses a slice of int64 values.
func EncodeDelta(values []int64) []byte {
	enc := NewDeltaEncoder()
	for _, v := range values {
		enc.Encode(v)
	}
	return enc.Bytes()
}

// DeltaDecoder decompresses delta-encoded int64 values.
type DeltaDecoder struct {
	br        *bits.Reader
	prevValue int64
	prevDelta int64
	count     int
	index     int
}

// DecodeDelta decompresses delta-encoded data.
func DecodeDelta(data []byte) ([]int64, error) {
	if len(data) < 4 {
		return nil, errors.New("delta: data too short")
	}
	count := int(binary.LittleEndian.Uint32(data))
	dec := &DeltaDecoder{
		br:    bits.NewReader(data[4:]),
		count: count,
	}

	out := make([]int64, 0, count)
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
func (d *DeltaDecoder) Next() (int64, error) {
	if d.index == 0 {
		b, err := d.br.ReadBits(64)
		if err != nil {
			return 0, err
		}
		d.prevValue = int64(b)
		d.index++
		return d.prevValue, nil
	}

	if d.index == 1 {
		delta, err := d.readVarint()
		if err != nil {
			return 0, err
		}
		d.prevDelta = delta
		d.prevValue += delta
		d.index++
		return d.prevValue, nil
	}

	bit, err := d.br.ReadBit()
	if err != nil {
		return 0, err
	}

	var dod int64
	if bit == 0 {
		dod = 0
	} else {
		b2, err := d.br.ReadBit()
		if err != nil {
			return 0, err
		}
		if b2 == 0 {
			v, err := d.br.ReadBits(7)
			if err != nil {
				return 0, err
			}
			dod = int64(v) - 63
		} else {
			b3, err := d.br.ReadBit()
			if err != nil {
				return 0, err
			}
			if b3 == 0 {
				v, err := d.br.ReadBits(9)
				if err != nil {
					return 0, err
				}
				dod = int64(v) - 255
			} else {
				b4, err := d.br.ReadBit()
				if err != nil {
					return 0, err
				}
				if b4 == 0 {
					v, err := d.br.ReadBits(12)
					if err != nil {
						return 0, err
					}
					dod = int64(v) - 2047
				} else {
					v, err := d.br.ReadBits(64)
					if err != nil {
						return 0, err
					}
					dod = int64(v)
				}
			}
		}
	}

	delta := d.prevDelta + dod
	d.prevDelta = delta
	d.prevValue += delta
	d.index++
	return d.prevValue, nil
}

func (d *DeltaDecoder) readVarint() (int64, error) {
	var shift uint
	var result uint64
	for {
		b, err := d.br.ReadBits(8)
		if err != nil {
			return 0, err
		}
		result |= (b & 0x7f) << shift
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
	}
	value := int64((result >> 1) ^ uint64((int64(result&1)<<63)>>63))
	return value, nil
}
