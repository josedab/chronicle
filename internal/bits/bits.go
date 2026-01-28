// Package bits provides bit-level I/O utilities for compression codecs.
package bits

import (
	"errors"
)

// Writer writes individual bits to a byte buffer.
type Writer struct {
	buf   []byte
	curr  byte
	nbits uint8
}

// NewWriter creates a new bit writer.
func NewWriter() *Writer {
	return &Writer{}
}

// WriteBit writes a single bit.
func (w *Writer) WriteBit(bit uint8) {
	w.curr = (w.curr << 1) | (bit & 1)
	w.nbits++
	if w.nbits == 8 {
		w.buf = append(w.buf, w.curr)
		w.curr = 0
		w.nbits = 0
	}
}

// WriteBits writes multiple bits from a value.
func (w *Writer) WriteBits(value uint64, bits int) {
	for i := bits - 1; i >= 0; i-- {
		w.WriteBit(uint8((value >> uint(i)) & 1))
	}
}

// Bytes returns the accumulated bytes, flushing any partial byte.
func (w *Writer) Bytes() []byte {
	if w.nbits > 0 {
		w.buf = append(w.buf, w.curr<<(8-w.nbits))
		w.curr = 0
		w.nbits = 0
	}
	return w.buf
}

// Reader reads individual bits from a byte buffer.
type Reader struct {
	buf   []byte
	index int
	curr  byte
	nbits uint8
}

// NewReader creates a new bit reader.
func NewReader(data []byte) *Reader {
	return &Reader{buf: data}
}

// ReadBit reads a single bit.
func (r *Reader) ReadBit() (uint8, error) {
	if r.nbits == 0 {
		if r.index >= len(r.buf) {
			return 0, errors.New("out of bits")
		}
		r.curr = r.buf[r.index]
		r.index++
		r.nbits = 8
	}

	bit := (r.curr >> 7) & 1
	r.curr <<= 1
	r.nbits--
	return bit, nil
}

// ReadBits reads multiple bits into a value.
func (r *Reader) ReadBits(bits int) (uint64, error) {
	var out uint64
	for i := 0; i < bits; i++ {
		bit, err := r.ReadBit()
		if err != nil {
			return 0, err
		}
		out = (out << 1) | uint64(bit)
	}
	return out, nil
}
