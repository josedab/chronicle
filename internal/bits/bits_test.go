package bits

import (
	"testing"
)

func TestBitWriterReader(t *testing.T) {
	tests := []struct {
		name   string
		values []uint64
		bits   []int
	}{
		{
			name:   "single bit",
			values: []uint64{1},
			bits:   []int{1},
		},
		{
			name:   "multiple bits",
			values: []uint64{0b11010110},
			bits:   []int{8},
		},
		{
			name:   "64 bits",
			values: []uint64{0xDEADBEEFCAFEBABE},
			bits:   []int{64},
		},
		{
			name:   "multiple values",
			values: []uint64{0b101, 0b11, 0b1111},
			bits:   []int{3, 2, 4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter()
			for i, v := range tt.values {
				w.WriteBits(v, tt.bits[i])
			}
			data := w.Bytes()

			r := NewReader(data)
			for i, expected := range tt.values {
				got, err := r.ReadBits(tt.bits[i])
				if err != nil {
					t.Fatalf("ReadBits failed: %v", err)
				}
				if got != expected {
					t.Errorf("value %d: got %d, want %d", i, got, expected)
				}
			}
		})
	}
}

func TestBitWriterSingleBits(t *testing.T) {
	w := NewWriter()
	w.WriteBit(1)
	w.WriteBit(0)
	w.WriteBit(1)
	w.WriteBit(1)
	w.WriteBit(0)
	w.WriteBit(0)
	w.WriteBit(1)
	w.WriteBit(0)

	data := w.Bytes()
	if len(data) != 1 {
		t.Fatalf("expected 1 byte, got %d", len(data))
	}
	if data[0] != 0b10110010 {
		t.Errorf("expected 0b10110010, got 0b%08b", data[0])
	}
}

func TestBitReaderEmpty(t *testing.T) {
	r := NewReader([]byte{})
	_, err := r.ReadBit()
	if err == nil {
		t.Error("expected error for empty buffer")
	}
}
