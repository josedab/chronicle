package chronicle

import "testing"

func TestColumnCodec(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "encode_decode_int64"},
		{name: "encode_decode_float64"},
		{name: "encode_rle_bool"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.name {
			case "encode_decode_int64":
				vals := []int64{1, 2, 3, 4, 5}
				data := encodeRawInt64(vals)
				decoded, err := decodeRawInt64(data)
				if err != nil {
					t.Fatalf("decodeRawInt64() error = %v", err)
				}
				if len(decoded) != len(vals) {
					t.Errorf("got %d, want %d", len(decoded), len(vals))
				}
			case "encode_decode_float64":
				vals := []float64{1.1, 2.2, 3.3}
				data := encodeRawFloat64(vals)
				decoded, err := decodeRawFloat64(data)
				if err != nil {
					t.Fatalf("decodeRawFloat64() error = %v", err)
				}
				if len(decoded) != len(vals) {
					t.Errorf("got %d, want %d", len(decoded), len(vals))
				}
			case "encode_rle_bool":
				vals := []bool{true, true, false, true}
				data := encodeRLEBool(vals)
				if len(data) == 0 {
					t.Error("expected non-empty encoded bool data")
				}
			}
		})
	}
}
