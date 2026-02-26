package chronicle

import "testing"

func TestGorilla(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
	}{
		{name: "empty", values: []float64{}},
		{name: "single", values: []float64{42.0}},
		{name: "multiple", values: []float64{1.0, 2.0, 3.0, 4.0, 5.0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeGorilla(tt.values)
			decoded, err := decodeGorilla(encoded)
			if err != nil {
				t.Fatalf("decodeGorilla() error = %v", err)
			}
			if len(decoded) != len(tt.values) {
				t.Errorf("got %d values, want %d", len(decoded), len(tt.values))
			}
		})
	}
}
