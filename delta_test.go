package chronicle

import "testing"

func TestDelta(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
	}{
		{name: "empty", values: []int64{}},
		{name: "single", values: []int64{100}},
		{name: "ascending", values: []int64{10, 20, 30, 40, 50}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeDelta(tt.values)
			decoded, err := decodeDelta(encoded)
			if err != nil {
				t.Fatalf("decodeDelta() error = %v", err)
			}
			if len(decoded) != len(tt.values) {
				t.Errorf("got %d values, want %d", len(decoded), len(tt.values))
			}
		})
	}
}
