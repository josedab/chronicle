package chronicle

import (
	"testing"
	"time"
)

func TestCodec(t *testing.T) {
	tests := []struct {
		name   string
		points []Point
	}{
		{name: "empty", points: []Point{}},
		{
			name: "single_point",
			points: []Point{
				{Metric: "m", Value: 1.0, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"k": "v"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := encodePoints(tt.points)
			if err != nil {
				t.Fatalf("encodePoints() error = %v", err)
			}
			decoded, err := decodePoints(data)
			if err != nil {
				t.Fatalf("decodePoints() error = %v", err)
			}
			if len(decoded) != len(tt.points) {
				t.Errorf("got %d points, want %d", len(decoded), len(tt.points))
			}
		})
	}
}
