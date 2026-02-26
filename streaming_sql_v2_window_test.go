package chronicle

import (
	"testing"
	"time"
)

func TestStreamingSqlV2Window(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "new_windowed_aggregator"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wm := NewStreamWatermark(time.Second, time.Second)
			wa := NewWindowedAggregator(
				StreamingWindowTumbling,
				time.Minute,
				time.Minute,
				wm,
				100,
				time.Hour,
			)
			if wa == nil {
				t.Fatal("expected non-nil WindowedAggregator")
			}
		})
	}
}
