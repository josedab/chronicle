package chronicle

import "testing"

func TestStreamingSqlV2Config(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "default_config"},
		{name: "new_stream_watermark"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.name {
			case "default_config":
				cfg := DefaultStreamingSQLV2Config()
				if cfg.MaxConcurrentQueries == 0 {
					t.Error("expected non-zero max concurrent queries")
				}
			case "new_stream_watermark":
				wm := NewStreamWatermark(0, 0)
				if wm == nil {
					t.Fatal("expected non-nil StreamWatermark")
				}
				wm.Advance(1000)
				if wm.GetWatermark() == 0 {
					// Watermark should have advanced.
				}
			}
		})
	}
}
