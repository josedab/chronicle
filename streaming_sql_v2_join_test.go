package chronicle

import (
	"testing"
	"time"
)

func TestStreamingSqlV2Join(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "new_stream_join_engine"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sje := NewStreamJoinEngine(100, time.Minute)
			if sje == nil {
				t.Fatal("expected non-nil StreamJoinEngine")
			}
		})
	}
}
