package chronicle

import "testing"

func TestSloBurnRate(t *testing.T) {
	db := setupTestDB(t)
	sloEngine := NewSLOEngine(db, DefaultSLOEngineConfig())

	tests := []struct {
		name string
	}{
		{name: "new_burn_rate_engine"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bre := NewBurnRateEngine(sloEngine)
			if bre == nil {
				t.Fatal("expected non-nil BurnRateEngine")
			}
		})
	}
}
