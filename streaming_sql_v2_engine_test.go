package chronicle

import "testing"

func TestStreamingSqlV2Engine(t *testing.T) {
	db := setupTestDB(t)
	hub := NewStreamHub(db, DefaultStreamConfig())

	tests := []struct {
		name string
	}{
		{name: "new_engine"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := NewStreamingSQLV2Engine(db, hub, DefaultStreamingSQLV2Config())
			if engine == nil {
				t.Fatal("expected non-nil StreamingSQLV2Engine")
			}
		})
	}
}
