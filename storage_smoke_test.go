package chronicle

import "testing"

func TestStorage(t *testing.T) {
	db := setupTestDB(t)

	tests := []struct {
		name string
	}{
		{name: "storage_init_via_open"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if db == nil {
				t.Fatal("expected non-nil DB with initialised storage")
			}
		})
	}
}
