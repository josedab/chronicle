package chronicle

import "testing"

func TestDbOpenHelpers(t *testing.T) {
	db := setupTestDB(t)

	tests := []struct {
		name string
	}{
		{name: "open_helpers_produce_valid_db"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if db == nil {
				t.Fatal("expected non-nil DB from open helpers")
			}
		})
	}
}
