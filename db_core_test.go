package chronicle

import "testing"

func TestDbCore(t *testing.T) {
	db := setupTestDB(t)

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "db_is_not_nil", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if db == nil {
				t.Fatal("expected non-nil DB")
			}
		})
	}
}
