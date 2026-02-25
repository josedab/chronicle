package chronicle

import "testing"

func TestFeatureRegistryBridge(t *testing.T) {
	db := setupTestDB(t)

	tests := []struct {
		name string
	}{
		{name: "register_core_features"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RegisterCoreFeatures(db)
		})
	}
}
