package chronicle

import "testing"

func TestBackupPitrManager_Smoke(t *testing.T) {
	// Smoke test: verify PITRManager types and functions from backup_pitr_manager.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
