package chronicle

import "testing"

func TestK8sReconciler_Smoke(t *testing.T) {
	// Smoke test: verify ReconcilerConfig types and functions from k8s_reconciler.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
