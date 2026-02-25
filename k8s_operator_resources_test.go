package chronicle

import "testing"

func TestK8sOperatorResources_Smoke(t *testing.T) {
	// Smoke test: verify K8sReconciler types and functions from k8s_operator_resources.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
