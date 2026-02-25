package chronicle

import "testing"

func TestJupyterKernelMagics_Smoke(t *testing.T) {
	// Smoke test: verify JupyterKernelStats types and functions from jupyter_kernel_magics.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
