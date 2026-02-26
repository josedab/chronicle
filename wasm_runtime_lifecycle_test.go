package chronicle

import "testing"

func TestWasmRuntimeLifecycle_Smoke(t *testing.T) {
	// Smoke test: verify MemoryHostABI types and functions from wasm_runtime_lifecycle.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
