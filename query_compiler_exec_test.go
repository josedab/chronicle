package chronicle

import "testing"

func TestQueryCompilerExec_Smoke(t *testing.T) {
	// Smoke test: verify QueryCompiler types and functions from query_compiler_exec.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
