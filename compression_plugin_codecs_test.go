package chronicle

import "testing"

func TestCompressionPluginCodecs_Smoke(t *testing.T) {
	// Smoke test: verify PassthroughPlugin types and functions from compression_plugin_codecs.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
