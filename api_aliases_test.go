package chronicle

import "testing"

func TestPluginBuilderAlias(t *testing.T) {
	builder := NewPluginBuilder("test-plugin")
	if builder == nil {
		t.Fatal("NewPluginBuilder returned nil")
	}
}

func TestPluginBuilderType(t *testing.T) {
	// Verify PluginBuilder is an alias for CustomPluginBuilder
	var _ PluginBuilder
	var _ *PluginBuilder = NewCustomPlugin("verify-alias")
	if NewCustomPlugin("a") == nil {
		t.Fatal("NewCustomPlugin returned nil")
	}
}
