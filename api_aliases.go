package chronicle

// API aliases for naming consistency.
//
// Chronicle follows the convention:
//   - DefaultXConfig() — returns a config struct with sensible defaults
//   - NewX(db, config) — creates an engine/manager instance
//   - XBuilder — fluent builder for constructing X
//
// The aliases below unify older naming patterns with this convention.

// PluginBuilder is an alias for [CustomPluginBuilder] for naming consistency
// with other builders (QueryBuilder, AlertBuilder, ConfigBuilder, etc.).
type PluginBuilder = CustomPluginBuilder

// NewPluginBuilder creates a new plugin builder. This is an alias for
// [NewCustomPlugin] for naming consistency with NewConfigBuilder, etc.
func NewPluginBuilder(name string) *PluginBuilder {
	return NewCustomPlugin(name)
}
