package pluginmkt

// SetLogger sets the logging function
func (sdk *PluginSDK) SetLogger(logger func(level, msg string, args ...any)) {
	sdk.logger = logger
}

// GetConfig returns configuration value
func (sdk *PluginSDK) GetConfig(key string) any {
	return sdk.config[key]
}

// GetConfigString returns configuration value as string
func (sdk *PluginSDK) GetConfigString(key string, defaultValue string) string {
	if v, ok := sdk.config[key].(string); ok {
		return v
	}
	return defaultValue
}

// GetConfigInt returns configuration value as int
func (sdk *PluginSDK) GetConfigInt(key string, defaultValue int) int {
	if v, ok := sdk.config[key].(float64); ok {
		return int(v)
	}
	if v, ok := sdk.config[key].(int); ok {
		return v
	}
	return defaultValue
}

// GetConfigBool returns configuration value as bool
func (sdk *PluginSDK) GetConfigBool(key string, defaultValue bool) bool {
	if v, ok := sdk.config[key].(bool); ok {
		return v
	}
	return defaultValue
}

// Log logs a message
func (sdk *PluginSDK) Log(level, msg string, args ...any) {
	sdk.logger(level, msg, args...)
}

// Info logs an info message
func (sdk *PluginSDK) Info(msg string, args ...any) {
	sdk.Log("info", msg, args...)
}

// Error logs an error message
func (sdk *PluginSDK) Error(msg string, args ...any) {
	sdk.Log("error", msg, args...)
}

// Debug logs a debug message
func (sdk *PluginSDK) Debug(msg string, args ...any) {
	sdk.Log("debug", msg, args...)
}
