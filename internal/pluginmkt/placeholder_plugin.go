package pluginmkt

func (p *placeholderPlugin) Init(config map[string]interface{}) error {
	p.config = config
	return nil
}

func (p *placeholderPlugin) Capabilities() []PluginCapability {
	return p.capabilities
}

func (p *placeholderPlugin) Close() error {
	return nil
}
