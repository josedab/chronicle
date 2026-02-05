package chronicle

// RegisterSchema adds or updates a metric schema at runtime.
func (db *DB) RegisterSchema(schema MetricSchema) error {
	return db.schemaRegistry.Register(schema)
}

// UnregisterSchema removes a metric schema.
func (db *DB) UnregisterSchema(name string) {
	db.schemaRegistry.Unregister(name)
}

// GetSchema returns the schema for a metric, or nil if not found.
func (db *DB) GetSchema(name string) *MetricSchema {
	return db.schemaRegistry.Get(name)
}

// ListSchemas returns all registered schemas.
func (db *DB) ListSchemas() []MetricSchema {
	return db.schemaRegistry.List()
}
