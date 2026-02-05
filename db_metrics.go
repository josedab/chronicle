package chronicle

// Metrics returns all registered metric names.
func (db *DB) Metrics() []string {
	return db.index.Metrics()
}

// TagKeys returns all unique tag keys across all series.
func (db *DB) TagKeys() []string {
	return db.index.TagKeys()
}

// TagKeysForMetric returns tag keys used by a specific metric.
func (db *DB) TagKeysForMetric(metric string) []string {
	return db.index.TagKeysForMetric(metric)
}

// TagValues returns all unique values for a given tag key.
func (db *DB) TagValues(key string) []string {
	return db.index.TagValues(key)
}

// TagValuesForMetric returns tag values for a key filtered by metric.
func (db *DB) TagValuesForMetric(metric, key string) []string {
	return db.index.TagValuesForMetric(metric, key)
}

// SeriesCount returns the total number of unique time series.
func (db *DB) SeriesCount() int {
	return db.index.SeriesCount()
}
