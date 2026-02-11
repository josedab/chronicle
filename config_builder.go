package chronicle

import "time"

// ConfigBuilder provides a fluent API for constructing a [Config].
// It starts from [DefaultConfig] defaults, so only fields that differ
// from the defaults need to be set.
//
//	cfg, err := chronicle.NewConfigBuilder("/data/metrics.db").
//	    WithMaxMemory(256 * 1024 * 1024).
//	    WithRetention(7 * 24 * time.Hour).
//	    WithHTTP(8086).
//	    Build()
type ConfigBuilder struct {
	cfg Config
}

// NewConfigBuilder creates a builder pre-populated with [DefaultConfig] values.
func NewConfigBuilder(path string) *ConfigBuilder {
	return &ConfigBuilder{cfg: DefaultConfig(path)}
}

// Storage settings

// WithMaxMemory sets the maximum memory budget for buffers and caches.
func (b *ConfigBuilder) WithMaxMemory(bytes int64) *ConfigBuilder {
	b.cfg.Storage.MaxMemory = bytes
	return b
}

// WithBufferSize sets the number of points to buffer before flushing.
func (b *ConfigBuilder) WithBufferSize(n int) *ConfigBuilder {
	b.cfg.Storage.BufferSize = n
	return b
}

// WithPartitionDuration sets the time span covered by each partition.
func (b *ConfigBuilder) WithPartitionDuration(d time.Duration) *ConfigBuilder {
	b.cfg.Storage.PartitionDuration = d
	return b
}

// WithMaxStorageBytes sets the maximum database size. 0 means unlimited.
func (b *ConfigBuilder) WithMaxStorageBytes(bytes int64) *ConfigBuilder {
	b.cfg.Storage.MaxStorageBytes = bytes
	return b
}

// WithStorageBackend sets a custom storage backend for partitions.
func (b *ConfigBuilder) WithStorageBackend(backend StorageBackend) *ConfigBuilder {
	b.cfg.StorageBackend = backend
	return b
}

// WAL settings

// WithWALSyncInterval sets how often the WAL is synced to disk.
func (b *ConfigBuilder) WithWALSyncInterval(d time.Duration) *ConfigBuilder {
	b.cfg.WAL.SyncInterval = d
	return b
}

// WithWALMaxSize sets the maximum WAL file size before rotation.
func (b *ConfigBuilder) WithWALMaxSize(bytes int64) *ConfigBuilder {
	b.cfg.WAL.WALMaxSize = bytes
	return b
}

// WithWALRetain sets the number of old WAL files to keep.
func (b *ConfigBuilder) WithWALRetain(n int) *ConfigBuilder {
	b.cfg.WAL.WALRetain = n
	return b
}

// Retention settings

// WithRetention sets the data retention duration. 0 means keep forever.
func (b *ConfigBuilder) WithRetention(d time.Duration) *ConfigBuilder {
	b.cfg.Retention.RetentionDuration = d
	return b
}

// WithCompaction configures compaction workers and interval.
func (b *ConfigBuilder) WithCompaction(workers int, interval time.Duration) *ConfigBuilder {
	b.cfg.Retention.CompactionWorkers = workers
	b.cfg.Retention.CompactionInterval = interval
	return b
}

// WithDownsampleRules sets automatic downsampling policies.
func (b *ConfigBuilder) WithDownsampleRules(rules ...DownsampleRule) *ConfigBuilder {
	b.cfg.Retention.DownsampleRules = rules
	return b
}

// Query settings

// WithQueryTimeout sets the maximum query execution time.
func (b *ConfigBuilder) WithQueryTimeout(d time.Duration) *ConfigBuilder {
	b.cfg.Query.QueryTimeout = d
	return b
}

// HTTP settings

// WithHTTP enables the HTTP API on the given port.
func (b *ConfigBuilder) WithHTTP(port int) *ConfigBuilder {
	b.cfg.HTTP.HTTPEnabled = true
	b.cfg.HTTP.HTTPPort = port
	return b
}

// WithPrometheusRemoteWrite enables the Prometheus remote write endpoint.
// Requires HTTP to be enabled via [ConfigBuilder.WithHTTP].
func (b *ConfigBuilder) WithPrometheusRemoteWrite() *ConfigBuilder {
	b.cfg.HTTP.PrometheusRemoteWriteEnabled = true
	return b
}

// Security settings

// WithAuth enables API key authentication.
func (b *ConfigBuilder) WithAuth(apiKeys ...string) *ConfigBuilder {
	b.cfg.Auth = &AuthConfig{
		Enabled: true,
		APIKeys: apiKeys,
	}
	return b
}

// WithEncryption enables AES-256-GCM encryption at rest.
func (b *ConfigBuilder) WithEncryption(keyPassword string) *ConfigBuilder {
	b.cfg.Encryption = &EncryptionConfig{
		Enabled:     true,
		KeyPassword: keyPassword,
	}
	return b
}

// WithRateLimit sets the maximum requests per second per IP. 0 disables.
func (b *ConfigBuilder) WithRateLimit(rps int) *ConfigBuilder {
	b.cfg.RateLimitPerSecond = rps
	return b
}

// Replication

// WithReplication enables outbound replication to a remote endpoint.
func (b *ConfigBuilder) WithReplication(targetURL string, batchSize int, flushInterval time.Duration) *ConfigBuilder {
	b.cfg.Replication = &ReplicationConfig{
		Enabled:       true,
		TargetURL:     targetURL,
		BatchSize:     batchSize,
		FlushInterval: flushInterval,
	}
	return b
}

// Schema validation

// WithSchemas sets metric schemas for write validation.
func (b *ConfigBuilder) WithSchemas(strict bool, schemas ...MetricSchema) *ConfigBuilder {
	b.cfg.StrictSchema = strict
	b.cfg.Schemas = schemas
	return b
}

// Build validates the configuration and returns it.
// Returns an error if validation fails.
func (b *ConfigBuilder) Build() (Config, error) {
	b.cfg.syncLegacyFields()
	if err := b.cfg.Validate(); err != nil {
		return Config{}, err
	}
	return b.cfg, nil
}

// MustBuild is like [ConfigBuilder.Build] but panics on validation errors.
func (b *ConfigBuilder) MustBuild() Config {
	cfg, err := b.Build()
	if err != nil {
		panic("chronicle: invalid config: " + err.Error())
	}
	return cfg
}
