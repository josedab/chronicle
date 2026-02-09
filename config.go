package chronicle

import "time"

// Config defines database configuration.
type Config struct {
    // Path is the file path for the database. Required unless StorageBackend is provided.
    Path string

    // StorageBackend is an optional backend for partition storage.
    // If provided, partitions are stored using this backend instead of the local file.
    // The local file is still used for the index and WAL.
    StorageBackend StorageBackend

    // Storage holds core storage settings.
    Storage StorageConfig

    // WAL configures write-ahead logging.
    WAL WALConfig

    // Retention configures data retention and compaction.
    Retention RetentionConfig

    // Query configures query execution limits.
    Query QueryConfig

    // HTTP configures the HTTP API server.
    HTTP HTTPConfig

    // Replication configures outbound replication to a remote endpoint.
    Replication *ReplicationConfig

    // Schemas defines metric schemas for validation.
    // If nil, no validation is performed.
    Schemas []MetricSchema

    // StrictSchema if true rejects points for metrics without a registered schema.
    StrictSchema bool

    // Encryption configures encryption at rest.
    // If nil or Enabled is false, data is stored unencrypted.
    Encryption *EncryptionConfig

    // Auth configures HTTP API authentication.
    // If nil or Enabled is false, no authentication is required.
    Auth *AuthConfig

    // RateLimitPerSecond is the maximum requests per second per IP.
    // Default: 1000. Set to 0 to disable rate limiting.
    RateLimitPerSecond int

    // ClickHouse configures ClickHouse-compatible HTTP interface.
    // If nil or Enabled is false, ClickHouse protocol is disabled.
    ClickHouse *ClickHouseConfig

    // Backward compatible fields.
    MaxMemory                    int64
    MaxStorageBytes              int64
    SyncInterval                 time.Duration
    PartitionDuration            time.Duration
    WALMaxSize                   int64
    WALRetain                    int
    RetentionDuration            time.Duration
    DownsampleRules              []DownsampleRule
    BufferSize                   int
    CompactionWorkers            int
    CompactionInterval           time.Duration
    QueryTimeout                 time.Duration
    HTTPEnabled                  bool
    HTTPPort                     int
    PrometheusRemoteWriteEnabled bool
    ContinuousQueries            []ContinuousQuery
}

// StorageConfig groups core storage settings.
type StorageConfig struct {
    // MaxMemory is the maximum memory budget for buffers and caches in bytes.
    // Default: 64MB.
    MaxMemory int64

    // MaxStorageBytes is the maximum database file size in bytes.
    // When exceeded, oldest partitions are removed. 0 means unlimited.
    MaxStorageBytes int64

    // PartitionDuration is the time span covered by each partition.
    // Default: 1 hour.
    PartitionDuration time.Duration

    // BufferSize is the number of points to buffer before flushing to storage.
    // Default: 10,000.
    BufferSize int
}

// WALConfig groups write-ahead log settings.
type WALConfig struct {
    // SyncInterval is how often the WAL is synced to disk.
    // Default: 1 second.
    SyncInterval time.Duration

    // WALMaxSize is the maximum size of a single WAL file before rotation.
    // Default: 128MB.
    WALMaxSize int64

    // WALRetain is the number of old WAL files to keep after rotation.
    // Default: 3.
    WALRetain int
}

// RetentionConfig groups retention and compaction settings.
type RetentionConfig struct {
    // RetentionDuration is how long data is kept before automatic deletion.
    // 0 means data is kept indefinitely.
    RetentionDuration time.Duration

    // DownsampleRules defines automatic downsampling policies.
    DownsampleRules []DownsampleRule

    // CompactionWorkers is the number of background compaction workers.
    // Default: 1.
    CompactionWorkers int

    // CompactionInterval is how often compaction is triggered.
    // Default: 30 minutes.
    CompactionInterval time.Duration
}

// QueryConfig groups query execution settings.
type QueryConfig struct {
    // QueryTimeout is the maximum duration for query execution.
    // Default: 30 seconds.
    QueryTimeout time.Duration
}

// HTTPConfig groups HTTP server settings.
type HTTPConfig struct {
    // HTTPEnabled enables the HTTP API server.
    // Default: false.
    HTTPEnabled bool

    // HTTPPort is the port for the HTTP API server.
    // Default: 8086.
    HTTPPort int

    // PrometheusRemoteWriteEnabled enables the Prometheus remote write endpoint.
    // Default: false.
    PrometheusRemoteWriteEnabled bool
}

// normalize populates grouped config from legacy fields when needed.
func (c *Config) normalize() {
	if c.Storage.MaxMemory == 0 && c.MaxMemory != 0 {
		c.Storage.MaxMemory = c.MaxMemory
	}
	if c.Storage.MaxStorageBytes == 0 && c.MaxStorageBytes != 0 {
		c.Storage.MaxStorageBytes = c.MaxStorageBytes
	}
	if c.Storage.PartitionDuration == 0 && c.PartitionDuration != 0 {
		c.Storage.PartitionDuration = c.PartitionDuration
	}
	if c.Storage.BufferSize == 0 && c.BufferSize != 0 {
		c.Storage.BufferSize = c.BufferSize
	}
	if c.WAL.SyncInterval == 0 && c.SyncInterval != 0 {
		c.WAL.SyncInterval = c.SyncInterval
	}
	if c.WAL.WALMaxSize == 0 && c.WALMaxSize != 0 {
		c.WAL.WALMaxSize = c.WALMaxSize
	}
	if c.WAL.WALRetain == 0 && c.WALRetain != 0 {
		c.WAL.WALRetain = c.WALRetain
	}
	if c.Retention.RetentionDuration == 0 && c.RetentionDuration != 0 {
		c.Retention.RetentionDuration = c.RetentionDuration
	}
	if len(c.Retention.DownsampleRules) == 0 && len(c.DownsampleRules) > 0 {
		c.Retention.DownsampleRules = c.DownsampleRules
	}
	if c.Retention.CompactionWorkers == 0 && c.CompactionWorkers != 0 {
		c.Retention.CompactionWorkers = c.CompactionWorkers
	}
	if c.Retention.CompactionInterval == 0 && c.CompactionInterval != 0 {
		c.Retention.CompactionInterval = c.CompactionInterval
	}
	if c.Query.QueryTimeout == 0 && c.QueryTimeout != 0 {
		c.Query.QueryTimeout = c.QueryTimeout
	}
	if !c.HTTP.HTTPEnabled && c.HTTPEnabled {
		c.HTTP.HTTPEnabled = c.HTTPEnabled
	}
	if c.HTTP.HTTPPort == 0 && c.HTTPPort != 0 {
		c.HTTP.HTTPPort = c.HTTPPort
	}
	if !c.HTTP.PrometheusRemoteWriteEnabled && c.PrometheusRemoteWriteEnabled {
		c.HTTP.PrometheusRemoteWriteEnabled = c.PrometheusRemoteWriteEnabled
	}
}

// syncLegacyFields mirrors grouped config into legacy fields for compatibility.
func (c *Config) syncLegacyFields() {
	if c.Storage.MaxMemory != 0 {
		c.MaxMemory = c.Storage.MaxMemory
	}
	c.MaxStorageBytes = c.Storage.MaxStorageBytes
	if c.Storage.PartitionDuration != 0 {
		c.PartitionDuration = c.Storage.PartitionDuration
	}
	if c.Storage.BufferSize != 0 {
		c.BufferSize = c.Storage.BufferSize
	}
	if c.WAL.SyncInterval != 0 {
		c.SyncInterval = c.WAL.SyncInterval
	}
	if c.WAL.WALMaxSize != 0 {
		c.WALMaxSize = c.WAL.WALMaxSize
	}
	if c.WAL.WALRetain != 0 {
		c.WALRetain = c.WAL.WALRetain
	}
	if c.Retention.RetentionDuration != 0 {
		c.RetentionDuration = c.Retention.RetentionDuration
	}
	if len(c.Retention.DownsampleRules) > 0 {
		c.DownsampleRules = c.Retention.DownsampleRules
	}
	if c.Retention.CompactionWorkers != 0 {
		c.CompactionWorkers = c.Retention.CompactionWorkers
	}
	if c.Retention.CompactionInterval != 0 {
		c.CompactionInterval = c.Retention.CompactionInterval
	}
	if c.Query.QueryTimeout != 0 {
		c.QueryTimeout = c.Query.QueryTimeout
	}
	if c.HTTP.HTTPEnabled {
		c.HTTPEnabled = c.HTTP.HTTPEnabled
	}
	if c.HTTP.HTTPPort != 0 {
		c.HTTPPort = c.HTTP.HTTPPort
	}
	if c.HTTP.PrometheusRemoteWriteEnabled {
		c.PrometheusRemoteWriteEnabled = c.HTTP.PrometheusRemoteWriteEnabled
	}
}

// AuthConfig configures HTTP API authentication.
type AuthConfig struct {
	// Enabled enables authentication on HTTP endpoints.
	Enabled bool

	// APIKeys is a list of valid API keys. At least one must be provided if Enabled is true.
	APIKeys []string

	// ReadOnlyKeys is a list of API keys that only allow read operations (queries).
	// These keys cannot write data.
	ReadOnlyKeys []string

	// ExcludePaths are paths that don't require authentication (e.g., /health).
	ExcludePaths []string
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig(path string) Config {
	return Config{
		Path: path,
		Storage: StorageConfig{
			MaxMemory:         64 * 1024 * 1024,
			MaxStorageBytes:   0,
			PartitionDuration: time.Hour,
			BufferSize:        10_000,
		},
		WAL: WALConfig{
			SyncInterval: time.Second,
			WALMaxSize:   128 * 1024 * 1024,
			WALRetain:    3,
		},
		Retention: RetentionConfig{
			RetentionDuration:  0,
			DownsampleRules:    nil,
			CompactionWorkers:  1,
			CompactionInterval: 30 * time.Minute,
		},
		Query: QueryConfig{
			QueryTimeout: 30 * time.Second,
		},
		HTTP: HTTPConfig{
			HTTPEnabled:                  false,
			HTTPPort:                     8086,
			PrometheusRemoteWriteEnabled: false,
		},
		Replication:        nil,
		RateLimitPerSecond: 1000,
		MaxMemory:                    64 * 1024 * 1024,
		MaxStorageBytes:              0,
		SyncInterval:                 time.Second,
		PartitionDuration:            time.Hour,
		WALMaxSize:                   128 * 1024 * 1024,
		WALRetain:                    3,
		RetentionDuration:            0,
		DownsampleRules:              nil,
		BufferSize:                   10_000,
		CompactionWorkers:            1,
		CompactionInterval:           30 * time.Minute,
		QueryTimeout:                 30 * time.Second,
		HTTPEnabled:                  false,
		HTTPPort:                     8086,
		PrometheusRemoteWriteEnabled: false,
		ContinuousQueries:            nil,
	}
}

// NormalizedCopy returns a copy of the configuration with grouped and legacy fields aligned.
func (c Config) NormalizedCopy() Config {
	c.normalize()
	c.syncLegacyFields()
	return c
}
