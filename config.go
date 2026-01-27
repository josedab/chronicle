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

	// MaxMemory is the maximum memory budget for buffers and caches in bytes.
	// Default: 64MB.
	MaxMemory int64

	// MaxStorageBytes is the maximum database file size in bytes.
	// When exceeded, oldest partitions are removed. 0 means unlimited.
	MaxStorageBytes int64

	// SyncInterval is how often the WAL is synced to disk.
	// Default: 1 second.
	SyncInterval time.Duration

	// PartitionDuration is the time span covered by each partition.
	// Default: 1 hour.
	PartitionDuration time.Duration

	// WALMaxSize is the maximum size of a single WAL file before rotation.
	// Default: 128MB.
	WALMaxSize int64

	// WALRetain is the number of old WAL files to keep after rotation.
	// Default: 3.
	WALRetain int

	// RetentionDuration is how long data is kept before automatic deletion.
	// 0 means data is kept indefinitely.
	RetentionDuration time.Duration

	// DownsampleRules defines automatic downsampling policies.
	DownsampleRules []DownsampleRule

	// BufferSize is the number of points to buffer before flushing to storage.
	// Default: 10,000.
	BufferSize int

	// CompactionWorkers is the number of background compaction workers.
	// Default: 1.
	CompactionWorkers int

	// CompactionInterval is how often compaction is triggered.
	// Default: 30 minutes.
	CompactionInterval time.Duration

	// QueryTimeout is the maximum duration for query execution.
	// Default: 30 seconds.
	QueryTimeout time.Duration

	// HTTPEnabled enables the HTTP API server.
	// Default: false.
	HTTPEnabled bool

	// HTTPPort is the port for the HTTP API server.
	// Default: 8086.
	HTTPPort int

	// PrometheusRemoteWriteEnabled enables the Prometheus remote write endpoint.
	// Default: false.
	PrometheusRemoteWriteEnabled bool

	// ContinuousQueries defines materialized view refresh rules.
	ContinuousQueries []ContinuousQuery

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
		Path:                         path,
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
		Replication:                  nil,
		RateLimitPerSecond:           1000,
	}
}
