package chronicle

import (
	"testing"
	"time"
)

func TestConfig_Defaults(t *testing.T) {
	cfg := DefaultConfig("/test/path.db")
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Path != "/test/path.db" {
		t.Errorf("expected path /test/path.db, got %s", cfg.Path)
	}
	if cfg.MaxMemory != 64*1024*1024 {
		t.Error("default MaxMemory should be 64MB")
	}
	if cfg.Storage.MaxMemory != 64*1024*1024 {
		t.Error("default Storage.MaxMemory should be 64MB")
	}
	if cfg.PartitionDuration != time.Hour {
		t.Error("default PartitionDuration should be 1 hour")
	}
	if cfg.Storage.PartitionDuration != time.Hour {
		t.Error("default Storage.PartitionDuration should be 1 hour")
	}
	if cfg.BufferSize != 10_000 {
		t.Error("default BufferSize should be 10000")
	}
	if cfg.Storage.BufferSize != 10_000 {
		t.Error("default Storage.BufferSize should be 10000")
	}
	if cfg.SyncInterval != time.Second {
		t.Error("default SyncInterval should be 1 second")
	}
	if cfg.WAL.SyncInterval != time.Second {
		t.Error("default WAL.SyncInterval should be 1 second")
	}
	if cfg.QueryTimeout != 30*time.Second {
		t.Error("default QueryTimeout should be 30 seconds")
	}
	if cfg.Query.QueryTimeout != 30*time.Second {
		t.Error("default Query.QueryTimeout should be 30 seconds")
	}
	if cfg.HTTPPort != 8086 {
		t.Error("default HTTPPort should be 8086")
	}
	if cfg.HTTP.HTTPPort != 8086 {
		t.Error("default HTTP.HTTPPort should be 8086")
	}
	if cfg.WALMaxSize != 128*1024*1024 {
		t.Error("default WALMaxSize should be 128MB")
	}
	if cfg.WAL.WALMaxSize != 128*1024*1024 {
		t.Error("default WAL.WALMaxSize should be 128MB")
	}
	if cfg.WALRetain != 3 {
		t.Error("default WALRetain should be 3")
	}
	if cfg.WAL.WALRetain != 3 {
		t.Error("default WAL.WALRetain should be 3")
	}
	if cfg.CompactionWorkers != 1 {
		t.Error("default CompactionWorkers should be 1")
	}
	if cfg.Retention.CompactionWorkers != 1 {
		t.Error("default Retention.CompactionWorkers should be 1")
	}
	if cfg.CompactionInterval != 30*time.Minute {
		t.Error("default CompactionInterval should be 30 minutes")
	}
	if cfg.Retention.CompactionInterval != 30*time.Minute {
		t.Error("default Retention.CompactionInterval should be 30 minutes")
	}
}

func TestDefaultConfig_Defaults(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Path != "" {
		t.Error("path should be empty when not provided")
	}
	if cfg.RetentionDuration != 0 {
		t.Error("default RetentionDuration should be 0 (unlimited)")
	}
	if cfg.Retention.RetentionDuration != 0 {
		t.Error("default Retention.RetentionDuration should be 0 (unlimited)")
	}
	if cfg.MaxStorageBytes != 0 {
		t.Error("default MaxStorageBytes should be 0 (unlimited)")
	}
	if cfg.Storage.MaxStorageBytes != 0 {
		t.Error("default Storage.MaxStorageBytes should be 0 (unlimited)")
	}
	if cfg.HTTPEnabled {
		t.Error("HTTPEnabled should default to false")
	}
	if cfg.HTTP.HTTPEnabled {
		t.Error("HTTP.HTTPEnabled should default to false")
	}
	if cfg.PrometheusRemoteWriteEnabled {
		t.Error("PrometheusRemoteWriteEnabled should default to false")
	}
	if cfg.HTTP.PrometheusRemoteWriteEnabled {
		t.Error("HTTP.PrometheusRemoteWriteEnabled should default to false")
	}
	if cfg.StrictSchema {
		t.Error("StrictSchema should default to false")
	}
}

func TestConfig_CustomValues(t *testing.T) {
	cfg := Config{
		Path:              "/custom/path",
		MaxMemory:         100 * 1024 * 1024,
		PartitionDuration: 2 * time.Hour,
		RetentionDuration: 24 * time.Hour,
		BufferSize:        5000,
		HTTPEnabled:       true,
		HTTPPort:          9000,
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Path != "/custom/path" {
		t.Error("custom path not set")
	}
	if cfg.MaxMemory != 100*1024*1024 {
		t.Error("custom MaxMemory not set")
	}
	if cfg.Storage.MaxMemory != 100*1024*1024 {
		t.Error("custom Storage.MaxMemory not set")
	}
	if cfg.PartitionDuration != 2*time.Hour {
		t.Error("custom PartitionDuration not set")
	}
	if cfg.Storage.PartitionDuration != 2*time.Hour {
		t.Error("custom Storage.PartitionDuration not set")
	}
	if cfg.RetentionDuration != 24*time.Hour {
		t.Error("custom RetentionDuration not set")
	}
	if cfg.Retention.RetentionDuration != 24*time.Hour {
		t.Error("custom Retention.RetentionDuration not set")
	}
	if cfg.BufferSize != 5000 {
		t.Error("custom BufferSize not set")
	}
	if cfg.Storage.BufferSize != 5000 {
		t.Error("custom Storage.BufferSize not set")
	}
	if !cfg.HTTPEnabled {
		t.Error("custom HTTPEnabled not set")
	}
	if !cfg.HTTP.HTTPEnabled {
		t.Error("custom HTTP.HTTPEnabled not set")
	}
	if cfg.HTTPPort != 9000 {
		t.Error("custom HTTPPort not set")
	}
	if cfg.HTTP.HTTPPort != 9000 {
		t.Error("custom HTTP.HTTPPort not set")
	}
}

func TestConfig_EncryptionConfig(t *testing.T) {
	cfg := Config{
		Path: "/test",
		Encryption: &EncryptionConfig{
			Enabled:     true,
			KeyPassword: "secret",
		},
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Encryption == nil {
		t.Fatal("Encryption config should not be nil")
	}
	if !cfg.Encryption.Enabled {
		t.Error("Encryption should be enabled")
	}
	if cfg.Encryption.KeyPassword != "secret" {
		t.Error("KeyPassword not set correctly")
	}
}

func TestConfig_ReplicationConfig(t *testing.T) {
	cfg := Config{
		Path: "/test",
		Replication: &ReplicationConfig{
			TargetURL:     "http://remote:8086",
			BatchSize:     100,
			FlushInterval: 5 * time.Second,
		},
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Replication == nil {
		t.Fatal("Replication config should not be nil")
	}
	if cfg.Replication.TargetURL != "http://remote:8086" {
		t.Error("TargetURL not set correctly")
	}
	if cfg.Replication.BatchSize != 100 {
		t.Error("BatchSize not set correctly")
	}
}

func TestConfig_DownsampleRules(t *testing.T) {
	cfg := Config{
		Path: "/test",
		DownsampleRules: []DownsampleRule{
			{
				SourceResolution: time.Minute,
				TargetResolution: time.Hour,
				Aggregations:     []AggFunc{AggMean, AggMax},
			},
		},
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if len(cfg.DownsampleRules) != 1 {
		t.Fatalf("expected 1 downsample rule, got %d", len(cfg.DownsampleRules))
	}
	if len(cfg.Retention.DownsampleRules) != 1 {
		t.Fatalf("expected 1 retention downsample rule, got %d", len(cfg.Retention.DownsampleRules))
	}

	rule := cfg.DownsampleRules[0]
	if rule.SourceResolution != time.Minute {
		t.Error("SourceResolution not set correctly")
	}
	if rule.TargetResolution != time.Hour {
		t.Error("TargetResolution not set correctly")
	}
	if len(rule.Aggregations) != 2 {
		t.Error("Aggregations count not correct")
	}
}

func TestConfig_ContinuousQueries(t *testing.T) {
	cfg := Config{
		Path: "/test",
		ContinuousQueries: []ContinuousQuery{
			{
				Name:         "hourly_avg",
				SourceMetric: "cpu",
				Function:     AggMean,
				Window:       time.Hour,
				Every:        time.Hour,
				TargetMetric: "cpu_hourly",
			},
		},
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if len(cfg.ContinuousQueries) != 1 {
		t.Fatalf("expected 1 continuous query, got %d", len(cfg.ContinuousQueries))
	}

	cq := cfg.ContinuousQueries[0]
	if cq.Name != "hourly_avg" {
		t.Error("CQ name not set correctly")
	}
	if cq.Window != time.Hour {
		t.Error("CQ window not set correctly")
	}
}

func TestConfig_Schemas(t *testing.T) {
	cfg := Config{
		Path:         "/test",
		StrictSchema: true,
		Schemas: []MetricSchema{
			{
				Name: "cpu",
				Tags: []TagSchema{
					{Name: "host", Required: true},
				},
			},
		},
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if !cfg.StrictSchema {
		t.Error("StrictSchema should be true")
	}
	if len(cfg.Schemas) != 1 {
		t.Fatal("expected 1 schema")
	}
	if cfg.Schemas[0].Name != "cpu" {
		t.Error("Schema name not set correctly")
	}
}

func TestConfig_Auth(t *testing.T) {
	cfg := Config{
		Path: "/test",
		Auth: &AuthConfig{
			Enabled:      true,
			APIKeys:      []string{"key1", "key2"},
			ReadOnlyKeys: []string{"readonly"},
			ExcludePaths: []string{"/custom"},
		},
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Auth == nil {
		t.Fatal("Auth config should not be nil")
	}
	if !cfg.Auth.Enabled {
		t.Error("Auth should be enabled")
	}
	if len(cfg.Auth.APIKeys) != 2 {
		t.Errorf("expected 2 API keys, got %d", len(cfg.Auth.APIKeys))
	}
	if len(cfg.Auth.ReadOnlyKeys) != 1 {
		t.Errorf("expected 1 read-only key, got %d", len(cfg.Auth.ReadOnlyKeys))
	}
	if len(cfg.Auth.ExcludePaths) != 1 {
		t.Errorf("expected 1 exclude path, got %d", len(cfg.Auth.ExcludePaths))
	}
}

func TestConfig_RateLimitDefault(t *testing.T) {
	cfg := DefaultConfig("/test")
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.RateLimitPerSecond != 1000 {
		t.Errorf("expected default RateLimitPerSecond 1000, got %d", cfg.RateLimitPerSecond)
	}
}

func TestConfig_RateLimitCustom(t *testing.T) {
	cfg := Config{
		Path:               "/test",
		RateLimitPerSecond: 500,
	}

	if cfg.RateLimitPerSecond != 500 {
		t.Errorf("expected RateLimitPerSecond 500, got %d", cfg.RateLimitPerSecond)
	}
}

func TestConfig_RateLimitDisabled(t *testing.T) {
	cfg := Config{
		Path:               "/test",
		RateLimitPerSecond: 0, // Disabled
	}

	if cfg.RateLimitPerSecond != 0 {
		t.Error("RateLimitPerSecond should be 0 when disabled")
	}
}

func TestAuthConfig_Defaults(t *testing.T) {
	// Nil auth config means disabled
	var cfg *AuthConfig
	if cfg != nil && cfg.Enabled {
		t.Error("nil AuthConfig should not be enabled")
	}

	// Empty struct also means disabled
	cfg = &AuthConfig{}
	if cfg.Enabled {
		t.Error("empty AuthConfig should not be enabled")
	}
}
