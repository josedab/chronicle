package chronicle

import (
	"testing"
	"time"
)

func TestConfigBuilder_Defaults(t *testing.T) {
	cfg, err := NewConfigBuilder("/test.db").Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if cfg.Path != "/test.db" {
		t.Errorf("Path = %q, want /test.db", cfg.Path)
	}
	if cfg.Storage.MaxMemory != 64*1024*1024 {
		t.Errorf("MaxMemory = %d, want 64MB", cfg.Storage.MaxMemory)
	}
	if cfg.Storage.BufferSize != 10_000 {
		t.Errorf("BufferSize = %d, want 10000", cfg.Storage.BufferSize)
	}
}

func TestConfigBuilder_Chaining(t *testing.T) {
	cfg, err := NewConfigBuilder("/test.db").
		WithMaxMemory(256 * 1024 * 1024).
		WithBufferSize(50_000).
		WithPartitionDuration(2 * time.Hour).
		WithRetention(7 * 24 * time.Hour).
		WithQueryTimeout(time.Minute).
		WithCompaction(4, 15*time.Minute).
		WithHTTP(9090).
		WithPrometheusRemoteWrite().
		WithRateLimit(500).
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if cfg.Storage.MaxMemory != 256*1024*1024 {
		t.Errorf("MaxMemory = %d, want 256MB", cfg.Storage.MaxMemory)
	}
	if cfg.Storage.BufferSize != 50_000 {
		t.Errorf("BufferSize = %d, want 50000", cfg.Storage.BufferSize)
	}
	if cfg.Storage.PartitionDuration != 2*time.Hour {
		t.Errorf("PartitionDuration = %v, want 2h", cfg.Storage.PartitionDuration)
	}
	if cfg.Retention.RetentionDuration != 7*24*time.Hour {
		t.Errorf("Retention = %v, want 7d", cfg.Retention.RetentionDuration)
	}
	if cfg.Query.QueryTimeout != time.Minute {
		t.Errorf("QueryTimeout = %v, want 1m", cfg.Query.QueryTimeout)
	}
	if cfg.Retention.CompactionWorkers != 4 {
		t.Errorf("CompactionWorkers = %d, want 4", cfg.Retention.CompactionWorkers)
	}
	if !cfg.HTTP.HTTPEnabled || cfg.HTTP.HTTPPort != 9090 {
		t.Errorf("HTTP = {%v, %d}, want {true, 9090}", cfg.HTTP.HTTPEnabled, cfg.HTTP.HTTPPort)
	}
	if !cfg.HTTP.PrometheusRemoteWriteEnabled {
		t.Error("PrometheusRemoteWrite should be enabled")
	}
	if cfg.RateLimitPerSecond != 500 {
		t.Errorf("RateLimit = %d, want 500", cfg.RateLimitPerSecond)
	}
}

func TestConfigBuilder_Auth(t *testing.T) {
	cfg, err := NewConfigBuilder("/test.db").
		WithAuth("key1", "key2").
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if cfg.Auth == nil || !cfg.Auth.Enabled {
		t.Fatal("Auth should be enabled")
	}
	if len(cfg.Auth.APIKeys) != 2 {
		t.Errorf("APIKeys count = %d, want 2", len(cfg.Auth.APIKeys))
	}
}

func TestConfigBuilder_Encryption(t *testing.T) {
	cfg, err := NewConfigBuilder("/test.db").
		WithEncryption("secret").
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if cfg.Encryption == nil || !cfg.Encryption.Enabled {
		t.Fatal("Encryption should be enabled")
	}
}

func TestConfigBuilder_ValidationError(t *testing.T) {
	_, err := NewConfigBuilder("").Build()
	if err == nil {
		t.Error("expected validation error for empty path")
	}
}

func TestConfigBuilder_LegacySync(t *testing.T) {
	cfg, err := NewConfigBuilder("/test.db").
		WithMaxMemory(128 * 1024 * 1024).
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	// Legacy field should be synced
	if cfg.MaxMemory != 128*1024*1024 {
		t.Errorf("legacy MaxMemory = %d, want 128MB", cfg.MaxMemory)
	}
}

func TestConfigBuilder_MustBuild_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from MustBuild with invalid config")
		}
	}()
	NewConfigBuilder("").MustBuild()
}

func TestConfigBuilder_Replication(t *testing.T) {
	cfg, err := NewConfigBuilder("/test.db").
		WithReplication("https://remote.example.com/write", 1000, 10*time.Second).
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if cfg.Replication == nil || !cfg.Replication.Enabled {
		t.Fatal("Replication should be enabled")
	}
	if cfg.Replication.TargetURL != "https://remote.example.com/write" {
		t.Errorf("TargetURL = %q", cfg.Replication.TargetURL)
	}
	if cfg.Replication.BatchSize != 1000 {
		t.Errorf("BatchSize = %d, want 1000", cfg.Replication.BatchSize)
	}
}
