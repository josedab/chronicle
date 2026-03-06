package chronicle

import (
	"testing"
	"time"
)

// --- WithMaxStorageBytes ---

func TestConfigBuilder_WithMaxStorageBytes(t *testing.T) {
	cfg, err := NewConfigBuilder(t.TempDir() + "/test.db").
		WithMaxStorageBytes(1024 * 1024 * 1024).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.MaxStorageBytes != 1024*1024*1024 {
		t.Errorf("MaxStorageBytes = %d, want 1GB", cfg.Storage.MaxStorageBytes)
	}
}

func TestConfigBuilder_WithMaxStorageBytes_Zero(t *testing.T) {
	cfg, err := NewConfigBuilder(t.TempDir() + "/test.db").
		WithMaxStorageBytes(0).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.MaxStorageBytes != 0 {
		t.Errorf("MaxStorageBytes = %d, want 0 (unlimited)", cfg.Storage.MaxStorageBytes)
	}
}

// --- WithStorageBackend ---

func TestConfigBuilder_WithStorageBackend_Nil(t *testing.T) {
	cfg, err := NewConfigBuilder(t.TempDir() + "/test.db").
		WithStorageBackend(nil).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StorageBackend != nil {
		t.Error("StorageBackend should be nil")
	}
}

// --- WAL config setters ---

func TestConfigBuilder_WithWALSyncInterval(t *testing.T) {
	cfg, err := NewConfigBuilder(t.TempDir() + "/test.db").
		WithWALSyncInterval(500 * time.Millisecond).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WAL.SyncInterval != 500*time.Millisecond {
		t.Errorf("WAL SyncInterval = %v, want 500ms", cfg.WAL.SyncInterval)
	}
}

func TestConfigBuilder_WithWALMaxSize(t *testing.T) {
	cfg, err := NewConfigBuilder(t.TempDir() + "/test.db").
		WithWALMaxSize(128 * 1024 * 1024).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WAL.WALMaxSize != 128*1024*1024 {
		t.Errorf("WAL MaxSize = %d, want 128MB", cfg.WAL.WALMaxSize)
	}
}

func TestConfigBuilder_WithWALRetain(t *testing.T) {
	cfg, err := NewConfigBuilder(t.TempDir() + "/test.db").
		WithWALRetain(5).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WAL.WALRetain != 5 {
		t.Errorf("WAL Retain = %d, want 5", cfg.WAL.WALRetain)
	}
}

// --- WithSchemas ---

func TestConfigBuilder_WithSchemas(t *testing.T) {
	schemas := []MetricSchema{
		{Name: "cpu", Tags: []TagSchema{{Name: "host", Required: true}}},
		{Name: "memory", Tags: []TagSchema{{Name: "host", Required: true}, {Name: "region", Required: true}}},
	}

	cfg, err := NewConfigBuilder(t.TempDir() + "/test.db").
		WithSchemas(true, schemas...).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.StrictSchema {
		t.Error("StrictSchema should be true")
	}
	if len(cfg.Schemas) != 2 {
		t.Errorf("Expected 2 schemas, got %d", len(cfg.Schemas))
	}
}

func TestConfigBuilder_WithSchemas_NonStrict(t *testing.T) {
	cfg, err := NewConfigBuilder(t.TempDir() + "/test.db").
		WithSchemas(false, MetricSchema{Name: "cpu"}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StrictSchema {
		t.Error("StrictSchema should be false")
	}
}

// --- WithDownsampleRules ---

func TestConfigBuilder_WithDownsampleRules(t *testing.T) {
	rules := []DownsampleRule{
		{SourceResolution: time.Hour, TargetResolution: 24 * time.Hour, Aggregations: []AggFunc{AggMean}},
	}

	cfg, err := NewConfigBuilder(t.TempDir() + "/test.db").
		WithDownsampleRules(rules...).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Retention.DownsampleRules) != 1 {
		t.Errorf("Expected 1 downsample rule, got %d", len(cfg.Retention.DownsampleRules))
	}
}

// --- MustBuild with valid config ---

func TestConfigBuilder_MustBuild_Valid(t *testing.T) {
	cfg := NewConfigBuilder(t.TempDir() + "/test.db").MustBuild()
	if cfg.Path == "" {
		t.Error("MustBuild with valid config should return non-zero Config")
	}
}

// --- Chaining all WAL options ---

func TestConfigBuilder_WALChaining(t *testing.T) {
	cfg, err := NewConfigBuilder(t.TempDir() + "/test.db").
		WithWALSyncInterval(100 * time.Millisecond).
		WithWALMaxSize(64 * 1024 * 1024).
		WithWALRetain(10).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WAL.SyncInterval != 100*time.Millisecond {
		t.Errorf("SyncInterval = %v", cfg.WAL.SyncInterval)
	}
	if cfg.WAL.WALMaxSize != 64*1024*1024 {
		t.Errorf("WALMaxSize = %d", cfg.WAL.WALMaxSize)
	}
	if cfg.WAL.WALRetain != 10 {
		t.Errorf("WALRetain = %d", cfg.WAL.WALRetain)
	}
}

// --- Full configuration ---

func TestConfigBuilder_FullConfig(t *testing.T) {
	cfg, err := NewConfigBuilder(t.TempDir() + "/test.db").
		WithMaxMemory(512 * 1024 * 1024).
		WithBufferSize(100000).
		WithPartitionDuration(4 * time.Hour).
		WithMaxStorageBytes(10 * 1024 * 1024 * 1024).
		WithWALSyncInterval(200 * time.Millisecond).
		WithWALMaxSize(256 * 1024 * 1024).
		WithWALRetain(3).
		WithRetention(30 * 24 * time.Hour).
		WithCompaction(8, 30*time.Minute).
		WithQueryTimeout(2 * time.Minute).
		WithHTTP(8086).
		WithPrometheusRemoteWrite().
		WithAuth("key1").
		WithRateLimit(1000).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Storage.MaxMemory != 512*1024*1024 {
		t.Errorf("MaxMemory mismatch")
	}
	if cfg.HTTP.HTTPPort != 8086 {
		t.Errorf("HTTPPort mismatch")
	}
	if cfg.RateLimitPerSecond != 1000 {
		t.Errorf("RateLimit mismatch")
	}
}

// --- NewConfigBuilder sets defaults ---

func TestNewConfigBuilder_SetsDefaults(t *testing.T) {
	b := NewConfigBuilder(t.TempDir() + "/test.db")
	cfg, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Storage.BufferSize <= 0 {
		t.Error("BufferSize should have default > 0")
	}
	if cfg.Storage.MaxMemory <= 0 {
		t.Error("MaxMemory should have default > 0")
	}
	if cfg.Storage.PartitionDuration <= 0 {
		t.Error("PartitionDuration should have default > 0")
	}
}
