package chronicle

import (
	"errors"
	"fmt"
	"os"
	"time"
)

func applyOpenDefaults(path string, cfg *Config) error {
	cfg.normalize()
	if cfg.Path == "" {
		cfg.Path = path
	}
	if cfg.Path == "" {
		return errors.New("path is required")
	}
	if cfg.Storage.PartitionDuration <= 0 {
		cfg.Storage.PartitionDuration = time.Hour
	}
	if cfg.Storage.BufferSize <= 0 {
		cfg.Storage.BufferSize = 10_000
	}
	if cfg.WAL.SyncInterval <= 0 {
		cfg.WAL.SyncInterval = time.Second
	}
	if cfg.Retention.CompactionWorkers <= 0 {
		cfg.Retention.CompactionWorkers = 1
	}
	if cfg.Retention.CompactionInterval <= 0 {
		cfg.Retention.CompactionInterval = 30 * time.Minute
	}
	if cfg.Query.QueryTimeout <= 0 {
		cfg.Query.QueryTimeout = 30 * time.Second
	}
	if cfg.Storage.MaxMemory <= 0 {
		cfg.Storage.MaxMemory = 64 * 1024 * 1024
	}
	if cfg.WAL.WALMaxSize <= 0 {
		cfg.WAL.WALMaxSize = 128 * 1024 * 1024
	}
	if cfg.WAL.WALRetain <= 0 {
		cfg.WAL.WALRetain = 3
	}
	if cfg.HTTP.HTTPPort == 0 {
		cfg.HTTP.HTTPPort = 8086
	}
	cfg.syncLegacyFields()
	return nil
}

func storageEngineConfig(cfg Config) StorageEngineConfig {
	return StorageEngineConfig{
		Path:         cfg.Path,
		Storage:      cfg.Storage,
		WAL:          cfg.WAL,
		BufferSize:   cfg.Storage.BufferSize,
		SyncInterval: cfg.WAL.SyncInterval,
		WALMaxSize:   cfg.WAL.WALMaxSize,
		WALRetain:    cfg.WAL.WALRetain,
	}
}

func initStorageEngine(db *DB, cfg Config) (*StorageEngine, error) {
	engine, err := NewStorageEngine(storageEngineConfig(cfg))
	if err != nil {
		return nil, err
	}
	db.file = engine.File()
	db.wal = engine.WAL()
	db.index = engine.Index()
	db.buffer = engine.Buffer()
	return engine, nil
}

func initDataStore(cfg Config, file *os.File) DataStore {
	if cfg.StorageBackend != nil {
		return NewBackendDataStore(cfg.StorageBackend, "")
	}
	return NewFileDataStore(file)
}

func initFeatureManager(db *DB, cfg Config) (*FeatureManager, error) {
	featureCfg := FeatureManagerConfig{
		ExemplarConfig:    DefaultExemplarConfig(),
		CardinalityConfig: DefaultCardinalityConfig(),
		StrictSchema:      cfg.StrictSchema,
		Schemas:           cfg.Schemas,
	}
	return NewFeatureManager(db, featureCfg)
}

func setLegacyReferences(db *DB) {
	db.schemaRegistry = db.features.SchemaRegistry()
	db.alertManager = db.features.AlertManager()
	db.exemplarStore = db.features.ExemplarStore()
	db.histogramStore = db.features.HistogramStore()
	db.cardinalityTracker = db.features.CardinalityTracker()
}

func closeOpenResources(db *DB) {
	if db.dataStore != nil {
		if _, ok := db.dataStore.(*FileDataStore); !ok {
			_ = db.dataStore.Close()
		}
	}
	if db.file != nil {
		_ = db.file.Close()
	}
	if db.wal != nil {
		_ = db.wal.Close()
	}
}

func wrapFeatureInitError(err error) error {
	return fmt.Errorf("failed to initialize features: %w", err)
}
