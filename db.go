package chronicle

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// DB is the main database handle.
type DB struct {
	// Core storage components
	path      string
	file      *os.File   // Used for index storage and legacy operations
	dataStore DataStore  // Abstraction for partition storage
	wal       *WAL
	index     *Index
	buffer    *WriteBuffer

	// Configuration
	config Config

	// Lifecycle management
	mu        sync.RWMutex
	closeCh   chan struct{}
	closed    bool
	lifecycle *lifecycleManager

	// Feature management (optional features)
	features *FeatureManager

	// Legacy direct references for backward compatibility
	// These delegate to features manager
	schemaRegistry     *SchemaRegistry
	alertManager       *AlertManager
	exemplarStore      *ExemplarStore
	histogramStore     *HistogramStore
	cardinalityTracker *CardinalityTracker
}

// lifecycleManager manages background workers and external services.
type lifecycleManager struct {
	db         *DB
	httpServer *httpServer
	replicator *replicator
	compactCh  chan struct{}
	cqRunners  []*cqRunner
	mu         sync.Mutex
}

func newLifecycleManager(db *DB) *lifecycleManager {
	return &lifecycleManager{
		db:        db,
		compactCh: make(chan struct{}, 1),
	}
}

func (lm *lifecycleManager) startHTTP(port int) error {
	server, err := startHTTPServer(lm.db, port)
	if err != nil {
		return err
	}
	lm.mu.Lock()
	lm.httpServer = server
	lm.mu.Unlock()
	return nil
}

func (lm *lifecycleManager) startReplication(cfg *ReplicationConfig) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	lm.mu.Lock()
	lm.replicator = newReplicator(cfg)
	lm.replicator.Start()
	lm.mu.Unlock()
}

func (lm *lifecycleManager) startBackgroundWorkers(compactionWorkers int, compactionInterval time.Duration) {
	for i := 0; i < compactionWorkers; i++ {
		go lm.db.compactionWorker()
	}
	go lm.db.backgroundWorker()
}

func (lm *lifecycleManager) stop() {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.httpServer != nil {
		_ = lm.httpServer.Close()
		lm.httpServer = nil
	}
	if lm.replicator != nil {
		lm.replicator.Stop()
		lm.replicator = nil
	}
}

func (lm *lifecycleManager) enqueueReplication(points []Point) {
	lm.mu.Lock()
	r := lm.replicator
	lm.mu.Unlock()

	if r == nil || len(points) == 0 {
		return
	}
	r.Enqueue(points)
}

func (lm *lifecycleManager) scheduleCompaction() {
	select {
	case lm.compactCh <- struct{}{}:
	default:
	}
}

// httpHandler returns the HTTP handler for testing purposes.
// This method is intended for internal use in tests.
func (lm *lifecycleManager) httpHandler() http.Handler {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if lm.httpServer == nil {
		return nil
	}
	return lm.httpServer.srv.Handler
}

// Open opens or creates a Chronicle database.
//
//nolint:gocritic // cfg passed by value for API simplicity; callers typically construct inline
func Open(path string, cfg Config) (*DB, error) {
	cfg.normalize()
	if cfg.Path == "" {
		cfg.Path = path
	}
	if cfg.Path == "" {
		return nil, errors.New("path is required")
	}
	if cfg.Storage.PartitionDuration == 0 {
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
	if cfg.Storage.PartitionDuration <= 0 {
		cfg.Storage.PartitionDuration = time.Hour
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

	db := &DB{
		path:    cfg.Path,
		config:  cfg,
		closeCh: make(chan struct{}),
	}

	// Initialize lifecycle manager
	db.lifecycle = newLifecycleManager(db)

	// Initialize storage components
	file, err := os.OpenFile(cfg.Path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	db.file = file

	if err := initStorage(file); err != nil {
		_ = file.Close()
		return nil, err
	}

	// Initialize DataStore based on configuration
	if cfg.StorageBackend != nil {
		// Use the provided StorageBackend for partition storage
		db.dataStore = NewBackendDataStore(cfg.StorageBackend, "")
	} else {
		// Use file-based storage (default)
		db.dataStore = NewFileDataStore(file)
	}

	db.wal, err = NewWAL(cfg.Path+".wal", cfg.WAL.SyncInterval, cfg.WAL.WALMaxSize, cfg.WAL.WALRetain)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	db.index, err = loadIndex(file)
	if err != nil {
		_ = file.Close()
		_ = db.wal.Close()
		return nil, err
	}

	db.buffer = NewWriteBuffer(cfg.Storage.BufferSize)

	// Initialize feature manager with all optional features
	featureCfg := FeatureManagerConfig{
		ExemplarConfig:    DefaultExemplarConfig(),
		CardinalityConfig: DefaultCardinalityConfig(),
		StrictSchema:      cfg.StrictSchema,
		Schemas:           cfg.Schemas,
	}
	db.features, err = NewFeatureManager(db, featureCfg)
	if err != nil {
		_ = file.Close()
		_ = db.wal.Close()
		return nil, fmt.Errorf("failed to initialize features: %w", err)
	}

	// Set up legacy references for backward compatibility
	db.schemaRegistry = db.features.SchemaRegistry()
	db.alertManager = db.features.AlertManager()
	db.exemplarStore = db.features.ExemplarStore()
	db.histogramStore = db.features.HistogramStore()
	db.cardinalityTracker = db.features.CardinalityTracker()

	if err := db.recover(); err != nil {
		_ = file.Close()
		_ = db.wal.Close()
		return nil, err
	}

	// Start HTTP server if enabled
	if cfg.HTTP.HTTPEnabled {
		if err := db.lifecycle.startHTTP(cfg.HTTP.HTTPPort); err != nil {
			_ = file.Close()
			_ = db.wal.Close()
			return nil, err
		}
	}

	// Start replication if configured
	db.lifecycle.startReplication(cfg.Replication)

	// Start feature background processes
	db.features.Start()

	// Start background workers
	db.lifecycle.startBackgroundWorkers(cfg.Retention.CompactionWorkers, cfg.Retention.CompactionInterval)
	db.startContinuousQueries()

	return db, nil
}

// Close flushes data and closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	close(db.closeCh)

	// Stop lifecycle services
	db.lifecycle.stop()

	// Stop features
	if db.features != nil {
		db.features.Stop()
	}

	db.stopContinuousQueries()

	_ = db.Flush()

	db.mu.Lock()
	if err := persistIndex(db.file, db.index); err != nil {
		db.mu.Unlock()
		_ = db.file.Close()
		_ = db.wal.Close()
		return err
	}
	db.mu.Unlock()

	if err := db.file.Sync(); err != nil {
		_ = db.file.Close()
		_ = db.wal.Close()
		return err
	}

	// Close DataStore if it's not backed by the same file
	if db.dataStore != nil {
		if _, ok := db.dataStore.(*FileDataStore); !ok {
			// Backend-based storage - close it separately
			_ = db.dataStore.Close()
		}
		// FileDataStore shares the file handle, so don't close it separately
	}

	if err := db.file.Close(); err != nil {
		_ = db.wal.Close()
		return err
	}

	return db.wal.Close()
}

// CQLEngine returns the CQL query engine.
func (db *DB) CQLEngine() *CQLEngine {
	if db.features == nil {
		return nil
	}
	return db.features.CQLEngine()
}

// Observability returns the observability suite.
func (db *DB) Observability() *ObservabilitySuite {
	if db.features == nil {
		return nil
	}
	return db.features.Observability()
}

// MaterializedViews returns the materialized view engine.
func (db *DB) MaterializedViews() *MaterializedViewEngine {
	if db.features == nil {
		return nil
	}
	return db.features.MaterializedViews()
}

// Flush writes buffered points to storage.
func (db *DB) Flush() error {
	points := db.buffer.Drain()
	if len(points) == 0 {
		return nil
	}
	return db.flush(points, true)
}

// Write writes a single point.
func (db *DB) Write(p Point) error {
	if p.Timestamp == 0 {
		p.Timestamp = time.Now().UnixNano()
	}
	if p.Tags == nil {
		p.Tags = map[string]string{}
	}

	// Validate against schema if registry exists
	if err := db.schemaRegistry.Validate(p); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	// Track cardinality
	if db.cardinalityTracker != nil {
		if err := db.cardinalityTracker.TrackPoint(p); err != nil {
			return fmt.Errorf("cardinality limit exceeded: %w", err)
		}
	}

	db.buffer.Add(p)
	if db.buffer.Len() >= db.buffer.capacity {
		if err := db.Flush(); err != nil {
			return err
		}
	}
	db.enqueueReplication([]Point{p})
	return nil
}

// WriteBatch writes multiple points efficiently.
func (db *DB) WriteBatch(points []Point) error {
	if len(points) == 0 {
		return nil
	}
	now := time.Now().UnixNano()
	for i := range points {
		if points[i].Timestamp == 0 {
			points[i].Timestamp = now
		}
		if points[i].Tags == nil {
			points[i].Tags = map[string]string{}
		}
	}

	// Validate all points against schema
	if err := db.schemaRegistry.ValidateBatch(points); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	// Track cardinality for all points
	if db.cardinalityTracker != nil {
		for _, p := range points {
			if err := db.cardinalityTracker.TrackPoint(p); err != nil {
				return fmt.Errorf("cardinality limit exceeded: %w", err)
			}
		}
	}

	if err := db.flush(points, true); err != nil {
		return err
	}
	db.enqueueReplication(points)
	return nil
}

func (db *DB) flush(points []Point, writeWAL bool) error {
	if len(points) == 0 {
		return nil
	}

	if writeWAL {
		if err := db.wal.Write(points); err != nil {
			return err
		}
	}

	byPartition := groupByPartition(points, db.config.Storage.PartitionDuration)

	db.mu.Lock()
	defer db.mu.Unlock()

	for _, batch := range byPartition {
		part := db.index.GetOrCreatePartition(batch.partitionID, batch.startTime, batch.endTime)
		for _, p := range batch.points {
			db.index.RegisterSeries(p.Metric, p.Tags)
		}
		if err := part.Append(batch.points, db.index); err != nil {
			return err
		}

		if err := db.persistPartitionData(part); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) findPartitionsLocked(start, end int64) []*Partition {
	return db.index.FindPartitions(start, end)
}

func (db *DB) recover() error {
	points, err := db.wal.ReadAll()
	if err != nil {
		return err
	}
	if len(points) == 0 {
		return nil
	}

	if err := db.flush(points, false); err != nil {
		return err
	}

	return db.wal.Reset()
}

func (db *DB) backgroundWorker() {
	retentionTicker := time.NewTicker(5 * time.Minute)
	downsampleTicker := time.NewTicker(5 * time.Minute)
	compactionTicker := time.NewTicker(db.config.Retention.CompactionInterval)
	defer retentionTicker.Stop()
	defer downsampleTicker.Stop()
	defer compactionTicker.Stop()

	for {
		select {
		case <-db.closeCh:
			return
		case <-retentionTicker.C:
			if db.config.Retention.RetentionDuration > 0 {
				db.applyRetention()
			}
			if db.config.Storage.MaxStorageBytes > 0 {
				_ = db.applySizeRetention()
			}
		case <-downsampleTicker.C:
			if len(db.config.Retention.DownsampleRules) > 0 {
				_ = db.applyDownsampling()
			}
		case <-compactionTicker.C:
			db.scheduleCompaction()
		}
	}
}

func (db *DB) scheduleCompaction() {
	db.lifecycle.scheduleCompaction()
}

func (db *DB) compactionWorker() {
	for {
		select {
		case <-db.closeCh:
			return
		case <-db.lifecycle.compactCh:
			_ = db.compact()
		}
	}
}

func (db *DB) applyRetention() {
	cutoff := time.Now().Add(-db.config.Retention.RetentionDuration).UnixNano()
	db.mu.Lock()
	removed := db.index.RemovePartitionsBefore(cutoff)
	db.mu.Unlock()
	if removed {
		db.scheduleCompaction()
	}
}

func (db *DB) applySizeRetention() error {
	if db.config.Storage.MaxStorageBytes <= 0 {
		return nil
	}
	size, err := db.dataStore.Stat()
	if err != nil {
		return err
	}
	for size > db.config.Storage.MaxStorageBytes {
		db.mu.Lock()
		removed := db.index.RemoveOldestPartition()
		db.mu.Unlock()
		if !removed {
			break
		}
		db.scheduleCompaction()
		size, err = db.dataStore.Stat()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) applyDownsampling() error {
	for _, rule := range db.config.Retention.DownsampleRules {
		if err := db.applyRule(rule); err != nil {
			return err
		}
		if rule.Retention > 0 {
			if err := db.pruneDownsampled(rule); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) applyRule(rule DownsampleRule) error {
	cutoff := time.Now().Add(-rule.SourceResolution * 2)
	metrics := db.index.Metrics()

	for _, metric := range metrics {
		q := &Query{
			Metric: metric,
			Start:  cutoff.Add(-rule.TargetResolution).UnixNano(),
			End:    cutoff.UnixNano(),
			Aggregation: &Aggregation{
				Window: rule.TargetResolution,
			},
		}

		for _, fn := range rule.Aggregations {
			q.Aggregation.Function = fn
			result, err := db.Execute(q)
			if err != nil {
				return err
			}

			for _, p := range result.Points {
				p.Metric = metric + ":" + rule.TargetResolution.String() + ":" + aggFuncName(fn)
				if err := db.Write(p); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (db *DB) pruneDownsampled(rule DownsampleRule) error {
	cutoff := time.Now().Add(-rule.Retention).UnixNano()
	prefix := ":" + rule.TargetResolution.String() + ":"

	db.mu.Lock()
	partitions := append([]*Partition(nil), db.index.partitions...)
	db.mu.Unlock()

	for _, part := range partitions {
		if part.EndTime > cutoff {
			continue
		}
		if err := part.ensureLoaded(db); err != nil {
			return err
		}
		changed := part.pruneMetricPrefix(prefix)
		if changed {
			db.mu.Lock()
			if len(part.Series) == 0 {
				_ = db.index.RemovePartitionByID(part.ID)
				db.mu.Unlock()
				db.scheduleCompaction()
				continue
			}
			db.mu.Unlock()
			if err := db.persistPartitionData(part); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tempPath := db.path + ".compact"
	tempFile, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	if err := initStorage(tempFile); err != nil {
		_ = tempFile.Close()
		return err
	}

	for _, part := range db.index.partitions {
		if err := persistPartition(tempFile, part); err != nil {
			_ = tempFile.Close()
			return err
		}
	}

	if err := persistIndex(tempFile, db.index); err != nil {
		_ = tempFile.Close()
		return err
	}

	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	if err := db.file.Close(); err != nil {
		return err
	}

	if err := os.Rename(tempPath, db.path); err != nil {
		return err
	}

	db.file, err = os.OpenFile(db.path, os.O_RDWR, 0o644)
	return err
}

func (db *DB) enqueueReplication(points []Point) {
	db.lifecycle.enqueueReplication(points)
}

// persistPartitionData writes partition data using the configured DataStore.
// For file-based storage, this uses the traditional offset-based approach.
// For backend-based storage, partitions are stored as separate objects.
func (db *DB) persistPartitionData(part *Partition) error {
	payload, err := encodePartition(part)
	if err != nil {
		return err
	}

	if bds, ok := db.dataStore.(*BackendDataStore); ok {
		// Backend-based storage - store partition by ID
		_, length, err := bds.WritePartition(context.Background(), part.ID, payload)
		if err != nil {
			return err
		}
		part.Offset = 0 // Not meaningful for backend storage
		part.Length = length
		part.Size = length
		return nil
	}

	// File-based storage - use traditional approach
	return persistPartition(db.file, part)
}

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

// ExemplarStore returns the exemplar store for direct access.
// Deprecated: Use db.Features().ExemplarStore() instead for cleaner architecture.
func (db *DB) ExemplarStore() *ExemplarStore {
	return db.exemplarStore
}

// HistogramStore returns the histogram store for direct access.
// Deprecated: Use db.Features().HistogramStore() instead for cleaner architecture.
func (db *DB) HistogramStore() *HistogramStore {
	return db.histogramStore
}

// CardinalityTracker returns the cardinality tracker for direct access.
// Deprecated: Use db.Features().CardinalityTracker() instead for cleaner architecture.
func (db *DB) CardinalityTracker() *CardinalityTracker {
	return db.cardinalityTracker
}

// CardinalityStats returns current cardinality statistics.
func (db *DB) CardinalityStats() CardinalityStats {
	if db.cardinalityTracker == nil {
		return CardinalityStats{}
	}
	return db.cardinalityTracker.Stats()
}

// AlertManager returns the alert manager for direct access.
// Deprecated: Use db.Features().AlertManager() instead for cleaner architecture.
func (db *DB) AlertManager() *AlertManager {
	return db.alertManager
}

// Features returns the feature manager for accessing optional features.
// This is the preferred way to access features like alerting, histograms, etc.
func (db *DB) Features() *FeatureManager {
	return db.features
}

// WriteHistogram writes a histogram point to the database.
func (db *DB) WriteHistogram(p HistogramPoint) error {
	if db.histogramStore == nil {
		return fmt.Errorf("histogram store not initialized")
	}
	return db.histogramStore.Write(p)
}

// WriteExemplar writes a point with an exemplar to the database.
func (db *DB) WriteExemplar(p ExemplarPoint) error {
	if db.exemplarStore == nil {
		return fmt.Errorf("exemplar store not initialized")
	}
	return db.exemplarStore.Write(p)
}
