package chronicle

import (
	"net/http"
	"os"
	"sync"
	"time"
)

// DB is the main database handle. Open one with [Open] and close it with [Close].
// All read/write operations (Write, Execute, Flush, etc.) are safe for concurrent use.
type DB struct {
	// Core storage components
	path      string
	file      *os.File  // Used for index storage and legacy operations
	dataStore DataStore // Abstraction for partition storage
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
	if err := applyOpenDefaults(path, &cfg); err != nil {
		return nil, err
	}

	db := &DB{
		path:    cfg.Path,
		config:  cfg,
		closeCh: make(chan struct{}),
	}

	// Initialize lifecycle manager
	db.lifecycle = newLifecycleManager(db)

	// Initialize storage components
	if _, err := initStorageEngine(db, cfg); err != nil {
		return nil, err
	}
	db.dataStore = initDataStore(cfg, db.file)

	var err error
	db.features, err = initFeatureManager(db, cfg)
	if err != nil {
		closeOpenResources(db)
		return nil, wrapFeatureInitError(err)
	}
	setLegacyReferences(db)

	if err := db.recover(); err != nil {
		closeOpenResources(db)
		return nil, err
	}

	// Start HTTP server if enabled
	if cfg.HTTP.HTTPEnabled {
		if err := db.lifecycle.startHTTP(cfg.HTTP.HTTPPort); err != nil {
			closeOpenResources(db)
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
