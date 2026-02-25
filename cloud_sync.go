package chronicle

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

// CloudSyncConfig configures the edge-to-cloud synchronization protocol.
type CloudSyncConfig struct {
	// Enabled enables/disables sync
	Enabled bool `json:"enabled"`

	// CloudEndpoint is the URL of the cloud Chronicle instance or data lake
	CloudEndpoint string `json:"cloud_endpoint"`

	// SyncMode determines the sync behavior
	SyncMode SyncMode `json:"sync_mode"`

	// BatchSize is the maximum number of points per sync batch
	BatchSize int `json:"batch_size"`

	// MaxBatchBytes limits batch size by bytes
	MaxBatchBytes int `json:"max_batch_bytes"`

	// SyncInterval is how often to attempt sync
	SyncInterval time.Duration `json:"sync_interval"`

	// RetryInterval for failed syncs
	RetryInterval time.Duration `json:"retry_interval"`

	// MaxRetries before giving up on a batch
	MaxRetries int `json:"max_retries"`

	// OfflineQueuePath is where to store data when offline
	OfflineQueuePath string `json:"offline_queue_path"`

	// MaxOfflineQueueSize limits the offline queue size in bytes
	MaxOfflineQueueSize int64 `json:"max_offline_queue_size"`

	// ConflictResolution strategy
	ConflictResolution ConflictResolutionStrategy `json:"conflict_resolution"`

	// MetricFilters filters which metrics to sync (empty = all)
	MetricFilters []string `json:"metric_filters"`

	// TimeRangeFilter optional time range for selective sync
	TimeRangeStart int64 `json:"time_range_start,omitempty"`
	TimeRangeEnd   int64 `json:"time_range_end,omitempty"`

	// EnableCompression enables gzip compression
	EnableCompression bool `json:"enable_compression"`

	// BandwidthLimit in bytes per second (0 = unlimited)
	BandwidthLimit int64 `json:"bandwidth_limit"`

	// SyncFormat for data lake uploads
	SyncFormat CloudSyncFormat `json:"sync_format"`

	// CloudProvider for cloud-specific optimizations
	CloudProvider CloudProvider `json:"cloud_provider"`

	// S3Config for S3-compatible storage
	S3Config *S3SyncConfig `json:"s3_config,omitempty"`

	// HTTPClient for custom HTTP client
	HTTPClient HTTPDoer `json:"-"`
}

// S3SyncConfig contains S3-specific sync configuration.
type S3SyncConfig struct {
	Bucket          string `json:"bucket"`
	Prefix          string `json:"prefix"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
}

// SyncMode determines how synchronization behaves.
type SyncMode int

const (
	// SyncModeEdgeToCloud syncs only from edge to cloud
	SyncModeEdgeToCloud SyncMode = iota
	// SyncModeCloudToEdge syncs only from cloud to edge
	SyncModeCloudToEdge
	// SyncModeBidirectional syncs in both directions
	SyncModeBidirectional
)

func (m SyncMode) String() string {
	switch m {
	case SyncModeEdgeToCloud:
		return "edge_to_cloud"
	case SyncModeCloudToEdge:
		return "cloud_to_edge"
	case SyncModeBidirectional:
		return "bidirectional"
	default:
		return "unknown"
	}
}

// ConflictResolutionStrategy determines how to handle conflicts.
type ConflictResolutionStrategy int

const (
	// ConflictLastWriteWins uses the most recent timestamp
	ConflictLastWriteWins ConflictResolutionStrategy = iota
	// ConflictCloudWins cloud data takes precedence
	ConflictCloudWins
	// ConflictEdgeWins edge data takes precedence
	ConflictEdgeWins
	// ConflictMerge merges both versions
	ConflictMerge
	// ConflictManual requires manual resolution
	ConflictManual
)

func (s ConflictResolutionStrategy) String() string {
	switch s {
	case ConflictLastWriteWins:
		return "last_write_wins"
	case ConflictCloudWins:
		return "cloud_wins"
	case ConflictEdgeWins:
		return "edge_wins"
	case ConflictMerge:
		return "merge"
	case ConflictManual:
		return "manual"
	default:
		return "unknown"
	}
}

// CloudSyncFormat determines the format for cloud sync.
type CloudSyncFormat int

const (
	CloudSyncFormatJSON CloudSyncFormat = iota
	CloudSyncFormatParquet
	CloudSyncFormatCSV
)

func (f CloudSyncFormat) String() string {
	switch f {
	case CloudSyncFormatJSON:
		return "json"
	case CloudSyncFormatParquet:
		return "parquet"
	case CloudSyncFormatCSV:
		return "csv"
	default:
		return "json"
	}
}

// CloudProvider identifies the cloud provider.
type CloudProvider int

const (
	CloudProviderGeneric CloudProvider = iota
	CloudProviderAWS
	CloudProviderGCP
	CloudProviderAzure
)

// DefaultCloudSyncConfig returns default configuration.
func DefaultCloudSyncConfig() CloudSyncConfig {
	return CloudSyncConfig{
		Enabled:             false,
		SyncMode:            SyncModeEdgeToCloud,
		BatchSize:           5000,
		MaxBatchBytes:       5 * 1024 * 1024, // 5MB
		SyncInterval:        time.Minute,
		RetryInterval:       30 * time.Second,
		MaxRetries:          5,
		OfflineQueuePath:    "/tmp/chronicle_sync_queue",
		MaxOfflineQueueSize: 1024 * 1024 * 1024, // 1GB
		ConflictResolution:  ConflictLastWriteWins,
		EnableCompression:   true,
		SyncFormat:          CloudSyncFormatJSON,
		CloudProvider:       CloudProviderGeneric,
	}
}

// CloudSync manages edge-to-cloud synchronization.
type CloudSync struct {
	config  CloudSyncConfig
	db      *DB
	mu      sync.RWMutex
	client  HTTPDoer
	retryer *Retryer
	cb      *CircuitBreaker

	// Sync state
	lastSyncTime   time.Time
	lastSyncStatus SyncStatus
	syncInProgress bool
	syncStats      SyncStats

	// Offline queue
	offlineQueue *OfflineQueue

	// Checkpoint for resumable sync
	checkpoint *SyncCheckpoint

	// Background sync
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	syncChan chan *SyncRequest

	// Metric filters
	metricFilters map[string]struct{}
}

// SyncStatus represents the current sync status.
type SyncStatus int

const (
	SyncStatusIdle SyncStatus = iota
	SyncStatusSyncing
	SyncStatusOffline
	SyncStatusError
	SyncStatusComplete
)

func (s SyncStatus) String() string {
	switch s {
	case SyncStatusIdle:
		return "idle"
	case SyncStatusSyncing:
		return "syncing"
	case SyncStatusOffline:
		return "offline"
	case SyncStatusError:
		return "error"
	case SyncStatusComplete:
		return "complete"
	default:
		return "unknown"
	}
}

// SyncStats contains synchronization statistics.
type SyncStats struct {
	TotalPointsSynced   int64         `json:"total_points_synced"`
	TotalBytesSynced    int64         `json:"total_bytes_synced"`
	TotalSyncAttempts   int64         `json:"total_sync_attempts"`
	SuccessfulSyncs     int64         `json:"successful_syncs"`
	FailedSyncs         int64         `json:"failed_syncs"`
	LastSyncDuration    time.Duration `json:"last_sync_duration"`
	AverageSyncDuration time.Duration `json:"average_sync_duration"`
	OfflineQueueSize    int64         `json:"offline_queue_size"`
	ConflictsResolved   int64         `json:"conflicts_resolved"`
}

// SyncCheckpoint tracks sync progress for resumability.
type SyncCheckpoint struct {
	LastSyncedTimestamp int64            `json:"last_synced_timestamp"`
	PendingBatches      []string         `json:"pending_batches"`
	MetricOffsets       map[string]int64 `json:"metric_offsets"`
	Version             int              `json:"version"`
	Checksum            string           `json:"checksum"`
}

// SyncRequest represents a sync operation request.
type SyncRequest struct {
	Points     []Point
	Timestamp  int64
	Priority   int
	ResultChan chan *SyncResult
}

// SyncResult contains the result of a sync operation.
type SyncResult struct {
	Success        bool          `json:"success"`
	PointsSynced   int           `json:"points_synced"`
	BytesSynced    int64         `json:"bytes_synced"`
	Duration       time.Duration `json:"duration"`
	Error          error         `json:"error,omitempty"`
	ConflictsFound int           `json:"conflicts_found"`
}

// OfflineQueue manages data storage when cloud is unavailable.
type OfflineQueue struct {
	path        string
	maxSize     int64
	mu          sync.Mutex
	currentSize int64
}

// NewCloudSync creates a new cloud sync manager.
func NewCloudSync(db *DB, config CloudSyncConfig) *CloudSync {
	if config.BatchSize <= 0 {
		config.BatchSize = 5000
	}
	if config.SyncInterval <= 0 {
		config.SyncInterval = time.Minute
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 5
	}

	ctx, cancel := context.WithCancel(context.Background())

	cs := &CloudSync{
		config:         config,
		db:             db,
		client:         config.HTTPClient,
		ctx:            ctx,
		cancel:         cancel,
		syncChan:       make(chan *SyncRequest, 100),
		lastSyncStatus: SyncStatusIdle,
	}

	if cs.client == nil {
		cs.client = &http.Client{Timeout: 30 * time.Second}
	}

	// Setup retryer
	cs.retryer = NewRetryer(RetryConfig{
		MaxAttempts:       config.MaxRetries,
		InitialBackoff:    time.Second,
		MaxBackoff:        config.RetryInterval,
		BackoffMultiplier: 2.0,
		Jitter:            0.1,
		RetryIf:           IsRetryable,
	})

	// Setup circuit breaker
	cs.cb = NewCircuitBreaker(5, 60*time.Second)

	// Setup offline queue
	if config.OfflineQueuePath != "" {
		cs.offlineQueue = &OfflineQueue{
			path:    config.OfflineQueuePath,
			maxSize: config.MaxOfflineQueueSize,
		}
		_ = os.MkdirAll(config.OfflineQueuePath, 0755) //nolint:errcheck // best-effort sync operation
	}

	// Setup metric filters
	if len(config.MetricFilters) > 0 {
		cs.metricFilters = make(map[string]struct{})
		for _, m := range config.MetricFilters {
			cs.metricFilters[m] = struct{}{}
		}
	}

	// Load checkpoint
	cs.loadCheckpoint()

	return cs
}

// Start begins background synchronization.
func (cs *CloudSync) Start() {
	if !cs.config.Enabled {
		return
	}

	cs.wg.Add(2)
	go cs.syncLoop()
	go cs.processQueue()
}

// Stop gracefully stops synchronization.
func (cs *CloudSync) Stop() {
	cs.cancel()
	cs.wg.Wait()
	cs.saveCheckpoint()
}

func (cs *CloudSync) syncLoop() {
	defer cs.wg.Done()

	ticker := time.NewTicker(cs.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cs.ctx.Done():
			// Final sync attempt before shutdown
			cs.performSync()
			return
		case <-ticker.C:
			cs.performSync()
		}
	}
}

func (cs *CloudSync) processQueue() {
	defer cs.wg.Done()

	for {
		select {
		case <-cs.ctx.Done():
			return
		case req := <-cs.syncChan:
			result := cs.syncBatch(req.Points)
			if req.ResultChan != nil {
				req.ResultChan <- result
			}
		}
	}
}

func (cs *CloudSync) performSync() {
	cs.mu.Lock()
	if cs.syncInProgress {
		cs.mu.Unlock()
		return
	}
	cs.syncInProgress = true
	cs.lastSyncStatus = SyncStatusSyncing
	cs.mu.Unlock()

	defer func() {
		cs.mu.Lock()
		cs.syncInProgress = false
		cs.mu.Unlock()
	}()

	startTime := time.Now()

	// First, try to sync any offline queue data
	if cs.offlineQueue != nil {
		cs.syncOfflineQueue()
	}

	// Then sync new data
	var points []Point
	var err error

	if cs.db != nil {
		points, err = cs.getUnsyncedPoints()
		if err != nil {
			slog.Error("cloud sync get points error", "err", err)
			cs.mu.Lock()
			cs.lastSyncStatus = SyncStatusError
			cs.mu.Unlock()
			return
		}
	}

	if len(points) == 0 {
		cs.mu.Lock()
		cs.lastSyncStatus = SyncStatusComplete
		cs.lastSyncTime = time.Now()
		cs.mu.Unlock()
		return
	}

	// Sync in batches
	result := cs.syncBatch(points)

	cs.mu.Lock()
	cs.syncStats.TotalSyncAttempts++
	if result.Success {
		cs.syncStats.SuccessfulSyncs++
		cs.syncStats.TotalPointsSynced += int64(result.PointsSynced)
		cs.syncStats.TotalBytesSynced += result.BytesSynced
		cs.lastSyncStatus = SyncStatusComplete
	} else {
		cs.syncStats.FailedSyncs++
		cs.lastSyncStatus = SyncStatusError
	}
	cs.syncStats.LastSyncDuration = time.Since(startTime)
	cs.lastSyncTime = time.Now()
	cs.mu.Unlock()
}

func (cs *CloudSync) getUnsyncedPoints() ([]Point, error) {
	if cs.db == nil {
		return nil, nil
	}

	// Get points since last sync
	start := int64(0)
	if cs.checkpoint != nil {
		start = cs.checkpoint.LastSyncedTimestamp
	}

	var allPoints []Point
	metrics := cs.db.Metrics()

	for _, metric := range metrics {
		// Check metric filter
		if cs.metricFilters != nil {
			if _, ok := cs.metricFilters[metric]; !ok {
				continue
			}
		}

		result, err := cs.db.Execute(&Query{
			Metric: metric,
			Start:  start,
			End:    time.Now().UnixNano(),
		})
		if err != nil {
			continue
		}

		allPoints = append(allPoints, result.Points...)
		if len(allPoints) >= cs.config.BatchSize*10 {
			break // Limit total points per sync cycle
		}
	}

	return allPoints, nil
}
