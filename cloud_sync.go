package chronicle

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
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
		_ = os.MkdirAll(config.OfflineQueuePath, 0755)
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

func (cs *CloudSync) syncBatch(points []Point) *SyncResult {
	if len(points) == 0 {
		return &SyncResult{Success: true}
	}

	result := &SyncResult{}
	startTime := time.Now()

	// Split into batches
	batches := cs.splitIntoBatches(points)

	for _, batch := range batches {
		batchResult := cs.syncSingleBatch(batch)
		result.PointsSynced += batchResult.PointsSynced
		result.BytesSynced += batchResult.BytesSynced
		result.ConflictsFound += batchResult.ConflictsFound
		if batchResult.Error != nil {
			result.Error = batchResult.Error
			// Queue failed batch for offline sync
			if cs.offlineQueue != nil {
				cs.queueOffline(batch)
			}
		}
	}

	result.Success = result.Error == nil
	result.Duration = time.Since(startTime)

	// Update checkpoint
	if result.Success && len(points) > 0 {
		maxTs := int64(0)
		for _, p := range points {
			if p.Timestamp > maxTs {
				maxTs = p.Timestamp
			}
		}
		cs.updateCheckpoint(maxTs)
	}

	return result
}

func (cs *CloudSync) splitIntoBatches(points []Point) [][]Point {
	var batches [][]Point
	for i := 0; i < len(points); i += cs.config.BatchSize {
		end := i + cs.config.BatchSize
		if end > len(points) {
			end = len(points)
		}
		batches = append(batches, points[i:end])
	}
	return batches
}

func (cs *CloudSync) syncSingleBatch(points []Point) *SyncResult {
	result := &SyncResult{}

	// Prepare payload
	payload, err := cs.preparePayload(points)
	if err != nil {
		result.Error = fmt.Errorf("prepare payload: %w", err)
		return result
	}

	result.BytesSynced = int64(len(payload))

	// Execute with circuit breaker
	err = cs.cb.Execute(func() error {
		return cs.sendToCloud(payload)
	})

	if err != nil {
		if errors.Is(err, ErrCircuitOpen) {
			cs.mu.Lock()
			cs.lastSyncStatus = SyncStatusOffline
			cs.mu.Unlock()
		}
		result.Error = err
		return result
	}

	result.Success = true
	result.PointsSynced = len(points)
	return result
}

func (cs *CloudSync) preparePayload(points []Point) ([]byte, error) {
	manifest := &SyncManifest{
		Version:   1,
		Timestamp: time.Now().UnixNano(),
		Format:    cs.config.SyncFormat.String(),
		Points:    points,
		EdgeID:    cs.getEdgeID(),
	}

	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}

	// Compress if enabled
	if cs.config.EnableCompression {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(data); err != nil {
			gz.Close()
			return nil, err
		}
		gz.Close()
		return buf.Bytes(), nil
	}

	return data, nil
}

// SyncManifest contains metadata about a sync batch.
type SyncManifest struct {
	Version   int     `json:"version"`
	Timestamp int64   `json:"timestamp"`
	Format    string  `json:"format"`
	Points    []Point `json:"points"`
	EdgeID    string  `json:"edge_id"`
	Checksum  string  `json:"checksum,omitempty"`
}

func (cs *CloudSync) getEdgeID() string {
	hostname, _ := os.Hostname()
	return hostname
}

func (cs *CloudSync) sendToCloud(payload []byte) error {
	if cs.config.CloudEndpoint == "" {
		return errors.New("cloud endpoint not configured")
	}

	ctx, cancel := context.WithTimeout(cs.ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		cs.config.CloudEndpoint+"/api/v1/sync/upload", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if cs.config.EnableCompression {
		req.Header.Set("Content-Encoding", "gzip")
	}
	req.Header.Set("X-Chronicle-Edge-ID", cs.getEdgeID())

	result := cs.retryer.Do(ctx, func() error {
		resp, err := cs.client.Do(req)
		if err != nil {
			return fmt.Errorf("send request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 {
			return fmt.Errorf("server error: status %d", resp.StatusCode)
		}
		if resp.StatusCode == 429 {
			return fmt.Errorf("rate limited")
		}
		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("client error %d: %s", resp.StatusCode, string(body))
		}

		return nil
	})

	return result.LastErr
}

func (cs *CloudSync) queueOffline(points []Point) {
	if cs.offlineQueue == nil {
		return
	}

	cs.offlineQueue.mu.Lock()
	defer cs.offlineQueue.mu.Unlock()

	// Check size limit
	if cs.offlineQueue.currentSize >= cs.offlineQueue.maxSize {
		slog.Warn("offline queue full, dropping points", "count", len(points))
		return
	}

	// Create batch file
	filename := fmt.Sprintf("batch_%d.json", time.Now().UnixNano())
	filepath := filepath.Join(cs.offlineQueue.path, filename)

	data, err := json.Marshal(points)
	if err != nil {
		slog.Error("offline queue marshal error", "err", err)
		return
	}

	if err := os.WriteFile(filepath, data, 0644); err != nil {
		slog.Error("offline queue write error", "err", err)
		return
	}

	cs.offlineQueue.currentSize += int64(len(data))
}

func (cs *CloudSync) syncOfflineQueue() {
	if cs.offlineQueue == nil {
		return
	}

	cs.offlineQueue.mu.Lock()
	files, err := os.ReadDir(cs.offlineQueue.path)
	cs.offlineQueue.mu.Unlock()

	if err != nil {
		return
	}

	// Sort by name (timestamp order)
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filepath := filepath.Join(cs.offlineQueue.path, file.Name())
		data, err := os.ReadFile(filepath)
		if err != nil {
			continue
		}

		var points []Point
		if err := json.Unmarshal(data, &points); err != nil {
			os.Remove(filepath)
			continue
		}

		result := cs.syncSingleBatch(points)
		if result.Success {
			os.Remove(filepath)
			cs.offlineQueue.mu.Lock()
			cs.offlineQueue.currentSize -= int64(len(data))
			cs.offlineQueue.mu.Unlock()
		} else {
			// Stop trying if we can't connect
			break
		}
	}
}

func (cs *CloudSync) loadCheckpoint() {
	if cs.config.OfflineQueuePath == "" {
		return
	}

	checkpointPath := filepath.Join(cs.config.OfflineQueuePath, "checkpoint.json")
	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		return
	}

	var checkpoint SyncCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return
	}

	// Verify checksum
	if !cs.verifyCheckpoint(&checkpoint) {
		slog.Warn("checkpoint checksum mismatch, starting fresh")
		return
	}

	cs.checkpoint = &checkpoint
}

func (cs *CloudSync) saveCheckpoint() {
	if cs.config.OfflineQueuePath == "" || cs.checkpoint == nil {
		return
	}

	// Calculate checksum
	cs.checkpoint.Checksum = cs.calculateChecksum(cs.checkpoint)

	data, err := json.Marshal(cs.checkpoint)
	if err != nil {
		return
	}

	checkpointPath := filepath.Join(cs.config.OfflineQueuePath, "checkpoint.json")
	_ = os.WriteFile(checkpointPath, data, 0644)
}

func (cs *CloudSync) updateCheckpoint(timestamp int64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.checkpoint == nil {
		cs.checkpoint = &SyncCheckpoint{
			Version:       1,
			MetricOffsets: make(map[string]int64),
		}
	}

	cs.checkpoint.LastSyncedTimestamp = timestamp
}

func (cs *CloudSync) calculateChecksum(checkpoint *SyncCheckpoint) string {
	data := fmt.Sprintf("%d:%v", checkpoint.LastSyncedTimestamp, checkpoint.MetricOffsets)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:8])
}

func (cs *CloudSync) verifyCheckpoint(checkpoint *SyncCheckpoint) bool {
	expected := cs.calculateChecksum(checkpoint)
	return expected == checkpoint.Checksum
}

// SyncNow triggers an immediate sync.
func (cs *CloudSync) SyncNow() *SyncResult {
	cs.mu.Lock()
	if cs.syncInProgress {
		cs.mu.Unlock()
		return &SyncResult{
			Error: errors.New("sync already in progress"),
		}
	}
	cs.mu.Unlock()

	resultChan := make(chan *SyncResult, 1)

	points, err := cs.getUnsyncedPoints()
	if err != nil {
		return &SyncResult{Error: err}
	}

	req := &SyncRequest{
		Points:     points,
		Timestamp:  time.Now().UnixNano(),
		Priority:   1,
		ResultChan: resultChan,
	}

	select {
	case cs.syncChan <- req:
		select {
		case result := <-resultChan:
			return result
		case <-time.After(5 * time.Minute):
			return &SyncResult{Error: errors.New("sync timeout")}
		}
	default:
		return &SyncResult{Error: errors.New("sync queue full")}
	}
}

// GetSyncStats returns current sync statistics.
func (cs *CloudSync) GetSyncStats() SyncStats {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	stats := cs.syncStats
	if cs.offlineQueue != nil {
		cs.offlineQueue.mu.Lock()
		stats.OfflineQueueSize = cs.offlineQueue.currentSize
		cs.offlineQueue.mu.Unlock()
	}
	return stats
}

// GetSyncStatus returns the current sync status.
func (cs *CloudSync) GetSyncStatus() SyncStatus {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.lastSyncStatus
}

// IsOnline returns whether cloud is reachable.
func (cs *CloudSync) IsOnline() bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.lastSyncStatus != SyncStatusOffline && cs.lastSyncStatus != SyncStatusError
}

// PullFromCloud pulls configuration/rules from cloud to edge.
func (cs *CloudSync) PullFromCloud() (*CloudPullResult, error) {
	if cs.config.SyncMode == SyncModeEdgeToCloud {
		return nil, errors.New("pull not enabled in edge-to-cloud mode")
	}

	ctx, cancel := context.WithTimeout(cs.ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		cs.config.CloudEndpoint+"/api/v1/sync/pull", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Chronicle-Edge-ID", cs.getEdgeID())

	resp, err := cs.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pull failed with status %d", resp.StatusCode)
	}

	var result CloudPullResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// CloudPullResult contains data pulled from cloud.
type CloudPullResult struct {
	AlertRules     []AlertRule           `json:"alert_rules,omitempty"`
	RecordingRules []RecordingRuleConfig `json:"recording_rules,omitempty"`
	Schemas        []MetricSchema        `json:"schemas,omitempty"`
	Configuration  map[string]any        `json:"configuration,omitempty"`
	Timestamp      int64                 `json:"timestamp"`
}

// RecordingRuleConfig holds recording rule configuration for sync.
type RecordingRuleConfig struct {
	Name     string `json:"name"`
	Query    string `json:"query"`
	Interval string `json:"interval"`
}

// ResolveConflict resolves a sync conflict based on configured strategy.
func (cs *CloudSync) ResolveConflict(edgePoint, cloudPoint Point) Point {
	switch cs.config.ConflictResolution {
	case ConflictLastWriteWins:
		if edgePoint.Timestamp > cloudPoint.Timestamp {
			return edgePoint
		}
		return cloudPoint

	case ConflictCloudWins:
		return cloudPoint

	case ConflictEdgeWins:
		return edgePoint

	case ConflictMerge:
		// For numeric values, average them
		merged := edgePoint
		merged.Value = (edgePoint.Value + cloudPoint.Value) / 2
		// Use latest timestamp
		if cloudPoint.Timestamp > merged.Timestamp {
			merged.Timestamp = cloudPoint.Timestamp
		}
		return merged

	default:
		return edgePoint
	}
}
