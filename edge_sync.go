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
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// EdgeSyncConfig configures the edge-to-cloud synchronization.
type EdgeSyncConfig struct {
	// Enabled enables edge sync functionality.
	Enabled bool

	// Endpoint is the remote cloud endpoint URL.
	Endpoint string

	// Provider specifies the cloud provider type.
	Provider EdgeSyncProvider

	// SyncInterval is how often to attempt sync.
	SyncInterval time.Duration

	// BatchSize is the maximum number of points per sync batch.
	BatchSize int

	// MaxRetries is the maximum retry attempts for failed syncs.
	MaxRetries int

	// RetryBackoff is the initial backoff duration between retries.
	RetryBackoff time.Duration

	// CompressionEnabled enables gzip compression for sync payloads.
	CompressionEnabled bool

	// ConflictResolution specifies how to handle conflicts.
	ConflictResolution ConflictResolution

	// QueuePath is the path for the persistent sync queue.
	QueuePath string

	// MaxQueueSize is the maximum queue size in bytes.
	MaxQueueSize int64

	// BandwidthLimitBps is the bandwidth limit in bytes per second (0 = unlimited).
	BandwidthLimitBps int64

	// Auth contains authentication credentials.
	Auth *EdgeSyncAuth

	// MetricFilter optionally filters which metrics to sync.
	MetricFilter []string

	// TagFilter optionally filters which tag values to sync.
	TagFilter map[string][]string
}

// EdgeSyncAuth contains authentication credentials.
type EdgeSyncAuth struct {
	// Type specifies the auth type: "api_key", "bearer", "basic".
	Type string

	// APIKey is the API key (for api_key auth).
	APIKey string

	// BearerToken is the bearer token (for bearer auth).
	BearerToken string

	// Username is the username (for basic auth).
	Username string

	// Password is the password (for basic auth).
	Password string
}

// EdgeSyncProvider specifies the cloud provider type.
type EdgeSyncProvider string

const (
	// EdgeSyncProviderGenericHTTP is a generic HTTP endpoint.
	EdgeSyncProviderGenericHTTP EdgeSyncProvider = "http"

	// EdgeSyncProviderInfluxDB is InfluxDB Cloud.
	EdgeSyncProviderInfluxDB EdgeSyncProvider = "influxdb"

	// EdgeSyncProviderS3 is AWS S3 or compatible storage.
	EdgeSyncProviderS3 EdgeSyncProvider = "s3"

	// EdgeSyncProviderChronicle is another Chronicle instance.
	EdgeSyncProviderChronicle EdgeSyncProvider = "chronicle"
)

// ConflictResolution specifies how to resolve sync conflicts.
type ConflictResolution string

const (
	// ConflictResolutionLastWriteWins uses the most recent timestamp.
	ConflictResolutionLastWriteWins ConflictResolution = "last_write_wins"

	// ConflictResolutionFirstWriteWins keeps the first value.
	ConflictResolutionFirstWriteWins ConflictResolution = "first_write_wins"

	// ConflictResolutionRemoteWins always uses remote value on conflict.
	ConflictResolutionRemoteWins ConflictResolution = "remote_wins"

	// ConflictResolutionLocalWins always uses local value on conflict.
	ConflictResolutionLocalWins ConflictResolution = "local_wins"
)

// DefaultEdgeSyncConfig returns default edge sync configuration.
func DefaultEdgeSyncConfig() EdgeSyncConfig {
	return EdgeSyncConfig{
		Enabled:            false,
		SyncInterval:       time.Minute,
		BatchSize:          10000,
		MaxRetries:         5,
		RetryBackoff:       time.Second,
		CompressionEnabled: true,
		ConflictResolution: ConflictResolutionLastWriteWins,
		MaxQueueSize:       100 * 1024 * 1024, // 100MB
		BandwidthLimitBps:  0,                 // Unlimited
	}
}

// EdgeSyncManager manages edge-to-cloud data synchronization.
type EdgeSyncManager struct {
	db     *DB
	config EdgeSyncConfig

	// Sync state
	queue            *syncQueue
	adapter          CloudAdapter
	lastSync         time.Time
	syncMu           sync.Mutex
	running          atomic.Bool
	stopCh           chan struct{}
	wg               sync.WaitGroup
	stats            EdgeSyncStats
	statsMu          sync.RWMutex
	httpClient       *http.Client
	bandwidthLimiter *bandwidthLimiter
}

// EdgeSyncStats contains sync statistics.
type EdgeSyncStats struct {
	// PointsSynced is the total points successfully synced.
	PointsSynced int64

	// BytesSynced is the total bytes synced.
	BytesSynced int64

	// SyncAttempts is the total sync attempts.
	SyncAttempts int64

	// SyncFailures is the total failed syncs.
	SyncFailures int64

	// LastSyncTime is the last successful sync time.
	LastSyncTime time.Time

	// LastSyncError is the last sync error (if any).
	LastSyncError string

	// QueuedPoints is the number of points waiting to sync.
	QueuedPoints int64

	// QueuedBytes is the bytes waiting to sync.
	QueuedBytes int64
}

// NewEdgeSyncManager creates a new edge sync manager.
func NewEdgeSyncManager(db *DB, config EdgeSyncConfig) (*EdgeSyncManager, error) {
	if !config.Enabled {
		return nil, errors.New("edge sync is not enabled")
	}

	if config.Endpoint == "" {
		return nil, errors.New("endpoint is required")
	}

	if config.QueuePath == "" {
		config.QueuePath = filepath.Join(filepath.Dir(db.path), "sync_queue")
	}

	queue, err := newSyncQueue(config.QueuePath, config.MaxQueueSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync queue: %w", err)
	}

	adapter, err := newCloudAdapter(config)
	if err != nil {
		queue.Close()
		return nil, fmt.Errorf("failed to create cloud adapter: %w", err)
	}

	manager := &EdgeSyncManager{
		db:      db,
		config:  config,
		queue:   queue,
		adapter: adapter,
		stopCh:  make(chan struct{}),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	if config.BandwidthLimitBps > 0 {
		manager.bandwidthLimiter = newBandwidthLimiter(config.BandwidthLimitBps)
	}

	return manager, nil
}

// Start begins the sync background process.
func (m *EdgeSyncManager) Start() {
	if m.running.Swap(true) {
		return // Already running
	}

	m.wg.Add(1)
	go m.syncLoop()
}

// Stop stops the sync background process.
func (m *EdgeSyncManager) Stop() error {
	if !m.running.Swap(false) {
		return nil
	}

	close(m.stopCh)
	m.wg.Wait()

	// Flush remaining queued items
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = m.Flush(ctx) //nolint:errcheck // best-effort sync operation

	return m.queue.Close()
}

// Enqueue adds points to the sync queue.
func (m *EdgeSyncManager) Enqueue(points []Point) error {
	if len(points) == 0 {
		return nil
	}

	// Apply filters
	filtered := m.filterPoints(points)
	if len(filtered) == 0 {
		return nil
	}

	return m.queue.Enqueue(filtered)
}

// Flush forces an immediate sync of queued data.
func (m *EdgeSyncManager) Flush(ctx context.Context) error {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	return m.syncOnce(ctx)
}

// Stats returns sync statistics.
func (m *EdgeSyncManager) Stats() EdgeSyncStats {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()

	stats := m.stats
	stats.QueuedPoints, stats.QueuedBytes = m.queue.Stats()
	return stats
}

// syncLoop runs the periodic sync process.
func (m *EdgeSyncManager) syncLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			m.syncMu.Lock()
			err := m.syncOnce(ctx)
			m.syncMu.Unlock()
			cancel()

			if err != nil {
				m.recordError(err)
			}
		}
	}
}

// syncOnce performs a single sync operation.
func (m *EdgeSyncManager) syncOnce(ctx context.Context) error {
	m.statsMu.Lock()
	m.stats.SyncAttempts++
	m.statsMu.Unlock()

	// Dequeue a batch
	batch, err := m.queue.Dequeue(m.config.BatchSize)
	if err != nil {
		return fmt.Errorf("failed to dequeue: %w", err)
	}

	if len(batch) == 0 {
		return nil
	}

	// Prepare payload
	payload, err := m.preparePayload(batch)
	if err != nil {
		// Re-queue on failure
		_ = m.queue.Enqueue(batch) //nolint:errcheck // best-effort sync operation
		return fmt.Errorf("failed to prepare payload: %w", err)
	}

	// Apply bandwidth limiting
	if m.bandwidthLimiter != nil {
		if err := m.bandwidthLimiter.Wait(ctx, int64(len(payload))); err != nil {
			_ = m.queue.Enqueue(batch) //nolint:errcheck // best-effort sync operation
			return fmt.Errorf("rate limiting: %w", err)
		}
	}

	// Send with retries
	var lastErr error
	for attempt := 0; attempt < m.config.MaxRetries; attempt++ {
		if err := m.adapter.Send(ctx, payload); err != nil {
			lastErr = err
			backoff := m.config.RetryBackoff * time.Duration(1<<attempt)
			select {
			case <-ctx.Done():
				_ = m.queue.Enqueue(batch) //nolint:errcheck // best-effort sync operation
				return ctx.Err()
			case <-time.After(backoff):
				continue
			}
		}

		// Success
		m.statsMu.Lock()
		m.stats.PointsSynced += int64(len(batch))
		m.stats.BytesSynced += int64(len(payload))
		m.stats.LastSyncTime = time.Now()
		m.stats.LastSyncError = ""
		m.statsMu.Unlock()

		m.lastSync = time.Now()
		return nil
	}

	// All retries failed - re-queue
	_ = m.queue.Enqueue(batch) //nolint:errcheck // best-effort sync operation

	m.statsMu.Lock()
	m.stats.SyncFailures++
	m.stats.LastSyncError = lastErr.Error()
	m.statsMu.Unlock()

	return fmt.Errorf("sync failed after %d attempts: %w", m.config.MaxRetries, lastErr)
}

// preparePayload creates the sync payload with optional compression.
func (m *EdgeSyncManager) preparePayload(points []Point) ([]byte, error) {
	// Create sync batch
	batch := syncBatch{
		Version:   1,
		Timestamp: time.Now().UnixNano(),
		Points:    points,
		Checksum:  "",
	}

	// Calculate checksum
	checksumData, _ := json.Marshal(batch.Points)
	hash := sha256.Sum256(checksumData)
	batch.Checksum = hex.EncodeToString(hash[:])

	// Marshal
	data, err := json.Marshal(batch)
	if err != nil {
		return nil, err
	}

	// Compress if enabled
	if m.config.CompressionEnabled {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		if _, err := gw.Write(data); err != nil {
			return nil, err
		}
		if err := gw.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	return data, nil
}

// filterPoints applies metric and tag filters.
func (m *EdgeSyncManager) filterPoints(points []Point) []Point {
	if len(m.config.MetricFilter) == 0 && len(m.config.TagFilter) == 0 {
		return points
	}

	var filtered []Point
	for _, p := range points {
		// Check metric filter
		if len(m.config.MetricFilter) > 0 {
			found := false
			for _, m := range m.config.MetricFilter {
				if p.Metric == m {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Check tag filter
		if len(m.config.TagFilter) > 0 {
			match := true
			for key, allowed := range m.config.TagFilter {
				val, exists := p.Tags[key]
				if !exists {
					match = false
					break
				}
				found := false
				for _, a := range allowed {
					if val == a {
						found = true
						break
					}
				}
				if !found {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		filtered = append(filtered, p)
	}

	return filtered
}
