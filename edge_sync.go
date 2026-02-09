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
	"net/http"
	"os"
	"path/filepath"
	"sort"
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
	queue       *syncQueue
	adapter     CloudAdapter
	lastSync    time.Time
	syncMu      sync.Mutex
	running     atomic.Bool
	stopCh      chan struct{}
	stats       EdgeSyncStats
	statsMu     sync.RWMutex
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
		db:     db,
		config: config,
		queue:  queue,
		adapter: adapter,
		stopCh: make(chan struct{}),
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

	go m.syncLoop()
}

// Stop stops the sync background process.
func (m *EdgeSyncManager) Stop() error {
	if !m.running.Swap(false) {
		return nil
	}

	close(m.stopCh)

	// Flush remaining queued items
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = m.Flush(ctx)

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
		_ = m.queue.Enqueue(batch)
		return fmt.Errorf("failed to prepare payload: %w", err)
	}

	// Apply bandwidth limiting
	if m.bandwidthLimiter != nil {
		if err := m.bandwidthLimiter.Wait(ctx, int64(len(payload))); err != nil {
			_ = m.queue.Enqueue(batch)
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
				_ = m.queue.Enqueue(batch)
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
	_ = m.queue.Enqueue(batch)

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

func (m *EdgeSyncManager) recordError(err error) {
	m.statsMu.Lock()
	m.stats.SyncFailures++
	m.stats.LastSyncError = err.Error()
	m.statsMu.Unlock()
}

// syncBatch represents a batch of points for syncing.
type syncBatch struct {
	Version   int     `json:"version"`
	Timestamp int64   `json:"timestamp"`
	Points    []Point `json:"points"`
	Checksum  string  `json:"checksum"`
}

// syncQueue is a persistent queue for points awaiting sync.
type syncQueue struct {
	path     string
	maxSize  int64
	mu       sync.Mutex
	items    []Point
	sizeBytes int64
}

func newSyncQueue(path string, maxSize int64) (*syncQueue, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	q := &syncQueue{
		path:    path,
		maxSize: maxSize,
	}

	// Load existing queue if present
	if err := q.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return q, nil
}

func (q *syncQueue) Enqueue(points []Point) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Estimate size
	addSize := int64(len(points) * 100) // Rough estimate

	// Check capacity
	if q.sizeBytes+addSize > q.maxSize {
		// Drop oldest points to make room
		dropCount := (q.sizeBytes + addSize - q.maxSize) / 100
		if dropCount > int64(len(q.items)) {
			dropCount = int64(len(q.items))
		}
		q.items = q.items[dropCount:]
		q.sizeBytes -= dropCount * 100
	}

	q.items = append(q.items, points...)
	q.sizeBytes += addSize

	return q.persist()
}

func (q *syncQueue) Dequeue(count int) ([]Point, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return nil, nil
	}

	if count > len(q.items) {
		count = len(q.items)
	}

	batch := make([]Point, count)
	copy(batch, q.items[:count])
	q.items = q.items[count:]
	q.sizeBytes -= int64(count * 100)

	if err := q.persist(); err != nil {
		return nil, err
	}

	return batch, nil
}

func (q *syncQueue) Stats() (int64, int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int64(len(q.items)), q.sizeBytes
}

func (q *syncQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.persist()
}

func (q *syncQueue) persist() error {
	data, err := json.Marshal(q.items)
	if err != nil {
		return err
	}
	return os.WriteFile(q.path, data, 0o644)
}

func (q *syncQueue) load() error {
	data, err := os.ReadFile(q.path)
	if err != nil {
		return err
	}

	var items []Point
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}

	q.items = items
	q.sizeBytes = int64(len(items) * 100)
	return nil
}

// CloudAdapter is the interface for cloud provider adapters.
type CloudAdapter interface {
	Send(ctx context.Context, payload []byte) error
}

// newCloudAdapter creates the appropriate cloud adapter.
func newCloudAdapter(config EdgeSyncConfig) (CloudAdapter, error) {
	switch config.Provider {
	case EdgeSyncProviderGenericHTTP, EdgeSyncProviderChronicle:
		return newHTTPAdapter(config)
	case EdgeSyncProviderInfluxDB:
		return newInfluxDBAdapter(config)
	case EdgeSyncProviderS3:
		return newS3Adapter(config)
	default:
		return newHTTPAdapter(config)
	}
}

// httpAdapter sends data to a generic HTTP endpoint.
type httpAdapter struct {
	endpoint   string
	auth       *EdgeSyncAuth
	compressed bool
	client     *http.Client
}

func newHTTPAdapter(config EdgeSyncConfig) (*httpAdapter, error) {
	return &httpAdapter{
		endpoint:   config.Endpoint,
		auth:       config.Auth,
		compressed: config.CompressionEnabled,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (a *httpAdapter) Send(ctx context.Context, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", a.endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if a.compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}

	// Add auth
	if a.auth != nil {
		switch a.auth.Type {
		case "api_key":
			req.Header.Set("X-API-Key", a.auth.APIKey)
		case "bearer":
			req.Header.Set("Authorization", "Bearer "+a.auth.BearerToken)
		case "basic":
			req.SetBasicAuth(a.auth.Username, a.auth.Password)
		}
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// influxDBAdapter sends data to InfluxDB Cloud.
type influxDBAdapter struct {
	endpoint string
	org      string
	bucket   string
	token    string
	client   *http.Client
}

func newInfluxDBAdapter(config EdgeSyncConfig) (*influxDBAdapter, error) {
	token := ""
	if config.Auth != nil {
		token = config.Auth.BearerToken
		if token == "" {
			token = config.Auth.APIKey
		}
	}

	return &influxDBAdapter{
		endpoint: config.Endpoint,
		token:    token,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (a *influxDBAdapter) Send(ctx context.Context, payload []byte) error {
	// Decompress if needed
	var batch syncBatch
	data := payload
	if len(payload) > 2 && payload[0] == 0x1f && payload[1] == 0x8b {
		// Gzip compressed
		gr, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return err
		}
		data, err = io.ReadAll(gr)
		gr.Close()
		if err != nil {
			return err
		}
	}

	if err := json.Unmarshal(data, &batch); err != nil {
		return err
	}

	// Convert to InfluxDB line protocol
	var lines []byte
	for _, p := range batch.Points {
		line := pointToLineProtocol(p)
		lines = append(lines, []byte(line+"\n")...)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", a.endpoint+"/api/v2/write", bytes.NewReader(lines))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Authorization", "Token "+a.token)

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("InfluxDB error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// pointToLineProtocol converts a Point to InfluxDB line protocol.
func pointToLineProtocol(p Point) string {
	// measurement,tags field=value timestamp
	var b bytes.Buffer
	b.WriteString(p.Metric)

	// Sort tags for consistent output
	var keys []string
	for k := range p.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		b.WriteByte(',')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(p.Tags[k])
	}

	b.WriteString(" value=")
	b.WriteString(fmt.Sprintf("%f", p.Value))

	b.WriteByte(' ')
	b.WriteString(fmt.Sprintf("%d", p.Timestamp))

	return b.String()
}

// s3Adapter uploads data to S3.
type s3Adapter struct {
	bucket   string
	prefix   string
	endpoint string
	auth     *EdgeSyncAuth
	client   *http.Client
}

func newS3Adapter(config EdgeSyncConfig) (*s3Adapter, error) {
	return &s3Adapter{
		endpoint: config.Endpoint,
		auth:     config.Auth,
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
	}, nil
}

func (a *s3Adapter) Send(ctx context.Context, payload []byte) error {
	// Generate unique object key
	key := fmt.Sprintf("chronicle/%d/%s.json.gz",
		time.Now().Unix(),
		hex.EncodeToString(sha256.New().Sum(payload)[:8]))

	req, err := http.NewRequestWithContext(ctx, "PUT", a.endpoint+"/"+key, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if len(payload) > 2 && payload[0] == 0x1f && payload[1] == 0x8b {
		req.Header.Set("Content-Encoding", "gzip")
	}

	// Add auth (simplified - real implementation would use AWS SDK)
	if a.auth != nil && a.auth.APIKey != "" {
		req.Header.Set("X-API-Key", a.auth.APIKey)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("S3 error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// bandwidthLimiter provides bandwidth limiting for sync operations.
type bandwidthLimiter struct {
	bytesPerSec int64
	tokens      int64
	lastRefill  time.Time
	mu          sync.Mutex
}

func newBandwidthLimiter(bytesPerSec int64) *bandwidthLimiter {
	return &bandwidthLimiter{
		bytesPerSec: bytesPerSec,
		tokens:      bytesPerSec,
		lastRefill:  time.Now(),
	}
}

func (r *bandwidthLimiter) Wait(ctx context.Context, bytes int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Refill tokens
	now := time.Now()
	elapsed := now.Sub(r.lastRefill)
	refill := int64(elapsed.Seconds() * float64(r.bytesPerSec))
	r.tokens += refill
	if r.tokens > r.bytesPerSec {
		r.tokens = r.bytesPerSec
	}
	r.lastRefill = now

	// Wait if needed
	if bytes > r.tokens {
		waitTime := time.Duration(float64(bytes-r.tokens) / float64(r.bytesPerSec) * float64(time.Second))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			r.tokens = 0
		}
	} else {
		r.tokens -= bytes
	}

	return nil
}

// EdgeSyncStatus represents the current sync status.
type EdgeSyncStatus struct {
	Running      bool      `json:"running"`
	LastSync     time.Time `json:"last_sync"`
	QueuedPoints int64     `json:"queued_points"`
	QueuedBytes  int64     `json:"queued_bytes"`
	TotalSynced  int64     `json:"total_synced"`
	Errors       int64     `json:"errors"`
	LastError    string    `json:"last_error,omitempty"`
}

// Status returns the current sync status.
func (m *EdgeSyncManager) Status() EdgeSyncStatus {
	stats := m.Stats()
	return EdgeSyncStatus{
		Running:      m.running.Load(),
		LastSync:     stats.LastSyncTime,
		QueuedPoints: stats.QueuedPoints,
		QueuedBytes:  stats.QueuedBytes,
		TotalSynced:  stats.PointsSynced,
		Errors:       stats.SyncFailures,
		LastError:    stats.LastSyncError,
	}
}
