package chronicle

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
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

// DeltaSyncConfig configures the delta synchronization protocol.
type DeltaSyncConfig struct {
	// Enabled enables delta sync functionality.
	Enabled bool `json:"enabled"`

	// CloudEndpoint is the remote cloud endpoint URL.
	CloudEndpoint string `json:"cloud_endpoint"`

	// DeviceID uniquely identifies this edge device.
	DeviceID string `json:"device_id"`

	// SyncDirection controls sync direction.
	SyncDirection DeltaSyncDirection `json:"sync_direction"`

	// SyncInterval is how often to attempt sync.
	SyncInterval time.Duration `json:"sync_interval"`

	// BatchSize is the maximum points per batch.
	BatchSize int `json:"batch_size"`

	// MaxBatchBytes limits batch size in bytes.
	MaxBatchBytes int64 `json:"max_batch_bytes"`

	// DeltaCompressionEnabled enables delta encoding.
	DeltaCompressionEnabled bool `json:"delta_compression_enabled"`

	// VectorClockEnabled enables vector clock conflict resolution.
	VectorClockEnabled bool `json:"vector_clock_enabled"`

	// ConflictStrategy for handling conflicts.
	ConflictStrategy DeltaConflictStrategy `json:"conflict_strategy"`

	// OfflineQueuePath for persistent queue.
	OfflineQueuePath string `json:"offline_queue_path"`

	// MaxOfflineQueueBytes limits offline queue.
	MaxOfflineQueueBytes int64 `json:"max_offline_queue_bytes"`

	// BandwidthLimitBps limits bandwidth (0=unlimited).
	BandwidthLimitBps int64 `json:"bandwidth_limit_bps"`

	// RetryConfig for failed syncs.
	MaxRetries       int           `json:"max_retries"`
	RetryBackoff     time.Duration `json:"retry_backoff"`
	MaxRetryBackoff  time.Duration `json:"max_retry_backoff"`

	// FilterMetrics syncs only these metrics (empty=all).
	FilterMetrics []string `json:"filter_metrics,omitempty"`

	// FilterTags syncs only matching tags.
	FilterTags map[string][]string `json:"filter_tags,omitempty"`

	// EnableChecksum validates data integrity.
	EnableChecksum bool `json:"enable_checksum"`

	// CompressPayload enables gzip compression.
	CompressPayload bool `json:"compress_payload"`

	// Auth contains authentication config.
	Auth *DeltaSyncAuth `json:"auth,omitempty"`

	// TLS configuration
	InsecureSkipVerify bool `json:"insecure_skip_verify"`
}

// DeltaSyncDirection specifies sync direction.
type DeltaSyncDirection int

const (
	DeltaSyncDirectionEdgeToCloud DeltaSyncDirection = iota
	DeltaSyncDirectionCloudToEdge
	DeltaSyncDirectionBidirectional
)

// DeltaConflictStrategy specifies conflict resolution.
type DeltaConflictStrategy int

const (
	DeltaConflictLastWriteWins DeltaConflictStrategy = iota
	DeltaConflictCloudWins
	DeltaConflictEdgeWins
	DeltaConflictMerge
	DeltaConflictVectorClock
)

// DeltaSyncAuth contains authentication configuration.
type DeltaSyncAuth struct {
	Type        string `json:"type"` // "api_key", "bearer", "basic", "mtls"
	APIKey      string `json:"api_key,omitempty"`
	BearerToken string `json:"bearer_token,omitempty"`
	Username    string `json:"username,omitempty"`
	Password    string `json:"password,omitempty"`
}

// DefaultDeltaSyncConfig returns default configuration.
func DefaultDeltaSyncConfig() DeltaSyncConfig {
	return DeltaSyncConfig{
		Enabled:                 false,
		SyncDirection:           DeltaSyncDirectionBidirectional,
		SyncInterval:            time.Minute,
		BatchSize:               10000,
		MaxBatchBytes:           5 * 1024 * 1024,
		DeltaCompressionEnabled: true,
		VectorClockEnabled:      true,
		ConflictStrategy:        DeltaConflictLastWriteWins,
		MaxOfflineQueueBytes:    100 * 1024 * 1024,
		MaxRetries:              5,
		RetryBackoff:            time.Second,
		MaxRetryBackoff:         time.Minute,
		EnableChecksum:          true,
		CompressPayload:         true,
	}
}

// DeltaSyncManager manages delta synchronization between edge and cloud.
type DeltaSyncManager struct {
	db     *DB
	config DeltaSyncConfig

	// State
	vectorClock *VectorClock
	checkpoint  *DeltaCheckpoint
	queue       *DeltaQueue
	client      *http.Client

	// Statistics
	stats   DeltaSyncStats
	statsMu sync.RWMutex

	// Lifecycle
	running atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
}

// DeltaSyncStats contains sync statistics.
type DeltaSyncStats struct {
	PointsSynced          int64     `json:"points_synced"`
	BytesSynced           int64     `json:"bytes_synced"`
	BytesSaved            int64     `json:"bytes_saved"` // Savings from delta encoding
	SyncAttempts          int64     `json:"sync_attempts"`
	SuccessfulSyncs       int64     `json:"successful_syncs"`
	FailedSyncs           int64     `json:"failed_syncs"`
	ConflictsResolved     int64     `json:"conflicts_resolved"`
	LastSyncTime          time.Time `json:"last_sync_time"`
	LastError             string    `json:"last_error,omitempty"`
	QueuedPoints          int64     `json:"queued_points"`
	QueuedBytes           int64     `json:"queued_bytes"`
	AverageSyncLatencyMs  float64   `json:"average_sync_latency_ms"`
	CompressionRatio      float64   `json:"compression_ratio"`
}

// VectorClock implements a vector clock for conflict detection.
type VectorClock struct {
	clocks map[string]uint64
	mu     sync.RWMutex
}

// NewVectorClock creates a new vector clock.
func NewVectorClock() *VectorClock {
	return &VectorClock{
		clocks: make(map[string]uint64),
	}
}

// Increment increments the clock for a node.
func (vc *VectorClock) Increment(nodeID string) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clocks[nodeID]++
}

// Get returns the clock value for a node.
func (vc *VectorClock) Get(nodeID string) uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.clocks[nodeID]
}

// Merge merges another vector clock into this one.
func (vc *VectorClock) Merge(other *VectorClock) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	for node, clock := range other.clocks {
		if clock > vc.clocks[node] {
			vc.clocks[node] = clock
		}
	}
}

// Compare compares two vector clocks.
// Returns: -1 if vc < other, 0 if concurrent, 1 if vc > other
func (vc *VectorClock) Compare(other *VectorClock) int {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	less, greater := false, false

	// Check all nodes in both clocks
	allNodes := make(map[string]bool)
	for n := range vc.clocks {
		allNodes[n] = true
	}
	for n := range other.clocks {
		allNodes[n] = true
	}

	for node := range allNodes {
		v1, v2 := vc.clocks[node], other.clocks[node]
		if v1 < v2 {
			less = true
		} else if v1 > v2 {
			greater = true
		}
	}

	if less && !greater {
		return -1
	}
	if greater && !less {
		return 1
	}
	return 0 // Concurrent
}

// Clone creates a copy of the vector clock.
func (vc *VectorClock) Clone() *VectorClock {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	clone := NewVectorClock()
	for k, v := range vc.clocks {
		clone.clocks[k] = v
	}
	return clone
}

// Serialize serializes the vector clock.
func (vc *VectorClock) Serialize() ([]byte, error) {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return json.Marshal(vc.clocks)
}

// Deserialize deserializes the vector clock.
func (vc *VectorClock) Deserialize(data []byte) error {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	return json.Unmarshal(data, &vc.clocks)
}

// DeltaCheckpoint tracks sync progress.
type DeltaCheckpoint struct {
	DeviceID            string            `json:"device_id"`
	LastSyncedTimestamp int64             `json:"last_synced_timestamp"`
	MetricOffsets       map[string]int64  `json:"metric_offsets"`
	VectorClock         map[string]uint64 `json:"vector_clock"`
	Version             int               `json:"version"`
	Checksum            string            `json:"checksum"`
	mu                  sync.RWMutex
}

// DeltaQueue is a persistent queue for offline sync.
type DeltaQueue struct {
	path      string
	maxBytes  int64
	mu        sync.Mutex
	items     []DeltaBatch
	sizeBytes int64
}

// DeltaBatch represents a batch of changes to sync.
type DeltaBatch struct {
	ID           string            `json:"id"`
	DeviceID     string            `json:"device_id"`
	Timestamp    int64             `json:"timestamp"`
	Points       []Point           `json:"points"`
	VectorClock  map[string]uint64 `json:"vector_clock"`
	Checksum     string            `json:"checksum,omitempty"`
	DeltaEncoded bool              `json:"delta_encoded"`
}

// NewDeltaSyncManager creates a new delta sync manager.
func NewDeltaSyncManager(db *DB, config DeltaSyncConfig) (*DeltaSyncManager, error) {
	if !config.Enabled {
		return nil, errors.New("delta sync is not enabled")
	}
	if config.CloudEndpoint == "" {
		return nil, errors.New("cloud endpoint is required")
	}
	if config.DeviceID == "" {
		hostname, _ := os.Hostname()
		config.DeviceID = fmt.Sprintf("edge-%s-%d", hostname, time.Now().UnixNano()%10000)
	}

	ctx, cancel := context.WithCancel(context.Background())

	m := &DeltaSyncManager{
		db:          db,
		config:      config,
		vectorClock: NewVectorClock(),
		checkpoint:  &DeltaCheckpoint{DeviceID: config.DeviceID, MetricOffsets: make(map[string]int64)},
		client:      &http.Client{Timeout: 30 * time.Second},
		ctx:         ctx,
		cancel:      cancel,
	}

	// Initialize queue
	if config.OfflineQueuePath != "" {
		m.queue = &DeltaQueue{
			path:     config.OfflineQueuePath,
			maxBytes: config.MaxOfflineQueueBytes,
		}
		os.MkdirAll(filepath.Dir(config.OfflineQueuePath), 0755)
		m.loadQueue()
	}

	// Load checkpoint
	m.loadCheckpoint()

	return m, nil
}

// Start begins background synchronization.
func (m *DeltaSyncManager) Start() {
	if m.running.Swap(true) {
		return
	}

	m.wg.Add(1)
	go m.syncLoop()
}

// Stop stops synchronization.
func (m *DeltaSyncManager) Stop() error {
	if !m.running.Swap(false) {
		return nil
	}

	m.cancel()
	m.wg.Wait()

	// Final checkpoint save
	m.saveCheckpoint()
	m.saveQueue()

	return nil
}

func (m *DeltaSyncManager) syncLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.performSync()
		}
	}
}

func (m *DeltaSyncManager) performSync() {
	m.mu.Lock()
	defer m.mu.Unlock()

	start := time.Now()
	m.statsMu.Lock()
	m.stats.SyncAttempts++
	m.statsMu.Unlock()

	// First try to sync offline queue
	if m.queue != nil {
		m.syncOfflineQueue()
	}

	// Then sync new data
	var err error
	switch m.config.SyncDirection {
	case DeltaSyncDirectionEdgeToCloud:
		err = m.pushToCloud()
	case DeltaSyncDirectionCloudToEdge:
		err = m.pullFromCloud()
	case DeltaSyncDirectionBidirectional:
		err = m.bidirectionalSync()
	}

	m.statsMu.Lock()
	if err != nil {
		m.stats.FailedSyncs++
		m.stats.LastError = err.Error()
	} else {
		m.stats.SuccessfulSyncs++
		m.stats.LastSyncTime = time.Now()
		m.stats.LastError = ""
	}
	m.statsMu.Unlock()

	// Update average latency
	elapsed := time.Since(start)
	m.updateAverageLatency(elapsed)
}

func (m *DeltaSyncManager) pushToCloud() error {
	// Get unsynced points
	points, err := m.getUnsyncedPoints()
	if err != nil {
		return fmt.Errorf("failed to get unsynced points: %w", err)
	}

	if len(points) == 0 {
		return nil
	}

	// Split into batches
	batches := m.splitIntoBatches(points)

	for _, batch := range batches {
		if err := m.sendBatch(batch); err != nil {
			// Queue for offline sync
			if m.queue != nil {
				m.queueBatch(batch)
			}
			return fmt.Errorf("failed to send batch: %w", err)
		}
	}

	return nil
}

func (m *DeltaSyncManager) pullFromCloud() error {
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		m.config.CloudEndpoint+"/api/v1/delta/pull", nil)
	if err != nil {
		return err
	}

	m.addAuthHeaders(req)
	req.Header.Set("X-Device-ID", m.config.DeviceID)

	// Send checkpoint info
	checkpointData, _ := json.Marshal(m.checkpoint)
	req.Header.Set("X-Checkpoint", string(checkpointData))

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("pull failed: %d - %s", resp.StatusCode, string(body))
	}

	var pullResp DeltaPullResponse
	if err := json.NewDecoder(resp.Body).Decode(&pullResp); err != nil {
		return err
	}

	// Apply changes with conflict resolution
	for _, batch := range pullResp.Batches {
		if err := m.applyBatch(batch); err != nil {
			return fmt.Errorf("failed to apply batch: %w", err)
		}
	}

	return nil
}

func (m *DeltaSyncManager) bidirectionalSync() error {
	// Push first, then pull
	if err := m.pushToCloud(); err != nil {
		return err
	}
	return m.pullFromCloud()
}

func (m *DeltaSyncManager) getUnsyncedPoints() ([]Point, error) {
	if m.db == nil {
		return nil, nil
	}

	m.checkpoint.mu.RLock()
	lastTs := m.checkpoint.LastSyncedTimestamp
	m.checkpoint.mu.RUnlock()

	var allPoints []Point
	metrics := m.db.Metrics()

	for _, metric := range metrics {
		// Apply metric filter
		if len(m.config.FilterMetrics) > 0 {
			found := false
			for _, f := range m.config.FilterMetrics {
				if f == metric {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		result, err := m.db.Execute(&Query{
			Metric: metric,
			Start:  lastTs,
			End:    time.Now().UnixNano(),
		})
		if err != nil {
			continue
		}

		// Apply tag filter
		for _, pt := range result.Points {
			if m.matchesTagFilter(pt) {
				allPoints = append(allPoints, pt)
			}
		}

		// Limit batch size
		if len(allPoints) >= m.config.BatchSize*10 {
			break
		}
	}

	return allPoints, nil
}

func (m *DeltaSyncManager) matchesTagFilter(pt Point) bool {
	if len(m.config.FilterTags) == 0 {
		return true
	}

	for key, allowedValues := range m.config.FilterTags {
		val, exists := pt.Tags[key]
		if !exists {
			return false
		}
		found := false
		for _, allowed := range allowedValues {
			if val == allowed {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (m *DeltaSyncManager) splitIntoBatches(points []Point) []DeltaBatch {
	var batches []DeltaBatch

	for i := 0; i < len(points); i += m.config.BatchSize {
		end := i + m.config.BatchSize
		if end > len(points) {
			end = len(points)
		}

		batchPoints := points[i:end]

		// Increment vector clock
		m.vectorClock.Increment(m.config.DeviceID)

		batch := DeltaBatch{
			ID:          fmt.Sprintf("%s-%d", m.config.DeviceID, time.Now().UnixNano()),
			DeviceID:    m.config.DeviceID,
			Timestamp:   time.Now().UnixNano(),
			Points:      batchPoints,
			VectorClock: m.vectorClock.clocks,
		}

		// Apply delta encoding if enabled
		if m.config.DeltaCompressionEnabled {
			batch.Points = m.deltaEncode(batchPoints)
			batch.DeltaEncoded = true
		}

		// Calculate checksum
		if m.config.EnableChecksum {
			batch.Checksum = m.calculateBatchChecksum(batch)
		}

		batches = append(batches, batch)
	}

	return batches
}

func (m *DeltaSyncManager) deltaEncode(points []Point) []Point {
	if len(points) == 0 {
		return points
	}

	// Sort by timestamp
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// Delta encode - store differences from previous point
	encoded := make([]Point, len(points))
	encoded[0] = points[0] // First point is stored as-is

	for i := 1; i < len(points); i++ {
		encoded[i] = Point{
			Metric:    points[i].Metric,
			Value:     points[i].Value - points[i-1].Value, // Delta value
			Timestamp: points[i].Timestamp - points[i-1].Timestamp, // Delta timestamp
			Tags:      points[i].Tags,
		}
	}

	return encoded
}

func (m *DeltaSyncManager) deltaDecode(points []Point) []Point {
	if len(points) == 0 {
		return points
	}

	decoded := make([]Point, len(points))
	decoded[0] = points[0]

	for i := 1; i < len(points); i++ {
		decoded[i] = Point{
			Metric:    points[i].Metric,
			Value:     decoded[i-1].Value + points[i].Value,
			Timestamp: decoded[i-1].Timestamp + points[i].Timestamp,
			Tags:      points[i].Tags,
		}
	}

	return decoded
}

func (m *DeltaSyncManager) calculateBatchChecksum(batch DeltaBatch) string {
	data, _ := json.Marshal(batch.Points)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func (m *DeltaSyncManager) sendBatch(batch DeltaBatch) error {
	// Prepare payload
	payload, err := json.Marshal(batch)
	if err != nil {
		return err
	}

	originalSize := len(payload)

	// Compress if enabled
	if m.config.CompressPayload {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		gw.Write(payload)
		gw.Close()
		payload = buf.Bytes()

		// Update compression stats
		m.statsMu.Lock()
		m.stats.BytesSaved += int64(originalSize - len(payload))
		if originalSize > 0 {
			m.stats.CompressionRatio = float64(len(payload)) / float64(originalSize)
		}
		m.statsMu.Unlock()
	}

	// Send with retry
	var lastErr error
	backoff := m.config.RetryBackoff

	for attempt := 0; attempt < m.config.MaxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			m.config.CloudEndpoint+"/api/v1/delta/push", bytes.NewReader(payload))
		cancel()

		if err != nil {
			lastErr = err
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		if m.config.CompressPayload {
			req.Header.Set("Content-Encoding", "gzip")
		}
		m.addAuthHeaders(req)
		req.Header.Set("X-Device-ID", m.config.DeviceID)
		req.Header.Set("X-Batch-ID", batch.ID)

		resp, err := m.client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(backoff)
			backoff = backoff * 2
		if backoff > m.config.MaxRetryBackoff {
			backoff = m.config.MaxRetryBackoff
		}
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
			// Success - update checkpoint
			m.updateCheckpoint(batch)

			m.statsMu.Lock()
			m.stats.PointsSynced += int64(len(batch.Points))
			m.stats.BytesSynced += int64(len(payload))
			m.statsMu.Unlock()

			return nil
		}

		if resp.StatusCode == http.StatusConflict {
			// Conflict - need resolution
			return m.handleConflict(batch, resp)
		}

		lastErr = fmt.Errorf("server returned %d", resp.StatusCode)
		time.Sleep(backoff)
		backoff = backoff * 2
		if backoff > m.config.MaxRetryBackoff {
			backoff = m.config.MaxRetryBackoff
		}
	}

	return fmt.Errorf("failed after %d attempts: %w", m.config.MaxRetries, lastErr)
}

func (m *DeltaSyncManager) handleConflict(batch DeltaBatch, resp *http.Response) error {
	m.statsMu.Lock()
	m.stats.ConflictsResolved++
	m.statsMu.Unlock()

	switch m.config.ConflictStrategy {
	case DeltaConflictCloudWins:
		// Discard local changes, pull cloud version
		return m.pullFromCloud()

	case DeltaConflictEdgeWins:
		// Force push local changes
		return m.forcePush(batch)

	case DeltaConflictMerge:
		// Merge both versions
		return m.mergeChanges(batch)

	case DeltaConflictVectorClock:
		// Use vector clock comparison
		return m.resolveWithVectorClock(batch)

	default: // LastWriteWins
		// Compare timestamps and keep latest
		return m.lastWriteWinsResolve(batch)
	}
}

func (m *DeltaSyncManager) forcePush(batch DeltaBatch) error {
	// Add force flag
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()

	payload, _ := json.Marshal(batch)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		m.config.CloudEndpoint+"/api/v1/delta/push?force=true", bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	m.addAuthHeaders(req)

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("force push failed: %d", resp.StatusCode)
	}

	return nil
}

func (m *DeltaSyncManager) mergeChanges(batch DeltaBatch) error {
	// Pull cloud version
	if err := m.pullFromCloud(); err != nil {
		return err
	}

	// Re-push local changes (they'll be merged at cloud)
	return m.sendBatch(batch)
}

func (m *DeltaSyncManager) resolveWithVectorClock(batch DeltaBatch) error {
	// The cloud should have sent its vector clock in the response
	// Compare and decide which version is newer
	cloudClock := NewVectorClock()
	cloudClock.clocks = batch.VectorClock

	cmp := m.vectorClock.Compare(cloudClock)

	if cmp > 0 {
		// Local is newer, force push
		return m.forcePush(batch)
	} else if cmp < 0 {
		// Cloud is newer, pull
		return m.pullFromCloud()
	}

	// Concurrent - merge
	return m.mergeChanges(batch)
}

func (m *DeltaSyncManager) lastWriteWinsResolve(batch DeltaBatch) error {
	// Simply retry with latest timestamp
	batch.Timestamp = time.Now().UnixNano()
	return m.sendBatch(batch)
}

func (m *DeltaSyncManager) applyBatch(batch DeltaBatch) error {
	points := batch.Points

	// Decode if delta encoded
	if batch.DeltaEncoded {
		points = m.deltaDecode(points)
	}

	// Verify checksum
	if m.config.EnableChecksum && batch.Checksum != "" {
		expected := m.calculateBatchChecksum(DeltaBatch{Points: batch.Points})
		if expected != batch.Checksum {
			return errors.New("checksum mismatch")
		}
	}

	// Write to database
	if err := m.db.WriteBatch(points); err != nil {
		return err
	}

	// Merge vector clock
	cloudClock := &VectorClock{clocks: batch.VectorClock}
	m.vectorClock.Merge(cloudClock)

	return nil
}

func (m *DeltaSyncManager) queueBatch(batch DeltaBatch) {
	if m.queue == nil {
		return
	}

	m.queue.mu.Lock()
	defer m.queue.mu.Unlock()

	batchSize := int64(len(batch.Points) * 100) // Rough estimate

	// Check capacity
	if m.queue.sizeBytes+batchSize > m.queue.maxBytes {
		// Remove oldest batches
		for m.queue.sizeBytes+batchSize > m.queue.maxBytes && len(m.queue.items) > 0 {
			removed := m.queue.items[0]
			m.queue.items = m.queue.items[1:]
			m.queue.sizeBytes -= int64(len(removed.Points) * 100)
		}
	}

	m.queue.items = append(m.queue.items, batch)
	m.queue.sizeBytes += batchSize
}

func (m *DeltaSyncManager) syncOfflineQueue() {
	if m.queue == nil {
		return
	}

	m.queue.mu.Lock()
	items := m.queue.items
	m.queue.items = nil
	m.queue.sizeBytes = 0
	m.queue.mu.Unlock()

	for _, batch := range items {
		if err := m.sendBatch(batch); err != nil {
			// Re-queue failed batch
			m.queueBatch(batch)
			return
		}
	}
}

func (m *DeltaSyncManager) updateCheckpoint(batch DeltaBatch) {
	m.checkpoint.mu.Lock()
	defer m.checkpoint.mu.Unlock()

	// Update last synced timestamp
	maxTs := m.checkpoint.LastSyncedTimestamp
	for _, pt := range batch.Points {
		if pt.Timestamp > maxTs {
			maxTs = pt.Timestamp
		}
	}
	m.checkpoint.LastSyncedTimestamp = maxTs

	// Update vector clock
	m.checkpoint.VectorClock = m.vectorClock.clocks

	// Save checkpoint
	go m.saveCheckpoint()
}

func (m *DeltaSyncManager) loadCheckpoint() {
	if m.config.OfflineQueuePath == "" {
		return
	}

	path := filepath.Join(filepath.Dir(m.config.OfflineQueuePath), "delta_checkpoint.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}

	var cp DeltaCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return
	}

	m.checkpoint = &cp
	if cp.VectorClock != nil {
		m.vectorClock.clocks = cp.VectorClock
	}
}

func (m *DeltaSyncManager) saveCheckpoint() {
	if m.config.OfflineQueuePath == "" {
		return
	}

	m.checkpoint.mu.RLock()
	data, err := json.Marshal(m.checkpoint)
	m.checkpoint.mu.RUnlock()

	if err != nil {
		return
	}

	path := filepath.Join(filepath.Dir(m.config.OfflineQueuePath), "delta_checkpoint.json")
	os.WriteFile(path, data, 0644)
}

func (m *DeltaSyncManager) loadQueue() {
	if m.queue == nil {
		return
	}

	data, err := os.ReadFile(m.queue.path)
	if err != nil {
		return
	}

	var items []DeltaBatch
	if err := json.Unmarshal(data, &items); err != nil {
		return
	}

	m.queue.items = items
	for _, item := range items {
		m.queue.sizeBytes += int64(len(item.Points) * 100)
	}
}

func (m *DeltaSyncManager) saveQueue() {
	if m.queue == nil {
		return
	}

	m.queue.mu.Lock()
	data, err := json.Marshal(m.queue.items)
	m.queue.mu.Unlock()

	if err != nil {
		return
	}

	os.WriteFile(m.queue.path, data, 0644)
}

func (m *DeltaSyncManager) addAuthHeaders(req *http.Request) {
	if m.config.Auth == nil {
		return
	}

	switch m.config.Auth.Type {
	case "api_key":
		req.Header.Set("X-API-Key", m.config.Auth.APIKey)
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+m.config.Auth.BearerToken)
	case "basic":
		req.SetBasicAuth(m.config.Auth.Username, m.config.Auth.Password)
	}
}

func (m *DeltaSyncManager) updateAverageLatency(elapsed time.Duration) {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()

	attempts := m.stats.SyncAttempts
	if attempts == 0 {
		attempts = 1
	}

	// Exponential moving average
	alpha := 0.2
	m.stats.AverageSyncLatencyMs = m.stats.AverageSyncLatencyMs*(1-alpha) +
		float64(elapsed.Milliseconds())*alpha
}

// SyncNow triggers an immediate sync.
func (m *DeltaSyncManager) SyncNow() error {
	m.performSync()
	return nil
}

// GetStats returns sync statistics.
func (m *DeltaSyncManager) GetStats() DeltaSyncStats {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()

	stats := m.stats
	if m.queue != nil {
		m.queue.mu.Lock()
		stats.QueuedPoints = int64(len(m.queue.items))
		stats.QueuedBytes = m.queue.sizeBytes
		m.queue.mu.Unlock()
	}
	return stats
}

// GetVectorClock returns the current vector clock state.
func (m *DeltaSyncManager) GetVectorClock() map[string]uint64 {
	return m.vectorClock.clocks
}

// DeltaPullResponse is the response from cloud pull.
type DeltaPullResponse struct {
	Batches     []DeltaBatch `json:"batches"`
	VectorClock map[string]uint64 `json:"vector_clock"`
	HasMore     bool         `json:"has_more"`
}

// --- Delta Encoding Utilities ---

// DeltaEncoder provides efficient delta encoding for time-series.
type DeltaEncoder struct {
	lastTimestamp int64
	lastValue     float64
	initialized   bool
}

func NewDeltaEncoder() *DeltaEncoder {
	return &DeltaEncoder{}
}

func (e *DeltaEncoder) Encode(timestamp int64, value float64) (int64, float64) {
	if !e.initialized {
		e.initialized = true
		e.lastTimestamp = timestamp
		e.lastValue = value
		return timestamp, value
	}

	deltaTs := timestamp - e.lastTimestamp
	deltaVal := value - e.lastValue

	e.lastTimestamp = timestamp
	e.lastValue = value

	return deltaTs, deltaVal
}

func (e *DeltaEncoder) Reset() {
	e.initialized = false
	e.lastTimestamp = 0
	e.lastValue = 0
}

// DeltaDecoder decodes delta-encoded values.
type DeltaDecoder struct {
	lastTimestamp int64
	lastValue     float64
	initialized   bool
}

func NewDeltaDecoder() *DeltaDecoder {
	return &DeltaDecoder{}
}

func (d *DeltaDecoder) Decode(deltaTs int64, deltaVal float64) (int64, float64) {
	if !d.initialized {
		d.initialized = true
		d.lastTimestamp = deltaTs
		d.lastValue = deltaVal
		return deltaTs, deltaVal
	}

	timestamp := d.lastTimestamp + deltaTs
	value := d.lastValue + deltaVal

	d.lastTimestamp = timestamp
	d.lastValue = value

	return timestamp, value
}

// VarintEncoder provides varint encoding for compressing integers.
type VarintEncoder struct {
	buf []byte
}

func NewVarintEncoder() *VarintEncoder {
	return &VarintEncoder{buf: make([]byte, binary.MaxVarintLen64)}
}

func (e *VarintEncoder) EncodeInt64(v int64) []byte {
	n := binary.PutVarint(e.buf, v)
	result := make([]byte, n)
	copy(result, e.buf[:n])
	return result
}

func (e *VarintEncoder) DecodeInt64(data []byte) (int64, int) {
	return binary.Varint(data)
}
