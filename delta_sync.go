package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
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
	MaxRetries      int           `json:"max_retries"`
	RetryBackoff    time.Duration `json:"retry_backoff"`
	MaxRetryBackoff time.Duration `json:"max_retry_backoff"`

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
	PointsSynced         int64     `json:"points_synced"`
	BytesSynced          int64     `json:"bytes_synced"`
	BytesSaved           int64     `json:"bytes_saved"` // Savings from delta encoding
	SyncAttempts         int64     `json:"sync_attempts"`
	SuccessfulSyncs      int64     `json:"successful_syncs"`
	FailedSyncs          int64     `json:"failed_syncs"`
	ConflictsResolved    int64     `json:"conflicts_resolved"`
	LastSyncTime         time.Time `json:"last_sync_time"`
	LastError            string    `json:"last_error,omitempty"`
	QueuedPoints         int64     `json:"queued_points"`
	QueuedBytes          int64     `json:"queued_bytes"`
	AverageSyncLatencyMs float64   `json:"average_sync_latency_ms"`
	CompressionRatio     float64   `json:"compression_ratio"`
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

	if config.InsecureSkipVerify {
		if os.Getenv("CHRONICLE_ALLOW_INSECURE") != "true" {
			cancel()
			return nil, errors.New("InsecureSkipVerify requires CHRONICLE_ALLOW_INSECURE=true environment variable; do not use in production")
		}
		log.Println("[WARN] chronicle: DeltaSync InsecureSkipVerify is enabled. TLS certificate validation is disabled, making connections vulnerable to MITM attacks. Do not use in production.")
	}

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
	// syncLoop runs the background delta synchronization loop.
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
