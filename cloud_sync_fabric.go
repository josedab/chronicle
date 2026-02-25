package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Cloud Sync Fabric — multi-cloud sync with Merkle trees
// ---------------------------------------------------------------------------

// CloudSyncFabricConfig configures the multi-cloud sync fabric.
type CloudSyncFabricConfig struct {
	MaxConnectors       int           `json:"max_connectors"`
	SyncInterval        time.Duration `json:"sync_interval"`
	BandwidthLimitBytes int64         `json:"bandwidth_limit_bytes"`
	CompressTransfers   bool          `json:"compress_transfers"`
	MerkleDepth         int           `json:"merkle_depth"`
	PriorityLevels      int           `json:"priority_levels"`
	MaxRetries          int           `json:"max_retries"`
	RetryBackoff        time.Duration `json:"retry_backoff"`
}

// DefaultCloudSyncFabricConfig returns sensible defaults.
func DefaultCloudSyncFabricConfig() CloudSyncFabricConfig {
	return CloudSyncFabricConfig{
		MaxConnectors:       16,
		SyncInterval:        30 * time.Second,
		BandwidthLimitBytes: 100 * 1024 * 1024, // 100MB/s
		CompressTransfers:   true,
		MerkleDepth:         16,
		PriorityLevels:      3,
		MaxRetries:          5,
		RetryBackoff:        time.Second,
	}
}

// CloudConnectorType identifies the cloud provider.
type CloudConnectorType string

const (
	ConnectorAWS     CloudConnectorType = "aws"
	ConnectorGCP     CloudConnectorType = "gcp"
	ConnectorAzure   CloudConnectorType = "azure"
	ConnectorHTTP    CloudConnectorType = "http"
	ConnectorGeneric CloudConnectorType = "generic"
)

// CloudConnectorConfig configures a cloud connector.
type CloudConnectorConfig struct {
	Name     string             `json:"name"`
	Type     CloudConnectorType `json:"type"`
	Endpoint string             `json:"endpoint"`
	Region   string             `json:"region,omitempty"`
	Bucket   string             `json:"bucket,omitempty"`
	Prefix   string             `json:"prefix,omitempty"`
	Priority int                `json:"priority"`
	Enabled  bool               `json:"enabled"`
	Config   map[string]string  `json:"config,omitempty"`
}

// CloudConnector defines the interface for cloud storage connectors.
type CloudConnector interface {
	Name() string
	Type() CloudConnectorType
	Connect(ctx context.Context) error
	Disconnect() error
	Push(ctx context.Context, batch *SyncBatch) error
	Pull(ctx context.Context, since time.Time) (*SyncBatch, error)
	GetManifest(ctx context.Context) (*SyncManifestV2, error)
	IsConnected() bool
}

// SyncBatch represents a batch of points for sync.
type SyncBatch struct {
	ID        string    `json:"id"`
	Points    []Point   `json:"points"`
	Checksum  string    `json:"checksum"`
	Priority  int       `json:"priority"`
	CreatedAt time.Time `json:"created_at"`
	Size      int64     `json:"size"`
}

// SyncManifestV2 describes what data a remote has.
type SyncManifestV2 struct {
	NodeID      string    `json:"node_id"`
	LastSync    time.Time `json:"last_sync"`
	PointCount  int64     `json:"point_count"`
	MetricCount int       `json:"metric_count"`
	MerkleRoot  string    `json:"merkle_root"`
}

// CloudSyncFabric orchestrates multi-cloud synchronization.
type CloudSyncFabric struct {
	db         *DB
	config     CloudSyncFabricConfig
	connectors map[string]CloudConnector
	mu         sync.RWMutex
	running    atomic.Bool
	cancel     context.CancelFunc

	// Merkle tree for diff detection
	merkle *FabricMerkleTree

	// Priority queue for batches
	priorityQueue []*SyncBatch

	// Stats
	totalPushed      atomic.Uint64
	totalPulled      atomic.Uint64
	totalConflicts   atomic.Uint64
	totalErrors      atomic.Uint64
	bytesTransferred atomic.Int64
}

// NewCloudSyncFabric creates a new sync fabric.
func NewCloudSyncFabric(db *DB, config CloudSyncFabricConfig) *CloudSyncFabric {
	return &CloudSyncFabric{
		db:            db,
		config:        config,
		connectors:    make(map[string]CloudConnector),
		merkle:        NewFabricMerkleTree(config.MerkleDepth),
		priorityQueue: make([]*SyncBatch, 0),
	}
}

// RegisterConnector adds a cloud connector.
func (csf *CloudSyncFabric) RegisterConnector(connector CloudConnector) error {
	csf.mu.Lock()
	defer csf.mu.Unlock()
	name := connector.Name()
	if _, exists := csf.connectors[name]; exists {
		return fmt.Errorf("connector %q already registered", name)
	}
	if len(csf.connectors) >= csf.config.MaxConnectors {
		return fmt.Errorf("max connectors (%d) reached", csf.config.MaxConnectors)
	}
	csf.connectors[name] = connector
	return nil
}

// UnregisterConnector removes a connector.
func (csf *CloudSyncFabric) UnregisterConnector(name string) error {
	csf.mu.Lock()
	defer csf.mu.Unlock()
	conn, ok := csf.connectors[name]
	if !ok {
		return fmt.Errorf("connector %q not found", name)
	}
	if conn.IsConnected() {
		conn.Disconnect()
	}
	delete(csf.connectors, name)
	return nil
}

// ListConnectors returns all registered connectors.
func (csf *CloudSyncFabric) ListConnectors() []CloudConnectorInfo {
	csf.mu.RLock()
	defer csf.mu.RUnlock()
	result := make([]CloudConnectorInfo, 0, len(csf.connectors))
	for _, c := range csf.connectors {
		result = append(result, CloudConnectorInfo{
			Name:      c.Name(),
			Type:      c.Type(),
			Connected: c.IsConnected(),
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// CloudConnectorInfo describes a connector.
type CloudConnectorInfo struct {
	Name      string             `json:"name"`
	Type      CloudConnectorType `json:"type"`
	Connected bool               `json:"connected"`
}

// Start begins sync operations.
func (csf *CloudSyncFabric) Start() error {
	if csf.running.Swap(true) {
		return fmt.Errorf("sync fabric already running")
	}
	ctx, cancel := context.WithCancel(context.Background())
	csf.cancel = cancel
	go csf.syncLoop(ctx)
	return nil
}

// Stop halts all sync operations.
func (csf *CloudSyncFabric) Stop() error {
	if !csf.running.Swap(false) {
		return nil
	}
	if csf.cancel != nil {
		csf.cancel()
	}
	csf.mu.RLock()
	defer csf.mu.RUnlock()
	for _, c := range csf.connectors {
		if c.IsConnected() {
			c.Disconnect()
		}
	}
	return nil
}

// EnqueueBatch adds a batch to the priority queue.
func (csf *CloudSyncFabric) EnqueueBatch(batch *SyncBatch) {
	csf.mu.Lock()
	defer csf.mu.Unlock()
	csf.priorityQueue = append(csf.priorityQueue, batch)
	// Sort by priority (lower = higher priority)
	sort.Slice(csf.priorityQueue, func(i, j int) bool {
		return csf.priorityQueue[i].Priority < csf.priorityQueue[j].Priority
	})
}

// QueueSize returns the number of pending batches.
func (csf *CloudSyncFabric) QueueSize() int {
	csf.mu.RLock()
	defer csf.mu.RUnlock()
	return len(csf.priorityQueue)
}

func (csf *CloudSyncFabric) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(csf.config.SyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			csf.processBatches(ctx)
		}
	}
}

func (csf *CloudSyncFabric) processBatches(ctx context.Context) {
	csf.mu.Lock()
	if len(csf.priorityQueue) == 0 {
		csf.mu.Unlock()
		return
	}
	batch := csf.priorityQueue[0]
	csf.priorityQueue = csf.priorityQueue[1:]
	csf.mu.Unlock()

	csf.mu.RLock()
	connectors := make([]CloudConnector, 0, len(csf.connectors))
	for _, c := range csf.connectors {
		if c.IsConnected() {
			connectors = append(connectors, c)
		}
	}
	csf.mu.RUnlock()

	for _, conn := range connectors {
		if err := conn.Push(ctx, batch); err != nil {
			csf.totalErrors.Add(1)
		} else {
			csf.totalPushed.Add(uint64(len(batch.Points)))
			csf.bytesTransferred.Add(batch.Size)
		}
	}
}

// UpdateMerkleTree updates the Merkle tree with new data.
func (csf *CloudSyncFabric) UpdateMerkleTree(metric string, timestamp int64, value float64) {
	data := fmt.Sprintf("%s:%d:%.6f", metric, timestamp, value)
	csf.merkle.Insert(data)
}

// GetMerkleRoot returns the current Merkle root hash.
func (csf *CloudSyncFabric) GetMerkleRoot() string {
	return csf.merkle.Root()
}

// DetectDrift compares local Merkle root with a remote root.
func (csf *CloudSyncFabric) DetectDrift(remoteRoot string) bool {
	return csf.merkle.Root() != remoteRoot
}

// CloudSyncFabricStats holds fabric-level stats.
type CloudSyncFabricStats struct {
	ConnectorCount   int    `json:"connector_count"`
	QueueSize        int    `json:"queue_size"`
	TotalPushed      uint64 `json:"total_pushed"`
	TotalPulled      uint64 `json:"total_pulled"`
	TotalConflicts   uint64 `json:"total_conflicts"`
	TotalErrors      uint64 `json:"total_errors"`
	BytesTransferred int64  `json:"bytes_transferred"`
	MerkleRoot       string `json:"merkle_root"`
	Running          bool   `json:"running"`
}

// Stats returns fabric statistics.
func (csf *CloudSyncFabric) Stats() CloudSyncFabricStats {
	csf.mu.RLock()
	connCount := len(csf.connectors)
	queueSize := len(csf.priorityQueue)
	csf.mu.RUnlock()
	return CloudSyncFabricStats{
		ConnectorCount:   connCount,
		QueueSize:        queueSize,
		TotalPushed:      csf.totalPushed.Load(),
		TotalPulled:      csf.totalPulled.Load(),
		TotalConflicts:   csf.totalConflicts.Load(),
		TotalErrors:      csf.totalErrors.Load(),
		BytesTransferred: csf.bytesTransferred.Load(),
		MerkleRoot:       csf.merkle.Root(),
		Running:          csf.running.Load(),
	}
}

// RegisterHTTPHandlers registers sync fabric HTTP endpoints.
func (csf *CloudSyncFabric) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sync/fabric/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := csf.Stats()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	})
	mux.HandleFunc("/api/v1/sync/fabric/connectors", func(w http.ResponseWriter, r *http.Request) {
		connectors := csf.ListConnectors()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(connectors)
	})
}

// ---------------------------------------------------------------------------
// Merkle Tree — efficient diff detection
// ---------------------------------------------------------------------------

// MerkleNode represents a node in the Merkle tree.
type MerkleNode struct {
	Hash   string      `json:"hash"`
	Left   *MerkleNode `json:"-"`
	Right  *MerkleNode `json:"-"`
	IsLeaf bool        `json:"is_leaf"`
	Data   string      `json:"-"`
}

// MerkleTree provides efficient data comparison via hash trees.
type FabricMerkleTree struct {
	maxDepth int
	leaves   []string
	root     string
	dirty    bool
	mu       sync.RWMutex
}

// NewMerkleTree creates a new Merkle tree.
func NewFabricMerkleTree(maxDepth int) *FabricMerkleTree {
	if maxDepth <= 0 {
		maxDepth = 16
	}
	return &FabricMerkleTree{
		maxDepth: maxDepth,
		leaves:   make([]string, 0, 1024),
	}
}

// Insert adds a data item to the tree.
func (mt *FabricMerkleTree) Insert(data string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.leaves = append(mt.leaves, fabricHashData(data))
	mt.dirty = true
}

// Root returns the Merkle root hash.
func (mt *FabricMerkleTree) Root() string {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if mt.dirty || mt.root == "" {
		mt.rebuild()
	}
	return mt.root
}

// LeafCount returns the number of leaves.
func (mt *FabricMerkleTree) LeafCount() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return len(mt.leaves)
}

// Verify checks if a data item exists in the tree.
