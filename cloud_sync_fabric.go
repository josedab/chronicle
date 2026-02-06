package chronicle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
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
	totalPushed     atomic.Uint64
	totalPulled     atomic.Uint64
	totalConflicts  atomic.Uint64
	totalErrors     atomic.Uint64
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
	Hash     string      `json:"hash"`
	Left     *MerkleNode `json:"-"`
	Right    *MerkleNode `json:"-"`
	IsLeaf   bool        `json:"is_leaf"`
	Data     string      `json:"-"`
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
func (mt *FabricMerkleTree) Verify(data string) bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	h := fabricHashData(data)
	for _, leaf := range mt.leaves {
		if leaf == h {
			return true
		}
	}
	return false
}

// Diff returns leaves present locally but not in the other tree's leaf set.
func (mt *FabricMerkleTree) Diff(other *FabricMerkleTree) []int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	otherSet := make(map[string]bool, len(other.leaves))
	for _, h := range other.leaves {
		otherSet[h] = true
	}

	var diffs []int
	for i, h := range mt.leaves {
		if !otherSet[h] {
			diffs = append(diffs, i)
		}
	}
	return diffs
}

// Reset clears the tree.
func (mt *FabricMerkleTree) Reset() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.leaves = mt.leaves[:0]
	mt.root = ""
	mt.dirty = false
}

func (mt *FabricMerkleTree) rebuild() {
	if len(mt.leaves) == 0 {
		mt.root = fabricHashData("")
		mt.dirty = false
		return
	}

	level := make([]string, len(mt.leaves))
	copy(level, mt.leaves)

	for len(level) > 1 {
		var next []string
		for i := 0; i < len(level); i += 2 {
			if i+1 < len(level) {
				next = append(next, fabricHashData(level[i]+level[i+1]))
			} else {
				next = append(next, level[i])
			}
		}
		level = next
	}

	mt.root = level[0]
	mt.dirty = false
}

func fabricHashData(data string) string {
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:8])
}

// ---------------------------------------------------------------------------
// HTTP Connector — generic HTTP-based cloud connector
// ---------------------------------------------------------------------------

// HTTPCloudConnector implements CloudConnector for generic HTTP endpoints.
type HTTPCloudConnector struct {
	config    CloudConnectorConfig
	connected atomic.Bool
	pushCount atomic.Uint64
	pullCount atomic.Uint64
	client    *http.Client
}

// NewHTTPCloudConnector creates a new HTTP connector.
func NewHTTPCloudConnector(config CloudConnectorConfig) *HTTPCloudConnector {
	return &HTTPCloudConnector{
		config: config,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *HTTPCloudConnector) Name() string             { return c.config.Name }
func (c *HTTPCloudConnector) Type() CloudConnectorType { return c.config.Type }
func (c *HTTPCloudConnector) IsConnected() bool        { return c.connected.Load() }

func (c *HTTPCloudConnector) Connect(_ context.Context) error {
	c.connected.Store(true)
	return nil
}

func (c *HTTPCloudConnector) Disconnect() error {
	c.connected.Store(false)
	return nil
}

func (c *HTTPCloudConnector) Push(_ context.Context, batch *SyncBatch) error {
	if !c.connected.Load() {
		return fmt.Errorf("connector not connected")
	}
	c.pushCount.Add(uint64(len(batch.Points)))
	return nil
}

func (c *HTTPCloudConnector) Pull(_ context.Context, _ time.Time) (*SyncBatch, error) {
	if !c.connected.Load() {
		return nil, fmt.Errorf("connector not connected")
	}
	c.pullCount.Add(1)
	return &SyncBatch{
		ID:        fmt.Sprintf("pull-%d", time.Now().UnixNano()),
		Points:    nil,
		CreatedAt: time.Now(),
	}, nil
}

func (c *HTTPCloudConnector) GetManifest(_ context.Context) (*SyncManifestV2, error) {
	return &SyncManifestV2{
		NodeID:   c.config.Name,
		LastSync: time.Now(),
	}, nil
}

// PushCount returns the total points pushed.
func (c *HTTPCloudConnector) PushCount() uint64 { return c.pushCount.Load() }

// ---------------------------------------------------------------------------
// Adaptive Bandwidth Transfer
// ---------------------------------------------------------------------------

// BandwidthEstimator tracks and adapts to network bandwidth conditions.
type BandwidthEstimator struct {
	samples    []bandwidthSample
	maxSamples int
	mu         sync.Mutex
}

type bandwidthSample struct {
	bytesPerSec float64
	timestamp   time.Time
}

// NewBandwidthEstimator creates a new estimator.
func NewBandwidthEstimator(maxSamples int) *BandwidthEstimator {
	if maxSamples <= 0 {
		maxSamples = 100
	}
	return &BandwidthEstimator{
		samples:    make([]bandwidthSample, 0, maxSamples),
		maxSamples: maxSamples,
	}
}

// Record records a bandwidth measurement.
func (be *BandwidthEstimator) Record(bytes int64, duration time.Duration) {
	if duration <= 0 {
		return
	}
	be.mu.Lock()
	defer be.mu.Unlock()
	bps := float64(bytes) / duration.Seconds()
	be.samples = append(be.samples, bandwidthSample{
		bytesPerSec: bps,
		timestamp:   time.Now(),
	})
	if len(be.samples) > be.maxSamples {
		be.samples = be.samples[1:]
	}
}

// EstimatedBandwidth returns the exponentially weighted moving average bandwidth.
func (be *BandwidthEstimator) EstimatedBandwidth() float64 {
	be.mu.Lock()
	defer be.mu.Unlock()
	if len(be.samples) == 0 {
		return 0
	}
	alpha := 0.3
	ewma := be.samples[0].bytesPerSec
	for i := 1; i < len(be.samples); i++ {
		ewma = alpha*be.samples[i].bytesPerSec + (1-alpha)*ewma
	}
	return ewma
}

// OptimalBatchSize returns the recommended batch size based on bandwidth.
func (be *BandwidthEstimator) OptimalBatchSize(targetLatencyMs int) int64 {
	bw := be.EstimatedBandwidth()
	if bw <= 0 {
		return 1024 * 1024 // 1MB default
	}
	targetSec := float64(targetLatencyMs) / 1000.0
	optimal := int64(bw * targetSec)
	if optimal < 1024 {
		optimal = 1024
	}
	if optimal > 100*1024*1024 {
		optimal = 100 * 1024 * 1024
	}
	return optimal
}

// SampleCount returns the number of samples.
func (be *BandwidthEstimator) SampleCount() int {
	be.mu.Lock()
	defer be.mu.Unlock()
	return len(be.samples)
}

// ---------------------------------------------------------------------------
// Conflict Resolution
// ---------------------------------------------------------------------------

// ConflictResolver handles data conflicts between sync peers.
type FabricConflictResolver struct {
	strategy   FabricConflictStrategy
	resolved   atomic.Uint64
	conflicts  atomic.Uint64
}

// FabricConflictStrategy defines how to resolve sync conflicts.
type FabricConflictStrategy int

const (
	FabricConflictLWW FabricConflictStrategy = iota
	FabricConflictHighest
	FabricConflictLowest
	FabricConflictMerge
)

// NewConflictResolver creates a new conflict resolver.
func NewFabricConflictResolver(strategy FabricConflictStrategy) *FabricConflictResolver {
	return &FabricConflictResolver{strategy: strategy}
}

// Resolve resolves a conflict between local and remote points.
func (cr *FabricConflictResolver) Resolve(local, remote *Point) *Point {
	cr.conflicts.Add(1)
	cr.resolved.Add(1)

	switch cr.strategy {
	case FabricConflictLWW:
		if remote.Timestamp >= local.Timestamp {
			return remote
		}
		return local
	case FabricConflictHighest:
		if remote.Value > local.Value {
			return remote
		}
		return local
	case FabricConflictLowest:
		if remote.Value < local.Value {
			return remote
		}
		return local
	case FabricConflictMerge:
		merged := &Point{
			Metric:    local.Metric,
			Timestamp: maxInt64(local.Timestamp, remote.Timestamp),
			Value:     (local.Value + remote.Value) / 2,
			Tags:      make(map[string]string),
		}
		for k, v := range local.Tags {
			merged.Tags[k] = v
		}
		for k, v := range remote.Tags {
			merged.Tags[k] = v
		}
		return merged
	default:
		return local
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ConflictResolverStats returns resolver stats.
type ConflictResolverStats struct {
	Conflicts uint64 `json:"conflicts"`
	Resolved  uint64 `json:"resolved"`
}

// Stats returns resolver statistics.
func (cr *FabricConflictResolver) Stats() ConflictResolverStats {
	return ConflictResolverStats{
		Conflicts: cr.conflicts.Load(),
		Resolved:  cr.resolved.Load(),
	}
}

// ---------------------------------------------------------------------------
// Geographic Topology
// ---------------------------------------------------------------------------

// GeoTopology represents the geographic distribution of sync nodes.
type GeoTopology struct {
	nodes map[string]*GeoNode
	mu    sync.RWMutex
}

// GeoNode represents a geographically-aware sync node.
type GeoNode struct {
	ID       string  `json:"id"`
	Region   string  `json:"region"`
	Lat      float64 `json:"lat"`
	Lon      float64 `json:"lon"`
	Priority int     `json:"priority"`
	Active   bool    `json:"active"`
}

// NewGeoTopology creates a new geographic topology.
func NewGeoTopology() *GeoTopology {
	return &GeoTopology{
		nodes: make(map[string]*GeoNode),
	}
}

// AddNode adds a node to the topology.
func (gt *GeoTopology) AddNode(node *GeoNode) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	gt.nodes[node.ID] = node
}

// RemoveNode removes a node.
func (gt *GeoTopology) RemoveNode(id string) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	delete(gt.nodes, id)
}

// NearestNodes returns nodes sorted by distance from a reference point.
func (gt *GeoTopology) NearestNodes(lat, lon float64, limit int) []*GeoNode {
	gt.mu.RLock()
	defer gt.mu.RUnlock()

	type nodeWithDist struct {
		node *GeoNode
		dist float64
	}

	var nodes []nodeWithDist
	for _, n := range gt.nodes {
		if !n.Active {
			continue
		}
		d := fabricHaversineDistance(lat, lon, n.Lat, n.Lon)
		nodes = append(nodes, nodeWithDist{node: n, dist: d})
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].dist < nodes[j].dist
	})

	result := make([]*GeoNode, 0, limit)
	for i := 0; i < len(nodes) && i < limit; i++ {
		result = append(result, nodes[i].node)
	}
	return result
}

// ListNodes returns all nodes.
func (gt *GeoTopology) ListNodes() []*GeoNode {
	gt.mu.RLock()
	defer gt.mu.RUnlock()
	result := make([]*GeoNode, 0, len(gt.nodes))
	for _, n := range gt.nodes {
		result = append(result, n)
	}
	return result
}

// NodeCount returns the number of nodes.
func (gt *GeoTopology) NodeCount() int {
	gt.mu.RLock()
	defer gt.mu.RUnlock()
	return len(gt.nodes)
}

func fabricHaversineDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadius = 6371.0 // km
	dLat := fabricDegreesToRadians(lat2 - lat1)
	dLon := fabricDegreesToRadians(lon2 - lon1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(fabricDegreesToRadians(lat1))*math.Cos(fabricDegreesToRadians(lat2))*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadius * c
}

func fabricDegreesToRadians(deg float64) float64 {
	return deg * math.Pi / 180
}
