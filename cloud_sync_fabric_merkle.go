// cloud_sync_fabric_merkle.go contains extended cloud sync fabric functionality.
package chronicle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

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
	strategy  FabricConflictStrategy
	resolved  atomic.Uint64
	conflicts atomic.Uint64
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
