package chronicle

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DataMeshConfig configures the real-time data mesh federation.
type DataMeshConfig struct {
	// Enabled enables the data mesh federation.
	Enabled bool `json:"enabled"`

	// NodeID uniquely identifies this node in the mesh.
	NodeID string `json:"node_id"`

	// ListenAddr is the gossip protocol listen address.
	ListenAddr string `json:"listen_addr"`

	// SeedNodes are initial peers for mesh discovery.
	SeedNodes []string `json:"seed_nodes"`

	// GossipInterval controls metadata propagation frequency.
	GossipInterval time.Duration `json:"gossip_interval"`

	// HeartbeatInterval for peer health checking.
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`

	// HeartbeatTimeout before marking a peer as unhealthy.
	HeartbeatTimeout time.Duration `json:"heartbeat_timeout"`

	// MaxConcurrentQueries limits parallel distributed queries.
	MaxConcurrentQueries int `json:"max_concurrent_queries"`

	// QueryTimeout for distributed queries.
	QueryTimeout time.Duration `json:"query_timeout"`

	// ReplicationFactor for query routing redundancy.
	ReplicationFactor int `json:"replication_factor"`

	// EnableLocalityRouting routes queries to data-local peers.
	EnableLocalityRouting bool `json:"enable_locality_routing"`

	// MaxPeers limits the number of mesh peers.
	MaxPeers int `json:"max_peers"`

	// EnableMTLS enables mutual TLS for peer communication.
	EnableMTLS bool `json:"enable_mtls"`
}

// DefaultDataMeshConfig returns sensible defaults for data mesh federation.
func DefaultDataMeshConfig() DataMeshConfig {
	return DataMeshConfig{
		Enabled:               true,
		NodeID:                fmt.Sprintf("node-%d", time.Now().UnixNano()%100000),
		ListenAddr:            ":9095",
		GossipInterval:        5 * time.Second,
		HeartbeatInterval:     10 * time.Second,
		HeartbeatTimeout:      30 * time.Second,
		MaxConcurrentQueries:  20,
		QueryTimeout:          30 * time.Second,
		ReplicationFactor:     2,
		EnableLocalityRouting: true,
		MaxPeers:              100,
	}
}

// DataMeshPeerState represents the state of a peer in the mesh.
type DataMeshPeerState string

const (
	DataMeshPeerHealthy   DataMeshPeerState = "healthy"
	DataMeshPeerSuspect   DataMeshPeerState = "suspect"
	DataMeshPeerUnhealthy DataMeshPeerState = "unhealthy"
	DataMeshPeerLeft      DataMeshPeerState = "left"
)

// MeshPeer represents a node in the data mesh.
type DataMeshPeer struct {
	ID         string               `json:"id"`
	Address    string               `json:"address"`
	State      DataMeshPeerState    `json:"state"`
	Metadata   DataMeshPeerMetadata `json:"metadata"`
	LastSeen   time.Time            `json:"last_seen"`
	JoinedAt   time.Time            `json:"joined_at"`
	Latency    time.Duration        `json:"latency"`
	QueryCount int64                `json:"query_count"`
	ErrorCount int64                `json:"error_count"`
}

// DataMeshPeerMetadata describes a peer's data holdings and capabilities.
type DataMeshPeerMetadata struct {
	Metrics      []string          `json:"metrics"`
	TimeRange    DataMeshTimeRange `json:"time_range"`
	Region       string            `json:"region"`
	Zone         string            `json:"zone"`
	Capabilities []string          `json:"capabilities"`
	Version      string            `json:"version"`
	Load         float64           `json:"load"`
	PointCount   int64             `json:"point_count"`
}

// DataMeshTimeRange represents a time range of data held by a peer.
type DataMeshTimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// DataMeshQueryRequest is a federated query across the data mesh.
type DataMeshQueryRequest struct {
	ID         string            `json:"id"`
	Query      string            `json:"query"`
	Metric     string            `json:"metric"`
	Start      time.Time         `json:"start"`
	End        time.Time         `json:"end"`
	Tags       map[string]string `json:"tags,omitempty"`
	MaxResults int               `json:"max_results"`
	Timeout    time.Duration     `json:"timeout,omitempty"`
}

// DataMeshQueryResult is the aggregated result from a distributed query.
type DataMeshQueryResult struct {
	RequestID      string               `json:"request_id"`
	Points         []Point              `json:"points"`
	PeerResults    []DataMeshPeerResult `json:"peer_results"`
	TotalPeers     int                  `json:"total_peers"`
	RespondedPeers int                  `json:"responded_peers"`
	Duration       time.Duration        `json:"duration"`
	Errors         []string             `json:"errors,omitempty"`
}

// DataMeshPeerResult is the result from a single peer.
type DataMeshPeerResult struct {
	PeerID   string        `json:"peer_id"`
	Points   []Point       `json:"points"`
	Duration time.Duration `json:"duration"`
	Error    string        `json:"error,omitempty"`
}

// DataMeshTopology represents the current mesh network topology.
type DataMeshTopology struct {
	Nodes        []DataMeshTopologyNode `json:"nodes"`
	Edges        []DataMeshTopologyEdge `json:"edges"`
	TotalPeers   int                    `json:"total_peers"`
	HealthyPeers int                    `json:"healthy_peers"`
	GeneratedAt  time.Time              `json:"generated_at"`
}

// DataMeshTopologyNode is a node in the topology visualization.
type DataMeshTopologyNode struct {
	ID     string            `json:"id"`
	State  DataMeshPeerState `json:"state"`
	Region string            `json:"region"`
	Load   float64           `json:"load"`
}

// DataMeshTopologyEdge is a connection between nodes.
type DataMeshTopologyEdge struct {
	Source  string        `json:"source"`
	Target  string        `json:"target"`
	Latency time.Duration `json:"latency"`
}

// DataMeshFedStats contains data mesh statistics.
type DataMeshFedStats struct {
	NodeID        string        `json:"node_id"`
	TotalPeers    int           `json:"total_peers"`
	HealthyPeers  int           `json:"healthy_peers"`
	TotalQueries  int64         `json:"total_queries"`
	FailedQueries int64         `json:"failed_queries"`
	AvgLatency    time.Duration `json:"avg_latency"`
	TotalPoints   int64         `json:"total_points_routed"`
	Uptime        time.Duration `json:"uptime"`
	GossipRounds  int64         `json:"gossip_rounds"`
	LastGossip    time.Time     `json:"last_gossip"`
}

// DataMesh implements decentralized query execution across Chronicle instances.
type DataMesh struct {
	db     *DB
	config DataMeshConfig

	peers     map[string]*DataMeshPeer
	localMeta DataMeshPeerMetadata
	sem       chan struct{}
	startTime time.Time

	totalQueries  atomic.Int64
	failedQueries atomic.Int64
	totalPoints   atomic.Int64
	gossipRounds  atomic.Int64
	lastGossip    time.Time

	routingTable *meshRoutingTable
	queryTracker *meshQueryTracker

	stopCh chan struct{}
	mu     sync.RWMutex
}

// NewDataMesh creates a new data mesh federation engine.
func NewDataMesh(db *DB, cfg DataMeshConfig) *DataMesh {
	dm := &DataMesh{
		db:           db,
		config:       cfg,
		peers:        make(map[string]*DataMeshPeer),
		sem:          make(chan struct{}, cfg.MaxConcurrentQueries),
		startTime:    time.Now(),
		routingTable: newMeshRoutingTable(),
		queryTracker: newMeshQueryTracker(1000),
		stopCh:       make(chan struct{}),
	}
	return dm
}

// AddPeer registers a new peer in the mesh.
func (dm *DataMesh) AddPeer(id, address string, meta DataMeshPeerMetadata) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if len(dm.peers) >= dm.config.MaxPeers {
		return fmt.Errorf("data_mesh: max peers (%d) reached", dm.config.MaxPeers)
	}

	peer := &DataMeshPeer{
		ID:       id,
		Address:  address,
		State:    DataMeshPeerHealthy,
		Metadata: meta,
		LastSeen: time.Now(),
		JoinedAt: time.Now(),
	}
	dm.peers[id] = peer
	dm.routingTable.addPeer(id, meta)
	return nil
}

// RemovePeer removes a peer from the mesh.
func (dm *DataMesh) RemovePeer(id string) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if peer, ok := dm.peers[id]; ok {
		peer.State = DataMeshPeerLeft
		delete(dm.peers, id)
		dm.routingTable.removePeer(id)
	}
}

// GetPeer returns a peer by ID.
func (dm *DataMesh) GetPeer(id string) *DataMeshPeer {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if p, ok := dm.peers[id]; ok {
		cp := *p
		return &cp
	}
	return nil
}

// ListPeers returns all known peers.
func (dm *DataMesh) ListPeers() []DataMeshPeer {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	result := make([]DataMeshPeer, 0, len(dm.peers))
	for _, p := range dm.peers {
		result = append(result, *p)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// UpdatePeerMetadata updates a peer's metadata (called during gossip).
func (dm *DataMesh) UpdatePeerMetadata(id string, meta DataMeshPeerMetadata) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	peer, ok := dm.peers[id]
	if !ok {
		return fmt.Errorf("data_mesh: peer %q not found", id)
	}
	peer.Metadata = meta
	peer.LastSeen = time.Now()
	dm.routingTable.updatePeer(id, meta)
	return nil
}

// Query executes a distributed query across the mesh.
func (dm *DataMesh) Query(ctx context.Context, req DataMeshQueryRequest) (*DataMeshQueryResult, error) {
	if req.ID == "" {
		req.ID = fmt.Sprintf("mq-%x", sha256.Sum256([]byte(fmt.Sprintf("%s-%d", req.Query, time.Now().UnixNano()))))[:16]
	}

	dm.totalQueries.Add(1)

	timeout := req.Timeout
	if timeout == 0 {
		timeout = dm.config.QueryTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Find relevant peers using locality-aware routing
	targetPeers := dm.selectPeers(req)
	if len(targetPeers) == 0 {
		dm.failedQueries.Add(1)
		return &DataMeshQueryResult{
			RequestID:  req.ID,
			TotalPeers: 0,
			Errors:     []string{"no peers available for query"},
		}, nil
	}

	start := time.Now()
	results := make([]DataMeshPeerResult, 0, len(targetPeers))
	var resultsMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range targetPeers {
		wg.Add(1)
		go func(p *DataMeshPeer) {
			defer wg.Done()

			dm.sem <- struct{}{}
			defer func() { <-dm.sem }()

			peerStart := time.Now()
			pr := DataMeshPeerResult{PeerID: p.ID}

			points, err := dm.queryPeer(ctx, p, req)
			pr.Duration = time.Since(peerStart)
			if err != nil {
				pr.Error = err.Error()
				atomic.AddInt64(&p.ErrorCount, 1)
			} else {
				pr.Points = points
				atomic.AddInt64(&p.QueryCount, 1)
			}

			resultsMu.Lock()
			results = append(results, pr)
			resultsMu.Unlock()
		}(peer)
	}

	wg.Wait()

	// Merge results from all peers
	merged := dm.mergeResults(results, req)
	merged.RequestID = req.ID
	merged.TotalPeers = len(targetPeers)
	merged.RespondedPeers = len(results)
	merged.Duration = time.Since(start)

	dm.totalPoints.Add(int64(len(merged.Points)))
	dm.queryTracker.record(req.ID, merged)

	return merged, nil
}

// selectPeers finds the best peers for a query using locality-aware routing.
func (dm *DataMesh) selectPeers(req DataMeshQueryRequest) []*DataMeshPeer {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.config.EnableLocalityRouting && req.Metric != "" {
		peerIDs := dm.routingTable.findPeersForMetric(req.Metric)
		if len(peerIDs) > 0 {
			var peers []*DataMeshPeer
			for _, id := range peerIDs {
				if p, ok := dm.peers[id]; ok && p.State == DataMeshPeerHealthy {
					peers = append(peers, p)
				}
			}
			if len(peers) > 0 {
				return peers
			}
		}
	}

	// Fallback: return all healthy peers
	var peers []*DataMeshPeer
	for _, p := range dm.peers {
		if p.State == DataMeshPeerHealthy {
			peers = append(peers, p)
		}
	}
	return peers
}

// queryPeer queries a single peer (simulated for in-process mesh).
func (dm *DataMesh) queryPeer(ctx context.Context, peer *DataMeshPeer, req DataMeshQueryRequest) ([]Point, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// For in-process queries against local DB
	if dm.db != nil && peer.ID == dm.config.NodeID {
		q := &Query{Metric: req.Metric, Start: req.Start.UnixNano(), End: req.End.UnixNano()}
		result, err := dm.db.Execute(q)
		if err != nil {
			return nil, fmt.Errorf("local query failed: %w", err)
		}
		return result.Points, nil
	}

	// Simulated latency for remote peers
	peer.Latency = time.Duration(rand.Intn(50)+5) * time.Millisecond
	return nil, nil
}

// mergeResults combines results from multiple peers, deduplicating by timestamp.
func (dm *DataMesh) mergeResults(peerResults []DataMeshPeerResult, req DataMeshQueryRequest) *DataMeshQueryResult {
	result := &DataMeshQueryResult{
		PeerResults: peerResults,
	}

	seen := make(map[int64]bool)
	for _, pr := range peerResults {
		if pr.Error != "" {
			result.Errors = append(result.Errors, fmt.Sprintf("peer %s: %s", pr.PeerID, pr.Error))
			continue
		}
		for _, p := range pr.Points {
			if !seen[p.Timestamp] {
				seen[p.Timestamp] = true
				result.Points = append(result.Points, p)
			}
		}
	}

	sort.Slice(result.Points, func(i, j int) bool {
		return result.Points[i].Timestamp < result.Points[j].Timestamp
	})

	if req.MaxResults > 0 && len(result.Points) > req.MaxResults {
		result.Points = result.Points[:req.MaxResults]
	}
	return result
}

// Gossip propagates metadata to peers (simulated gossip round).
func (dm *DataMesh) Gossip() {
	dm.mu.Lock()
	dm.lastGossip = time.Now()
	dm.mu.Unlock()

	dm.gossipRounds.Add(1)

	dm.mu.RLock()
	defer dm.mu.RUnlock()

	now := time.Now()
	for _, peer := range dm.peers {
		timeSinceSeen := now.Sub(peer.LastSeen)
		switch {
		case timeSinceSeen > dm.config.HeartbeatTimeout:
			peer.State = DataMeshPeerUnhealthy
		case timeSinceSeen > dm.config.HeartbeatTimeout/2:
			peer.State = DataMeshPeerSuspect
		default:
			peer.State = DataMeshPeerHealthy
		}
	}
}

// Topology returns the current mesh topology for visualization.
func (dm *DataMesh) Topology() *DataMeshTopology {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	topo := &DataMeshTopology{
		GeneratedAt: time.Now(),
	}

	for _, p := range dm.peers {
		topo.Nodes = append(topo.Nodes, DataMeshTopologyNode{
			ID:     p.ID,
			State:  p.State,
			Region: p.Metadata.Region,
			Load:   p.Metadata.Load,
		})
		topo.TotalPeers++
		if p.State == DataMeshPeerHealthy {
			topo.HealthyPeers++
		}
	}

	// Generate edges between peers in same region
	for i := range topo.Nodes {
		for j := i + 1; j < len(topo.Nodes); j++ {
			if topo.Nodes[i].Region == topo.Nodes[j].Region && topo.Nodes[i].Region != "" {
				topo.Edges = append(topo.Edges, DataMeshTopologyEdge{
					Source:  topo.Nodes[i].ID,
					Target:  topo.Nodes[j].ID,
					Latency: time.Millisecond * 5,
				})
			}
		}
	}
	return topo
}

// Stats returns current mesh statistics.
func (dm *DataMesh) Stats() DataMeshFedStats {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	healthy := 0
	var totalLatency time.Duration
	latencyCount := 0
	for _, p := range dm.peers {
		if p.State == DataMeshPeerHealthy {
			healthy++
		}
		if p.Latency > 0 {
			totalLatency += p.Latency
			latencyCount++
		}
	}

	var avgLatency time.Duration
	if latencyCount > 0 {
		avgLatency = totalLatency / time.Duration(latencyCount)
	}

	return DataMeshFedStats{
		NodeID:        dm.config.NodeID,
		TotalPeers:    len(dm.peers),
		HealthyPeers:  healthy,
		TotalQueries:  dm.totalQueries.Load(),
		FailedQueries: dm.failedQueries.Load(),
		AvgLatency:    avgLatency,
		TotalPoints:   dm.totalPoints.Load(),
		Uptime:        time.Since(dm.startTime),
		GossipRounds:  dm.gossipRounds.Load(),
		LastGossip:    dm.lastGossip,
	}
}

// Start begins background gossip and health check loops.
func (dm *DataMesh) Start() {
	go dm.gossipLoop()
	go dm.healthCheckLoop()
}

// Stop stops the data mesh.
func (dm *DataMesh) Stop() {
	close(dm.stopCh)
}

func (dm *DataMesh) gossipLoop() {
	ticker := time.NewTicker(dm.config.GossipInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			dm.Gossip()
		case <-dm.stopCh:
			return
		}
	}
}

func (dm *DataMesh) healthCheckLoop() {
	ticker := time.NewTicker(dm.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			dm.checkPeerHealth()
		case <-dm.stopCh:
			return
		}
	}
}

func (dm *DataMesh) checkPeerHealth() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	now := time.Now()
	for id, peer := range dm.peers {
		if now.Sub(peer.LastSeen) > dm.config.HeartbeatTimeout*3 {
			delete(dm.peers, id)
			dm.routingTable.removePeer(id)
		}
	}
}

// RegisterHTTPHandlers registers data mesh HTTP endpoints.
func (dm *DataMesh) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/mesh/peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(dm.ListPeers())
	})
	mux.HandleFunc("/api/v1/mesh/topology", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(dm.Topology())
	})
	mux.HandleFunc("/api/v1/mesh/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(dm.Stats())
	})
}

// --- Internal routing table ---

type meshRoutingTable struct {
	metricToPeers map[string][]string
	mu            sync.RWMutex
}

func newMeshRoutingTable() *meshRoutingTable {
	return &meshRoutingTable{
		metricToPeers: make(map[string][]string),
	}
}

func (rt *meshRoutingTable) addPeer(id string, meta DataMeshPeerMetadata) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, m := range meta.Metrics {
		rt.metricToPeers[m] = append(rt.metricToPeers[m], id)
	}
}

func (rt *meshRoutingTable) removePeer(id string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for metric, peers := range rt.metricToPeers {
		var filtered []string
		for _, p := range peers {
			if p != id {
				filtered = append(filtered, p)
			}
		}
		if len(filtered) == 0 {
			delete(rt.metricToPeers, metric)
		} else {
			rt.metricToPeers[metric] = filtered
		}
	}
}

func (rt *meshRoutingTable) updatePeer(id string, meta DataMeshPeerMetadata) {
	rt.removePeer(id)
	rt.addPeer(id, meta)
}

func (rt *meshRoutingTable) findPeersForMetric(metric string) []string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.metricToPeers[metric]
}

// --- Internal query tracker ---

type meshQueryTracker struct {
	queries map[string]*DataMeshQueryResult
	order   []string
	maxSize int
	mu      sync.RWMutex
}

func newMeshQueryTracker(maxSize int) *meshQueryTracker {
	return &meshQueryTracker{
		queries: make(map[string]*DataMeshQueryResult),
		maxSize: maxSize,
	}
}

func (qt *meshQueryTracker) record(id string, result *DataMeshQueryResult) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	qt.queries[id] = result
	qt.order = append(qt.order, id)
	if len(qt.order) > qt.maxSize {
		oldest := qt.order[0]
		qt.order = qt.order[1:]
		delete(qt.queries, oldest)
	}
}
