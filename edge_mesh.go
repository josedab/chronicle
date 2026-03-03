package chronicle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// EdgeMeshConfig configures the distributed edge mesh network.
type EdgeMeshConfig struct {
	// Enabled enables edge mesh networking.
	Enabled bool `json:"enabled"`

	// NodeID is a unique identifier for this node. Auto-generated if empty.
	NodeID string `json:"node_id"`

	// ListenAddr is the address for mesh protocol communication.
	ListenAddr string `json:"listen_addr"`

	// AdvertiseAddr is the externally reachable address.
	AdvertiseAddr string `json:"advertise_addr"`

	// SeedNodes are initial peers for bootstrapping.
	SeedNodes []string `json:"seed_nodes"`

	// GossipInterval controls how often state is gossiped.
	GossipInterval time.Duration `json:"gossip_interval"`

	// HealthCheckInterval controls health probe frequency.
	HealthCheckInterval time.Duration `json:"health_check_interval"`

	// HealthCheckTimeout is the deadline for a health probe.
	HealthCheckTimeout time.Duration `json:"health_check_timeout"`

	// ReplicationFactor is the number of copies per partition.
	ReplicationFactor int `json:"replication_factor"`

	// VirtualNodes per physical node in the hash ring.
	VirtualNodes int `json:"virtual_nodes"`

	// MaxPeers limits the number of connected peers.
	MaxPeers int `json:"max_peers"`

	// QueryTimeout is the deadline for cross-node queries.
	QueryTimeout time.Duration `json:"query_timeout"`

	// EnableAutoRebalance automatically rebalances on topology change.
	EnableAutoRebalance bool `json:"enable_auto_rebalance"`
}

// DefaultEdgeMeshConfig returns sensible defaults for edge mesh.
func DefaultEdgeMeshConfig() EdgeMeshConfig {
	return EdgeMeshConfig{
		Enabled:             false,
		ListenAddr:          ":9090",
		GossipInterval:      5 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		HealthCheckTimeout:  3 * time.Second,
		ReplicationFactor:   2,
		VirtualNodes:        64,
		MaxPeers:            32,
		QueryTimeout:        30 * time.Second,
		EnableAutoRebalance: true,
	}
}

// PeerState represents the known state of a peer in the mesh.
type PeerState int

const (
	PeerAlive PeerState = iota
	PeerSuspect
	PeerDead
	PeerLeft
)

func (ps PeerState) String() string {
	switch ps {
	case PeerAlive:
		return "alive"
	case PeerSuspect:
		return "suspect"
	case PeerDead:
		return "dead"
	case PeerLeft:
		return "left"
	default:
		return "unknown"
	}
}

// MeshPeer represents a peer node in the edge mesh.
type MeshPeer struct {
	ID            string            `json:"id"`
	Addr          string            `json:"addr"`
	State         PeerState         `json:"state"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	JoinedAt      time.Time         `json:"joined_at"`
	Incarnation   uint64            `json:"incarnation"`
	Partitions    []uint32          `json:"partitions"`
}

// MeshStats tracks mesh network statistics.
type MeshStats struct {
	NodeID          string    `json:"node_id"`
	PeerCount       int       `json:"peer_count"`
	AlivePeers      int       `json:"alive_peers"`
	PartitionCount  int       `json:"partition_count"`
	GossipsSent     int64     `json:"gossips_sent"`
	GossipsReceived int64     `json:"gossips_received"`
	QueriesRouted   int64     `json:"queries_routed"`
	RebalanceCount  int64     `json:"rebalance_count"`
	Uptime          string    `json:"uptime"`
	StartTime       time.Time `json:"start_time"`
}

// hashRingEntry is an entry in the consistent hash ring.
type hashRingEntry struct {
	hash   uint32
	nodeID string
}

// ConsistentHashRing provides partition-to-node mapping.
type ConsistentHashRing struct {
	entries      []hashRingEntry
	virtualNodes int
	mu           sync.RWMutex
}

// NewConsistentHashRing creates a new consistent hash ring.
func NewConsistentHashRing(virtualNodes int) *ConsistentHashRing {
	if virtualNodes <= 0 {
		virtualNodes = 64
	}
	return &ConsistentHashRing{
		virtualNodes: virtualNodes,
	}
}

// AddNode adds a node to the hash ring with virtual nodes.
func (r *ConsistentHashRing) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.virtualNodes; i++ {
		key := fmt.Sprintf("%s#%d", nodeID, i)
		h := fnv.New32a()
		h.Write([]byte(key))
		r.entries = append(r.entries, hashRingEntry{hash: h.Sum32(), nodeID: nodeID})
	}
	sort.Slice(r.entries, func(i, j int) bool {
		return r.entries[i].hash < r.entries[j].hash
	})
}

// RemoveNode removes all entries for a node.
func (r *ConsistentHashRing) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	filtered := r.entries[:0]
	for _, e := range r.entries {
		if e.nodeID != nodeID {
			filtered = append(filtered, e)
		}
	}
	r.entries = filtered
}

// GetNode returns the node responsible for a given key.
func (r *ConsistentHashRing) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.entries) == 0 {
		return ""
	}

	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	idx := sort.Search(len(r.entries), func(i int) bool {
		return r.entries[i].hash >= hash
	})
	if idx >= len(r.entries) {
		idx = 0
	}
	return r.entries[idx].nodeID
}

// GetNodes returns the top N distinct nodes for a key (for replication).
func (r *ConsistentHashRing) GetNodes(key string, count int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.entries) == 0 {
		return nil
	}

	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	idx := sort.Search(len(r.entries), func(i int) bool {
		return r.entries[i].hash >= hash
	})

	seen := make(map[string]bool)
	var nodes []string
	for i := 0; i < len(r.entries) && len(nodes) < count; i++ {
		entry := r.entries[(idx+i)%len(r.entries)]
		if !seen[entry.nodeID] {
			seen[entry.nodeID] = true
			nodes = append(nodes, entry.nodeID)
		}
	}
	return nodes
}

// NodeCount returns the number of distinct nodes.
func (r *ConsistentHashRing) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]bool)
	for _, e := range r.entries {
		seen[e.nodeID] = true
	}
	return len(seen)
}

// EdgeMesh manages the distributed edge mesh network.
type EdgeMesh struct {
	config EdgeMeshConfig
	db     *DB
	nodeID string

	ring  *ConsistentHashRing
	peers map[string]*MeshPeer
	mu    sync.RWMutex

	client  *http.Client
	running atomic.Bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	gossipsSent     atomic.Int64
	gossipsReceived atomic.Int64
	queriesRouted   atomic.Int64
	rebalanceCount  atomic.Int64
	startTime       time.Time
}

// NewEdgeMesh creates a new edge mesh network manager.
func NewEdgeMesh(db *DB, config EdgeMeshConfig) (*EdgeMesh, error) {
	if config.NodeID == "" {
		b := make([]byte, 8)
		if _, err := rand.Read(b); err != nil {
			return nil, fmt.Errorf("edge_mesh: failed to generate node ID: %w", err)
		}
		config.NodeID = "node-" + hex.EncodeToString(b)
	}

	em := &EdgeMesh{
		config:    config,
		db:        db,
		nodeID:    config.NodeID,
		ring:      NewConsistentHashRing(config.VirtualNodes),
		peers:     make(map[string]*MeshPeer),
		client:    &http.Client{Timeout: config.HealthCheckTimeout},
		stopCh:    make(chan struct{}),
		startTime: time.Now(),
	}

	// Add self to ring
	em.ring.AddNode(config.NodeID)

	return em, nil
}

// Start begins mesh operations: gossip, health checking, and rebalancing.
func (em *EdgeMesh) Start() error {
	if em.running.Swap(true) {
		return errors.New("edge_mesh: already running")
	}

	// Bootstrap from seed nodes
	for _, seed := range em.config.SeedNodes {
		em.wg.Add(1)
		go func(s string) {
			defer em.wg.Done()
			em.joinPeer(s)
		}(seed)
	}

	em.wg.Add(2)
	go func(stopCh <-chan struct{}) {
		defer em.wg.Done()
		em.gossipLoop()
	}(em.stopCh)
	go func(stopCh <-chan struct{}) {
		defer em.wg.Done()
		em.healthCheckLoop()
	}(em.stopCh)

	return nil
}

// Stop gracefully leaves the mesh.
func (em *EdgeMesh) Stop() error {
	if !em.running.Swap(false) {
		return nil
	}
	close(em.stopCh)
	em.wg.Wait()

	// Notify peers of departure
	em.mu.RLock()
	peers := make([]*MeshPeer, 0, len(em.peers))
	for _, p := range em.peers {
		peers = append(peers, p)
	}
	em.mu.RUnlock()

	for _, p := range peers {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		em.notifyLeave(ctx, p.Addr)
		cancel()
	}

	return nil
}

// NodeID returns this node's identifier.
func (em *EdgeMesh) NodeID() string {
	return em.nodeID
}

// Peers returns a snapshot of all known peers.
func (em *EdgeMesh) Peers() []*MeshPeer {
	em.mu.RLock()
	defer em.mu.RUnlock()

	result := make([]*MeshPeer, 0, len(em.peers))
	for _, p := range em.peers {
		cp := *p
		result = append(result, &cp)
	}
	return result
}

// Stats returns mesh network statistics.
func (em *EdgeMesh) Stats() MeshStats {
	em.mu.RLock()
	alive := 0
	for _, p := range em.peers {
		if p.State == PeerAlive {
			alive++
		}
	}
	peerCount := len(em.peers)
	em.mu.RUnlock()

	return MeshStats{
		NodeID:          em.nodeID,
		PeerCount:       peerCount,
		AlivePeers:      alive,
		PartitionCount:  em.ring.NodeCount(),
		GossipsSent:     em.gossipsSent.Load(),
		GossipsReceived: em.gossipsReceived.Load(),
		QueriesRouted:   em.queriesRouted.Load(),
		RebalanceCount:  em.rebalanceCount.Load(),
		Uptime:          time.Since(em.startTime).Round(time.Second).String(),
		StartTime:       em.startTime,
	}
}

// RouteQuery determines which node(s) should handle a query and fans out.
func (em *EdgeMesh) RouteQuery(ctx context.Context, q *Query) (*Result, error) {
	if q == nil {
		return nil, errors.New("edge_mesh: nil query")
	}

	partitionKey := q.Metric
	targetNode := em.ring.GetNode(partitionKey)

	// Local execution
	if targetNode == em.nodeID || targetNode == "" {
		return em.db.ExecuteContext(ctx, q)
	}

	// Remote execution
	em.queriesRouted.Add(1)
	return em.forwardQuery(ctx, targetNode, q)
}

// ScatterGatherQuery fans out a query to all nodes and merges results.
func (em *EdgeMesh) ScatterGatherQuery(ctx context.Context, q *Query) (*Result, error) {
	if q == nil {
		return nil, errors.New("edge_mesh: nil query")
	}

	em.mu.RLock()
	alivePeers := make([]*MeshPeer, 0)
	for _, p := range em.peers {
		if p.State == PeerAlive {
			alivePeers = append(alivePeers, p)
		}
	}
	em.mu.RUnlock()

	type queryResult struct {
		result *Result
		err    error
		nodeID string
	}

	resultCh := make(chan queryResult, len(alivePeers)+1)

	// Query self
	go func(ctx context.Context) {
		r, err := em.db.ExecuteContext(ctx, q)
		resultCh <- queryResult{result: r, err: err, nodeID: em.nodeID}
	}(ctx)

	// Query all alive peers
	for _, peer := range alivePeers {
		go func(p *MeshPeer) {
			r, err := em.forwardQuery(ctx, p.ID, q)
			resultCh <- queryResult{result: r, err: err, nodeID: p.ID}
		}(peer)
	}

	// Gather results
	var allPoints []Point
	expectedResults := len(alivePeers) + 1
	var firstErr error

	for i := 0; i < expectedResults; i++ {
		select {
		case qr := <-resultCh:
			if qr.err != nil {
				if firstErr == nil {
					firstErr = qr.err
				}
				continue
			}
			if qr.result != nil {
				allPoints = append(allPoints, qr.result.Points...)
			}
		case <-ctx.Done():
			if len(allPoints) == 0 {
				return nil, ctx.Err()
			}
			// Return partial results
			break
		}
	}

	if len(allPoints) == 0 && firstErr != nil {
		return nil, firstErr
	}

	// Deduplicate and sort by timestamp
	allPoints = deduplicatePoints(allPoints)
	sort.Slice(allPoints, func(i, j int) bool {
		return allPoints[i].Timestamp < allPoints[j].Timestamp
	})

	return &Result{Points: allPoints}, nil
}

// JoinCluster adds a node to the mesh by contacting an existing member.
func (em *EdgeMesh) JoinCluster(addr string) error {
	return em.joinPeer(addr)
}

func (em *EdgeMesh) joinPeer(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), em.config.HealthCheckTimeout)
	defer cancel()

	joinMsg := meshMessage{
		Type:     meshMessageJoin,
		SenderID: em.nodeID,
		Addr:     em.config.AdvertiseAddr,
		Time:     time.Now(),
	}

	data, err := json.Marshal(joinMsg)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/mesh/join", addr), io.NopCloser(jsonReader(data)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := em.client.Do(req)
	if err != nil {
		return fmt.Errorf("edge_mesh: failed to join %s: %w", addr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("edge_mesh: join rejected by %s: %s", addr, string(body))
	}

	// Parse response to learn about the peer
	var peerInfo meshMessage
	if err := json.NewDecoder(resp.Body).Decode(&peerInfo); err != nil {
		return fmt.Errorf("edge_mesh: invalid join response: %w", err)
	}

	em.addPeer(peerInfo.SenderID, addr, nil)
	return nil
}
