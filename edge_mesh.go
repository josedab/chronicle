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
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// EdgeMeshConfig configures the distributed edge mesh network.
type EdgeMeshConfig struct {
	// NodeID uniquely identifies this node in the mesh.
	NodeID string `json:"node_id"`

	// BindAddr is the address for mesh communication.
	BindAddr string `json:"bind_addr"`

	// AdvertiseAddr is the address advertised to peers.
	AdvertiseAddr string `json:"advertise_addr"`

	// Seeds are initial peers to connect to.
	Seeds []string `json:"seeds"`

	// EnableMDNS enables multicast DNS for peer discovery.
	EnableMDNS bool `json:"enable_mdns"`

	// MDNSService is the mDNS service name.
	MDNSService string `json:"mdns_service"`

	// SyncInterval is how often to sync with peers.
	SyncInterval time.Duration `json:"sync_interval"`

	// GossipInterval is how often to exchange peer information.
	GossipInterval time.Duration `json:"gossip_interval"`

	// MaxPeers is the maximum number of connected peers.
	MaxPeers int `json:"max_peers"`

	// MergeStrategy specifies how to merge conflicting data.
	MergeStrategy CRDTMergeStrategy `json:"merge_strategy"`

	// BandwidthLimitBps limits sync bandwidth (0=unlimited).
	BandwidthLimitBps int64 `json:"bandwidth_limit_bps"`

	// CompressSync enables compression for sync payloads.
	CompressSync bool `json:"compress_sync"`

	// EnableEncryption enables TLS for mesh communication.
	EnableEncryption bool `json:"enable_encryption"`

	// SyncBatchSize is the maximum points per sync batch.
	SyncBatchSize int `json:"sync_batch_size"`

	// OfflineQueueSize is the max queued operations when offline.
	OfflineQueueSize int `json:"offline_queue_size"`

	// HeartbeatInterval for peer health checks.
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`

	// PeerTimeout before marking a peer as disconnected.
	PeerTimeout time.Duration `json:"peer_timeout"`
}

// CRDTMergeStrategy specifies how to merge conflicting CRDT updates.
type CRDTMergeStrategy int

const (
	// CRDTMergeLastWriteWins uses wall-clock timestamp.
	CRDTMergeLastWriteWins CRDTMergeStrategy = iota
	// CRDTMergeLamportClock uses logical Lamport timestamps.
	CRDTMergeLamportClock
	// CRDTMergeVectorClock uses vector clocks for causality.
	CRDTMergeVectorClock
	// CRDTMergeMaxValue keeps the maximum value (for counters).
	CRDTMergeMaxValue
	// CRDTMergeUnion merges all values (for sets).
	CRDTMergeUnion
)

// DefaultEdgeMeshConfig returns default mesh configuration.
func DefaultEdgeMeshConfig() EdgeMeshConfig {
	return EdgeMeshConfig{
		NodeID:            fmt.Sprintf("mesh-%d", time.Now().UnixNano()%100000),
		BindAddr:          ":7946",
		EnableMDNS:        true,
		MDNSService:       "_chronicle._tcp",
		SyncInterval:      5 * time.Second,
		GossipInterval:    time.Second,
		MaxPeers:          50,
		MergeStrategy:     CRDTMergeVectorClock,
		CompressSync:      true,
		SyncBatchSize:     10000,
		OfflineQueueSize:  100000,
		HeartbeatInterval: time.Second,
		PeerTimeout:       10 * time.Second,
	}
}

// EdgeMesh manages peer-to-peer mesh network for edge synchronization.
type EdgeMesh struct {
	db     *DB
	config EdgeMeshConfig

	// Peer management
	peers   map[string]*MeshPeer
	peersMu sync.RWMutex

	// CRDT state
	vectorClock *MeshVectorClock
	lamportTime uint64
	stateHash   string

	// Operation log (CRDT operations)
	opLog   *CRDTOpLog
	opLogMu sync.RWMutex

	// Sync state
	syncState   map[string]*PeerSyncState
	syncStateMu sync.RWMutex

	// mDNS discovery
	mdnsServer *mdnsServer

	// HTTP server for mesh protocol
	server *http.Server
	client *http.Client

	// Statistics
	stats   EdgeMeshStats
	statsMu sync.RWMutex

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running atomic.Bool
}

// MeshPeer represents a peer in the mesh network.
type MeshPeer struct {
	ID            string            `json:"id"`
	Addr          string            `json:"addr"`
	LastSeen      time.Time         `json:"last_seen"`
	LastSynced    time.Time         `json:"last_synced"`
	Healthy       bool              `json:"healthy"`
	VectorClock   map[string]uint64 `json:"vector_clock"`
	Metadata      map[string]string `json:"metadata"`
	SyncOffset    uint64            `json:"sync_offset"`
	BytesSent     int64             `json:"bytes_sent"`
	BytesReceived int64             `json:"bytes_received"`
}

// MeshVectorClock provides vector clock for CRDT causality tracking.
type MeshVectorClock struct {
	clocks map[string]uint64
	mu     sync.RWMutex
}

// NewMeshVectorClock creates a new vector clock.
func NewMeshVectorClock() *MeshVectorClock {
	return &MeshVectorClock{
		clocks: make(map[string]uint64),
	}
}

// Tick increments the clock for a node.
func (vc *MeshVectorClock) Tick(nodeID string) uint64 {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clocks[nodeID]++
	return vc.clocks[nodeID]
}

// Get returns the clock value for a node.
func (vc *MeshVectorClock) Get(nodeID string) uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.clocks[nodeID]
}

// GetAll returns a copy of all clock values.
func (vc *MeshVectorClock) GetAll() map[string]uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	result := make(map[string]uint64, len(vc.clocks))
	for k, v := range vc.clocks {
		result[k] = v
	}
	return result
}

// Merge merges another vector clock, taking max values.
func (vc *MeshVectorClock) Merge(other map[string]uint64) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	for node, clock := range other {
		if clock > vc.clocks[node] {
			vc.clocks[node] = clock
		}
	}
}

// HappensBefore returns true if vc happened before other.
func (vc *MeshVectorClock) HappensBefore(other map[string]uint64) bool {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	atLeastOneLess := false
	for node := range vc.clocks {
		if vc.clocks[node] > other[node] {
			return false
		}
		if vc.clocks[node] < other[node] {
			atLeastOneLess = true
		}
	}
	for node := range other {
		if _, exists := vc.clocks[node]; !exists && other[node] > 0 {
			atLeastOneLess = true
		}
	}
	return atLeastOneLess
}

// Concurrent returns true if the clocks are concurrent (incomparable).
func (vc *MeshVectorClock) Concurrent(other map[string]uint64) bool {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	hasLess, hasGreater := false, false
	allNodes := make(map[string]bool)
	for n := range vc.clocks {
		allNodes[n] = true
	}
	for n := range other {
		allNodes[n] = true
	}

	for node := range allNodes {
		v1, v2 := vc.clocks[node], other[node]
		if v1 < v2 {
			hasLess = true
		} else if v1 > v2 {
			hasGreater = true
		}
	}
	return hasLess && hasGreater
}

// CRDTOpLog stores CRDT operations for synchronization.
type CRDTOpLog struct {
	operations []CRDTOperation
	maxSize    int
	mu         sync.RWMutex
}

// CRDTOperation represents a single CRDT operation.
type CRDTOperation struct {
	ID          string            `json:"id"`
	Type        CRDTOpType        `json:"type"`
	NodeID      string            `json:"node_id"`
	Timestamp   int64             `json:"timestamp"`
	LamportTime uint64            `json:"lamport_time"`
	VectorClock map[string]uint64 `json:"vector_clock"`
	Metric      string            `json:"metric"`
	Tags        map[string]string `json:"tags"`
	Value       float64           `json:"value"`
	Checksum    string            `json:"checksum"`
}

// CRDTOpType identifies the type of CRDT operation.
type CRDTOpType int

const (
	CRDTOpWrite CRDTOpType = iota
	CRDTOpDelete
	CRDTOpMerge
)

// NewCRDTOpLog creates a new operation log.
func NewCRDTOpLog(maxSize int) *CRDTOpLog {
	return &CRDTOpLog{
		operations: make([]CRDTOperation, 0, maxSize),
		maxSize:    maxSize,
	}
}

// Append adds an operation to the log.
func (l *CRDTOpLog) Append(op CRDTOperation) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Evict oldest if at capacity
	if len(l.operations) >= l.maxSize {
		l.operations = l.operations[1:]
	}
	l.operations = append(l.operations, op)
}

// GetSince returns operations since the given vector clock.
func (l *CRDTOpLog) GetSince(vc map[string]uint64) []CRDTOperation {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var result []CRDTOperation
	for _, op := range l.operations {
		// Check if this operation is newer than the given clock
		if isNewer(op.VectorClock, vc) {
			result = append(result, op)
		}
	}
	return result
}

// GetAll returns all operations.
func (l *CRDTOpLog) GetAll() []CRDTOperation {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := make([]CRDTOperation, len(l.operations))
	copy(result, l.operations)
	return result
}

// Len returns the number of operations.
func (l *CRDTOpLog) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.operations)
}

func isNewer(opClock, refClock map[string]uint64) bool {
	for node, opTime := range opClock {
		if opTime > refClock[node] {
			return true
		}
	}
	return false
}

// PeerSyncState tracks sync progress with a peer.
type PeerSyncState struct {
	PeerID         string            `json:"peer_id"`
	LastSyncTime   time.Time         `json:"last_sync_time"`
	LastVectorClock map[string]uint64 `json:"last_vector_clock"`
	SentOpCount    uint64            `json:"sent_op_count"`
	ReceivedOpCount uint64           `json:"received_op_count"`
	Conflicts      uint64            `json:"conflicts"`
	LastError      string            `json:"last_error,omitempty"`
}

// EdgeMeshStats contains mesh statistics.
type EdgeMeshStats struct {
	NodeID           string    `json:"node_id"`
	PeerCount        int       `json:"peer_count"`
	HealthyPeers     int       `json:"healthy_peers"`
	OperationCount   uint64    `json:"operation_count"`
	SyncCount        uint64    `json:"sync_count"`
	ConflictsResolved uint64   `json:"conflicts_resolved"`
	BytesSent        int64     `json:"bytes_sent"`
	BytesReceived    int64     `json:"bytes_received"`
	LastSyncTime     time.Time `json:"last_sync_time"`
	Uptime           time.Duration `json:"uptime"`
}

// NewEdgeMesh creates a new edge mesh network.
func NewEdgeMesh(db *DB, config EdgeMeshConfig) (*EdgeMesh, error) {
	if config.NodeID == "" {
		config.NodeID = fmt.Sprintf("mesh-%d", time.Now().UnixNano()%100000)
	}
	if config.SyncInterval <= 0 {
		config.SyncInterval = 5 * time.Second
	}
	if config.SyncBatchSize <= 0 {
		config.SyncBatchSize = 10000
	}
	if config.OfflineQueueSize <= 0 {
		config.OfflineQueueSize = 100000
	}

	ctx, cancel := context.WithCancel(context.Background())

	mesh := &EdgeMesh{
		db:          db,
		config:      config,
		peers:       make(map[string]*MeshPeer),
		vectorClock: NewMeshVectorClock(),
		opLog:       NewCRDTOpLog(config.OfflineQueueSize),
		syncState:   make(map[string]*PeerSyncState),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		ctx:    ctx,
		cancel: cancel,
	}

	// Initialize our vector clock entry
	mesh.vectorClock.Tick(config.NodeID)

	return mesh, nil
}

// Start begins mesh network operations.
func (m *EdgeMesh) Start() error {
	if m.running.Swap(true) {
		return errors.New("mesh already running")
	}

	// Start HTTP server for mesh protocol
	if m.config.BindAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/mesh/sync", m.handleSync)
		mux.HandleFunc("/mesh/gossip", m.handleGossip)
		mux.HandleFunc("/mesh/heartbeat", m.handleHeartbeat)
		mux.HandleFunc("/mesh/operations", m.handleOperations)
		mux.HandleFunc("/mesh/state", m.handleState)

		m.server = &http.Server{
			Addr:    m.config.BindAddr,
			Handler: mux,
		}

		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			if err := m.server.ListenAndServe(); err != http.ErrServerClosed {
				// Server error - log but continue
			}
		}()
	}

	// Start mDNS discovery if enabled
	if m.config.EnableMDNS {
		m.startMDNS()
	}

	// Connect to seed peers
	for _, seed := range m.config.Seeds {
		go m.connectPeer(seed)
	}

	// Start background loops
	m.wg.Add(3)
	go m.syncLoop()
	go m.gossipLoop()
	go m.heartbeatLoop()

	return nil
}

// Stop stops mesh network operations.
func (m *EdgeMesh) Stop() error {
	if !m.running.Swap(false) {
		return nil
	}

	m.cancel()

	if m.mdnsServer != nil {
		m.mdnsServer.Stop()
	}

	if m.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.server.Shutdown(ctx)
	}

	m.wg.Wait()
	return nil
}

// Write performs a CRDT-aware write operation.
func (m *EdgeMesh) Write(p Point) error {
	// Create CRDT operation
	lamport := atomic.AddUint64(&m.lamportTime, 1)
	vc := m.vectorClock.Tick(m.config.NodeID)

	op := CRDTOperation{
		ID:          fmt.Sprintf("%s-%d-%d", m.config.NodeID, p.Timestamp, lamport),
		Type:        CRDTOpWrite,
		NodeID:      m.config.NodeID,
		Timestamp:   p.Timestamp,
		LamportTime: lamport,
		VectorClock: m.vectorClock.GetAll(),
		Metric:      p.Metric,
		Tags:        p.Tags,
		Value:       p.Value,
	}
	op.Checksum = m.calculateOpChecksum(op)

	// Add to operation log
	m.opLog.Append(op)

	// Write to local database
	if err := m.db.Write(p); err != nil {
		return err
	}

	// Trigger async sync to peers
	go m.broadcastOperation(op)

	// Update vector clock in stats
	_ = vc

	return nil
}

// WriteBatch performs batch CRDT-aware writes.
func (m *EdgeMesh) WriteBatch(points []Point) error {
	for _, p := range points {
		if err := m.Write(p); err != nil {
			return err
		}
	}
	return nil
}

// Peers returns all known peers.
func (m *EdgeMesh) Peers() []*MeshPeer {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()

	peers := make([]*MeshPeer, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, &MeshPeer{
			ID:            p.ID,
			Addr:          p.Addr,
			LastSeen:      p.LastSeen,
			LastSynced:    p.LastSynced,
			Healthy:       p.Healthy,
			VectorClock:   p.VectorClock,
			Metadata:      p.Metadata,
			BytesSent:     p.BytesSent,
			BytesReceived: p.BytesReceived,
		})
	}
	return peers
}

// Stats returns mesh statistics.
func (m *EdgeMesh) Stats() EdgeMeshStats {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()

	m.peersMu.RLock()
	peerCount := len(m.peers)
	healthyPeers := 0
	for _, p := range m.peers {
		if p.Healthy {
			healthyPeers++
		}
	}
	m.peersMu.RUnlock()

	stats := m.stats
	stats.NodeID = m.config.NodeID
	stats.PeerCount = peerCount
	stats.HealthyPeers = healthyPeers
	stats.OperationCount = uint64(m.opLog.Len())
	return stats
}

// SyncNow triggers immediate sync with all peers.
func (m *EdgeMesh) SyncNow() error {
	m.peersMu.RLock()
	peers := make([]*MeshPeer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	m.peersMu.RUnlock()

	var lastErr error
	for _, peer := range peers {
		if err := m.syncWithPeer(peer); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// --- Background loops ---

func (m *EdgeMesh) syncLoop() {
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

func (m *EdgeMesh) gossipLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.performGossip()
		}
	}
}

func (m *EdgeMesh) heartbeatLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.sendHeartbeats()
			m.checkPeerHealth()
		}
	}
}

func (m *EdgeMesh) performSync() {
	m.peersMu.RLock()
	peers := make([]*MeshPeer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	m.peersMu.RUnlock()

	for _, peer := range peers {
		go func(p *MeshPeer) {
			if err := m.syncWithPeer(p); err != nil {
				m.recordSyncError(p.ID, err)
			}
		}(peer)
	}
}

func (m *EdgeMesh) syncWithPeer(peer *MeshPeer) error {
	// Get peer's sync state
	m.syncStateMu.RLock()
	peerState, exists := m.syncState[peer.ID]
	m.syncStateMu.RUnlock()

	var lastVC map[string]uint64
	if exists {
		lastVC = peerState.LastVectorClock
	} else {
		lastVC = make(map[string]uint64)
	}

	// Get operations since peer's last known state
	ops := m.opLog.GetSince(lastVC)
	if len(ops) == 0 {
		return nil // Nothing to sync
	}

	// Prepare sync request
	req := MeshSyncRequest{
		NodeID:      m.config.NodeID,
		VectorClock: m.vectorClock.GetAll(),
		Operations:  ops,
	}

	// Send sync request
	resp, err := m.sendSyncRequest(peer, req)
	if err != nil {
		return err
	}

	// Apply received operations
	for _, op := range resp.Operations {
		if err := m.applyOperation(op); err != nil {
			continue // Log and continue
		}
	}

	// Merge vector clocks
	m.vectorClock.Merge(resp.VectorClock)

	// Update sync state
	m.syncStateMu.Lock()
	if m.syncState[peer.ID] == nil {
		m.syncState[peer.ID] = &PeerSyncState{PeerID: peer.ID}
	}
	m.syncState[peer.ID].LastSyncTime = time.Now()
	m.syncState[peer.ID].LastVectorClock = resp.VectorClock
	m.syncState[peer.ID].SentOpCount += uint64(len(ops))
	m.syncState[peer.ID].ReceivedOpCount += uint64(len(resp.Operations))
	m.syncStateMu.Unlock()

	// Update peer state
	m.peersMu.Lock()
	if p, ok := m.peers[peer.ID]; ok {
		p.LastSynced = time.Now()
		p.VectorClock = resp.VectorClock
	}
	m.peersMu.Unlock()

	// Update stats
	m.statsMu.Lock()
	m.stats.SyncCount++
	m.stats.LastSyncTime = time.Now()
	m.statsMu.Unlock()

	return nil
}

func (m *EdgeMesh) sendSyncRequest(peer *MeshPeer, req MeshSyncRequest) (*MeshSyncResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	// Compress if enabled
	if m.config.CompressSync {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		gw.Write(data)
		gw.Close()
		data = buf.Bytes()
	}

	httpReq, err := http.NewRequestWithContext(m.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/mesh/sync", peer.Addr), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if m.config.CompressSync {
		httpReq.Header.Set("Content-Encoding", "gzip")
	}
	httpReq.Header.Set("X-Node-ID", m.config.NodeID)

	resp, err := m.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("sync failed: %d - %s", resp.StatusCode, string(body))
	}

	// Update stats
	m.peersMu.Lock()
	if p, ok := m.peers[peer.ID]; ok {
		p.BytesSent += int64(len(data))
	}
	m.peersMu.Unlock()

	var syncResp MeshSyncResponse
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Decompress if needed
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		body, err = io.ReadAll(gr)
		gr.Close()
		if err != nil {
			return nil, err
		}
	}

	if err := json.Unmarshal(body, &syncResp); err != nil {
		return nil, err
	}

	// Update received bytes
	m.peersMu.Lock()
	if p, ok := m.peers[peer.ID]; ok {
		p.BytesReceived += int64(len(body))
	}
	m.peersMu.Unlock()

	m.statsMu.Lock()
	m.stats.BytesSent += int64(len(data))
	m.stats.BytesReceived += int64(len(body))
	m.statsMu.Unlock()

	return &syncResp, nil
}

func (m *EdgeMesh) applyOperation(op CRDTOperation) error {
	// Verify checksum
	if op.Checksum != "" {
		expected := m.calculateOpChecksum(op)
		if op.Checksum != expected {
			return errors.New("operation checksum mismatch")
		}
	}

	// Check for conflicts using CRDT merge strategy
	shouldApply, err := m.resolveConflict(op)
	if err != nil {
		return err
	}

	if !shouldApply {
		return nil // Operation was superseded
	}

	// Apply to local database
	switch op.Type {
	case CRDTOpWrite:
		p := Point{
			Metric:    op.Metric,
			Tags:      op.Tags,
			Value:     op.Value,
			Timestamp: op.Timestamp,
		}
		if err := m.db.Write(p); err != nil {
			return err
		}

	case CRDTOpDelete:
		// Delete operations would require additional implementation
		// For now, just track the tombstone
	}

	// Add to our op log (if not already present)
	m.opLog.Append(op)

	// Merge vector clock
	m.vectorClock.Merge(op.VectorClock)

	return nil
}

func (m *EdgeMesh) resolveConflict(op CRDTOperation) (bool, error) {
	switch m.config.MergeStrategy {
	case CRDTMergeLastWriteWins:
		// Use wall-clock timestamp - always apply newer
		return true, nil

	case CRDTMergeLamportClock:
		// Compare Lamport timestamps
		if op.LamportTime > atomic.LoadUint64(&m.lamportTime) {
			atomic.StoreUint64(&m.lamportTime, op.LamportTime)
			return true, nil
		}
		// Tie-breaker: node ID
		if op.LamportTime == atomic.LoadUint64(&m.lamportTime) {
			return op.NodeID > m.config.NodeID, nil
		}
		return false, nil

	case CRDTMergeVectorClock:
		// Check causality with vector clocks
		if m.vectorClock.HappensBefore(op.VectorClock) {
			// Our state is older, apply the operation
			return true, nil
		}
		if m.vectorClock.Concurrent(op.VectorClock) {
			// Concurrent operations - use tie-breaker
			m.statsMu.Lock()
			m.stats.ConflictsResolved++
			m.statsMu.Unlock()
			// Tie-breaker: higher node ID wins
			return op.NodeID > m.config.NodeID, nil
		}
		// Our state is newer, skip
		return false, nil

	case CRDTMergeMaxValue:
		// For counters: keep max value
		return true, nil

	case CRDTMergeUnion:
		// For sets: always merge
		return true, nil

	default:
		return true, nil
	}
}

func (m *EdgeMesh) broadcastOperation(op CRDTOperation) {
	m.peersMu.RLock()
	peers := make([]*MeshPeer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	m.peersMu.RUnlock()

	for _, peer := range peers {
		go func(p *MeshPeer) {
			// Send single operation (lightweight)
			req := MeshOperationRequest{
				NodeID:    m.config.NodeID,
				Operation: op,
			}

			data, _ := json.Marshal(req)
			httpReq, err := http.NewRequestWithContext(m.ctx, http.MethodPost,
				fmt.Sprintf("http://%s/mesh/operations", p.Addr), bytes.NewReader(data))
			if err != nil {
				return
			}

			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("X-Node-ID", m.config.NodeID)

			resp, err := m.client.Do(httpReq)
			if err != nil {
				return
			}
			resp.Body.Close()
		}(peer)
	}
}

func (m *EdgeMesh) calculateOpChecksum(op CRDTOperation) string {
	// Create deterministic representation
	data := fmt.Sprintf("%s:%s:%d:%d:%s:%.15f",
		op.NodeID, op.Metric, op.Timestamp, op.LamportTime,
		sortedTagsString(op.Tags), op.Value)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:8])
}

func sortedTagsString(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(tags[k])
	}
	return sb.String()
}

func (m *EdgeMesh) performGossip() {
	m.peersMu.RLock()
	peers := make([]*MeshPeer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	m.peersMu.RUnlock()

	if len(peers) == 0 {
		return
	}

	// Select random subset of peers for gossip
	numTargets := 3
	if len(peers) < numTargets {
		numTargets = len(peers)
	}

	// Shuffle and take first numTargets
	for i := len(peers) - 1; i > 0; i-- {
		j := int(time.Now().UnixNano()) % (i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}

	gossip := MeshGossipMessage{
		NodeID:      m.config.NodeID,
		Addr:        m.config.AdvertiseAddr,
		VectorClock: m.vectorClock.GetAll(),
		Peers:       m.getPeerList(),
	}

	data, _ := json.Marshal(gossip)

	for i := 0; i < numTargets; i++ {
		go func(peer *MeshPeer) {
			httpReq, err := http.NewRequestWithContext(m.ctx, http.MethodPost,
				fmt.Sprintf("http://%s/mesh/gossip", peer.Addr), bytes.NewReader(data))
			if err != nil {
				return
			}

			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("X-Node-ID", m.config.NodeID)

			resp, err := m.client.Do(httpReq)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var respGossip MeshGossipMessage
			if err := json.NewDecoder(resp.Body).Decode(&respGossip); err != nil {
				return
			}

			// Add newly discovered peers
			for _, p := range respGossip.Peers {
				m.addPeer(p.ID, p.Addr)
			}

			// Merge vector clock
			m.vectorClock.Merge(respGossip.VectorClock)
		}(peers[i])
	}
}

func (m *EdgeMesh) getPeerList() []MeshPeerInfo {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()

	peers := make([]MeshPeerInfo, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, MeshPeerInfo{
			ID:   p.ID,
			Addr: p.Addr,
		})
	}
	return peers
}

func (m *EdgeMesh) sendHeartbeats() {
	m.peersMu.RLock()
	peers := make([]*MeshPeer, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, p)
	}
	m.peersMu.RUnlock()

	heartbeat := MeshHeartbeat{
		NodeID:      m.config.NodeID,
		Timestamp:   time.Now().UnixNano(),
		VectorClock: m.vectorClock.GetAll(),
	}

	data, _ := json.Marshal(heartbeat)

	for _, peer := range peers {
		go func(p *MeshPeer) {
			httpReq, err := http.NewRequestWithContext(m.ctx, http.MethodPost,
				fmt.Sprintf("http://%s/mesh/heartbeat", p.Addr), bytes.NewReader(data))
			if err != nil {
				return
			}

			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("X-Node-ID", m.config.NodeID)

			resp, err := m.client.Do(httpReq)
			if err != nil {
				m.markPeerUnhealthy(p.ID)
				return
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				m.markPeerHealthy(p.ID)
			}
		}(peer)
	}
}

func (m *EdgeMesh) checkPeerHealth() {
	now := time.Now()

	m.peersMu.Lock()
	defer m.peersMu.Unlock()

	for _, peer := range m.peers {
		if now.Sub(peer.LastSeen) > m.config.PeerTimeout {
			peer.Healthy = false
		}
	}
}

func (m *EdgeMesh) addPeer(id, addr string) {
	if id == m.config.NodeID {
		return // Don't add ourselves
	}

	m.peersMu.Lock()
	defer m.peersMu.Unlock()

	if len(m.peers) >= m.config.MaxPeers {
		return // At capacity
	}

	if _, exists := m.peers[id]; !exists {
		m.peers[id] = &MeshPeer{
			ID:          id,
			Addr:        addr,
			LastSeen:    time.Now(),
			Healthy:     true,
			VectorClock: make(map[string]uint64),
			Metadata:    make(map[string]string),
		}
	}
}

func (m *EdgeMesh) markPeerHealthy(id string) {
	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	if peer, ok := m.peers[id]; ok {
		peer.Healthy = true
		peer.LastSeen = time.Now()
	}
}

func (m *EdgeMesh) markPeerUnhealthy(id string) {
	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	if peer, ok := m.peers[id]; ok {
		peer.Healthy = false
	}
}

func (m *EdgeMesh) connectPeer(addr string) {
	// Send initial gossip to discover peer info
	gossip := MeshGossipMessage{
		NodeID:      m.config.NodeID,
		Addr:        m.config.AdvertiseAddr,
		VectorClock: m.vectorClock.GetAll(),
		Peers:       m.getPeerList(),
	}

	data, _ := json.Marshal(gossip)

	httpReq, err := http.NewRequestWithContext(m.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/mesh/gossip", addr), bytes.NewReader(data))
	if err != nil {
		return
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Node-ID", m.config.NodeID)

	resp, err := m.client.Do(httpReq)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var respGossip MeshGossipMessage
	if err := json.NewDecoder(resp.Body).Decode(&respGossip); err != nil {
		return
	}

	// Add the peer
	m.addPeer(respGossip.NodeID, addr)

	// Add any peers it knows about
	for _, p := range respGossip.Peers {
		m.addPeer(p.ID, p.Addr)
	}
}

func (m *EdgeMesh) recordSyncError(peerID string, err error) {
	m.syncStateMu.Lock()
	defer m.syncStateMu.Unlock()
	if state, ok := m.syncState[peerID]; ok {
		state.LastError = err.Error()
	}
}

// --- HTTP Handlers ---

func (m *EdgeMesh) handleSync(w http.ResponseWriter, r *http.Request) {
	var req MeshSyncRequest

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Decompress if needed
	if r.Header.Get("Content-Encoding") == "gzip" {
		gr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body, _ = io.ReadAll(gr)
		gr.Close()
	}

	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Apply received operations
	for _, op := range req.Operations {
		_ = m.applyOperation(op)
	}

	// Get our operations that the peer doesn't have
	opsToSend := m.opLog.GetSince(req.VectorClock)

	// Limit response size
	if len(opsToSend) > m.config.SyncBatchSize {
		opsToSend = opsToSend[:m.config.SyncBatchSize]
	}

	resp := MeshSyncResponse{
		NodeID:      m.config.NodeID,
		VectorClock: m.vectorClock.GetAll(),
		Operations:  opsToSend,
	}

	respData, _ := json.Marshal(resp)

	// Compress response if requested
	if m.config.CompressSync && r.Header.Get("Accept-Encoding") == "gzip" {
		w.Header().Set("Content-Encoding", "gzip")
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		gw.Write(respData)
		gw.Close()
		respData = buf.Bytes()
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(respData)
}

func (m *EdgeMesh) handleGossip(w http.ResponseWriter, r *http.Request) {
	var gossip MeshGossipMessage
	if err := json.NewDecoder(r.Body).Decode(&gossip); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Add/update the sender
	m.addPeer(gossip.NodeID, gossip.Addr)
	m.markPeerHealthy(gossip.NodeID)

	// Add any peers they know about
	for _, p := range gossip.Peers {
		m.addPeer(p.ID, p.Addr)
	}

	// Merge vector clock
	m.vectorClock.Merge(gossip.VectorClock)

	// Respond with our state
	resp := MeshGossipMessage{
		NodeID:      m.config.NodeID,
		Addr:        m.config.AdvertiseAddr,
		VectorClock: m.vectorClock.GetAll(),
		Peers:       m.getPeerList(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (m *EdgeMesh) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var heartbeat MeshHeartbeat
	if err := json.NewDecoder(r.Body).Decode(&heartbeat); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Update peer state
	m.markPeerHealthy(heartbeat.NodeID)

	// Merge vector clock
	m.vectorClock.Merge(heartbeat.VectorClock)

	w.WriteHeader(http.StatusOK)
}

func (m *EdgeMesh) handleOperations(w http.ResponseWriter, r *http.Request) {
	var req MeshOperationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Apply the operation
	if err := m.applyOperation(req.Operation); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (m *EdgeMesh) handleState(w http.ResponseWriter, r *http.Request) {
	state := MeshStateResponse{
		NodeID:      m.config.NodeID,
		VectorClock: m.vectorClock.GetAll(),
		PeerCount:   len(m.peers),
		OpLogSize:   m.opLog.Len(),
		Stats:       m.Stats(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}

// --- mDNS Discovery ---

type mdnsServer struct {
	service string
	nodeID  string
	addr    string
	running atomic.Bool
	cancel  context.CancelFunc
}

func (m *EdgeMesh) startMDNS() {
	if m.config.MDNSService == "" {
		return
	}

	ctx, cancel := context.WithCancel(m.ctx)
	m.mdnsServer = &mdnsServer{
		service: m.config.MDNSService,
		nodeID:  m.config.NodeID,
		addr:    m.config.AdvertiseAddr,
		cancel:  cancel,
	}
	m.mdnsServer.running.Store(true)

	// Start mDNS advertiser
	go m.mdnsAdvertise(ctx)

	// Start mDNS browser
	go m.mdnsBrowse(ctx)
}

func (m *EdgeMesh) mdnsAdvertise(ctx context.Context) {
	// Simplified mDNS - in production would use proper mDNS library
	addr, err := net.ResolveUDPAddr("udp4", "224.0.0.251:5353")
	if err != nil {
		return
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	announcement := fmt.Sprintf("%s._chronicle._tcp.local\tIN\tTXT\t\"id=%s\" \"addr=%s\"",
		m.config.NodeID, m.config.NodeID, m.config.AdvertiseAddr)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conn.WriteToUDP([]byte(announcement), addr)
		}
	}
}

func (m *EdgeMesh) mdnsBrowse(ctx context.Context) {
	addr, err := net.ResolveUDPAddr("udp4", "224.0.0.251:5353")
	if err != nil {
		return
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		return
	}
	defer conn.Close()

	buf := make([]byte, 1500)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn.SetReadDeadline(time.Now().Add(time.Second))
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				continue
			}

			// Parse mDNS response (simplified)
			response := string(buf[:n])
			if strings.Contains(response, "_chronicle._tcp") {
				// Extract peer info and add
				// This is simplified - real implementation would parse DNS records
			}
		}
	}
}

func (s *mdnsServer) Stop() {
	if !s.running.Swap(false) {
		return
	}
	s.cancel()
}

// --- Message Types ---

// MeshSyncRequest is sent to synchronize with a peer.
type MeshSyncRequest struct {
	NodeID      string            `json:"node_id"`
	VectorClock map[string]uint64 `json:"vector_clock"`
	Operations  []CRDTOperation   `json:"operations"`
}

// MeshSyncResponse is the response to a sync request.
type MeshSyncResponse struct {
	NodeID      string            `json:"node_id"`
	VectorClock map[string]uint64 `json:"vector_clock"`
	Operations  []CRDTOperation   `json:"operations"`
}

// MeshGossipMessage is used for peer discovery.
type MeshGossipMessage struct {
	NodeID      string            `json:"node_id"`
	Addr        string            `json:"addr"`
	VectorClock map[string]uint64 `json:"vector_clock"`
	Peers       []MeshPeerInfo    `json:"peers"`
}

// MeshPeerInfo contains basic peer information.
type MeshPeerInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// MeshHeartbeat is sent for health checking.
type MeshHeartbeat struct {
	NodeID      string            `json:"node_id"`
	Timestamp   int64             `json:"timestamp"`
	VectorClock map[string]uint64 `json:"vector_clock"`
}

// MeshOperationRequest sends a single operation to a peer.
type MeshOperationRequest struct {
	NodeID    string        `json:"node_id"`
	Operation CRDTOperation `json:"operation"`
}

// MeshStateResponse returns mesh state information.
type MeshStateResponse struct {
	NodeID      string            `json:"node_id"`
	VectorClock map[string]uint64 `json:"vector_clock"`
	PeerCount   int               `json:"peer_count"`
	OpLogSize   int               `json:"op_log_size"`
	Stats       EdgeMeshStats     `json:"stats"`
}
