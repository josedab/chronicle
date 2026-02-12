package edgemesh

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
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
	db     *chronicle.DB
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
	PeerID          string            `json:"peer_id"`
	LastSyncTime    time.Time         `json:"last_sync_time"`
	LastVectorClock map[string]uint64 `json:"last_vector_clock"`
	SentOpCount     uint64            `json:"sent_op_count"`
	ReceivedOpCount uint64            `json:"received_op_count"`
	Conflicts       uint64            `json:"conflicts"`
	LastError       string            `json:"last_error,omitempty"`
}

// EdgeMeshStats contains mesh statistics.
type EdgeMeshStats struct {
	NodeID            string        `json:"node_id"`
	PeerCount         int           `json:"peer_count"`
	HealthyPeers      int           `json:"healthy_peers"`
	OperationCount    uint64        `json:"operation_count"`
	SyncCount         uint64        `json:"sync_count"`
	ConflictsResolved uint64        `json:"conflicts_resolved"`
	BytesSent         int64         `json:"bytes_sent"`
	BytesReceived     int64         `json:"bytes_received"`
	LastSyncTime      time.Time     `json:"last_sync_time"`
	Uptime            time.Duration `json:"uptime"`
}

// NewEdgeMesh creates a new edge mesh network.
func NewEdgeMesh(db *chronicle.DB, config EdgeMeshConfig) (*EdgeMesh, error) {
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

	mesh.vectorClock.Tick(config.NodeID)

	return mesh, nil
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

type mdnsServer struct {
	service string
	nodeID  string
	addr    string
	running atomic.Bool
	cancel  context.CancelFunc
}

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
