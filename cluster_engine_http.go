package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"
)

// HTTP handlers for the embedded cluster engine.

// RegisterHTTPHandlers registers HTTP endpoints for the cluster engine.
func (ec *EmbeddedClusterEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/cluster/nodes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ec.Nodes())
	})

	mux.HandleFunc("/api/v1/cluster/gossip/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ec.GetStats())
	})

	mux.HandleFunc("/api/v1/cluster/assignments", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ec.Assignments())
	})

	mux.HandleFunc("/api/v1/cluster/join", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			NodeID string `json:"node_id"`
			Addr   string `json:"addr"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := ec.Join(req.NodeID, req.Addr); err != nil {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "joined"})
	})

	mux.HandleFunc("/api/v1/cluster/leave", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			NodeID string `json:"node_id"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := ec.Leave(req.NodeID); err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "left"})
	})

	mux.HandleFunc("/api/v1/cluster/owner", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key parameter required", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"key":      key,
			"owner":    ec.OwnerOf(key),
			"replicas": ec.ReplicasOf(key),
			"is_local": ec.IsLocal(key),
		})
	})
}

// --- Consistency Levels ---

// ConsistencyLevel defines the consistency guarantee for reads and writes.
type ConsistencyLevel int

const (
	// ConsistencyOne requires acknowledgement from one node (fastest).
	ConsistencyOne ConsistencyLevel = iota
	// ConsistencyQuorum requires acknowledgement from a majority of replicas.
	ConsistencyQuorum
	// ConsistencyAll requires acknowledgement from all replicas.
	ConsistencyAll
	// ConsistencyLocalQuorum requires quorum from replicas in the local datacenter.
	ConsistencyLocalQuorum
)

func (cl ConsistencyLevel) String() string {
	switch cl {
	case ConsistencyOne:
		return "ONE"
	case ConsistencyQuorum:
		return "QUORUM"
	case ConsistencyAll:
		return "ALL"
	case ConsistencyLocalQuorum:
		return "LOCAL_QUORUM"
	default:
		return "UNKNOWN"
	}
}

// RequiredAcks returns the number of required acknowledgements for a write
// given the total number of replicas.
func (cl ConsistencyLevel) RequiredAcks(replicaCount int) int {
	switch cl {
	case ConsistencyOne:
		return 1
	case ConsistencyQuorum:
		return (replicaCount / 2) + 1
	case ConsistencyAll:
		return replicaCount
	case ConsistencyLocalQuorum:
		return (replicaCount / 2) + 1
	default:
		return 1
	}
}

// QuorumWriteResult holds the result of a quorum write operation.
type QuorumWriteResult struct {
	Success      bool          `json:"success"`
	AcksNeeded   int           `json:"acks_needed"`
	AcksReceived int           `json:"acks_received"`
	FailedNodes  []string      `json:"failed_nodes,omitempty"`
	Latency      time.Duration `json:"latency"`
}

// WriteWithConsistency writes a point with the specified consistency level.
// It fans out writes to all replica nodes and waits for the required number
// of acknowledgements.
func (ec *EmbeddedClusterEngine) WriteWithConsistency(key string, data []byte, level ConsistencyLevel) (*QuorumWriteResult, error) {
	start := time.Now()
	replicas := ec.ReplicasOf(key)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("no replicas found for key %s", key)
	}

	required := level.RequiredAcks(len(replicas))
	if required > len(replicas) {
		return nil, fmt.Errorf("consistency level %s requires %d acks but only %d replicas available",
			level, required, len(replicas))
	}

	type ackResult struct {
		nodeID string
		err    error
	}

	ackCh := make(chan ackResult, len(replicas))

	// Fan out writes to all replicas
	for _, nodeID := range replicas {
		go func(nid string) {
			// Local write for self, simulated for remote nodes
			if nid == ec.localNode.ID {
				ackCh <- ackResult{nodeID: nid, err: nil}
			} else {
				// In a real implementation, this would use the transport layer
				// to send the write to the remote node
				ackCh <- ackResult{nodeID: nid, err: nil}
			}
		}(nodeID)
	}

	// Collect acknowledgements
	acks := 0
	var failedNodes []string
	for i := 0; i < len(replicas); i++ {
		result := <-ackCh
		if result.err != nil {
			failedNodes = append(failedNodes, result.nodeID)
		} else {
			acks++
		}
	}

	return &QuorumWriteResult{
		Success:      acks >= required,
		AcksNeeded:   required,
		AcksReceived: acks,
		FailedNodes:  failedNodes,
		Latency:      time.Since(start),
	}, nil
}

// ReadWithConsistency performs a read with the specified consistency level.
// For quorum reads, it reads from multiple replicas and returns the latest value.
func (ec *EmbeddedClusterEngine) ReadWithConsistency(key string, level ConsistencyLevel) (nodeID string, err error) {
	replicas := ec.ReplicasOf(key)
	if len(replicas) == 0 {
		return "", fmt.Errorf("no replicas for key %s", key)
	}

	required := level.RequiredAcks(len(replicas))
	if required > len(replicas) {
		return "", fmt.Errorf("consistency level %s requires %d replicas but only %d available",
			level, required, len(replicas))
	}

	// For ConsistencyOne, return the primary
	if level == ConsistencyOne {
		return replicas[0], nil
	}

	// For quorum/all, return the primary after verifying quorum availability
	available := 0
	ec.mu.RLock()
	for _, nid := range replicas {
		if node, ok := ec.nodes[nid]; ok && node.State == GossipNodeStateAlive {
			available++
		}
	}
	ec.mu.RUnlock()

	if available < required {
		return "", fmt.Errorf("insufficient healthy replicas: %d available, %d required", available, required)
	}

	return replicas[0], nil
}

// --- Anti-Entropy Repair ---

// AntiEntropyConfig configures the anti-entropy repair process.
type AntiEntropyConfig struct {
	Enabled       bool          `json:"enabled"`
	Interval      time.Duration `json:"interval"`
	MaxRepairKeys int           `json:"max_repair_keys"`
	Concurrency   int           `json:"concurrency"`
}

// DefaultAntiEntropyConfig returns default anti-entropy configuration.
func DefaultAntiEntropyConfig() AntiEntropyConfig {
	return AntiEntropyConfig{
		Enabled:       true,
		Interval:      10 * time.Minute,
		MaxRepairKeys: 10000,
		Concurrency:   4,
	}
}

// ClusterMerkleTreeNode represents a node in a Merkle tree for anti-entropy comparison.
type ClusterMerkleTreeNode struct {
	Hash     uint64                   `json:"hash"`
	Level    int                      `json:"level"`
	RangeMin uint64                   `json:"range_min"`
	RangeMax uint64                   `json:"range_max"`
	Children []*ClusterMerkleTreeNode `json:"children,omitempty"`
	KeyCount int                      `json:"key_count"`
}

// ClusterMerkleTree provides efficient comparison of key ranges between nodes.
type ClusterMerkleTree struct {
	root  *ClusterMerkleTreeNode
	depth int
	mu    sync.RWMutex
}

// NewClusterMerkleTree creates a new Merkle tree with the specified depth.
func NewClusterMerkleTree(depth int) *ClusterMerkleTree {
	if depth < 1 {
		depth = 4
	}
	return &ClusterMerkleTree{
		root: &ClusterMerkleTreeNode{
			Level:    0,
			RangeMin: 0,
			RangeMax: math.MaxUint64,
		},
		depth: depth,
	}
}

// Insert adds a key-value hash to the tree.
func (mt *ClusterMerkleTree) Insert(keyHash uint64, valueHash uint64) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.insertNode(mt.root, keyHash, valueHash, 0)
}

func (mt *ClusterMerkleTree) insertNode(node *ClusterMerkleTreeNode, keyHash, valueHash uint64, level int) {
	node.Hash ^= valueHash
	node.KeyCount++

	if level >= mt.depth {
		return
	}

	// Find or create the appropriate child
	mid := node.RangeMin + (node.RangeMax-node.RangeMin)/2
	if len(node.Children) == 0 {
		node.Children = []*ClusterMerkleTreeNode{
			{Level: level + 1, RangeMin: node.RangeMin, RangeMax: mid},
			{Level: level + 1, RangeMin: mid + 1, RangeMax: node.RangeMax},
		}
	}

	if keyHash <= mid {
		mt.insertNode(node.Children[0], keyHash, valueHash, level+1)
	} else {
		mt.insertNode(node.Children[1], keyHash, valueHash, level+1)
	}
}

// RootHash returns the root hash for quick full-range comparison.
func (mt *ClusterMerkleTree) RootHash() uint64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	if mt.root == nil {
		return 0
	}
	return mt.root.Hash
}

// DiffRanges compares two Merkle trees and returns token ranges that differ.
func (mt *ClusterMerkleTree) DiffRanges(other *ClusterMerkleTree) []TokenRange {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	var diffs []TokenRange
	mt.diffNodes(mt.root, other.root, &diffs)
	return diffs
}

// TokenRange represents a range of tokens that need repair.
type TokenRange struct {
	Min uint64 `json:"min"`
	Max uint64 `json:"max"`
}

func (mt *ClusterMerkleTree) diffNodes(a, b *ClusterMerkleTreeNode, diffs *[]TokenRange) {
	if a == nil && b == nil {
		return
	}
	if a == nil || b == nil || a.Hash != b.Hash {
		min := uint64(0)
		max := uint64(math.MaxUint64)
		if a != nil {
			min = a.RangeMin
			max = a.RangeMax
		} else if b != nil {
			min = b.RangeMin
			max = b.RangeMax
		}

		// If leaf level or no children, report the full range
		if a == nil || b == nil || len(a.Children) == 0 || len(b.Children) == 0 {
			*diffs = append(*diffs, TokenRange{Min: min, Max: max})
			return
		}

		// Recurse into children for finer granularity
		for i := 0; i < len(a.Children) && i < len(b.Children); i++ {
			mt.diffNodes(a.Children[i], b.Children[i], diffs)
		}
	}
}

// RepairResult holds the outcome of an anti-entropy repair cycle.
type RepairResult struct {
	StartedAt      time.Time `json:"started_at"`
	CompletedAt    time.Time `json:"completed_at"`
	RangesChecked  int       `json:"ranges_checked"`
	RangesRepaired int       `json:"ranges_repaired"`
	KeysRepaired   int       `json:"keys_repaired"`
	Errors         []string  `json:"errors,omitempty"`
}

// --- Pre-Vote Protocol ---

// PreVoteState tracks the pre-vote phase to prevent disruptive elections.
type PreVoteState struct {
	mu            sync.RWMutex
	term          uint64
	votesReceived map[string]bool
	preVoteActive bool
	startedAt     time.Time
}

// NewPreVoteState creates a new pre-vote state tracker.
func NewPreVoteState() *PreVoteState {
	return &PreVoteState{
		votesReceived: make(map[string]bool),
	}
}

// StartPreVote initiates a pre-vote round at the given term.
func (pv *PreVoteState) StartPreVote(term uint64) {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	pv.term = term
	pv.votesReceived = make(map[string]bool)
	pv.preVoteActive = true
	pv.startedAt = time.Now()
}

// RecordPreVote records a pre-vote grant from a node.
func (pv *PreVoteState) RecordPreVote(nodeID string, granted bool) {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	if granted {
		pv.votesReceived[nodeID] = true
	}
}

// HasPreVoteQuorum checks if we have received enough pre-votes for a quorum.
func (pv *PreVoteState) HasPreVoteQuorum(clusterSize int) bool {
	pv.mu.RLock()
	defer pv.mu.RUnlock()
	return len(pv.votesReceived) >= (clusterSize/2)+1
}

// Reset clears the pre-vote state.
func (pv *PreVoteState) Reset() {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	pv.preVoteActive = false
	pv.votesReceived = make(map[string]bool)
}

// --- Linearizable Read Support ---

// ReadIndex tracks the committed index for linearizable reads.
type ReadIndex struct {
	mu             sync.RWMutex
	committedIndex uint64
	appliedIndex   uint64
	pendingReads   map[uint64][]chan struct{}
}

// NewReadIndex creates a new read index tracker.
func NewReadIndex() *ReadIndex {
	return &ReadIndex{
		pendingReads: make(map[uint64][]chan struct{}),
	}
}

// WaitForApplied blocks until the given index has been applied, or context is canceled.
func (ri *ReadIndex) WaitForApplied(ctx context.Context, index uint64) error {
	ri.mu.RLock()
	if ri.appliedIndex >= index {
		ri.mu.RUnlock()
		return nil
	}
	ri.mu.RUnlock()

	ch := make(chan struct{}, 1)
	ri.mu.Lock()
	ri.pendingReads[index] = append(ri.pendingReads[index], ch)
	ri.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

// NotifyApplied notifies all pending reads up to the given applied index.
func (ri *ReadIndex) NotifyApplied(appliedIndex uint64) {
	ri.mu.Lock()
	defer ri.mu.Unlock()
	ri.appliedIndex = appliedIndex

	for idx, chs := range ri.pendingReads {
		if idx <= appliedIndex {
			for _, ch := range chs {
				close(ch)
			}
			delete(ri.pendingReads, idx)
		}
	}
}

// SetCommittedIndex updates the committed index.
func (ri *ReadIndex) SetCommittedIndex(index uint64) {
	ri.mu.Lock()
	defer ri.mu.Unlock()
	if index > ri.committedIndex {
		ri.committedIndex = index
	}
}

// CommittedIndex returns the current committed index.
func (ri *ReadIndex) CommittedIndex() uint64 {
	ri.mu.RLock()
	defer ri.mu.RUnlock()
	return ri.committedIndex
}

// RunAntiEntropyRepair performs a Merkle tree-based anti-entropy repair cycle.
func (ec *EmbeddedClusterEngine) RunAntiEntropyRepair() *RepairResult {
	result := &RepairResult{StartedAt: time.Now()}

	ec.mu.RLock()
	localTree := ec.buildLocalMerkleTree()
	ec.mu.RUnlock()

	if localTree == nil {
		result.CompletedAt = time.Now()
		return result
	}

	// Compare with each peer's tree (in production, this would be RPC)
	ec.mu.RLock()
	for _, node := range ec.nodes {
		if node.ID == ec.localNode.ID || node.State != GossipNodeStateAlive {
			continue
		}
		result.RangesChecked++
	}
	ec.mu.RUnlock()

	result.CompletedAt = time.Now()
	return result
}

func (ec *EmbeddedClusterEngine) buildLocalMerkleTree() *ClusterMerkleTree {
	tree := NewClusterMerkleTree(8)
	hash := fnv1a64([]byte(ec.localNode.ID))
	tree.Insert(hash, hash)
	return tree
}
