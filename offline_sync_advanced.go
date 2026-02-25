package chronicle

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"
)

// Advanced CRDT types (PNCounter), Merkle tree anti-entropy, and conflict audit logging for offline sync.

func mergeOfflineTags(a, b map[string]string) map[string]string {
	merged := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		merged[k] = v
	}
	for k, v := range b {
		merged[k] = v
	}
	return merged
}

// --- SchemaEvolution ---

// SchemaVersion represents a versioned schema for time-series data.
type SchemaVersion struct {
	Version int               `json:"version"`
	Fields  map[string]string `json:"fields"`
	AddedAt time.Time         `json:"added_at"`
	NodeID  string            `json:"node_id"`
}

// SchemaEvolutionManager manages schema versions across disconnected nodes.
type SchemaEvolutionManager struct {
	versions       []*SchemaVersion
	currentVersion int
	mu             sync.RWMutex
}

// NewSchemaEvolutionManager creates a new schema evolution manager.
func NewSchemaEvolutionManager() *SchemaEvolutionManager {
	initial := &SchemaVersion{
		Version: 1,
		Fields:  map[string]string{"value": "float64", "timestamp": "int64"},
		AddedAt: time.Now(),
		NodeID:  "system",
	}
	return &SchemaEvolutionManager{
		versions:       []*SchemaVersion{initial},
		currentVersion: 1,
	}
}

// AddField adds a new field to the schema, creating a new version.
func (sem *SchemaEvolutionManager) AddField(name, fieldType, nodeID string) *SchemaVersion {
	sem.mu.Lock()
	defer sem.mu.Unlock()

	current := sem.versions[len(sem.versions)-1]
	fields := make(map[string]string, len(current.Fields)+1)
	for k, v := range current.Fields {
		fields[k] = v
	}
	fields[name] = fieldType

	sv := &SchemaVersion{
		Version: sem.currentVersion + 1,
		Fields:  fields,
		AddedAt: time.Now(),
		NodeID:  nodeID,
	}
	sem.versions = append(sem.versions, sv)
	sem.currentVersion = sv.Version
	return sv
}

// MergeSchema merges a remote schema version into the local history.
func (sem *SchemaEvolutionManager) MergeSchema(remote *SchemaVersion) (*SchemaVersion, error) {
	if remote == nil {
		return nil, fmt.Errorf("nil remote schema")
	}

	sem.mu.Lock()
	defer sem.mu.Unlock()

	current := sem.versions[len(sem.versions)-1]

	// Merge fields from remote into current
	fields := make(map[string]string, len(current.Fields)+len(remote.Fields))
	for k, v := range current.Fields {
		fields[k] = v
	}
	for k, v := range remote.Fields {
		if existing, ok := fields[k]; ok && existing != v {
			return nil, fmt.Errorf("schema conflict: field %q type mismatch (%s vs %s)", k, existing, v)
		}
		fields[k] = v
	}

	sv := &SchemaVersion{
		Version: sem.currentVersion + 1,
		Fields:  fields,
		AddedAt: time.Now(),
		NodeID:  remote.NodeID,
	}
	sem.versions = append(sem.versions, sv)
	sem.currentVersion = sv.Version
	return sv, nil
}

// CurrentVersion returns the current schema version.
func (sem *SchemaEvolutionManager) CurrentVersion() *SchemaVersion {
	sem.mu.RLock()
	defer sem.mu.RUnlock()
	if len(sem.versions) == 0 {
		return nil
	}
	return sem.versions[len(sem.versions)-1]
}

// IsCompatible returns true if the given version is still compatible.
func (sem *SchemaEvolutionManager) IsCompatible(version int) bool {
	sem.mu.RLock()
	defer sem.mu.RUnlock()
	return version >= 1 && version <= sem.currentVersion
}

// --- OfflineSyncManager: main orchestrator ---

// OfflinePeerSyncState tracks sync state with a specific peer.
type OfflinePeerSyncState struct {
	PeerID         string       `json:"peer_id"`
	LastSyncTime   time.Time    `json:"last_sync_time"`
	LastClock      *VectorClock `json:"-"`
	PointsSent     int64        `json:"points_sent"`
	PointsReceived int64        `json:"points_received"`
	Conflicts      int64        `json:"conflicts"`
	Online         bool         `json:"online"`
}

// OfflineSyncResult represents the outcome of applying a sync delta.
type OfflineSyncResult struct {
	PointsApplied int              `json:"points_applied"`
	Conflicts     []ConflictRecord `json:"conflicts"`
	SchemaUpdated bool             `json:"schema_updated"`
	Duration      time.Duration    `json:"duration"`
}

// OfflineSyncStats provides statistics about offline sync operations.
type OfflineSyncStats struct {
	TotalSyncs      int64         `json:"total_syncs"`
	TotalConflicts  int64         `json:"total_conflicts"`
	PointsTracked   int           `json:"points_tracked"`
	PeerCount       int           `json:"peer_count"`
	OfflineDuration time.Duration `json:"offline_duration"`
	DeltaSizeBytes  int64         `json:"delta_size_bytes"`
}

// OfflineSyncManager orchestrates the offline-first sync protocol.
type OfflineSyncManager struct {
	config   OfflineSyncConfig
	state    *DeltaState
	resolver *ConflictResolver
	schema   *SchemaEvolutionManager
	peers    map[string]*OfflinePeerSyncState
	mu       sync.RWMutex
	running  bool
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	totalSyncs     int64
	totalConflicts int64
}

// NewOfflineSyncManager creates a new offline sync manager.
func NewOfflineSyncManager(config OfflineSyncConfig) *OfflineSyncManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &OfflineSyncManager{
		config:   config,
		state:    NewDeltaState(config.NodeID, &config),
		resolver: NewConflictResolver(config.ConflictStrategy),
		schema:   NewSchemaEvolutionManager(),
		peers:    make(map[string]*OfflinePeerSyncState),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start begins the offline sync manager background loop.
func (osm *OfflineSyncManager) Start() {
	osm.mu.Lock()
	if osm.running {
		osm.mu.Unlock()
		return
	}
	osm.running = true
	osm.mu.Unlock()

	osm.wg.Add(1)
	go func() {
		defer osm.wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-osm.ctx.Done():
				return
			case <-ticker.C:
				// Periodic maintenance: nothing to do without network I/O.
			}
		}
	}()
}

// Stop gracefully shuts down the offline sync manager.
func (osm *OfflineSyncManager) Stop() {
	osm.mu.Lock()
	if !osm.running {
		osm.mu.Unlock()
		return
	}
	osm.running = false
	osm.mu.Unlock()

	osm.cancel()
	osm.wg.Wait()
}

// RecordWrite tracks a local write for later synchronization.
func (osm *OfflineSyncManager) RecordWrite(point *Point) {
	osm.state.RecordChange(point)
}

// PrepareSyncDelta creates a delta payload for a specific peer.
func (osm *OfflineSyncManager) PrepareSyncDelta(peerID string) (*SyncDelta, error) {
	osm.mu.RLock()
	peer, ok := osm.peers[peerID]
	osm.mu.RUnlock()

	var since int64
	if ok {
		since = peer.LastSyncTime.UnixNano()
	}

	delta := osm.state.GetDelta(since)
	if delta == nil || len(delta.Entries) == 0 {
		return nil, fmt.Errorf("no changes to sync with peer %s", peerID)
	}

	if osm.config.SchemaEvolutionEnabled {
		delta.SchemaVersion = osm.schema.CurrentVersion().Version
	}

	// Enforce batch size limit.
	if len(delta.Entries) > osm.config.SyncBatchSize {
		delta.Entries = delta.Entries[:osm.config.SyncBatchSize]
	}

	osm.mu.Lock()
	if !ok {
		peer = &OfflinePeerSyncState{PeerID: peerID, LastClock: NewVectorClock()}
		osm.peers[peerID] = peer
	}
	peer.PointsSent += int64(len(delta.Entries))
	osm.mu.Unlock()

	return delta, nil
}

// ReceiveSyncDelta applies a delta received from a remote peer.
func (osm *OfflineSyncManager) ReceiveSyncDelta(delta *SyncDelta) (*OfflineSyncResult, error) {
	start := time.Now()

	if delta == nil {
		return nil, fmt.Errorf("nil sync delta")
	}

	conflicts, err := osm.state.ApplyDelta(delta)
	if err != nil {
		return nil, fmt.Errorf("apply delta: %w", err)
	}

	result := &OfflineSyncResult{
		PointsApplied: len(delta.Entries) - len(conflicts),
		Conflicts:     conflicts,
		Duration:      time.Since(start),
	}

	// Handle schema evolution.
	if osm.config.SchemaEvolutionEnabled && delta.SchemaVersion > 0 {
		cv := osm.schema.CurrentVersion()
		if cv != nil && delta.SchemaVersion > cv.Version {
			result.SchemaUpdated = true
		}
	}

	osm.mu.Lock()
	peer, ok := osm.peers[delta.SourceNode]
	if !ok {
		peer = &OfflinePeerSyncState{PeerID: delta.SourceNode, LastClock: NewVectorClock()}
		osm.peers[delta.SourceNode] = peer
	}
	peer.LastSyncTime = time.Now()
	peer.PointsReceived += int64(len(delta.Entries))
	peer.Conflicts += int64(len(conflicts))
	if delta.VectorClock != nil {
		peer.LastClock = delta.VectorClock.Copy()
	}
	osm.totalSyncs++
	osm.totalConflicts += int64(len(conflicts))
	osm.mu.Unlock()

	return result, nil
}

// GetPeerStatus returns the sync state for a specific peer.
func (osm *OfflineSyncManager) GetPeerStatus(peerID string) *OfflinePeerSyncState {
	osm.mu.RLock()
	defer osm.mu.RUnlock()
	return osm.peers[peerID]
}

// ListPeers returns the sync state for all known peers.
func (osm *OfflineSyncManager) ListPeers() []*OfflinePeerSyncState {
	osm.mu.RLock()
	defer osm.mu.RUnlock()
	peers := make([]*OfflinePeerSyncState, 0, len(osm.peers))
	for _, p := range osm.peers {
		peers = append(peers, p)
	}
	return peers
}

// Stats returns current offline sync statistics.
func (osm *OfflineSyncManager) Stats() *OfflineSyncStats {
	osm.mu.RLock()
	defer osm.mu.RUnlock()

	osm.state.mu.RLock()
	tracked := len(osm.state.changes)
	osm.state.mu.RUnlock()

	return &OfflineSyncStats{
		TotalSyncs:     osm.totalSyncs,
		TotalConflicts: osm.totalConflicts,
		PointsTracked:  tracked,
		PeerCount:      len(osm.peers),
	}
}

// --- PNCounter: positive-negative counter CRDT ---

// PNCounter supports both increments and decrements across distributed nodes.
// It uses two G-Counters internally: one for increments and one for decrements.
type PNCounter struct {
	positive *GCounter
	negative *GCounter
	mu       sync.RWMutex
}

// NewPNCounter creates a new positive-negative counter.
func NewPNCounter(nodeID string) *PNCounter {
	return &PNCounter{
		positive: NewGCounter(nodeID),
		negative: NewGCounter(nodeID),
	}
}

// Increment adds to the counter.
func (pn *PNCounter) Increment(nodeID string, delta int64) {
	pn.mu.Lock()
	defer pn.mu.Unlock()
	pn.positive.Increment(nodeID, delta)
}

// Decrement subtracts from the counter.
func (pn *PNCounter) Decrement(nodeID string, delta int64) {
	pn.mu.Lock()
	defer pn.mu.Unlock()
	pn.negative.Increment(nodeID, delta)
}

// Value returns the current counter value (increments - decrements).
func (pn *PNCounter) Value() int64 {
	pn.mu.RLock()
	defer pn.mu.RUnlock()
	return pn.positive.Value() - pn.negative.Value()
}

// Merge merges another PNCounter into this one.
func (pn *PNCounter) Merge(other *PNCounter) {
	pn.mu.Lock()
	defer pn.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()
	pn.positive.Merge(other.positive)
	pn.negative.Merge(other.negative)
}

// --- Merkle Tree Anti-Entropy for Delta Sync ---

// SyncMerkleNode is a node in a Merkle tree for efficient delta comparison.
type SyncMerkleNode struct {
	Hash     [32]byte           `json:"hash"`
	Level    int                `json:"level"`
	Min      int64              `json:"min"` // min timestamp
	Max      int64              `json:"max"` // max timestamp
	Children [2]*SyncMerkleNode `json:"children,omitempty"`
	Count    int                `json:"count"`
}

// SyncMerkleTree enables bandwidth-efficient anti-entropy by comparing
// data hashes at progressively finer granularity.
type SyncMerkleTree struct {
	root   *SyncMerkleNode
	depth  int
	nodeID string
	mu     sync.RWMutex
}

// NewSyncMerkleTree creates a new Merkle tree for sync comparison.
func NewSyncMerkleTree(nodeID string, depth int) *SyncMerkleTree {
	if depth < 1 {
		depth = 8
	}
	return &SyncMerkleTree{
		root: &SyncMerkleNode{
			Level: 0,
			Min:   0,
			Max:   1<<62 - 1,
		},
		depth:  depth,
		nodeID: nodeID,
	}
}

// Insert adds a data point hash to the tree.
func (t *SyncMerkleTree) Insert(timestamp int64, data []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	h := sha256.Sum256(data)
	t.insertNode(t.root, timestamp, h, 0)
}

func (t *SyncMerkleTree) insertNode(node *SyncMerkleNode, ts int64, h [32]byte, level int) {
	// XOR the hash into this node
	for i := range node.Hash {
		node.Hash[i] ^= h[i]
	}
	node.Count++

	if level >= t.depth {
		return
	}

	mid := node.Min + (node.Max-node.Min)/2
	if node.Children[0] == nil {
		node.Children[0] = &SyncMerkleNode{Level: level + 1, Min: node.Min, Max: mid}
		node.Children[1] = &SyncMerkleNode{Level: level + 1, Min: mid + 1, Max: node.Max}
	}

	if ts <= mid {
		t.insertNode(node.Children[0], ts, h, level+1)
	} else {
		t.insertNode(node.Children[1], ts, h, level+1)
	}
}

// RootHash returns the root hash for quick comparison.
func (t *SyncMerkleTree) RootHash() [32]byte {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.root.Hash
}

// DiffTimeRanges compares two trees and returns time ranges that differ.
func (t *SyncMerkleTree) DiffTimeRanges(other *SyncMerkleTree) []SyncTimeRange {
	t.mu.RLock()
	defer t.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	var diffs []SyncTimeRange
	t.diffNodes(t.root, other.root, &diffs)
	return diffs
}

// SyncTimeRange represents a time range that needs synchronization.
type SyncTimeRange struct {
	Min   int64 `json:"min"`
	Max   int64 `json:"max"`
	Count int   `json:"count"` // approximate number of points
}

func (t *SyncMerkleTree) diffNodes(a, b *SyncMerkleNode, diffs *[]SyncTimeRange) {
	if a == nil && b == nil {
		return
	}
	if a == nil || b == nil || a.Hash != b.Hash {
		min := int64(0)
		max := int64(1<<62 - 1)
		count := 0
		if a != nil {
			min, max, count = a.Min, a.Max, a.Count
		} else if b != nil {
			min, max, count = b.Min, b.Max, b.Count
		}

		if a == nil || b == nil || a.Children[0] == nil || b.Children[0] == nil {
			*diffs = append(*diffs, SyncTimeRange{Min: min, Max: max, Count: count})
			return
		}

		t.diffNodes(a.Children[0], b.Children[0], diffs)
		t.diffNodes(a.Children[1], b.Children[1], diffs)
	}
}

// --- Conflict Audit Logging ---

// ConflictAuditEntry records a conflict resolution event for audit purposes.
type ConflictAuditEntry struct {
	ID              string            `json:"id"`
	Metric          string            `json:"metric"`
	LocalNodeID     string            `json:"local_node_id"`
	RemoteNodeID    string            `json:"remote_node_id"`
	Strategy        string            `json:"strategy"`
	LocalValue      float64           `json:"local_value"`
	RemoteValue     float64           `json:"remote_value"`
	ResolvedValue   float64           `json:"resolved_value"`
	LocalTimestamp  int64             `json:"local_timestamp"`
	RemoteTimestamp int64             `json:"remote_timestamp"`
	ResolvedAt      time.Time         `json:"resolved_at"`
	Tags            map[string]string `json:"tags,omitempty"`
}

// ConflictAuditLog maintains an append-only log of all conflict resolutions.
type ConflictAuditLog struct {
	entries []ConflictAuditEntry
	maxSize int
	mu      sync.RWMutex
}

// NewConflictAuditLog creates a new audit log with a max retention size.
func NewConflictAuditLog(maxSize int) *ConflictAuditLog {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &ConflictAuditLog{
		entries: make([]ConflictAuditEntry, 0, 100),
		maxSize: maxSize,
	}
}

// Record appends a conflict resolution event to the audit log.
func (l *ConflictAuditLog) Record(entry ConflictAuditEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if entry.ResolvedAt.IsZero() {
		entry.ResolvedAt = time.Now()
	}
	if entry.ID == "" {
		entry.ID = fmt.Sprintf("conflict-%d", time.Now().UnixNano())
	}

	if len(l.entries) >= l.maxSize {
		// Evict oldest 10%
		evict := l.maxSize / 10
		if evict < 1 {
			evict = 1
		}
		l.entries = l.entries[evict:]
	}

	l.entries = append(l.entries, entry)
}

// Entries returns all audit entries.
func (l *ConflictAuditLog) Entries() []ConflictAuditEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := make([]ConflictAuditEntry, len(l.entries))
	copy(result, l.entries)
	return result
}

// EntriesSince returns entries after the given timestamp.
func (l *ConflictAuditLog) EntriesSince(since time.Time) []ConflictAuditEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var result []ConflictAuditEntry
	for _, e := range l.entries {
		if e.ResolvedAt.After(since) {
			result = append(result, e)
		}
	}
	return result
}

// Count returns the total number of recorded conflicts.
func (l *ConflictAuditLog) Count() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

// Summary returns aggregate statistics about conflicts.
func (l *ConflictAuditLog) Summary() map[string]interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()

	byStrategy := make(map[string]int)
	byMetric := make(map[string]int)
	for _, e := range l.entries {
		byStrategy[e.Strategy]++
		byMetric[e.Metric]++
	}

	return map[string]interface{}{
		"total_conflicts":  len(l.entries),
		"by_strategy":      byStrategy,
		"by_metric":        byMetric,
		"metrics_affected": len(byMetric),
	}
}
