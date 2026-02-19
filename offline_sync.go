package chronicle

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

// CRDTConflictStrategy defines the conflict resolution strategy for CRDTs.
type CRDTConflictStrategy int

const (
	// CRDTLastWriterWins resolves conflicts by choosing the latest write.
	CRDTLastWriterWins CRDTConflictStrategy = iota
	// CRDTMaxValueWins resolves conflicts by choosing the maximum value.
	CRDTMaxValueWins
	// CRDTMergeAll merges all concurrent writes.
	CRDTMergeAll
)

// DeltaOp represents the type of delta operation.
type DeltaOp int

const (
	// DeltaInsert represents a new point insertion.
	DeltaInsert DeltaOp = iota
	// DeltaUpdate represents a point value update.
	DeltaUpdate
	// DeltaDelete represents a point deletion.
	DeltaDelete
)

// OfflineSyncConfig configures the offline-first sync protocol.
type OfflineSyncConfig struct {
	NodeID                 string               `json:"node_id"`
	MaxOfflineDuration     time.Duration        `json:"max_offline_duration"`
	SyncBatchSize          int                  `json:"sync_batch_size"`
	BloomFilterSize        uint                 `json:"bloom_filter_size"`
	BloomFilterHashes      uint                 `json:"bloom_filter_hashes"`
	MaxDeltaSize           int64                `json:"max_delta_size"`
	CompressDeltas         bool                 `json:"compress_deltas"`
	ConflictStrategy       CRDTConflictStrategy `json:"conflict_strategy"`
	SchemaEvolutionEnabled bool                 `json:"schema_evolution_enabled"`
	BandwidthBudgetBps     int64                `json:"bandwidth_budget_bps"`
}

// DefaultOfflineSyncConfig returns a default OfflineSyncConfig.
func DefaultOfflineSyncConfig() OfflineSyncConfig {
	return OfflineSyncConfig{
		NodeID:                 "node-1",
		MaxOfflineDuration:     24 * time.Hour,
		SyncBatchSize:          5000,
		BloomFilterSize:        1024,
		BloomFilterHashes:      3,
		MaxDeltaSize:           64 * 1024 * 1024,
		CompressDeltas:         true,
		ConflictStrategy:       CRDTLastWriterWins,
		SchemaEvolutionEnabled: true,
		BandwidthBudgetBps:     1024 * 1024,
	}
}

// --- VectorClock extensions (struct defined in delta_sync.go) ---

// HappensBefore returns true if this clock causally precedes the other.
func (vc *VectorClock) HappensBefore(other *VectorClock) bool {
	return vc.Compare(other) == -1
}

// IsConcurrent returns true if neither clock causally precedes the other.
func (vc *VectorClock) IsConcurrent(other *VectorClock) bool {
	return vc.Compare(other) == 0
}

// Copy creates a deep copy of the vector clock.
func (vc *VectorClock) Copy() *VectorClock {
	return vc.Clone()
}

// Encode serializes the vector clock to bytes.
func (vc *VectorClock) Encode() []byte {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	data, _ := json.Marshal(vc.clocks)
	return data
}

// DecodeVectorClock deserializes a vector clock from bytes.
func DecodeVectorClock(data []byte) (*VectorClock, error) {
	vc := NewVectorClock()
	vc.mu.Lock()
	defer vc.mu.Unlock()
	if err := json.Unmarshal(data, &vc.clocks); err != nil {
		return nil, fmt.Errorf("decode vector clock: %w", err)
	}
	return vc, nil
}

// --- GCounter: grow-only counter CRDT ---

// GCounter is a grow-only counter that supports distributed increments.
type GCounter struct {
	counts map[string]int64
	mu     sync.RWMutex
}

// NewGCounter creates a new grow-only counter.
func NewGCounter(nodeID string) *GCounter {
	return &GCounter{
		counts: map[string]int64{nodeID: 0},
	}
}

// Increment adds delta to the counter for the given node.
func (gc *GCounter) Increment(nodeID string, delta int64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.counts[nodeID] += delta
}

// Value returns the total counter value across all nodes.
func (gc *GCounter) Value() int64 {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	var total int64
	for _, v := range gc.counts {
		total += v
	}
	return total
}

// Merge merges another GCounter into this one, taking the max per node.
func (gc *GCounter) Merge(other *GCounter) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	for node, count := range other.counts {
		if count > gc.counts[node] {
			gc.counts[node] = count
		}
	}
}

// --- LWWRegister: last-writer-wins register CRDT ---

// LWWRegister is a last-writer-wins register for conflict-free state.
type LWWRegister struct {
	value     any
	timestamp int64
	nodeID    string
	mu        sync.RWMutex
}

// NewLWWRegister creates a new last-writer-wins register.
func NewLWWRegister() *LWWRegister {
	return &LWWRegister{}
}

// Set updates the register if the timestamp is newer, or same timestamp with
// higher nodeID (deterministic tiebreak).
func (r *LWWRegister) Set(value any, timestamp int64, nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if timestamp > r.timestamp || (timestamp == r.timestamp && nodeID > r.nodeID) {
		r.value = value
		r.timestamp = timestamp
		r.nodeID = nodeID
	}
}

// Get returns the current value and its timestamp.
func (r *LWWRegister) Get() (any, int64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.value, r.timestamp
}

// Merge merges another LWWRegister into this one.
func (r *LWWRegister) Merge(other *LWWRegister) {
	other.mu.RLock()
	val, ts, nid := other.value, other.timestamp, other.nodeID
	other.mu.RUnlock()
	r.Set(val, ts, nid)
}

// --- ORSet: observed-remove set CRDT ---

type orSetEntry struct {
	tags map[string]bool
}

// ORSet is an observed-remove set supporting add and remove without conflicts.
type ORSet struct {
	elements map[string]*orSetEntry
	mu       sync.RWMutex
}

// NewORSet creates a new observed-remove set.
func NewORSet() *ORSet {
	return &ORSet{
		elements: make(map[string]*orSetEntry),
	}
}

// Add adds an element with a unique tag.
func (s *ORSet) Add(element string, tag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.elements[element]
	if !ok {
		entry = &orSetEntry{tags: make(map[string]bool)}
		s.elements[element] = entry
	}
	entry.tags[tag] = true
}

// Remove removes an element by clearing all observed tags.
func (s *ORSet) Remove(element string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.elements, element)
}

// Contains returns true if the element is in the set.
func (s *ORSet) Contains(element string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.elements[element]
	return ok && len(entry.tags) > 0
}

// Elements returns all elements currently in the set.
func (s *ORSet) Elements() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, 0, len(s.elements))
	for elem, entry := range s.elements {
		if len(entry.tags) > 0 {
			result = append(result, elem)
		}
	}
	sort.Strings(result)
	return result
}

// Merge merges another ORSet into this one. Tags are unioned; elements with
// no remaining tags after the merge are removed.
func (s *ORSet) Merge(other *ORSet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	for elem, otherEntry := range other.elements {
		entry, ok := s.elements[elem]
		if !ok {
			entry = &orSetEntry{tags: make(map[string]bool)}
			s.elements[elem] = entry
		}
		for tag := range otherEntry.tags {
			entry.tags[tag] = true
		}
	}
}

// --- BloomFilter: space-efficient probabilistic set ---

// BloomFilter provides probabilistic set membership testing.
type BloomFilter struct {
	bits   []byte
	size   uint
	hashes uint
}

// NewBloomFilter creates a new Bloom filter with the given size (in bits) and
// number of hash functions.
func NewBloomFilter(size, hashes uint) *BloomFilter {
	if size == 0 {
		size = 1024
	}
	if hashes == 0 {
		hashes = 3
	}
	byteSize := (size + 7) / 8
	return &BloomFilter{
		bits:   make([]byte, byteSize),
		size:   size,
		hashes: hashes,
	}
}

func (bf *BloomFilter) hash(data []byte, seed uint) uint {
	h := fnv.New64a()
	_ = binary.Write(h, binary.LittleEndian, uint64(seed))
	h.Write(data)
	return uint(h.Sum64() % uint64(bf.size))
}

// Add inserts data into the bloom filter.
func (bf *BloomFilter) Add(data []byte) {
	for i := uint(0); i < bf.hashes; i++ {
		idx := bf.hash(data, i)
		bf.bits[idx/8] |= 1 << (idx % 8)
	}
}

// Contains returns true if data may be in the set (false positives possible).
func (bf *BloomFilter) Contains(data []byte) bool {
	for i := uint(0); i < bf.hashes; i++ {
		idx := bf.hash(data, i)
		if bf.bits[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}
	return true
}

// Merge performs a bitwise OR union with another bloom filter.
func (bf *BloomFilter) Merge(other *BloomFilter) {
	if other == nil || bf.size != other.size {
		return
	}
	for i := range bf.bits {
		if i < len(other.bits) {
			bf.bits[i] |= other.bits[i]
		}
	}
}

// Encode serializes the bloom filter to bytes.
func (bf *BloomFilter) Encode() []byte {
	data, _ := json.Marshal(struct {
		Bits   []byte `json:"bits"`
		Size   uint   `json:"size"`
		Hashes uint   `json:"hashes"`
	}{bf.bits, bf.size, bf.hashes})
	return data
}

// DecodeBloomFilter deserializes a bloom filter from bytes.
func DecodeBloomFilter(data []byte) (*BloomFilter, error) {
	var raw struct {
		Bits   []byte `json:"bits"`
		Size   uint   `json:"size"`
		Hashes uint   `json:"hashes"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode bloom filter: %w", err)
	}
	return &BloomFilter{
		bits:   raw.Bits,
		size:   raw.Size,
		hashes: raw.Hashes,
	}, nil
}

// --- DeltaState: tracks changes since last sync ---

// DeltaEntry represents a single change in the delta log.
type DeltaEntry struct {
	Point         *Point       `json:"point"`
	VectorClock   *VectorClock `json:"-"`
	OperationType DeltaOp      `json:"operation_type"`
	CreatedAt     int64        `json:"created_at"`
}

// SyncDelta represents a batch of changes to send to a peer.
type SyncDelta struct {
	SourceNode     string        `json:"source_node"`
	VectorClock    *VectorClock  `json:"-"`
	Bloom          *BloomFilter  `json:"-"`
	Entries        []*DeltaEntry `json:"entries"`
	CompressedSize int64         `json:"compressed_size"`
	SchemaVersion  int           `json:"schema_version"`
}

// DeltaState tracks local changes since last synchronization.
type DeltaState struct {
	nodeID         string
	changes        []*DeltaEntry
	bloom          *BloomFilter
	vectorClock    *VectorClock
	sinceTimestamp int64
	config         *OfflineSyncConfig
	mu             sync.RWMutex
}

// NewDeltaState creates a new delta state tracker.
func NewDeltaState(nodeID string, config *OfflineSyncConfig) *DeltaState {
	return &DeltaState{
		nodeID:      nodeID,
		changes:     make([]*DeltaEntry, 0),
		bloom:       NewBloomFilter(config.BloomFilterSize, config.BloomFilterHashes),
		vectorClock: NewVectorClock(),
		config:      config,
	}
}

// RecordChange records a local write operation.
func (ds *DeltaState) RecordChange(point *Point) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.vectorClock.Increment(ds.nodeID)

	entry := &DeltaEntry{
		Point:         point,
		VectorClock:   ds.vectorClock.Copy(),
		OperationType: DeltaInsert,
		CreatedAt:     time.Now().UnixNano(),
	}

	ds.changes = append(ds.changes, entry)

	key := []byte(point.Metric + fmt.Sprintf("%d", point.Timestamp))
	ds.bloom.Add(key)
}

// GetDelta returns all changes since the given timestamp.
func (ds *DeltaState) GetDelta(sinceTimestamp int64) *SyncDelta {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	var entries []*DeltaEntry
	for _, e := range ds.changes {
		if e.CreatedAt > sinceTimestamp {
			entries = append(entries, e)
		}
	}

	return &SyncDelta{
		SourceNode:  ds.nodeID,
		VectorClock: ds.vectorClock.Copy(),
		Bloom:       ds.bloom,
		Entries:     entries,
	}
}

// ApplyDelta applies a received delta and returns any conflicts.
func (ds *DeltaState) ApplyDelta(delta *SyncDelta) ([]ConflictRecord, error) {
	if delta == nil {
		return nil, fmt.Errorf("nil delta")
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	var conflicts []ConflictRecord
	resolver := NewConflictResolver(ds.config.ConflictStrategy)
	preExistingCount := len(ds.changes)

	for _, remote := range delta.Entries {
		key := []byte(remote.Point.Metric + fmt.Sprintf("%d", remote.Point.Timestamp))

		if ds.bloom.Contains(key) {
			// Potential conflict - only check pre-existing local entries
			var localEntry *DeltaEntry
			for i := 0; i < preExistingCount; i++ {
				local := ds.changes[i]
				if local.Point.Metric == remote.Point.Metric &&
					local.Point.Timestamp == remote.Point.Timestamp {
					localEntry = local
					break
				}
			}
			if localEntry != nil {
				resolved := resolver.Resolve(localEntry, remote)
				conflicts = append(conflicts, ConflictRecord{
					LocalEntry:    localEntry,
					RemoteEntry:   remote,
					ResolvedEntry: resolved,
					Strategy:      strategyName(ds.config.ConflictStrategy),
					ResolvedAt:    time.Now(),
				})
				continue
			}
		}

		// No conflict; apply the remote change.
		ds.changes = append(ds.changes, remote)
		ds.bloom.Add(key)
	}

	ds.vectorClock.Merge(delta.VectorClock)
	return conflicts, nil
}

// HasChanges returns true if there are unsynced changes.
func (ds *DeltaState) HasChanges() bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return len(ds.changes) > 0
}

func strategyName(s CRDTConflictStrategy) string {
	switch s {
	case CRDTLastWriterWins:
		return "last-writer-wins"
	case CRDTMaxValueWins:
		return "max-value-wins"
	case CRDTMergeAll:
		return "merge-all"
	default:
		return "unknown"
	}
}

// --- ConflictResolver ---

// ConflictRecord records the details of a resolved conflict.
type ConflictRecord struct {
	LocalEntry    *DeltaEntry `json:"local_entry"`
	RemoteEntry   *DeltaEntry `json:"remote_entry"`
	ResolvedEntry *DeltaEntry `json:"resolved_entry"`
	Strategy      string      `json:"strategy"`
	ResolvedAt    time.Time   `json:"resolved_at"`
}

// ConflictResolver resolves conflicts between concurrent delta entries.
type ConflictResolver struct {
	strategy      CRDTConflictStrategy
	resolvedCount int64
	mu            sync.RWMutex
}

// NewConflictResolver creates a new conflict resolver with the given strategy.
func NewConflictResolver(strategy CRDTConflictStrategy) *ConflictResolver {
	return &ConflictResolver{strategy: strategy}
}

// Resolve resolves a conflict between a local and remote entry.
func (cr *ConflictResolver) Resolve(local, remote *DeltaEntry) *DeltaEntry {
	cr.mu.Lock()
	cr.resolvedCount++
	cr.mu.Unlock()

	switch cr.strategy {
	case CRDTMaxValueWins:
		if remote.Point.Value > local.Point.Value {
			return remote
		}
		return local
	case CRDTMergeAll:
		merged := &DeltaEntry{
			Point: &Point{
				Metric:    local.Point.Metric,
				Tags:      mergeOfflineTags(local.Point.Tags, remote.Point.Tags),
				Value:     local.Point.Value + remote.Point.Value,
				Timestamp: local.Point.Timestamp,
			},
			OperationType: local.OperationType,
			CreatedAt:     time.Now().UnixNano(),
		}
		if remote.Point.Timestamp > local.Point.Timestamp {
			merged.Point.Timestamp = remote.Point.Timestamp
		}
		return merged
	default: // CRDTLastWriterWins
		if remote.CreatedAt > local.CreatedAt {
			return remote
		}
		if remote.CreatedAt == local.CreatedAt && remote.Point.Value > local.Point.Value {
			return remote
		}
		return local
	}
}

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
