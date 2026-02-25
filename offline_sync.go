package chronicle

import (
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
	_ = binary.Write(h, binary.LittleEndian, uint64(seed)) //nolint:errcheck // hash.Write never fails
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
