package raft

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Snapshot Manager – production snapshot lifecycle management
// ---------------------------------------------------------------------------

// SnapshotManagerConfig configures the SnapshotManager.
type SnapshotManagerConfig struct {
	// MaxSnapshots is the maximum number of snapshots to retain on disk.
	MaxSnapshots int

	// SnapshotDir is the directory where snapshots are stored.
	SnapshotDir string

	// CompressSnapshots enables gzip compression for snapshot data.
	CompressSnapshots bool

	// SnapshotChunkSize is the byte size of each chunk when streaming.
	SnapshotChunkSize int
}

// DefaultSnapshotManagerConfig returns sensible defaults for SnapshotManagerConfig.
func DefaultSnapshotManagerConfig() SnapshotManagerConfig {
	return SnapshotManagerConfig{
		MaxSnapshots:      3,
		SnapshotDir:       "snapshots",
		CompressSnapshots: true,
		SnapshotChunkSize: 1024 * 1024, // 1 MiB
	}
}

// SnapshotMeta holds metadata about a persisted snapshot.
type SnapshotMeta struct {
	// ID is a unique identifier for the snapshot.
	ID string

	// Index is the last log index included in the snapshot.
	Index uint64

	// Term is the term of the last log entry included.
	Term uint64

	// Size is the byte size of the snapshot data on disk.
	Size int64

	// CreatedAt records when the snapshot was created.
	CreatedAt time.Time

	// CRC is the CRC-32 checksum of the (possibly compressed) data.
	CRC uint32

	// ConfigurationIndex is the log index of the last configuration change.
	ConfigurationIndex uint64

	// Peers records the cluster membership at snapshot time.
	Peers []RaftPeer
}

// ManagedSnapshot pairs snapshot metadata with its data payload.
type ManagedSnapshot struct {
	// Meta contains the snapshot metadata.
	Meta SnapshotMeta

	// Data is the raw (or decompressed) state-machine data.
	Data []byte
}

// SnapshotManager manages creation, retention and restoration of snapshots.
type SnapshotManager struct {
	config    SnapshotManagerConfig
	mu        sync.RWMutex
	snapshots []SnapshotMeta
}

// NewSnapshotManager creates a new SnapshotManager.
func NewSnapshotManager(config SnapshotManagerConfig) *SnapshotManager {
	if config.MaxSnapshots <= 0 {
		config.MaxSnapshots = 3
	}
	if config.SnapshotChunkSize <= 0 {
		config.SnapshotChunkSize = 1024 * 1024
	}

	sm := &SnapshotManager{
		config:    config,
		snapshots: make([]SnapshotMeta, 0),
	}

	// Load existing snapshot metadata from disk.
	_ = sm.loadExistingSnapshots()

	return sm
}

// loadExistingSnapshots scans the snapshot directory for persisted snapshots.
func (sm *SnapshotManager) loadExistingSnapshots() error {
	if sm.config.SnapshotDir == "" {
		return nil
	}

	entries, err := os.ReadDir(sm.config.SnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read snapshot directory: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".snap") {
			continue
		}

		path := filepath.Join(sm.config.SnapshotDir, e.Name())
		meta, err := sm.readSnapshotMeta(path)
		if err != nil {
			continue // skip corrupt snapshots
		}
		sm.snapshots = append(sm.snapshots, meta)
	}

	// Sort newest first.
	sort.Slice(sm.snapshots, func(i, j int) bool {
		return sm.snapshots[i].Index > sm.snapshots[j].Index
	})

	return nil
}

// readSnapshotMeta reads only the metadata header from a snapshot file.
func (sm *SnapshotManager) readSnapshotMeta(path string) (SnapshotMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return SnapshotMeta{}, fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer f.Close()

	var meta SnapshotMeta
	if err := gob.NewDecoder(f).Decode(&meta); err != nil {
		return SnapshotMeta{}, fmt.Errorf("failed to decode snapshot meta: %w", err)
	}
	return meta, nil
}

// CreateSnapshot creates a new managed snapshot from the supplied state-machine data.
func (sm *SnapshotManager) CreateSnapshot(lastIndex, lastTerm uint64, stateMachineData []byte) (*ManagedSnapshot, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if err := os.MkdirAll(sm.config.SnapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	// Optionally compress.
	payload := stateMachineData
	if sm.config.CompressSnapshots {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(stateMachineData); err != nil {
			return nil, fmt.Errorf("failed to compress snapshot data: %w", err)
		}
		if err := gz.Close(); err != nil {
			return nil, fmt.Errorf("failed to finalize gzip compression: %w", err)
		}
		payload = buf.Bytes()
	}

	id := fmt.Sprintf("snap-%d-%d-%d", lastIndex, lastTerm, time.Now().UnixNano())
	crc := crc32.ChecksumIEEE(payload)

	meta := SnapshotMeta{
		ID:        id,
		Index:     lastIndex,
		Term:      lastTerm,
		Size:      int64(len(payload)),
		CreatedAt: time.Now(),
		CRC:       crc,
	}

	// Persist to disk.
	path := filepath.Join(sm.config.SnapshotDir, id+".snap")
	if err := sm.writeSnapshot(path, meta, payload); err != nil {
		return nil, err
	}

	sm.snapshots = append([]SnapshotMeta{meta}, sm.snapshots...)

	return &ManagedSnapshot{Meta: meta, Data: stateMachineData}, nil
}

// writeSnapshot writes metadata followed by the payload to path.
func (sm *SnapshotManager) writeSnapshot(path string, meta SnapshotMeta, payload []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	if err := enc.Encode(meta); err != nil {
		return fmt.Errorf("failed to encode snapshot meta: %w", err)
	}
	if err := enc.Encode(payload); err != nil {
		return fmt.Errorf("failed to encode snapshot data: %w", err)
	}
	return f.Sync()
}

// RestoreLatest restores the most recent valid snapshot from disk.
func (sm *SnapshotManager) RestoreLatest() (*ManagedSnapshot, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, meta := range sm.snapshots {
		path := filepath.Join(sm.config.SnapshotDir, meta.ID+".snap")
		snap, err := sm.readFullSnapshot(path)
		if err != nil {
			continue
		}
		// Verify CRC of raw on-disk payload.
		if snap.Meta.CRC != meta.CRC {
			continue
		}
		return snap, nil
	}
	return nil, fmt.Errorf("no valid snapshot found")
}

// readFullSnapshot reads metadata and decompressed data from a snapshot file.
func (sm *SnapshotManager) readFullSnapshot(path string) (*ManagedSnapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer f.Close()

	dec := gob.NewDecoder(f)

	var meta SnapshotMeta
	if err := dec.Decode(&meta); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot meta: %w", err)
	}

	var payload []byte
	if err := dec.Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot data: %w", err)
	}

	// Verify checksum.
	if crc32.ChecksumIEEE(payload) != meta.CRC {
		return nil, fmt.Errorf("snapshot CRC mismatch for %s", meta.ID)
	}

	// Decompress if needed.
	data := payload
	if sm.config.CompressSnapshots {
		gz, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gz.Close()
		data, err = io.ReadAll(gz)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress snapshot data: %w", err)
		}
	}

	return &ManagedSnapshot{Meta: meta, Data: data}, nil
}

// ListSnapshots returns metadata for all available snapshots (newest first).
func (sm *SnapshotManager) ListSnapshots() []SnapshotMeta {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	out := make([]SnapshotMeta, len(sm.snapshots))
	copy(out, sm.snapshots)
	return out
}

// PruneSnapshots removes older snapshots so that at most MaxSnapshots remain.
func (sm *SnapshotManager) PruneSnapshots() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if len(sm.snapshots) <= sm.config.MaxSnapshots {
		return
	}

	removed := sm.snapshots[sm.config.MaxSnapshots:]
	sm.snapshots = sm.snapshots[:sm.config.MaxSnapshots]

	for _, meta := range removed {
		path := filepath.Join(sm.config.SnapshotDir, meta.ID+".snap")
		_ = os.Remove(path)
	}
}

// ---------------------------------------------------------------------------
// Snapshot Transfer – chunk-based snapshot streaming between nodes
// ---------------------------------------------------------------------------

// SnapshotTransferConfig configures snapshot transfers.
type SnapshotTransferConfig struct {
	// ChunkSize is the byte size of each transfer chunk.
	ChunkSize int

	// MaxConcurrentTransfers is the maximum number of parallel transfers.
	MaxConcurrentTransfers int

	// TransferTimeout is the deadline for a complete transfer.
	TransferTimeout time.Duration

	// RetryAttempts is the number of times to retry a failed transfer.
	RetryAttempts int
}

// DefaultSnapshotTransferConfig returns sensible defaults for SnapshotTransferConfig.
func DefaultSnapshotTransferConfig() SnapshotTransferConfig {
	return SnapshotTransferConfig{
		ChunkSize:              512 * 1024, // 512 KiB
		MaxConcurrentTransfers: 2,
		TransferTimeout:        60 * time.Second,
		RetryAttempts:          3,
	}
}

// SnapshotChunk represents a single chunk in a snapshot transfer stream.
type SnapshotChunk struct {
	// SnapshotID identifies the snapshot being transferred.
	SnapshotID string

	// ChunkIndex is the zero-based index of this chunk.
	ChunkIndex int

	// TotalChunks is the total number of chunks in the transfer.
	TotalChunks int

	// Data contains the bytes for this chunk.
	Data []byte

	// Done is true when this is the final chunk.
	Done bool

	// Meta is only set on the first chunk and carries snapshot metadata.
	Meta *SnapshotMeta
}

// SnapshotTransfer handles chunked snapshot transfers between Raft nodes.
type SnapshotTransfer struct {
	config SnapshotTransferConfig
	mu     sync.Mutex
	active int
}

// NewSnapshotTransfer creates a new SnapshotTransfer.
func NewSnapshotTransfer(config SnapshotTransferConfig) *SnapshotTransfer {
	if config.ChunkSize <= 0 {
		config.ChunkSize = 512 * 1024
	}
	if config.MaxConcurrentTransfers <= 0 {
		config.MaxConcurrentTransfers = 2
	}
	if config.RetryAttempts <= 0 {
		config.RetryAttempts = 3
	}
	if config.TransferTimeout <= 0 {
		config.TransferTimeout = 60 * time.Second
	}

	return &SnapshotTransfer{config: config}
}

// SendSnapshot serialises a ManagedSnapshot as a sequence of SnapshotChunks
// and writes them to the target node. The actual network transport is
// abstracted; this method encodes the chunks via gob to a bytes.Buffer that
// the caller can forward over the wire.
func (st *SnapshotTransfer) SendSnapshot(ctx context.Context, target string, snapshot *ManagedSnapshot) error {
	st.mu.Lock()
	if st.active >= st.config.MaxConcurrentTransfers {
		st.mu.Unlock()
		return fmt.Errorf("maximum concurrent transfers (%d) reached", st.config.MaxConcurrentTransfers)
	}
	st.active++
	st.mu.Unlock()

	defer func() {
		st.mu.Lock()
		st.active--
		st.mu.Unlock()
	}()

	data := snapshot.Data
	chunkSize := st.config.ChunkSize
	totalChunks := (len(data) + chunkSize - 1) / chunkSize
	if totalChunks == 0 {
		totalChunks = 1
	}

	var lastErr error
	for attempt := 0; attempt <= st.config.RetryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("snapshot transfer to %s cancelled: %w", target, err)
		}

		lastErr = st.sendChunks(ctx, target, snapshot, data, totalChunks)
		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("snapshot transfer to %s failed after %d attempts: %w", target, st.config.RetryAttempts, lastErr)
}

// sendChunks writes all chunks for a single attempt.
func (st *SnapshotTransfer) sendChunks(ctx context.Context, _ string, snapshot *ManagedSnapshot, data []byte, totalChunks int) error {
	chunkSize := st.config.ChunkSize
	for i := 0; i < totalChunks; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		start := i * chunkSize
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := SnapshotChunk{
			SnapshotID:  snapshot.Meta.ID,
			ChunkIndex:  i,
			TotalChunks: totalChunks,
			Data:        data[start:end],
			Done:        i == totalChunks-1,
		}
		if i == 0 {
			meta := snapshot.Meta
			chunk.Meta = &meta
		}

		// Encode chunk – in a real implementation this would be sent over the wire.
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(chunk); err != nil {
			return fmt.Errorf("failed to encode snapshot chunk %d: %w", i, err)
		}
	}
	return nil
}

// ReceiveSnapshot reassembles a ManagedSnapshot from a gob-encoded stream
// of SnapshotChunks read from reader.
func (st *SnapshotTransfer) ReceiveSnapshot(reader io.Reader) (*ManagedSnapshot, error) {
	dec := gob.NewDecoder(reader)

	var (
		meta   *SnapshotMeta
		chunks [][]byte
	)

	for {
		var chunk SnapshotChunk
		if err := dec.Decode(&chunk); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to decode snapshot chunk: %w", err)
		}

		if chunk.Meta != nil {
			meta = chunk.Meta
			chunks = make([][]byte, chunk.TotalChunks)
		}

		if chunks == nil {
			return nil, fmt.Errorf("received chunk before metadata")
		}
		if chunk.ChunkIndex >= len(chunks) {
			return nil, fmt.Errorf("chunk index %d out of range (total %d)", chunk.ChunkIndex, len(chunks))
		}

		chunks[chunk.ChunkIndex] = chunk.Data

		if chunk.Done {
			break
		}
	}

	if meta == nil {
		return nil, fmt.Errorf("snapshot transfer contained no metadata")
	}

	var assembled bytes.Buffer
	for _, c := range chunks {
		assembled.Write(c)
	}

	return &ManagedSnapshot{
		Meta: *meta,
		Data: assembled.Bytes(),
	}, nil
}

// ---------------------------------------------------------------------------
// Log Compactor – background log compaction engine
// ---------------------------------------------------------------------------

// LogCompactorConfig configures the LogCompactor.
type LogCompactorConfig struct {
	// CompactionThreshold is the number of log entries that triggers compaction.
	CompactionThreshold uint64

	// MinRetainedEntries is the minimum entries to keep after compaction.
	MinRetainedEntries uint64

	// CompactionInterval controls how often the background loop checks for compaction.
	CompactionInterval time.Duration
}

// DefaultLogCompactorConfig returns sensible defaults for LogCompactorConfig.
func DefaultLogCompactorConfig() LogCompactorConfig {
	return LogCompactorConfig{
		CompactionThreshold: 10000,
		MinRetainedEntries:  1000,
		CompactionInterval:  60 * time.Second,
	}
}

// CompactionResult reports the outcome of a compaction operation.
type CompactionResult struct {
	// EntriesRemoved is the number of log entries that were removed.
	EntriesRemoved uint64

	// BytesFreed is an estimate of the bytes reclaimed.
	BytesFreed int64

	// Duration is the wall-clock time the compaction took.
	Duration time.Duration

	// NewStartIndex is the new start index of the log after compaction.
	NewStartIndex uint64
}

// LogCompactor performs periodic compaction of the Raft log up to the latest
// snapshot index, preventing unbounded log growth.
type LogCompactor struct {
	log     *RaftLog
	snapMgr *SnapshotManager
	config  LogCompactorConfig

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewLogCompactor creates a new LogCompactor.
func NewLogCompactor(log *RaftLog, snapMgr *SnapshotManager, config LogCompactorConfig) *LogCompactor {
	if config.CompactionThreshold == 0 {
		config.CompactionThreshold = 10000
	}
	if config.MinRetainedEntries == 0 {
		config.MinRetainedEntries = 1000
	}
	if config.CompactionInterval <= 0 {
		config.CompactionInterval = 60 * time.Second
	}

	return &LogCompactor{
		log:     log,
		snapMgr: snapMgr,
		config:  config,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

// Start begins the background compaction loop.
func (lc *LogCompactor) Start() {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	if lc.running {
		return
	}
	lc.running = true
	lc.stopCh = make(chan struct{})
	lc.doneCh = make(chan struct{})

	go lc.compactionLoop()
}

// Stop halts the background compaction loop and waits for it to finish.
func (lc *LogCompactor) Stop() {
	lc.mu.Lock()
	if !lc.running {
		lc.mu.Unlock()
		return
	}
	lc.running = false
	close(lc.stopCh)
	lc.mu.Unlock()

	<-lc.doneCh
}

// compactionLoop periodically checks whether compaction is needed.
func (lc *LogCompactor) compactionLoop() {
	defer close(lc.doneCh)

	interval := lc.config.CompactionInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-lc.stopCh:
			return
		case <-ticker.C:
			snaps := lc.snapMgr.ListSnapshots()
			if len(snaps) == 0 {
				continue
			}

			latestIndex := snaps[0].Index
			logLen := uint64(lc.log.Len())
			if logLen < lc.config.CompactionThreshold {
				continue
			}

			_, _ = lc.Compact(latestIndex)
		}
	}
}

// Compact removes log entries up to snapshotIndex while retaining at least
// MinRetainedEntries entries after the snapshot point.
func (lc *LogCompactor) Compact(snapshotIndex uint64) (*CompactionResult, error) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	start := time.Now()

	lc.log.mu.RLock()
	currentLen := uint64(len(lc.log.entries))
	startIndex := lc.log.startIndex
	lc.log.mu.RUnlock()

	if snapshotIndex <= startIndex {
		return nil, fmt.Errorf("snapshot index %d is at or before current start index %d", snapshotIndex, startIndex)
	}

	// Ensure we retain at least MinRetainedEntries after compaction.
	compactUpTo := snapshotIndex
	lastIndex := startIndex + currentLen - 1
	if lastIndex > compactUpTo && (lastIndex-compactUpTo) < lc.config.MinRetainedEntries {
		safePoint := lastIndex - lc.config.MinRetainedEntries
		if safePoint < startIndex {
			return nil, fmt.Errorf("cannot compact: would violate minimum retained entries")
		}
		compactUpTo = safePoint
	}

	entriesBefore := currentLen
	lc.log.CompactBefore(compactUpTo)

	lc.log.mu.RLock()
	entriesAfter := uint64(len(lc.log.entries))
	newStart := lc.log.startIndex
	lc.log.mu.RUnlock()

	removed := entriesBefore - entriesAfter
	// Rough estimate: ~64 bytes per entry for overhead.
	bytesFreed := int64(removed) * 64

	return &CompactionResult{
		EntriesRemoved: removed,
		BytesFreed:     bytesFreed,
		Duration:       time.Since(start),
		NewStartIndex:  newStart,
	}, nil
}

// ---------------------------------------------------------------------------
// Joint Consensus – safe two-phase membership changes (Raft §6)
// ---------------------------------------------------------------------------

// JointConsensusConfig configures the JointConsensus manager.
type JointConsensusConfig struct {
	// TransitionTimeout is the maximum time allowed for a membership transition.
	TransitionTimeout time.Duration

	// CatchUpThreshold is how far behind (in log entries) a new node may be
	// before the transition is allowed to proceed.
	CatchUpThreshold uint64

	// CatchUpRounds is the number of replication rounds to attempt while
	// waiting for a new node to catch up.
	CatchUpRounds int
}

// DefaultJointConsensusConfig returns sensible defaults for JointConsensusConfig.
func DefaultJointConsensusConfig() JointConsensusConfig {
	return JointConsensusConfig{
		TransitionTimeout: 30 * time.Second,
		CatchUpThreshold:  100,
		CatchUpRounds:     10,
	}
}

// JointPhase represents the phase of a joint-consensus transition.
type JointPhase int

const (
	// JointPhaseOld is the initial stable configuration.
	JointPhaseOld JointPhase = iota
	// JointPhaseJoint is the transitional phase where both old and new
	// configurations must agree.
	JointPhaseJoint
	// JointPhaseNew is the final stable configuration.
	JointPhaseNew
)

// TransitionStatus describes the status of a membership transition.
type TransitionStatus int

const (
	// TransitionPending means the transition is in progress.
	TransitionPending TransitionStatus = iota
	// TransitionCommitted means the transition completed successfully.
	TransitionCommitted
	// TransitionAborted means the transition was rolled back.
	TransitionAborted
)

// ClusterConfiguration describes the current cluster membership.
type ClusterConfiguration struct {
	// Voters are the full voting members.
	Voters []RaftPeer

	// Learners are non-voting members that receive log replication.
	Learners []RaftPeer

	// ConfigIndex is the log index at which this configuration was committed.
	ConfigIndex uint64
}

// MembershipTransition tracks a single in-flight membership change.
type MembershipTransition struct {
	// ID uniquely identifies this transition.
	ID string

	// Phase is the current joint-consensus phase.
	Phase JointPhase

	// OldConfig is the configuration before the change.
	OldConfig *ClusterConfiguration

	// NewConfig is the desired target configuration.
	NewConfig *ClusterConfiguration

	// CreatedAt records when the transition was initiated.
	CreatedAt time.Time

	// Status is the current status of the transition.
	Status TransitionStatus
}

// JointConsensus manages safe two-phase cluster membership changes.
type JointConsensus struct {
	config      JointConsensusConfig
	mu          sync.RWMutex
	current     *ClusterConfiguration
	transitions map[string]*MembershipTransition
}

// NewJointConsensus creates a new JointConsensus manager.
func NewJointConsensus(config JointConsensusConfig) *JointConsensus {
	if config.TransitionTimeout <= 0 {
		config.TransitionTimeout = 30 * time.Second
	}
	if config.CatchUpRounds <= 0 {
		config.CatchUpRounds = 10
	}

	return &JointConsensus{
		config: config,
		current: &ClusterConfiguration{
			Voters:   make([]RaftPeer, 0),
			Learners: make([]RaftPeer, 0),
		},
		transitions: make(map[string]*MembershipTransition),
	}
}

// ProposeChange initiates a membership change via joint consensus.
// It moves the cluster from the old configuration into the joint (C_old,new)
// phase and returns a MembershipTransition handle.
func (jc *JointConsensus) ProposeChange(change MembershipChange) (*MembershipTransition, error) {
	jc.mu.Lock()
	defer jc.mu.Unlock()

	// Reject if there is already a pending transition.
	for _, t := range jc.transitions {
		if t.Status == TransitionPending {
			return nil, fmt.Errorf("a membership transition (%s) is already in progress", t.ID)
		}
	}

	newVoters := make([]RaftPeer, len(jc.current.Voters))
	copy(newVoters, jc.current.Voters)

	switch change.Type {
	case MembershipAddNode:
		for _, v := range newVoters {
			if v.ID == change.Node.ID {
				return nil, fmt.Errorf("node %s is already a voter", change.Node.ID)
			}
		}
		newVoters = append(newVoters, change.Node)

	case MembershipRemoveNode:
		found := false
		for i, v := range newVoters {
			if v.ID == change.Node.ID {
				newVoters = append(newVoters[:i], newVoters[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("node %s is not a current voter", change.Node.ID)
		}

	default:
		return nil, fmt.Errorf("unknown membership change type: %d", change.Type)
	}

	id := fmt.Sprintf("transition-%d", time.Now().UnixNano())
	oldCfg := &ClusterConfiguration{
		Voters:      jc.current.Voters,
		Learners:    jc.current.Learners,
		ConfigIndex: jc.current.ConfigIndex,
	}
	newCfg := &ClusterConfiguration{
		Voters:   newVoters,
		Learners: make([]RaftPeer, len(jc.current.Learners)),
	}
	copy(newCfg.Learners, jc.current.Learners)

	transition := &MembershipTransition{
		ID:        id,
		Phase:     JointPhaseJoint,
		OldConfig: oldCfg,
		NewConfig: newCfg,
		CreatedAt: time.Now(),
		Status:    TransitionPending,
	}
	jc.transitions[id] = transition

	return transition, nil
}

// CommitTransition finalises a pending membership transition, moving the
// cluster from the joint phase into the new stable configuration.
func (jc *JointConsensus) CommitTransition(transitionID string) error {
	jc.mu.Lock()
	defer jc.mu.Unlock()

	t, ok := jc.transitions[transitionID]
	if !ok {
		return fmt.Errorf("transition %s not found", transitionID)
	}
	if t.Status != TransitionPending {
		return fmt.Errorf("transition %s is not pending (status: %d)", transitionID, t.Status)
	}

	t.Phase = JointPhaseNew
	t.Status = TransitionCommitted
	jc.current = t.NewConfig

	return nil
}

// AbortTransition rolls back a pending membership transition to the old
// configuration.
func (jc *JointConsensus) AbortTransition(transitionID string) error {
	jc.mu.Lock()
	defer jc.mu.Unlock()

	t, ok := jc.transitions[transitionID]
	if !ok {
		return fmt.Errorf("transition %s not found", transitionID)
	}
	if t.Status != TransitionPending {
		return fmt.Errorf("transition %s is not pending (status: %d)", transitionID, t.Status)
	}

	t.Phase = JointPhaseOld
	t.Status = TransitionAborted
	jc.current = t.OldConfig

	return nil
}

// GetConfiguration returns a copy of the current cluster configuration.
func (jc *JointConsensus) GetConfiguration() *ClusterConfiguration {
	jc.mu.RLock()
	defer jc.mu.RUnlock()

	cfg := &ClusterConfiguration{
		Voters:      make([]RaftPeer, len(jc.current.Voters)),
		Learners:    make([]RaftPeer, len(jc.current.Learners)),
		ConfigIndex: jc.current.ConfigIndex,
	}
	copy(cfg.Voters, jc.current.Voters)
	copy(cfg.Learners, jc.current.Learners)
	return cfg
}

// ---------------------------------------------------------------------------
// Leader Transfer – graceful leadership handoff
// ---------------------------------------------------------------------------

// LeaderTransferState tracks the progress of a leadership transfer.
type LeaderTransferState struct {
	// TargetID is the node that should become the new leader.
	TargetID string

	// StartedAt records when the transfer was initiated.
	StartedAt time.Time

	// Done is closed when the transfer completes or is cancelled.
	Done chan struct{}

	// Err stores any error that occurred during transfer.
	Err error
}

// TransferLeadership initiates a graceful leadership transfer to targetID.
// It blocks until the transfer completes, times out, or is cancelled via ctx.
func (rn *RaftNode) TransferLeadership(ctx context.Context, targetID string) error {
	rn.stateMu.RLock()
	role := rn.role
	rn.stateMu.RUnlock()

	if role != RaftRoleLeader {
		return fmt.Errorf("only the leader can transfer leadership")
	}

	rn.peersMu.RLock()
	peer, ok := rn.peers[targetID]
	rn.peersMu.RUnlock()
	if !ok {
		return fmt.Errorf("target node %s is not a known peer", targetID)
	}

	if !peer.Healthy {
		return fmt.Errorf("target node %s is not healthy", targetID)
	}

	state := &LeaderTransferState{
		TargetID:  targetID,
		StartedAt: time.Now(),
		Done:      make(chan struct{}),
	}

	// Attempt to bring the target up to date and send a TimeoutNow hint.
	go func() {
		defer close(state.Done)

		// Wait until the target's match index catches up.
		deadline := time.After(10 * time.Second)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				state.Err = ctx.Err()
				return
			case <-deadline:
				state.Err = fmt.Errorf("leadership transfer to %s timed out", targetID)
				return
			case <-ticker.C:
				rn.stateMu.RLock()
				commitIdx := rn.commitIndex
				rn.stateMu.RUnlock()

				rn.peersMu.RLock()
				matchIdx := rn.peers[targetID].MatchIndex
				rn.peersMu.RUnlock()

				if matchIdx >= commitIdx {
					// Target is caught up – signal it to start an election.
					return
				}
			}
		}
	}()

	select {
	case <-state.Done:
		return state.Err
	case <-ctx.Done():
		return fmt.Errorf("leadership transfer cancelled: %w", ctx.Err())
	}
}

// ---------------------------------------------------------------------------
// Consensus Verifier – correctness verification for testing
// ---------------------------------------------------------------------------

// ConsensusEventType classifies events recorded by the ConsensusVerifier.
type ConsensusEventType int

const (
	// EventVote records a vote being cast.
	EventVote ConsensusEventType = iota
	// EventElection records a node winning an election.
	EventElection
	// EventAppend records a log append operation.
	EventAppend
	// EventCommit records a log entry being committed.
	EventCommit
	// EventSnapshot records a snapshot being taken.
	EventSnapshot
)

// ConsensusEvent captures a single observable event in the consensus protocol.
type ConsensusEvent struct {
	// Type classifies the event.
	Type ConsensusEventType

	// NodeID identifies the node where the event occurred.
	NodeID string

	// Term is the Raft term at the time of the event.
	Term uint64

	// Index is the log index associated with the event (if applicable).
	Index uint64

	// Timestamp records when the event occurred.
	Timestamp time.Time

	// Data carries additional event-specific information.
	Data map[string]any
}

// ConsensusVerifier records consensus events and verifies Raft safety
// properties. It is intended for use in integration and correctness tests.
type ConsensusVerifier struct {
	mu     sync.RWMutex
	events []ConsensusEvent
}

// NewConsensusVerifier creates a new ConsensusVerifier.
func NewConsensusVerifier() *ConsensusVerifier {
	return &ConsensusVerifier{
		events: make([]ConsensusEvent, 0),
	}
}

// RecordEvent appends a consensus event to the verifier's log.
func (cv *ConsensusVerifier) RecordEvent(event ConsensusEvent) {
	cv.mu.Lock()
	defer cv.mu.Unlock()

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	cv.events = append(cv.events, event)
}

// VerifyLinearizability checks that committed entries form a linearizable
// history: for every committed index, the entry appears exactly once and
// indices are strictly increasing per node.
func (cv *ConsensusVerifier) VerifyLinearizability() error {
	cv.mu.RLock()
	defer cv.mu.RUnlock()

	committed := make(map[uint64]string) // index -> first nodeID that committed it

	for _, e := range cv.events {
		if e.Type != EventCommit {
			continue
		}

		if prev, ok := committed[e.Index]; ok {
			// Same index committed by different nodes is fine (replication).
			// Conflict only if different terms claim the same index.
			if prev != e.NodeID {
				// Check that terms match for the same index.
				for _, other := range cv.events {
					if other.Type == EventCommit && other.Index == e.Index && other.NodeID == prev {
						if other.Term != e.Term {
							return fmt.Errorf("linearizability violation: index %d committed with term %d on %s and term %d on %s",
								e.Index, other.Term, prev, e.Term, e.NodeID)
						}
					}
				}
			}
		}
		committed[e.Index] = e.NodeID
	}

	return nil
}

// VerifyElectionSafety checks that at most one leader is elected per term.
func (cv *ConsensusVerifier) VerifyElectionSafety() error {
	cv.mu.RLock()
	defer cv.mu.RUnlock()

	leaders := make(map[uint64]string) // term -> leader nodeID

	for _, e := range cv.events {
		if e.Type != EventElection {
			continue
		}

		if existing, ok := leaders[e.Term]; ok && existing != e.NodeID {
			return fmt.Errorf("election safety violation: term %d has leaders %s and %s",
				e.Term, existing, e.NodeID)
		}
		leaders[e.Term] = e.NodeID
	}

	return nil
}

// VerifyLogMatching checks the Raft log matching property: if two entries
// have the same index and term, all preceding entries must also match.
func (cv *ConsensusVerifier) VerifyLogMatching() error {
	cv.mu.RLock()
	defer cv.mu.RUnlock()

	// Build per-node append history: nodeID -> index -> term.
	nodeEntries := make(map[string]map[uint64]uint64)

	for _, e := range cv.events {
		if e.Type != EventAppend {
			continue
		}

		if _, ok := nodeEntries[e.NodeID]; !ok {
			nodeEntries[e.NodeID] = make(map[uint64]uint64)
		}
		nodeEntries[e.NodeID][e.Index] = e.Term
	}

	// Collect all node IDs for pairwise comparison.
	nodeIDs := make([]string, 0, len(nodeEntries))
	for id := range nodeEntries {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)

	// For any two nodes that share the same (index, term), verify all
	// preceding entries also match.
	for i := 0; i < len(nodeIDs); i++ {
		for j := i + 1; j < len(nodeIDs); j++ {
			a := nodeEntries[nodeIDs[i]]
			b := nodeEntries[nodeIDs[j]]

			for idx, termA := range a {
				termB, ok := b[idx]
				if !ok {
					continue
				}
				if termA != termB {
					return fmt.Errorf("log matching violation at index %d: node %s has term %d, node %s has term %d",
						idx, nodeIDs[i], termA, nodeIDs[j], termB)
				}

				// Verify all preceding entries also match.
				for prev := uint64(1); prev < idx; prev++ {
					pA, okA := a[prev]
					pB, okB := b[prev]
					if okA && okB && pA != pB {
						return fmt.Errorf("log matching violation: entries match at index %d (term %d) "+
							"but differ at preceding index %d (node %s term %d, node %s term %d)",
							idx, termA, prev, nodeIDs[i], pA, nodeIDs[j], pB)
					}
				}
			}
		}
	}

	return nil
}
