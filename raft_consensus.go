package chronicle

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// RaftConfig configures the Raft consensus algorithm.
type RaftConfig struct {
	// NodeID is the unique identifier for this node.
	NodeID string

	// DataDir is the directory for persistent storage.
	DataDir string

	// BindAddr is the address to bind for Raft communication.
	BindAddr string

	// AdvertiseAddr is the address to advertise to other nodes.
	AdvertiseAddr string

	// Peers are the initial cluster members (excluding self).
	Peers []RaftPeer

	// ElectionTimeoutMin is the minimum election timeout.
	ElectionTimeoutMin time.Duration

	// ElectionTimeoutMax is the maximum election timeout.
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval is how often the leader sends heartbeats.
	HeartbeatInterval time.Duration

	// SnapshotInterval is how often to check for snapshot conditions.
	SnapshotInterval time.Duration

	// SnapshotThreshold is the log size that triggers snapshotting.
	SnapshotThreshold uint64

	// MaxAppendEntries is the maximum entries per AppendEntries RPC.
	MaxAppendEntries int

	// MaxInflightRequests is the maximum concurrent replication requests.
	MaxInflightRequests int

	// LeaseTimeout for linearizable reads (leader lease).
	LeaseTimeout time.Duration

	// PreVoteEnabled enables the pre-vote protocol to prevent disruptions.
	PreVoteEnabled bool

	// CheckQuorumEnabled checks quorum connectivity periodically.
	CheckQuorumEnabled bool

	// BatchingEnabled enables request batching for throughput.
	BatchingEnabled bool

	// BatchMaxSize is the maximum batch size.
	BatchMaxSize int

	// BatchMaxWait is the maximum time to wait for batching.
	BatchMaxWait time.Duration
}

// RaftPeer represents a peer in the Raft cluster.
type RaftPeer struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// DefaultRaftConfig returns default Raft configuration.
func DefaultRaftConfig() RaftConfig {
	return RaftConfig{
		NodeID:              fmt.Sprintf("raft-%d", time.Now().UnixNano()%10000),
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		SnapshotInterval:    30 * time.Second,
		SnapshotThreshold:   10000,
		MaxAppendEntries:    100,
		MaxInflightRequests: 10,
		LeaseTimeout:        100 * time.Millisecond,
		PreVoteEnabled:      true,
		CheckQuorumEnabled:  true,
		BatchingEnabled:     true,
		BatchMaxSize:        100,
		BatchMaxWait:        5 * time.Millisecond,
	}
}

// RaftRole represents the role of a Raft node.
type RaftRole int

const (
	RaftRoleFollower RaftRole = iota
	RaftRoleCandidate
	RaftRoleLeader
	RaftRolePreCandidate // For pre-vote protocol
)

func (r RaftRole) String() string {
	switch r {
	case RaftRoleFollower:
		return "follower"
	case RaftRoleCandidate:
		return "candidate"
	case RaftRoleLeader:
		return "leader"
	case RaftRolePreCandidate:
		return "pre-candidate"
	default:
		return "unknown"
	}
}

// RaftLogEntryType identifies the type of log entry.
type RaftLogEntryType int

const (
	RaftLogCommand RaftLogEntryType = iota
	RaftLogConfiguration
	RaftLogNoop
	RaftLogBarrier // For linearizable reads
)

// RaftLogEntry represents a log entry in the Raft log.
type RaftLogEntry struct {
	Index uint64           `json:"index"`
	Term  uint64           `json:"term"`
	Type  RaftLogEntryType `json:"type"`
	Data  []byte           `json:"data"`
	CRC   uint32           `json:"crc"`
}

// Validate checks if the log entry is valid.
func (e *RaftLogEntry) Validate() bool {
	return crc32.ChecksumIEEE(e.Data) == e.CRC
}

// RaftCommand represents a command to be applied to the state machine.
type RaftCommand struct {
	Op    string          `json:"op"`
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

// RaftNode implements the Raft consensus algorithm.
type RaftNode struct {
	config RaftConfig
	db     *DB

	// Persistent state (must be saved to stable storage)
	currentTerm uint64
	votedFor    string
	log         *RaftLog

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Role management
	role     RaftRole
	leaderID string

	// Cluster membership
	peers   map[string]*RaftPeerState
	peersMu sync.RWMutex

	// Leader lease for linearizable reads
	leaseExpiry   time.Time
	leaseExtended atomic.Bool

	// Pending proposals
	proposals     map[uint64]*RaftProposal
	proposalsMu   sync.Mutex
	proposalIndex uint64

	// Request batching
	batchCh     chan *RaftProposal
	batchDoneCh chan struct{}

	// Communication
	transport *RaftTransport
	server    *http.Server

	// Timers
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	// Snapshots
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64
	snapshotting      atomic.Bool

	// Lifecycle
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	running   atomic.Bool
	stateMu   sync.RWMutex
	applyCh   chan *RaftLogEntry
	listeners []RaftEventListener
}

// RaftPeerState tracks replication state for a peer.
type RaftPeerState struct {
	ID         string
	Addr       string
	NextIndex  uint64
	MatchIndex uint64
	LastSeen   time.Time
	Healthy    bool
	Inflight   int32
}

// RaftProposal represents a pending proposal.
type RaftProposal struct {
	Index    uint64
	Term     uint64
	Command  []byte
	Response chan error
	Done     chan struct{}
}

// RaftEventListener receives Raft events.
type RaftEventListener interface {
	OnLeaderChange(leaderID string, term uint64)
	OnRoleChange(role RaftRole)
	OnCommit(index uint64)
	OnSnapshot(index uint64, term uint64)
}

// RaftLog manages the Raft log with persistence.
type RaftLog struct {
	entries    []*RaftLogEntry
	startIndex uint64
	mu         sync.RWMutex
	file       *os.File
	encoder    *gob.Encoder
}

// NewRaftLog creates a new Raft log.
func NewRaftLog(path string) (*RaftLog, error) {
	rl := &RaftLog{
		entries:    make([]*RaftLogEntry, 0),
		startIndex: 1,
	}

	if path != "" {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}

		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		rl.file = file
		rl.encoder = gob.NewEncoder(file)

		// Load existing entries
		if err := rl.load(); err != nil {
			return nil, err
		}
	}

	return rl, nil
}

func (rl *RaftLog) load() error {
	if rl.file == nil {
		return nil
	}

	// Seek to start
	if _, err := rl.file.Seek(0, 0); err != nil {
		return err
	}

	decoder := gob.NewDecoder(rl.file)
	for {
		var entry RaftLogEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if entry.Validate() {
			rl.entries = append(rl.entries, &entry)
		}
	}

	if len(rl.entries) > 0 {
		rl.startIndex = rl.entries[0].Index
	}

	return nil
}

// Append adds entries to the log.
func (rl *RaftLog) Append(entries ...*RaftLogEntry) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	for _, entry := range entries {
		entry.CRC = crc32.ChecksumIEEE(entry.Data)
		rl.entries = append(rl.entries, entry)

		if rl.encoder != nil {
			if err := rl.encoder.Encode(entry); err != nil {
				return err
			}
		}
	}

	if rl.file != nil {
		return rl.file.Sync()
	}
	return nil
}

// Get returns the entry at the given index.
func (rl *RaftLog) Get(index uint64) *RaftLogEntry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	if index < rl.startIndex || index > rl.LastIndex() {
		return nil
	}

	arrayIndex := index - rl.startIndex
	if int(arrayIndex) >= len(rl.entries) {
		return nil
	}
	return rl.entries[arrayIndex]
}

// GetRange returns entries from startIdx to endIdx (inclusive).
func (rl *RaftLog) GetRange(startIdx, endIdx uint64) []*RaftLogEntry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	if startIdx < rl.startIndex {
		startIdx = rl.startIndex
	}
	if endIdx > rl.LastIndex() {
		endIdx = rl.LastIndex()
	}
	if startIdx > endIdx {
		return nil
	}

	start := startIdx - rl.startIndex
	end := endIdx - rl.startIndex + 1
	if int(end) > len(rl.entries) {
		end = uint64(len(rl.entries))
	}

	result := make([]*RaftLogEntry, end-start)
	copy(result, rl.entries[start:end])
	return result
}

// LastIndex returns the last log index.
func (rl *RaftLog) LastIndex() uint64 {
	if len(rl.entries) == 0 {
		return rl.startIndex - 1
	}
	return rl.entries[len(rl.entries)-1].Index
}

// LastTerm returns the term of the last entry.
func (rl *RaftLog) LastTerm() uint64 {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	if len(rl.entries) == 0 {
		return 0
	}
	return rl.entries[len(rl.entries)-1].Term
}

// TermAt returns the term at the given index.
func (rl *RaftLog) TermAt(index uint64) uint64 {
	entry := rl.Get(index)
	if entry == nil {
		return 0
	}
	return entry.Term
}

// TruncateAfter removes all entries after the given index.
func (rl *RaftLog) TruncateAfter(index uint64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if index < rl.startIndex {
		rl.entries = rl.entries[:0]
		return
	}

	keepCount := index - rl.startIndex + 1
	if uint64(len(rl.entries)) > keepCount {
		rl.entries = rl.entries[:keepCount]
	}
}

// CompactBefore removes entries before the given index.
func (rl *RaftLog) CompactBefore(index uint64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if index <= rl.startIndex {
		return
	}

	removeCount := index - rl.startIndex
	if removeCount >= uint64(len(rl.entries)) {
		rl.entries = rl.entries[:0]
		rl.startIndex = index
		return
	}

	rl.entries = rl.entries[removeCount:]
	rl.startIndex = index
}

// Len returns the number of entries.
func (rl *RaftLog) Len() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return len(rl.entries)
}

// Close closes the log file.
func (rl *RaftLog) Close() error {
	if rl.file != nil {
		return rl.file.Close()
	}
	return nil
}

// RaftTransport handles RPC communication.
type RaftTransport struct {
	localAddr string
	client    *http.Client
	timeout   time.Duration
}

// NewRaftTransport creates a new Raft transport.
func NewRaftTransport(localAddr string, timeout time.Duration) *RaftTransport {
	return &RaftTransport{
		localAddr: localAddr,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		timeout: timeout,
	}
}

// NewRaftNode creates a new Raft node.
func NewRaftNode(db *DB, config RaftConfig) (*RaftNode, error) {
	if config.NodeID == "" {
		config.NodeID = fmt.Sprintf("raft-%d", time.Now().UnixNano()%10000)
	}
	if config.ElectionTimeoutMin <= 0 {
		config.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if config.ElectionTimeoutMax <= 0 {
		config.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 50 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())

	logPath := ""
	if config.DataDir != "" {
		logPath = filepath.Join(config.DataDir, "raft.log")
	}

	log, err := NewRaftLog(logPath)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create raft log: %w", err)
	}

	rn := &RaftNode{
		config:      config,
		db:          db,
		currentTerm: 0,
		votedFor:    "",
		log:         log,
		commitIndex: 0,
		lastApplied: 0,
		role:        RaftRoleFollower,
		peers:       make(map[string]*RaftPeerState),
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		proposals:   make(map[uint64]*RaftProposal),
		transport:   NewRaftTransport(config.AdvertiseAddr, config.ElectionTimeoutMin),
		ctx:         ctx,
		cancel:      cancel,
		applyCh:     make(chan *RaftLogEntry, 1000),
		batchCh:     make(chan *RaftProposal, config.BatchMaxSize),
		batchDoneCh: make(chan struct{}),
	}

	// Initialize peers
	for _, peer := range config.Peers {
		rn.peers[peer.ID] = &RaftPeerState{
			ID:        peer.ID,
			Addr:      peer.Addr,
			NextIndex: 1,
			Healthy:   true,
		}
	}

	// Load persistent state
	if err := rn.loadState(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	return rn, nil
}

// Start begins Raft operations.
func (rn *RaftNode) Start() error {
	if rn.running.Load() {
		return errors.New("raft node already running")
	}
	rn.running.Store(true)

	// Start HTTP server
	if rn.config.BindAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/raft/vote", rn.handleRequestVote)
		mux.HandleFunc("/raft/prevote", rn.handlePreVote)
		mux.HandleFunc("/raft/append", rn.handleAppendEntries)
		mux.HandleFunc("/raft/snapshot", rn.handleInstallSnapshot)
		mux.HandleFunc("/raft/forward", rn.handleForwardProposal)

		rn.server = &http.Server{
			Addr:    rn.config.BindAddr,
			Handler: mux,
		}

		rn.wg.Add(1)
		go func() {
			defer rn.wg.Done()
			if err := rn.server.ListenAndServe(); err != http.ErrServerClosed {
				// Log error
			}
		}()
	}

	// Start apply goroutine
	rn.wg.Add(1)
	go rn.applyLoop()

	// Start election timer
	rn.resetElectionTimer()

	// Start batching if enabled
	if rn.config.BatchingEnabled {
		rn.wg.Add(1)
		go rn.batchLoop()
	}

	// Start quorum check if enabled
	if rn.config.CheckQuorumEnabled {
		rn.wg.Add(1)
		go rn.quorumCheckLoop()
	}

	// Start snapshot loop
	if rn.config.SnapshotInterval > 0 {
		rn.wg.Add(1)
		go rn.snapshotLoop()
	}

	return nil
}

// Stop stops Raft operations.
func (rn *RaftNode) Stop() error {
	if !rn.running.Load() {
		return nil
	}
	rn.running.Store(false)
	rn.cancel()

	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	if rn.heartbeatTicker != nil {
		rn.heartbeatTicker.Stop()
	}

	close(rn.batchDoneCh)

	if rn.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = rn.server.Shutdown(ctx)
	}

	rn.wg.Wait()

	if err := rn.log.Close(); err != nil {
		return err
	}

	return rn.saveState()
}

// Propose submits a command to the Raft cluster.
func (rn *RaftNode) Propose(ctx context.Context, command []byte) error {
	if !rn.running.Load() {
		return errors.New("raft node not running")
	}

	rn.stateMu.RLock()
	role := rn.role
	leaderID := rn.leaderID
	rn.stateMu.RUnlock()

	// If not leader, forward to leader
	if role != RaftRoleLeader {
		if leaderID == "" {
			return errors.New("no leader available")
		}
		return rn.forwardToLeader(ctx, command)
	}

	// Create proposal
	proposal := &RaftProposal{
		Command:  command,
		Response: make(chan error, 1),
		Done:     make(chan struct{}),
	}

	// Use batching if enabled
	if rn.config.BatchingEnabled {
		select {
		case rn.batchCh <- proposal:
		case <-ctx.Done():
			return ctx.Err()
		case <-rn.ctx.Done():
			return errors.New("raft node stopped")
		}
	} else {
		if err := rn.appendProposal(proposal); err != nil {
			return err
		}
	}

	// Wait for commit
	select {
	case err := <-proposal.Response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-rn.ctx.Done():
		return errors.New("raft node stopped")
	}
}

func (rn *RaftNode) appendProposal(proposal *RaftProposal) error {
	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()

	if rn.role != RaftRoleLeader {
		return errors.New("not the leader")
	}

	// Create log entry
	entry := &RaftLogEntry{
		Index: rn.log.LastIndex() + 1,
		Term:  rn.currentTerm,
		Type:  RaftLogCommand,
		Data:  proposal.Command,
	}

	if err := rn.log.Append(entry); err != nil {
		return err
	}

	proposal.Index = entry.Index
	proposal.Term = entry.Term

	rn.proposalsMu.Lock()
	rn.proposals[entry.Index] = proposal
	rn.proposalsMu.Unlock()

	// Trigger replication
	go rn.replicateToFollowers()

	return nil
}

func (rn *RaftNode) batchLoop() {
	defer rn.wg.Done()

	batch := make([]*RaftProposal, 0, rn.config.BatchMaxSize)
	timer := time.NewTimer(rn.config.BatchMaxWait)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		rn.stateMu.Lock()
		if rn.role != RaftRoleLeader {
			rn.stateMu.Unlock()
			for _, p := range batch {
				p.Response <- errors.New("not the leader")
			}
			batch = batch[:0]
			return
		}

		// Create entries for all proposals
		baseIndex := rn.log.LastIndex() + 1
		entries := make([]*RaftLogEntry, len(batch))

		for i, p := range batch {
			entry := &RaftLogEntry{
				Index: baseIndex + uint64(i),
				Term:  rn.currentTerm,
				Type:  RaftLogCommand,
				Data:  p.Command,
			}
			entries[i] = entry
			p.Index = entry.Index
			p.Term = entry.Term
		}

		if err := rn.log.Append(entries...); err != nil {
			rn.stateMu.Unlock()
			for _, p := range batch {
				p.Response <- err
			}
			batch = batch[:0]
			return
		}

		rn.proposalsMu.Lock()
		for _, p := range batch {
			rn.proposals[p.Index] = p
		}
		rn.proposalsMu.Unlock()

		rn.stateMu.Unlock()

		// Trigger replication
		go rn.replicateToFollowers()

		batch = batch[:0]
	}

	for {
		select {
		case <-rn.batchDoneCh:
			flush()
			return
		case <-rn.ctx.Done():
			flush()
			return
		case p := <-rn.batchCh:
			batch = append(batch, p)
			if len(batch) >= rn.config.BatchMaxSize {
				flush()
				timer.Reset(rn.config.BatchMaxWait)
			}
		case <-timer.C:
			flush()
			timer.Reset(rn.config.BatchMaxWait)
		}
	}
}

// LinearizableRead performs a linearizable read.
func (rn *RaftNode) LinearizableRead(ctx context.Context) error {
	rn.stateMu.RLock()
	role := rn.role
	leaseExpiry := rn.leaseExpiry
	rn.stateMu.RUnlock()

	if role != RaftRoleLeader {
		return errors.New("not the leader")
	}

	// Check leader lease
	if time.Now().Before(leaseExpiry) && rn.leaseExtended.Load() {
		return nil
	}

	// Need to confirm leadership via heartbeat round
	return rn.confirmLeadership(ctx)
}

func (rn *RaftNode) confirmLeadership(ctx context.Context) error {
	rn.peersMu.RLock()
	peers := make([]*RaftPeerState, 0, len(rn.peers))
	for _, p := range rn.peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	rn.peersMu.RUnlock()

	quorum := (len(peers)+1)/2 + 1
	responses := make(chan bool, len(peers))

	for _, peer := range peers {
		go func(p *RaftPeerState) {
			success := rn.sendHeartbeat(p)
			responses <- success
		}(peer)
	}

	successCount := 1 // Self
	timeout := time.After(rn.config.HeartbeatInterval * 2)

	for i := 0; i < len(peers); i++ {
		select {
		case success := <-responses:
			if success {
				successCount++
			}
		case <-timeout:
			// Continue with what we have
		case <-ctx.Done():
			return ctx.Err()
		}

		if successCount >= quorum {
			rn.stateMu.Lock()
			rn.leaseExpiry = time.Now().Add(rn.config.LeaseTimeout)
			rn.leaseExtended.Store(true)
			rn.stateMu.Unlock()
			return nil
		}
	}

	return errors.New("failed to confirm leadership")
}

// Role returns the current role.
func (rn *RaftNode) Role() RaftRole {
	rn.stateMu.RLock()
	defer rn.stateMu.RUnlock()
	return rn.role
}

// Leader returns the current leader ID.
func (rn *RaftNode) Leader() string {
	rn.stateMu.RLock()
	defer rn.stateMu.RUnlock()
	return rn.leaderID
}

// Term returns the current term.
func (rn *RaftNode) Term() uint64 {
	rn.stateMu.RLock()
	defer rn.stateMu.RUnlock()
	return rn.currentTerm
}

// IsLeader returns true if this node is the leader.
func (rn *RaftNode) IsLeader() bool {
	return rn.Role() == RaftRoleLeader
}

// Stats returns Raft statistics.
func (rn *RaftNode) Stats() RaftStats {
	rn.stateMu.RLock()
	role := rn.role
	term := rn.currentTerm
	leaderID := rn.leaderID
	commitIndex := rn.commitIndex
	lastApplied := rn.lastApplied
	rn.stateMu.RUnlock()

	rn.peersMu.RLock()
	peerCount := len(rn.peers)
	healthyPeers := 0
	for _, p := range rn.peers {
		if p.Healthy {
			healthyPeers++
		}
	}
	rn.peersMu.RUnlock()

	return RaftStats{
		NodeID:            rn.config.NodeID,
		Role:              role.String(),
		Term:              term,
		LeaderID:          leaderID,
		CommitIndex:       commitIndex,
		LastApplied:       lastApplied,
		LastLogIndex:      rn.log.LastIndex(),
		LastLogTerm:       rn.log.LastTerm(),
		LogLength:         rn.log.Len(),
		PeerCount:         peerCount,
		HealthyPeers:      healthyPeers,
		SnapshotIndex:     rn.lastSnapshotIndex,
		SnapshotTerm:      rn.lastSnapshotTerm,
		LeaseValid:        time.Now().Before(rn.leaseExpiry),
		PendingProposals:  len(rn.proposals),
		BatchingEnabled:   rn.config.BatchingEnabled,
		PreVoteEnabled:    rn.config.PreVoteEnabled,
		CheckQuorumActive: rn.config.CheckQuorumEnabled,
	}
}

// RaftStats contains Raft statistics.
type RaftStats struct {
	NodeID            string `json:"node_id"`
	Role              string `json:"role"`
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader_id"`
	CommitIndex       uint64 `json:"commit_index"`
	LastApplied       uint64 `json:"last_applied"`
	LastLogIndex      uint64 `json:"last_log_index"`
	LastLogTerm       uint64 `json:"last_log_term"`
	LogLength         int    `json:"log_length"`
	PeerCount         int    `json:"peer_count"`
	HealthyPeers      int    `json:"healthy_peers"`
	SnapshotIndex     uint64 `json:"snapshot_index"`
	SnapshotTerm      uint64 `json:"snapshot_term"`
	LeaseValid        bool   `json:"lease_valid"`
	PendingProposals  int    `json:"pending_proposals"`
	BatchingEnabled   bool   `json:"batching_enabled"`
	PreVoteEnabled    bool   `json:"pre_vote_enabled"`
	CheckQuorumActive bool   `json:"check_quorum_active"`
}

// Election management

func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}

	// Randomize timeout
	timeoutRange := rn.config.ElectionTimeoutMax - rn.config.ElectionTimeoutMin
	timeout := rn.config.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(timeoutRange)))
	rn.electionTimer = time.AfterFunc(timeout, rn.electionTimeout)
}

func (rn *RaftNode) electionTimeout() {
	if !rn.running.Load() {
		return
	}

	rn.stateMu.RLock()
	role := rn.role
	rn.stateMu.RUnlock()

	if role == RaftRoleLeader {
		return
	}

	// Use pre-vote if enabled
	if rn.config.PreVoteEnabled {
		rn.startPreVote()
	} else {
		rn.startElection()
	}
}

func (rn *RaftNode) startPreVote() {
	rn.stateMu.Lock()
	rn.role = RaftRolePreCandidate
	term := rn.currentTerm + 1 // Don't increment yet
	lastLogIndex := rn.log.LastIndex()
	lastLogTerm := rn.log.LastTerm()
	rn.stateMu.Unlock()

	rn.notifyRoleChange(RaftRolePreCandidate)

	rn.peersMu.RLock()
	peers := make([]*RaftPeerState, 0, len(rn.peers))
	for _, p := range rn.peers {
		peers = append(peers, p)
	}
	rn.peersMu.RUnlock()

	votesNeeded := (len(peers)+1)/2 + 1
	votesCh := make(chan bool, len(peers))

	for _, peer := range peers {
		go func(p *RaftPeerState) {
			granted := rn.sendPreVote(p, term, lastLogIndex, lastLogTerm)
			votesCh <- granted
		}(peer)
	}

	votes := 1 // Vote for self
	for i := 0; i < len(peers); i++ {
		select {
		case granted := <-votesCh:
			if granted {
				votes++
			}
		case <-time.After(rn.config.ElectionTimeoutMin):
			// Timeout
		case <-rn.ctx.Done():
			return
		}

		if votes >= votesNeeded {
			rn.startElection()
			return
		}
	}

	// Pre-vote failed, reset timer
	rn.stateMu.Lock()
	rn.role = RaftRoleFollower
	rn.stateMu.Unlock()
	rn.notifyRoleChange(RaftRoleFollower)
	rn.resetElectionTimer()
}

func (rn *RaftNode) startElection() {
	rn.stateMu.Lock()
	rn.role = RaftRoleCandidate
	rn.currentTerm++
	rn.votedFor = rn.config.NodeID
	term := rn.currentTerm
	lastLogIndex := rn.log.LastIndex()
	lastLogTerm := rn.log.LastTerm()
	rn.stateMu.Unlock()

	rn.notifyRoleChange(RaftRoleCandidate)
	_ = rn.saveState()

	rn.peersMu.RLock()
	peers := make([]*RaftPeerState, 0, len(rn.peers))
	for _, p := range rn.peers {
		peers = append(peers, p)
	}
	rn.peersMu.RUnlock()

	votesNeeded := (len(peers)+1)/2 + 1
	votesCh := make(chan bool, len(peers))

	for _, peer := range peers {
		go func(p *RaftPeerState) {
			granted := rn.sendRequestVote(p, term, lastLogIndex, lastLogTerm)
			votesCh <- granted
		}(peer)
	}

	votes := 1 // Vote for self
	for i := 0; i < len(peers); i++ {
		select {
		case granted := <-votesCh:
			if granted {
				votes++
			}
		case <-time.After(rn.config.ElectionTimeoutMax):
			// Timeout
		case <-rn.ctx.Done():
			return
		}

		rn.stateMu.RLock()
		currentRole := rn.role
		currentTerm := rn.currentTerm
		rn.stateMu.RUnlock()

		// Check if we're still a candidate for the same term
		if currentRole != RaftRoleCandidate || currentTerm != term {
			return
		}

		if votes >= votesNeeded {
			rn.becomeLeader()
			return
		}
	}

	// Election failed, reset timer
	rn.resetElectionTimer()
}

func (rn *RaftNode) becomeLeader() {
	rn.stateMu.Lock()
	rn.role = RaftRoleLeader
	rn.leaderID = rn.config.NodeID
	term := rn.currentTerm
	lastIndex := rn.log.LastIndex()
	rn.stateMu.Unlock()

	// Initialize leader state
	rn.peersMu.Lock()
	for _, p := range rn.peers {
		p.NextIndex = lastIndex + 1
		p.MatchIndex = 0
	}
	rn.peersMu.Unlock()

	// Stop election timer
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}

	// Append noop entry to commit previous term entries
	entry := &RaftLogEntry{
		Index: rn.log.LastIndex() + 1,
		Term:  term,
		Type:  RaftLogNoop,
	}
	_ = rn.log.Append(entry)

	// Start heartbeat ticker
	rn.heartbeatTicker = time.NewTicker(rn.config.HeartbeatInterval)
	go rn.heartbeatLoop()

	rn.notifyRoleChange(RaftRoleLeader)
	rn.notifyLeaderChange(rn.config.NodeID, term)
}

func (rn *RaftNode) becomeFollower(term uint64, leaderID string) {
	rn.stateMu.Lock()
	oldRole := rn.role
	rn.role = RaftRoleFollower
	rn.currentTerm = term
	rn.votedFor = ""
	rn.leaderID = leaderID
	rn.leaseExtended.Store(false)
	rn.stateMu.Unlock()

	// Stop heartbeat ticker if we were leader
	if oldRole == RaftRoleLeader && rn.heartbeatTicker != nil {
		rn.heartbeatTicker.Stop()
	}

	rn.resetElectionTimer()
	_ = rn.saveState()

	if oldRole != RaftRoleFollower {
		rn.notifyRoleChange(RaftRoleFollower)
	}
	if leaderID != "" {
		rn.notifyLeaderChange(leaderID, term)
	}
}

func (rn *RaftNode) heartbeatLoop() {
	for {
		select {
		case <-rn.ctx.Done():
			return
		case <-rn.heartbeatTicker.C:
			rn.stateMu.RLock()
			isLeader := rn.role == RaftRoleLeader
			rn.stateMu.RUnlock()

			if !isLeader {
				return
			}

			rn.replicateToFollowers()
		}
	}
}

func (rn *RaftNode) replicateToFollowers() {
	rn.peersMu.RLock()
	peers := make([]*RaftPeerState, 0, len(rn.peers))
	for _, p := range rn.peers {
		peers = append(peers, p)
	}
	rn.peersMu.RUnlock()

	for _, peer := range peers {
		go rn.replicateToPeer(peer)
	}
}

func (rn *RaftNode) replicateToPeer(peer *RaftPeerState) {
	// Check inflight limit
	if atomic.LoadInt32(&peer.Inflight) >= int32(rn.config.MaxInflightRequests) {
		return
	}
	atomic.AddInt32(&peer.Inflight, 1)
	defer atomic.AddInt32(&peer.Inflight, -1)

	rn.peersMu.RLock()
	nextIndex := peer.NextIndex
	rn.peersMu.RUnlock()

	rn.stateMu.RLock()
	term := rn.currentTerm
	commitIndex := rn.commitIndex
	rn.stateMu.RUnlock()

	// Get entries to send
	lastLogIndex := rn.log.LastIndex()
	var entries []*RaftLogEntry
	if nextIndex <= lastLogIndex {
		endIndex := nextIndex + uint64(rn.config.MaxAppendEntries) - 1
		if endIndex > lastLogIndex {
			endIndex = lastLogIndex
		}
		entries = rn.log.GetRange(nextIndex, endIndex)
	}

	// Get prev log info
	prevLogIndex := nextIndex - 1
	prevLogTerm := rn.log.TermAt(prevLogIndex)

	req := &AppendEntriesRPCRequest{
		Term:         term,
		LeaderID:     rn.config.NodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	resp, err := rn.sendAppendEntries(peer, req)
	if err != nil {
		rn.peersMu.Lock()
		peer.Healthy = false
		rn.peersMu.Unlock()
		return
	}

	rn.peersMu.Lock()
	peer.LastSeen = time.Now()
	peer.Healthy = true
	rn.peersMu.Unlock()

	if resp.Term > term {
		rn.becomeFollower(resp.Term, "")
		return
	}

	if resp.Success {
		rn.peersMu.Lock()
		if len(entries) > 0 {
			peer.NextIndex = entries[len(entries)-1].Index + 1
			peer.MatchIndex = entries[len(entries)-1].Index
		}
		rn.peersMu.Unlock()

		// Update commit index
		rn.maybeAdvanceCommitIndex()
	} else {
		// Decrement nextIndex and retry
		rn.peersMu.Lock()
		if resp.ConflictIndex > 0 {
			peer.NextIndex = resp.ConflictIndex
		} else if peer.NextIndex > 1 {
			peer.NextIndex--
		}
		rn.peersMu.Unlock()

		// Retry with lower nextIndex
		go rn.replicateToPeer(peer)
	}
}

func (rn *RaftNode) maybeAdvanceCommitIndex() {
	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()

	if rn.role != RaftRoleLeader {
		return
	}

	// Find the highest index replicated on a majority
	rn.peersMu.RLock()
	matchIndexes := make([]uint64, 0, len(rn.peers)+1)
	matchIndexes = append(matchIndexes, rn.log.LastIndex()) // Self
	for _, p := range rn.peers {
		matchIndexes = append(matchIndexes, p.MatchIndex)
	}
	rn.peersMu.RUnlock()

	sort.Slice(matchIndexes, func(i, j int) bool {
		return matchIndexes[i] > matchIndexes[j]
	})

	majority := len(matchIndexes) / 2
	newCommitIndex := matchIndexes[majority]

	// Only commit entries from the current term
	if newCommitIndex > rn.commitIndex && rn.log.TermAt(newCommitIndex) == rn.currentTerm {
		oldCommitIndex := rn.commitIndex
		rn.commitIndex = newCommitIndex

		// Notify proposals
		rn.proposalsMu.Lock()
		for idx := oldCommitIndex + 1; idx <= newCommitIndex; idx++ {
			if proposal, ok := rn.proposals[idx]; ok {
				proposal.Response <- nil
				delete(rn.proposals, idx)
			}
		}
		rn.proposalsMu.Unlock()

		// Trigger apply
		go rn.applyCommitted()

		// Extend leader lease
		rn.leaseExpiry = time.Now().Add(rn.config.LeaseTimeout)
		rn.leaseExtended.Store(true)

		rn.notifyCommit(newCommitIndex)
	}
}

func (rn *RaftNode) applyLoop() {
	defer rn.wg.Done()

	for {
		select {
		case <-rn.ctx.Done():
			return
		case entry := <-rn.applyCh:
			if entry.Type == RaftLogCommand {
				var cmd RaftCommand
				if err := json.Unmarshal(entry.Data, &cmd); err == nil {
					_ = rn.applyCommand(cmd)
				}
			}
		}
	}
}

func (rn *RaftNode) applyCommitted() {
	rn.stateMu.Lock()
	commitIndex := rn.commitIndex
	lastApplied := rn.lastApplied
	rn.stateMu.Unlock()

	for idx := lastApplied + 1; idx <= commitIndex; idx++ {
		entry := rn.log.Get(idx)
		if entry == nil {
			continue
		}

		select {
		case rn.applyCh <- entry:
		case <-rn.ctx.Done():
			return
		}

		rn.stateMu.Lock()
		rn.lastApplied = idx
		rn.stateMu.Unlock()
	}
}

func (rn *RaftNode) applyCommand(cmd RaftCommand) error {
	switch cmd.Op {
	case "write":
		var p Point
		if err := json.Unmarshal(cmd.Value, &p); err != nil {
			return err
		}
		return rn.db.Write(p)
	case "delete":
		// Handle delete
	}
	return nil
}

// Quorum check loop

func (rn *RaftNode) quorumCheckLoop() {
	defer rn.wg.Done()

	ticker := time.NewTicker(rn.config.ElectionTimeoutMin)
	defer ticker.Stop()

	for {
		select {
		case <-rn.ctx.Done():
			return
		case <-ticker.C:
			rn.checkQuorum()
		}
	}
}

func (rn *RaftNode) checkQuorum() {
	rn.stateMu.RLock()
	isLeader := rn.role == RaftRoleLeader
	rn.stateMu.RUnlock()

	if !isLeader {
		return
	}

	rn.peersMu.RLock()
	healthyCount := 1 // Self
	totalCount := len(rn.peers) + 1
	for _, p := range rn.peers {
		if p.Healthy && time.Since(p.LastSeen) < rn.config.ElectionTimeoutMax*2 {
			healthyCount++
		}
	}
	rn.peersMu.RUnlock()

	quorum := totalCount/2 + 1
	if healthyCount < quorum {
		// Lost quorum, step down
		rn.stateMu.RLock()
		term := rn.currentTerm
		rn.stateMu.RUnlock()
		rn.becomeFollower(term, "")
	}
}

// Snapshot management

func (rn *RaftNode) snapshotLoop() {
	defer rn.wg.Done()

	ticker := time.NewTicker(rn.config.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rn.ctx.Done():
			return
		case <-ticker.C:
			rn.maybeSnapshot()
		}
	}
}

func (rn *RaftNode) maybeSnapshot() {
	if rn.snapshotting.Load() {
		return
	}

	rn.stateMu.RLock()
	lastApplied := rn.lastApplied
	rn.stateMu.RUnlock()

	logLen := uint64(rn.log.Len())
	if logLen < rn.config.SnapshotThreshold {
		return
	}

	rn.snapshotting.Store(true)
	defer rn.snapshotting.Store(false)

	// Take snapshot
	snapshot, err := rn.createSnapshot(lastApplied)
	if err != nil {
		return
	}

	// Save snapshot
	if err := rn.saveSnapshot(snapshot); err != nil {
		return
	}

	// Compact log
	rn.log.CompactBefore(lastApplied)

	rn.stateMu.Lock()
	rn.lastSnapshotIndex = lastApplied
	rn.lastSnapshotTerm = rn.log.TermAt(lastApplied)
	rn.stateMu.Unlock()

	rn.notifySnapshot(rn.lastSnapshotIndex, rn.lastSnapshotTerm)
}

// RaftSnapshot represents a snapshot of the state machine.
type RaftSnapshot struct {
	LastIndex uint64 `json:"last_index"`
	LastTerm  uint64 `json:"last_term"`
	Data      []byte `json:"data"`
}

func (rn *RaftNode) createSnapshot(lastIndex uint64) (*RaftSnapshot, error) {
	// For Chronicle, we'd snapshot the database state
	// This is a placeholder implementation
	return &RaftSnapshot{
		LastIndex: lastIndex,
		LastTerm:  rn.log.TermAt(lastIndex),
		Data:      []byte{},
	}, nil
}

func (rn *RaftNode) saveSnapshot(snapshot *RaftSnapshot) error {
	if rn.config.DataDir == "" {
		return nil
	}

	path := filepath.Join(rn.config.DataDir, "snapshot.dat")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(snapshot)
}

// RPC methods

func (rn *RaftNode) sendPreVote(peer *RaftPeerState, term, lastLogIndex, lastLogTerm uint64) bool {
	req := &PreVoteRPCRequest{
		Term:         term,
		CandidateID:  rn.config.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return false
	}

	httpReq, err := http.NewRequestWithContext(rn.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/raft/prevote", peer.Addr),
		bytes.NewReader(data))
	if err != nil {
		return false
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := rn.transport.client.Do(httpReq)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	var voteResp PreVoteRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		return false
	}

	return voteResp.VoteGranted
}

func (rn *RaftNode) sendRequestVote(peer *RaftPeerState, term, lastLogIndex, lastLogTerm uint64) bool {
	req := &RequestVoteRPCRequest{
		Term:         term,
		CandidateID:  rn.config.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return false
	}

	httpReq, err := http.NewRequestWithContext(rn.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/raft/vote", peer.Addr),
		bytes.NewReader(data))
	if err != nil {
		return false
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := rn.transport.client.Do(httpReq)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	var voteResp RequestVoteRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		return false
	}

	if voteResp.Term > term {
		rn.becomeFollower(voteResp.Term, "")
		return false
	}

	return voteResp.VoteGranted
}

func (rn *RaftNode) sendHeartbeat(peer *RaftPeerState) bool {
	rn.stateMu.RLock()
	term := rn.currentTerm
	commitIndex := rn.commitIndex
	rn.stateMu.RUnlock()

	req := &AppendEntriesRPCRequest{
		Term:         term,
		LeaderID:     rn.config.NodeID,
		LeaderCommit: commitIndex,
	}

	resp, err := rn.sendAppendEntries(peer, req)
	if err != nil {
		return false
	}

	return resp.Success
}

func (rn *RaftNode) sendAppendEntries(peer *RaftPeerState, req *AppendEntriesRPCRequest) (*AppendEntriesRPCResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(rn.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/raft/append", peer.Addr),
		bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := rn.transport.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var appendResp AppendEntriesRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&appendResp); err != nil {
		return nil, err
	}

	return &appendResp, nil
}

func (rn *RaftNode) forwardToLeader(ctx context.Context, command []byte) error {
	rn.stateMu.RLock()
	leaderID := rn.leaderID
	rn.stateMu.RUnlock()

	if leaderID == "" {
		return errors.New("no leader available")
	}

	rn.peersMu.RLock()
	leader, ok := rn.peers[leaderID]
	rn.peersMu.RUnlock()

	if !ok {
		return errors.New("leader not found in peers")
	}

	req := &ForwardProposalRequest{
		Command: command,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://%s/raft/forward", leader.Addr),
		bytes.NewReader(data))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := rn.transport.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("forward failed: %s", string(body))
	}

	return nil
}

// HTTP handlers

func (rn *RaftNode) handlePreVote(w http.ResponseWriter, r *http.Request) {
	var req PreVoteRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()

	resp := PreVoteRPCResponse{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	// Pre-vote: don't update term
	if req.Term < rn.currentTerm {
		rn.writeJSON(w, resp)
		return
	}

	// Check if candidate's log is up-to-date
	logOK := req.LastLogTerm > rn.log.LastTerm() ||
		(req.LastLogTerm == rn.log.LastTerm() && req.LastLogIndex >= rn.log.LastIndex())

	if logOK {
		resp.VoteGranted = true
	}

	rn.writeJSON(w, resp)
}

func (rn *RaftNode) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var req RequestVoteRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()

	resp := RequestVoteRPCResponse{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	if req.Term < rn.currentTerm {
		rn.writeJSON(w, resp)
		return
	}

	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.role = RaftRoleFollower
		rn.votedFor = ""
		rn.leaderID = ""
		rn.resetElectionTimer()
	}

	resp.Term = rn.currentTerm

	// Check if candidate's log is up-to-date
	logOK := req.LastLogTerm > rn.log.LastTerm() ||
		(req.LastLogTerm == rn.log.LastTerm() && req.LastLogIndex >= rn.log.LastIndex())

	if (rn.votedFor == "" || rn.votedFor == req.CandidateID) && logOK {
		rn.votedFor = req.CandidateID
		resp.VoteGranted = true
		rn.resetElectionTimer()
		_ = rn.saveState()
	}

	rn.writeJSON(w, resp)
}

func (rn *RaftNode) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()

	resp := AppendEntriesRPCResponse{
		Term:    rn.currentTerm,
		Success: false,
	}

	if req.Term < rn.currentTerm {
		rn.writeJSON(w, resp)
		return
	}

	rn.resetElectionTimer()

	if req.Term > rn.currentTerm || rn.role != RaftRoleFollower {
		rn.currentTerm = req.Term
		rn.role = RaftRoleFollower
		rn.votedFor = ""
	}
	rn.leaderID = req.LeaderID
	resp.Term = rn.currentTerm

	// Check log consistency
	if req.PrevLogIndex > 0 {
		prevTerm := rn.log.TermAt(req.PrevLogIndex)
		if prevTerm == 0 || prevTerm != req.PrevLogTerm {
			// Log doesn't contain entry at prevLogIndex with matching term
			resp.ConflictIndex = rn.log.LastIndex() + 1
			if prevTerm != 0 {
				// Find first index of conflicting term
				for i := req.PrevLogIndex; i > 0; i-- {
					if rn.log.TermAt(i) != prevTerm {
						resp.ConflictIndex = i + 1
						break
					}
				}
			}
			rn.writeJSON(w, resp)
			return
		}
	}

	// Append entries
	if len(req.Entries) > 0 {
		// Find conflict point
		for i, entry := range req.Entries {
			existingTerm := rn.log.TermAt(entry.Index)
			if existingTerm == 0 {
				// Need to append from here
				_ = rn.log.Append(req.Entries[i:]...)
				break
			} else if existingTerm != entry.Term {
				// Conflict: truncate and append
				rn.log.TruncateAfter(entry.Index - 1)
				_ = rn.log.Append(req.Entries[i:]...)
				break
			}
		}
	}

	// Update commit index
	if req.LeaderCommit > rn.commitIndex {
		newCommit := req.LeaderCommit
		if lastIndex := rn.log.LastIndex(); lastIndex < newCommit {
			newCommit = lastIndex
		}
		rn.commitIndex = newCommit
		go rn.applyCommitted()
	}

	resp.Success = true
	rn.writeJSON(w, resp)
}

func (rn *RaftNode) handleInstallSnapshot(w http.ResponseWriter, r *http.Request) {
	var req InstallSnapshotRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()

	resp := InstallSnapshotRPCResponse{
		Term: rn.currentTerm,
	}

	if req.Term < rn.currentTerm {
		rn.writeJSON(w, resp)
		return
	}

	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.role = RaftRoleFollower
		rn.votedFor = ""
	}
	rn.leaderID = req.LeaderID

	// Apply snapshot
	rn.lastSnapshotIndex = req.LastIncludedIndex
	rn.lastSnapshotTerm = req.LastIncludedTerm
	rn.log.CompactBefore(req.LastIncludedIndex + 1)
	rn.commitIndex = req.LastIncludedIndex
	rn.lastApplied = req.LastIncludedIndex

	// TODO: Actually restore state machine from snapshot

	resp.Term = rn.currentTerm
	rn.writeJSON(w, resp)
}

func (rn *RaftNode) handleForwardProposal(w http.ResponseWriter, r *http.Request) {
	var req ForwardProposalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), rn.config.ElectionTimeoutMax*2)
	defer cancel()

	if err := rn.Propose(ctx, req.Command); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (rn *RaftNode) writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// Persistence

func (rn *RaftNode) loadState() error {
	if rn.config.DataDir == "" {
		return nil
	}

	path := filepath.Join(rn.config.DataDir, "raft.state")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var state persistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	rn.currentTerm = state.CurrentTerm
	rn.votedFor = state.VotedFor
	return nil
}

func (rn *RaftNode) saveState() error {
	if rn.config.DataDir == "" {
		return nil
	}

	state := persistentState{
		CurrentTerm: rn.currentTerm,
		VotedFor:    rn.votedFor,
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	path := filepath.Join(rn.config.DataDir, "raft.state")
	return os.WriteFile(path, data, 0644)
}

type persistentState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

// Event notifications

func (rn *RaftNode) AddListener(l RaftEventListener) {
	rn.stateMu.Lock()
	rn.listeners = append(rn.listeners, l)
	rn.stateMu.Unlock()
}

func (rn *RaftNode) notifyLeaderChange(leaderID string, term uint64) {
	rn.stateMu.RLock()
	listeners := rn.listeners
	rn.stateMu.RUnlock()

	for _, l := range listeners {
		go l.OnLeaderChange(leaderID, term)
	}
}

func (rn *RaftNode) notifyRoleChange(role RaftRole) {
	rn.stateMu.RLock()
	listeners := rn.listeners
	rn.stateMu.RUnlock()

	for _, l := range listeners {
		go l.OnRoleChange(role)
	}
}

func (rn *RaftNode) notifyCommit(index uint64) {
	rn.stateMu.RLock()
	listeners := rn.listeners
	rn.stateMu.RUnlock()

	for _, l := range listeners {
		go l.OnCommit(index)
	}
}

func (rn *RaftNode) notifySnapshot(index, term uint64) {
	rn.stateMu.RLock()
	listeners := rn.listeners
	rn.stateMu.RUnlock()

	for _, l := range listeners {
		go l.OnSnapshot(index, term)
	}
}

// RPC message types

// PreVoteRPCRequest is a pre-vote request.
type PreVoteRPCRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// PreVoteRPCResponse is a pre-vote response.
type PreVoteRPCResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// RequestVoteRPCRequest is a request vote request.
type RequestVoteRPCRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteRPCResponse is a request vote response.
type RequestVoteRPCResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesRPCRequest is an append entries request.
type AppendEntriesRPCRequest struct {
	Term         uint64           `json:"term"`
	LeaderID     string           `json:"leader_id"`
	PrevLogIndex uint64           `json:"prev_log_index"`
	PrevLogTerm  uint64           `json:"prev_log_term"`
	Entries      []*RaftLogEntry  `json:"entries"`
	LeaderCommit uint64           `json:"leader_commit"`
}

// AppendEntriesRPCResponse is an append entries response.
type AppendEntriesRPCResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
}

// InstallSnapshotRPCRequest is an install snapshot request.
type InstallSnapshotRPCRequest struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader_id"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
}

// InstallSnapshotRPCResponse is an install snapshot response.
type InstallSnapshotRPCResponse struct {
	Term uint64 `json:"term"`
}

// ForwardProposalRequest is a forward proposal request.
type ForwardProposalRequest struct {
	Command []byte `json:"command"`
}

// Membership change types

// MembershipChangeType represents a membership change type.
type MembershipChangeType int

const (
	MembershipAddNode MembershipChangeType = iota
	MembershipRemoveNode
)

// MembershipChange represents a cluster membership change.
type MembershipChange struct {
	Type MembershipChangeType `json:"type"`
	Node RaftPeer             `json:"node"`
}

// AddNode adds a node to the cluster.
func (rn *RaftNode) AddNode(ctx context.Context, peer RaftPeer) error {
	if !rn.IsLeader() {
		return errors.New("not the leader")
	}

	change := MembershipChange{
		Type: MembershipAddNode,
		Node: peer,
	}

	data, err := json.Marshal(change)
	if err != nil {
		return err
	}

	// Create configuration change entry
	entry := &RaftLogEntry{
		Index: rn.log.LastIndex() + 1,
		Term:  rn.currentTerm,
		Type:  RaftLogConfiguration,
		Data:  data,
	}

	rn.stateMu.Lock()
	if err := rn.log.Append(entry); err != nil {
		rn.stateMu.Unlock()
		return err
	}
	rn.stateMu.Unlock()

	// Add to peers immediately (joint consensus)
	rn.peersMu.Lock()
	rn.peers[peer.ID] = &RaftPeerState{
		ID:        peer.ID,
		Addr:      peer.Addr,
		NextIndex: entry.Index,
		Healthy:   true,
	}
	rn.peersMu.Unlock()

	// Wait for commit
	// In a full implementation, we'd wait for the config entry to be committed
	return nil
}

// RemoveNode removes a node from the cluster.
func (rn *RaftNode) RemoveNode(ctx context.Context, nodeID string) error {
	if !rn.IsLeader() {
		return errors.New("not the leader")
	}

	rn.peersMu.RLock()
	peer, ok := rn.peers[nodeID]
	if !ok {
		rn.peersMu.RUnlock()
		return errors.New("node not found")
	}
	rn.peersMu.RUnlock()

	change := MembershipChange{
		Type: MembershipRemoveNode,
		Node: RaftPeer{ID: peer.ID, Addr: peer.Addr},
	}

	data, err := json.Marshal(change)
	if err != nil {
		return err
	}

	entry := &RaftLogEntry{
		Index: rn.log.LastIndex() + 1,
		Term:  rn.currentTerm,
		Type:  RaftLogConfiguration,
		Data:  data,
	}

	rn.stateMu.Lock()
	if err := rn.log.Append(entry); err != nil {
		rn.stateMu.Unlock()
		return err
	}
	rn.stateMu.Unlock()

	// Remove from peers after commit
	// For simplicity, remove immediately
	rn.peersMu.Lock()
	delete(rn.peers, nodeID)
	rn.peersMu.Unlock()

	return nil
}

// RaftCluster wraps RaftNode for easier cluster management.
type RaftCluster struct {
	node *RaftNode
	db   *DB
}

// NewRaftCluster creates a new Raft cluster.
func NewRaftCluster(db *DB, config RaftConfig) (*RaftCluster, error) {
	node, err := NewRaftNode(db, config)
	if err != nil {
		return nil, err
	}

	return &RaftCluster{
		node: node,
		db:   db,
	}, nil
}

// Start starts the cluster.
func (rc *RaftCluster) Start() error {
	return rc.node.Start()
}

// Stop stops the cluster.
func (rc *RaftCluster) Stop() error {
	return rc.node.Stop()
}

// Write performs a linearizable write.
func (rc *RaftCluster) Write(ctx context.Context, p Point) error {
	cmd := RaftCommand{
		Op:  "write",
		Key: p.Metric,
	}
	cmd.Value, _ = json.Marshal(p)

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	return rc.node.Propose(ctx, data)
}

// Read performs a linearizable read.
func (rc *RaftCluster) Read(ctx context.Context, metric string, start, end time.Time) ([]Point, error) {
	if err := rc.node.LinearizableRead(ctx); err != nil {
		return nil, err
	}
	// Query the database
	result, err := rc.db.ExecuteContext(ctx, &Query{
		Metric: metric,
		Start:  start.UnixNano(),
		End:    end.UnixNano(),
	})
	if err != nil {
		return nil, err
	}
	return result.Points, nil
}

// Node returns the underlying Raft node.
func (rc *RaftCluster) Node() *RaftNode {
	return rc.node
}

// IsLeader returns true if this node is the leader.
func (rc *RaftCluster) IsLeader() bool {
	return rc.node.IsLeader()
}

// Leader returns the current leader ID.
func (rc *RaftCluster) Leader() string {
	return rc.node.Leader()
}

// Stats returns cluster statistics.
func (rc *RaftCluster) Stats() RaftStats {
	return rc.node.Stats()
}
