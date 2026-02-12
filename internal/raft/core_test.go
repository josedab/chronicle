package raft

import (
	"context"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// mockStorageEngine implements StorageEngine for testing.
type mockStorageEngine struct {
	mu      sync.Mutex
	points  []Point
	metrics []string
}

func (m *mockStorageEngine) Write(p Point) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.points = append(m.points, p)
	return nil
}

func (m *mockStorageEngine) ExecuteQuery(_ context.Context, metric string, start, end int64) (*QueryResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var pts []Point
	for _, p := range m.points {
		if p.Metric == metric && p.Timestamp >= start && p.Timestamp <= end {
			pts = append(pts, p)
		}
	}
	return &QueryResult{Points: pts}, nil
}

func (m *mockStorageEngine) Metrics() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.metrics
}

func newTestStore() *mockStorageEngine {
	return &mockStorageEngine{}
}

func TestRaftConfig(t *testing.T) {
	config := DefaultRaftConfig()

	if config.ElectionTimeoutMin <= 0 {
		t.Error("Expected positive election timeout min")
	}
	if config.ElectionTimeoutMax <= config.ElectionTimeoutMin {
		t.Error("Expected election timeout max > min")
	}
	if config.HeartbeatInterval <= 0 {
		t.Error("Expected positive heartbeat interval")
	}
	if config.SnapshotThreshold <= 0 {
		t.Error("Expected positive snapshot threshold")
	}
	if !config.PreVoteEnabled {
		t.Error("Expected pre-vote to be enabled by default")
	}
	if !config.BatchingEnabled {
		t.Error("Expected batching to be enabled by default")
	}
}

func TestRaftLogEntry(t *testing.T) {
	entry := &RaftLogEntry{
		Index: 1,
		Term:  1,
		Type:  RaftLogCommand,
		Data:  []byte("test command"),
	}

	// Calculate CRC
	entry.CRC = 0 // Will be set by Append

	// Manually validate after setting CRC
	entry.CRC = crc32.ChecksumIEEE(entry.Data)
	if !entry.Validate() {
		t.Error("Entry validation failed")
	}

	// Corrupt data
	entry.Data = []byte("corrupted")
	if entry.Validate() {
		t.Error("Corrupted entry should fail validation")
	}
}

func TestRaftLog(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	log, err := NewRaftLog(logPath)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Test empty log
	if log.LastIndex() != 0 {
		t.Errorf("Expected last index 0, got %d", log.LastIndex())
	}
	if log.LastTerm() != 0 {
		t.Errorf("Expected last term 0, got %d", log.LastTerm())
	}

	// Append entries
	entries := []*RaftLogEntry{
		{Index: 1, Term: 1, Type: RaftLogCommand, Data: []byte("cmd1")},
		{Index: 2, Term: 1, Type: RaftLogCommand, Data: []byte("cmd2")},
		{Index: 3, Term: 2, Type: RaftLogCommand, Data: []byte("cmd3")},
	}

	if err := log.Append(entries...); err != nil {
		t.Fatalf("Failed to append entries: %v", err)
	}

	// Verify
	if log.LastIndex() != 3 {
		t.Errorf("Expected last index 3, got %d", log.LastIndex())
	}
	if log.LastTerm() != 2 {
		t.Errorf("Expected last term 2, got %d", log.LastTerm())
	}
	if log.Len() != 3 {
		t.Errorf("Expected length 3, got %d", log.Len())
	}

	// Get entry
	entry := log.Get(2)
	if entry == nil {
		t.Fatal("Entry at index 2 not found")
	}
	if string(entry.Data) != "cmd2" {
		t.Errorf("Expected cmd2, got %s", string(entry.Data))
	}

	// Get range
	rangeEntries := log.GetRange(1, 2)
	if len(rangeEntries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(rangeEntries))
	}

	// Term at index
	if log.TermAt(2) != 1 {
		t.Errorf("Expected term 1 at index 2, got %d", log.TermAt(2))
	}
	if log.TermAt(3) != 2 {
		t.Errorf("Expected term 2 at index 3, got %d", log.TermAt(3))
	}
}

func TestRaftLogTruncation(t *testing.T) {
	log, err := NewRaftLog("")
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	entries := []*RaftLogEntry{
		{Index: 1, Term: 1, Type: RaftLogCommand, Data: []byte("cmd1")},
		{Index: 2, Term: 1, Type: RaftLogCommand, Data: []byte("cmd2")},
		{Index: 3, Term: 2, Type: RaftLogCommand, Data: []byte("cmd3")},
		{Index: 4, Term: 2, Type: RaftLogCommand, Data: []byte("cmd4")},
	}
	_ = log.Append(entries...)

	// Truncate after index 2
	log.TruncateAfter(2)

	if log.LastIndex() != 2 {
		t.Errorf("Expected last index 2 after truncation, got %d", log.LastIndex())
	}
	if log.Len() != 2 {
		t.Errorf("Expected length 2 after truncation, got %d", log.Len())
	}
}

func TestRaftLogCompaction(t *testing.T) {
	log, err := NewRaftLog("")
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	entries := []*RaftLogEntry{
		{Index: 1, Term: 1, Type: RaftLogCommand, Data: []byte("cmd1")},
		{Index: 2, Term: 1, Type: RaftLogCommand, Data: []byte("cmd2")},
		{Index: 3, Term: 2, Type: RaftLogCommand, Data: []byte("cmd3")},
		{Index: 4, Term: 2, Type: RaftLogCommand, Data: []byte("cmd4")},
	}
	_ = log.Append(entries...)

	// Compact before index 3
	log.CompactBefore(3)

	if log.Len() != 2 {
		t.Errorf("Expected length 2 after compaction, got %d", log.Len())
	}

	// Old entries should be gone
	if log.Get(1) != nil {
		t.Error("Entry 1 should be compacted")
	}
	if log.Get(2) != nil {
		t.Error("Entry 2 should be compacted")
	}

	// New entries should exist
	if log.Get(3) == nil {
		t.Error("Entry 3 should exist")
	}
	if log.Get(4) == nil {
		t.Error("Entry 4 should exist")
	}
}

func TestRaftLogPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "persist.log")

	// Create and populate log
	log1, err := NewRaftLog(logPath)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	entries := []*RaftLogEntry{
		{Index: 1, Term: 1, Type: RaftLogCommand, Data: []byte("cmd1")},
		{Index: 2, Term: 2, Type: RaftLogCommand, Data: []byte("cmd2")},
	}
	_ = log1.Append(entries...)
	log1.Close()

	// Reopen and verify
	log2, err := NewRaftLog(logPath)
	if err != nil {
		t.Fatalf("Failed to reopen log: %v", err)
	}
	defer log2.Close()

	if log2.Len() != 2 {
		t.Errorf("Expected 2 entries after reload, got %d", log2.Len())
	}
	if log2.LastIndex() != 2 {
		t.Errorf("Expected last index 2, got %d", log2.LastIndex())
	}

	entry := log2.Get(1)
	if entry == nil || string(entry.Data) != "cmd1" {
		t.Error("Entry 1 not properly persisted")
	}
}

func TestRaftNodeCreation(t *testing.T) {
	store := newTestStore()

	config := DefaultRaftConfig()
	config.NodeID = "test-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	if node.config.NodeID != "test-node" {
		t.Errorf("Expected node ID test-node, got %s", node.config.NodeID)
	}
	if node.Role() != RaftRoleFollower {
		t.Errorf("Expected follower role, got %s", node.Role())
	}
	if node.Term() != 0 {
		t.Errorf("Expected term 0, got %d", node.Term())
	}
}

func TestRaftNodeStartStop(t *testing.T) {
	store := newTestStore()

	config := DefaultRaftConfig()
	config.NodeID = "test-node"
	config.BindAddr = "" // No server

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Start
	if err := node.Start(); err != nil {
		t.Fatalf("Failed to start node: %v", err)
	}

	// Double start should fail
	if err := node.Start(); err == nil {
		t.Error("Expected error on double start")
	}

	// Stop
	if err := node.Stop(); err != nil {
		t.Fatalf("Failed to stop node: %v", err)
	}
}

func TestRaftNodeStats(t *testing.T) {
	store := newTestStore()

	config := DefaultRaftConfig()
	config.NodeID = "stats-node"
	config.Peers = []RaftPeer{
		{ID: "peer1", Addr: "localhost:9001"},
		{ID: "peer2", Addr: "localhost:9002"},
	}

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	stats := node.Stats()

	if stats.NodeID != "stats-node" {
		t.Errorf("Expected node ID stats-node, got %s", stats.NodeID)
	}
	if stats.Role != "follower" {
		t.Errorf("Expected role follower, got %s", stats.Role)
	}
	if stats.PeerCount != 2 {
		t.Errorf("Expected 2 peers, got %d", stats.PeerCount)
	}
	if !stats.BatchingEnabled {
		t.Error("Expected batching to be enabled")
	}
	if !stats.PreVoteEnabled {
		t.Error("Expected pre-vote to be enabled")
	}
}

func TestRaftRole(t *testing.T) {
	tests := []struct {
		role     RaftRole
		expected string
	}{
		{RaftRoleFollower, "follower"},
		{RaftRoleCandidate, "candidate"},
		{RaftRoleLeader, "leader"},
		{RaftRolePreCandidate, "pre-candidate"},
		{RaftRole(99), "unknown"},
	}

	for _, tc := range tests {
		if tc.role.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.role.String())
		}
	}
}

func TestRaftTransport(t *testing.T) {
	transport := NewRaftTransport("localhost:9000", 100*time.Millisecond)

	if transport.localAddr != "localhost:9000" {
		t.Errorf("Expected localhost:9000, got %s", transport.localAddr)
	}
	if transport.client == nil {
		t.Error("Expected non-nil client")
	}
	if transport.timeout != 100*time.Millisecond {
		t.Errorf("Expected 100ms timeout, got %v", transport.timeout)
	}
}

func TestRaftCommand(t *testing.T) {
	point := Point{
		Metric:    "test",
		Value:     42.0,
		Timestamp: time.Now().UnixNano(),
	}

	pointData, _ := json.Marshal(point)
	cmd := RaftCommand{
		Op:    "write",
		Key:   "test",
		Value: pointData,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}

	var decoded RaftCommand
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal command: %v", err)
	}

	if decoded.Op != "write" {
		t.Errorf("Expected op write, got %s", decoded.Op)
	}
	if decoded.Key != "test" {
		t.Errorf("Expected key test, got %s", decoded.Key)
	}
}

func TestRaftPersistentState(t *testing.T) {
	tmpDir := t.TempDir()

	store := newTestStore()

	config := DefaultRaftConfig()
	config.NodeID = "persist-node"
	config.DataDir = tmpDir
	config.BindAddr = ""

	// Create node and set state
	node1, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	node1.stateMu.Lock()
	node1.currentTerm = 5
	node1.votedFor = "other-node"
	node1.stateMu.Unlock()

	if err := node1.saveState(); err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}

	// Create new node and verify state was loaded
	node2, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatalf("Failed to create second node: %v", err)
	}

	if node2.Term() != 5 {
		t.Errorf("Expected term 5, got %d", node2.Term())
	}

	node2.stateMu.RLock()
	votedFor := node2.votedFor
	node2.stateMu.RUnlock()

	if votedFor != "other-node" {
		t.Errorf("Expected votedFor other-node, got %s", votedFor)
	}
}

func TestRaftProposal(t *testing.T) {
	proposal := &RaftProposal{
		Index:    1,
		Term:     1,
		Command:  []byte("test command"),
		Response: make(chan error, 1),
		Done:     make(chan struct{}),
	}

	// Simulate response
	go func() {
		proposal.Response <- nil
	}()

	select {
	case err := <-proposal.Response:
		if err != nil {
			t.Errorf("Expected nil error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Error("Proposal response timeout")
	}
}

func TestRaftEventListener(t *testing.T) {
	listener := &testRaftEventListener{
		leaderChanges: make([]string, 0),
		roleChanges:   make([]RaftRole, 0),
		commits:       make([]uint64, 0),
	}

	store := newTestStore()

	config := DefaultRaftConfig()
	config.NodeID = "listener-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	node.AddListener(listener)

	// Trigger notifications
	node.notifyLeaderChange("leader-1", 1)
	node.notifyRoleChange(RaftRoleLeader)
	node.notifyCommit(10)

	// Give goroutines time to execute
	time.Sleep(50 * time.Millisecond)

	listener.mu.Lock()
	defer listener.mu.Unlock()

	if len(listener.leaderChanges) != 1 || listener.leaderChanges[0] != "leader-1" {
		t.Errorf("Expected leader change to leader-1, got %v", listener.leaderChanges)
	}
	if len(listener.roleChanges) != 1 || listener.roleChanges[0] != RaftRoleLeader {
		t.Errorf("Expected role change to leader, got %v", listener.roleChanges)
	}
	if len(listener.commits) != 1 || listener.commits[0] != 10 {
		t.Errorf("Expected commit at index 10, got %v", listener.commits)
	}
}

type testRaftEventListener struct {
	mu            sync.Mutex
	leaderChanges []string
	roleChanges   []RaftRole
	commits       []uint64
	snapshots     []uint64
}

func (l *testRaftEventListener) OnLeaderChange(leaderID string, term uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.leaderChanges = append(l.leaderChanges, leaderID)
}

func (l *testRaftEventListener) OnRoleChange(role RaftRole) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.roleChanges = append(l.roleChanges, role)
}

func (l *testRaftEventListener) OnCommit(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.commits = append(l.commits, index)
}

func (l *testRaftEventListener) OnSnapshot(index uint64, term uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.snapshots = append(l.snapshots, index)
}

func TestRaftSnapshot(t *testing.T) {
	tmpDir := t.TempDir()

	store := newTestStore()

	config := DefaultRaftConfig()
	config.NodeID = "snapshot-node"
	config.DataDir = tmpDir
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Create snapshot
	snapshot, err := node.createSnapshot(100)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	if snapshot.LastIndex != 100 {
		t.Errorf("Expected last index 100, got %d", snapshot.LastIndex)
	}

	// Save snapshot
	if err := node.saveSnapshot(snapshot); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// Verify file exists
	snapshotPath := filepath.Join(tmpDir, "snapshot.dat")
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Error("Snapshot file not created")
	}
}

func TestRaftMembershipChange(t *testing.T) {
	change := MembershipChange{
		Type: MembershipAddNode,
		Node: RaftPeer{ID: "new-node", Addr: "localhost:9003"},
	}

	data, err := json.Marshal(change)
	if err != nil {
		t.Fatalf("Failed to marshal membership change: %v", err)
	}

	var decoded MembershipChange
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal membership change: %v", err)
	}

	if decoded.Type != MembershipAddNode {
		t.Errorf("Expected add node type, got %d", decoded.Type)
	}
	if decoded.Node.ID != "new-node" {
		t.Errorf("Expected new-node, got %s", decoded.Node.ID)
	}
}

func TestRaftCluster(t *testing.T) {
	store := newTestStore()

	config := DefaultRaftConfig()
	config.NodeID = "cluster-node"
	config.BindAddr = ""

	cluster, err := NewRaftCluster(store, config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	if cluster.Node() == nil {
		t.Error("Expected non-nil node")
	}
	if cluster.IsLeader() {
		t.Error("Expected not to be leader initially")
	}

	stats := cluster.Stats()
	if stats.NodeID != "cluster-node" {
		t.Errorf("Expected cluster-node, got %s", stats.NodeID)
	}
}

func TestRaftRPCMessages(t *testing.T) {
	// Test PreVote request/response
	preVoteReq := PreVoteRPCRequest{
		Term:         1,
		CandidateID:  "node1",
		LastLogIndex: 10,
		LastLogTerm:  1,
	}
	data, _ := json.Marshal(preVoteReq)
	var decodedPreVote PreVoteRPCRequest
	json.Unmarshal(data, &decodedPreVote)
	if decodedPreVote.CandidateID != "node1" {
		t.Error("PreVote request serialization failed")
	}

	// Test RequestVote request/response
	voteReq := RequestVoteRPCRequest{
		Term:         2,
		CandidateID:  "node2",
		LastLogIndex: 20,
		LastLogTerm:  2,
	}
	data, _ = json.Marshal(voteReq)
	var decodedVote RequestVoteRPCRequest
	json.Unmarshal(data, &decodedVote)
	if decodedVote.Term != 2 {
		t.Error("RequestVote request serialization failed")
	}

	// Test AppendEntries request/response
	appendReq := AppendEntriesRPCRequest{
		Term:         3,
		LeaderID:     "leader",
		PrevLogIndex: 5,
		PrevLogTerm:  2,
		Entries: []*RaftLogEntry{
			{Index: 6, Term: 3, Type: RaftLogCommand, Data: []byte("test")},
		},
		LeaderCommit: 5,
	}
	data, _ = json.Marshal(appendReq)
	var decodedAppend AppendEntriesRPCRequest
	json.Unmarshal(data, &decodedAppend)
	if decodedAppend.LeaderID != "leader" {
		t.Error("AppendEntries request serialization failed")
	}
	if len(decodedAppend.Entries) != 1 {
		t.Error("AppendEntries entries not serialized correctly")
	}

	// Test InstallSnapshot request/response
	snapshotReq := InstallSnapshotRPCRequest{
		Term:              4,
		LeaderID:          "leader",
		LastIncludedIndex: 100,
		LastIncludedTerm:  3,
		Data:              []byte("snapshot data"),
	}
	data, _ = json.Marshal(snapshotReq)
	var decodedSnapshot InstallSnapshotRPCRequest
	json.Unmarshal(data, &decodedSnapshot)
	if decodedSnapshot.LastIncludedIndex != 100 {
		t.Error("InstallSnapshot request serialization failed")
	}
}

func TestRaftLogEntryTypes(t *testing.T) {
	types := []struct {
		entryType RaftLogEntryType
		expected  int
	}{
		{RaftLogCommand, 0},
		{RaftLogConfiguration, 1},
		{RaftLogNoop, 2},
		{RaftLogBarrier, 3},
	}

	for _, tc := range types {
		if int(tc.entryType) != tc.expected {
			t.Errorf("Expected %d for entry type, got %d", tc.expected, tc.entryType)
		}
	}
}

func TestRaftBecomeFollower(t *testing.T) {
	store := newTestStore()

	config := DefaultRaftConfig()
	config.NodeID = "test-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Manually set to candidate
	node.stateMu.Lock()
	node.role = RaftRoleCandidate
	node.currentTerm = 5
	node.votedFor = "test-node"
	node.stateMu.Unlock()

	// Become follower
	node.becomeFollower(10, "leader-node")

	if node.Role() != RaftRoleFollower {
		t.Errorf("Expected follower role, got %s", node.Role())
	}
	if node.Term() != 10 {
		t.Errorf("Expected term 10, got %d", node.Term())
	}
	if node.Leader() != "leader-node" {
		t.Errorf("Expected leader-node, got %s", node.Leader())
	}

	node.stateMu.RLock()
	votedFor := node.votedFor
	node.stateMu.RUnlock()

	if votedFor != "" {
		t.Errorf("Expected votedFor to be empty, got %s", votedFor)
	}
}

func TestRaftPeerState(t *testing.T) {
	peer := &RaftPeerState{
		ID:         "peer1",
		Addr:       "localhost:9001",
		NextIndex:  100,
		MatchIndex: 95,
		LastSeen:   time.Now(),
		Healthy:    true,
		Inflight:   0,
	}

	if peer.ID != "peer1" {
		t.Errorf("Expected peer1, got %s", peer.ID)
	}
	if !peer.Healthy {
		t.Error("Expected peer to be healthy")
	}
}

func TestRaftLogConcurrency(t *testing.T) {
	log, err := NewRaftLog("")
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	var wg sync.WaitGroup
	numWriters := 10
	entriesPerWriter := 100

	// Concurrent appends
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < entriesPerWriter; j++ {
				entry := &RaftLogEntry{
					Index: uint64(writerID*entriesPerWriter + j + 1),
					Term:  1,
					Type:  RaftLogCommand,
					Data:  []byte("test"),
				}
				_ = log.Append(entry)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = log.LastIndex()
				_ = log.LastTerm()
				_ = log.Len()
			}
		}()
	}

	wg.Wait()
}

func TestRaftNodeNotRunning(t *testing.T) {
	store := newTestStore()

	config := DefaultRaftConfig()
	config.NodeID = "test-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Propose without starting should fail
	ctx := context.Background()
	err = node.Propose(ctx, []byte("test"))
	if err == nil {
		t.Error("Expected error when proposing to non-running node")
	}
}

func TestRaftLinearizableReadNotLeader(t *testing.T) {
	store := newTestStore()

	config := DefaultRaftConfig()
	config.NodeID = "test-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	ctx := context.Background()
	err = node.LinearizableRead(ctx)
	if err == nil {
		t.Error("Expected error when reading from non-leader")
	}
}

func BenchmarkRaftLogAppend(b *testing.B) {
	log, _ := NewRaftLog("")

	entry := &RaftLogEntry{
		Term: 1,
		Type: RaftLogCommand,
		Data: []byte("benchmark command data"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.Index = uint64(i + 1)
		_ = log.Append(entry)
	}
}

func BenchmarkRaftLogGet(b *testing.B) {
	log, _ := NewRaftLog("")

	// Populate log
	for i := 1; i <= 10000; i++ {
		entry := &RaftLogEntry{
			Index: uint64(i),
			Term:  1,
			Type:  RaftLogCommand,
			Data:  []byte("data"),
		}
		_ = log.Append(entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = log.Get(uint64((i % 10000) + 1))
	}
}

func BenchmarkRaftCommandSerialization(b *testing.B) {
	cmd := RaftCommand{
		Op:    "write",
		Key:   "measurement",
		Value: json.RawMessage(`{"value": 42.0, "timestamp": "2024-01-01T00:00:00Z"}`),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, _ := json.Marshal(cmd)
		var decoded RaftCommand
		_ = json.Unmarshal(data, &decoded)
	}
}
