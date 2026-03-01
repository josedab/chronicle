package cluster

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// mockPointWriter implements PointWriter for testing.
type mockPointWriter struct {
	mu     sync.Mutex
	points []Point
	err    error
}

func (m *mockPointWriter) WritePoint(p Point) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.points = append(m.points, p)
	return nil
}

func (m *mockPointWriter) pointCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.points)
}

func newTestCluster(t *testing.T, nodeID string) (*Cluster, *mockPointWriter) {
	t.Helper()
	pw := &mockPointWriter{}
	config := DefaultClusterConfig()
	config.NodeID = nodeID
	config.BindAddr = ""
	config.GossipInterval = time.Hour // disable gossip in tests
	config.FailureDetectorTimeout = time.Hour
	c, err := NewCluster(pw, config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	return c, pw
}

func makeTestPoint() Point {
	return Point{
		Metric:    "cpu.usage",
		Timestamp: time.Now().UnixNano(),
		Value:     42.5,
		Tags:      map[string]string{"host": "test"},
	}
}

// --- Write Path Tests ---

func TestWriteAsLeaderWithAsyncReplication(t *testing.T) {
	c, pw := newTestCluster(t, "leader-1")
	defer c.cancel()
	c.config.ReplicationMode = ReplicationAsync

	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.leaderID = "leader-1"
	c.stateMu.Unlock()

	p := makeTestPoint()
	err := c.Write(p)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if pw.pointCount() != 1 {
		t.Errorf("Expected 1 point written, got %d", pw.pointCount())
	}
}

func TestWriteAsLeaderAppendsToLog(t *testing.T) {
	c, _ := newTestCluster(t, "leader-1")
	defer c.cancel()
	c.config.ReplicationMode = ReplicationAsync

	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.leaderID = "leader-1"
	c.stateMu.Unlock()

	_ = c.Write(makeTestPoint())

	c.logMu.RLock()
	logLen := len(c.log)
	lastIdx := c.lastLogIndex
	c.logMu.RUnlock()

	if logLen != 1 {
		t.Errorf("Expected 1 log entry, got %d", logLen)
	}
	if lastIdx != 1 {
		t.Errorf("Expected lastLogIndex=1, got %d", lastIdx)
	}
}

func TestWriteAsFollowerForwardsToLeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, _ := newTestCluster(t, "follower-1")
	defer c.cancel()
	c.config.EnableLocalWrites = false

	c.stateMu.Lock()
	c.state = NodeStateFollower
	c.leaderID = "leader-1"
	c.stateMu.Unlock()

	c.nodesMu.Lock()
	c.nodes["leader-1"] = &ClusterNode{
		ID: "leader-1", Addr: srv.Listener.Addr().String(),
		Healthy: true,
	}
	c.nodesMu.Unlock()

	err := c.Write(makeTestPoint())
	if err != nil {
		t.Fatalf("Forward to leader failed: %v", err)
	}
}

func TestWriteAsFollowerWithLocalWritesEnabled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, pw := newTestCluster(t, "follower-1")
	defer c.cancel()
	c.config.EnableLocalWrites = true

	c.stateMu.Lock()
	c.state = NodeStateFollower
	c.leaderID = "leader-1"
	c.stateMu.Unlock()

	c.nodesMu.Lock()
	c.nodes["leader-1"] = &ClusterNode{
		ID: "leader-1", Addr: srv.Listener.Addr().String(),
		Healthy: true,
	}
	c.nodesMu.Unlock()

	err := c.Write(makeTestPoint())
	if err != nil {
		t.Fatalf("Write with local writes failed: %v", err)
	}

	// With local writes enabled, point should be written locally.
	if pw.pointCount() != 1 {
		t.Errorf("Expected 1 local point written, got %d", pw.pointCount())
	}
}

func TestWriteForwardFailsWithNoLeader(t *testing.T) {
	c, _ := newTestCluster(t, "follower-1")
	defer c.cancel()
	c.config.EnableLocalWrites = false

	// No leader set.
	err := c.Write(makeTestPoint())
	if err == nil {
		t.Error("Expected error when no leader available")
	}
}

// --- Sync Replication ---

func TestLeaderWriteSyncReplication(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, pw := newTestCluster(t, "leader-1")
	defer c.cancel()
	c.config.ReplicationMode = ReplicationSync

	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.leaderID = "leader-1"
	c.stateMu.Unlock()

	c.nodesMu.Lock()
	c.nodes["peer-1"] = &ClusterNode{
		ID: "peer-1", Addr: srv.Listener.Addr().String(),
		Healthy: true,
	}
	c.nodesMu.Unlock()

	err := c.Write(makeTestPoint())
	if err != nil {
		t.Fatalf("Sync write failed: %v", err)
	}

	if pw.pointCount() != 1 {
		t.Errorf("Expected 1 point written after sync replication, got %d", pw.pointCount())
	}
}

func TestLeaderWriteSyncReplicationFailsOnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c, _ := newTestCluster(t, "leader-1")
	defer c.cancel()
	c.config.ReplicationMode = ReplicationSync

	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.leaderID = "leader-1"
	c.stateMu.Unlock()

	c.nodesMu.Lock()
	c.nodes["peer-1"] = &ClusterNode{
		ID: "peer-1", Addr: srv.Listener.Addr().String(),
		Healthy: true,
	}
	c.nodesMu.Unlock()

	err := c.Write(makeTestPoint())
	if err == nil {
		t.Error("Expected error when sync replication fails")
	}
}

// --- Quorum Replication ---

func TestLeaderWriteQuorumReplication(t *testing.T) {
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv1.Close()
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "fail", http.StatusInternalServerError)
	}))
	defer srv2.Close()

	c, pw := newTestCluster(t, "leader-1")
	defer c.cancel()
	c.config.ReplicationMode = ReplicationQuorum

	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.leaderID = "leader-1"
	c.stateMu.Unlock()

	// 3 nodes total (self + 2 peers), quorum = 2. Self counts as 1, srv1 succeeds = 2.
	c.nodesMu.Lock()
	c.nodes["peer-1"] = &ClusterNode{ID: "peer-1", Addr: srv1.Listener.Addr().String(), Healthy: true}
	c.nodes["peer-2"] = &ClusterNode{ID: "peer-2", Addr: srv2.Listener.Addr().String(), Healthy: true}
	c.nodesMu.Unlock()

	err := c.Write(makeTestPoint())
	if err != nil {
		t.Fatalf("Quorum write should succeed with majority: %v", err)
	}

	if pw.pointCount() != 1 {
		t.Errorf("Expected 1 point after quorum write, got %d", pw.pointCount())
	}
}

func TestLeaderWriteQuorumFailsWithoutMajority(t *testing.T) {
	// Both peers fail.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "fail", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c, _ := newTestCluster(t, "leader-1")
	defer c.cancel()
	c.config.ReplicationMode = ReplicationQuorum

	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.leaderID = "leader-1"
	c.stateMu.Unlock()

	// 3 nodes, quorum=2. Self=1, both peers fail, so only 1 success < quorum.
	c.nodesMu.Lock()
	c.nodes["peer-1"] = &ClusterNode{ID: "peer-1", Addr: srv.Listener.Addr().String(), Healthy: true}
	c.nodes["peer-2"] = &ClusterNode{ID: "peer-2", Addr: srv.Listener.Addr().String(), Healthy: true}
	c.nodesMu.Unlock()

	err := c.Write(makeTestPoint())
	if err == nil {
		t.Error("Expected quorum failure when peers are unreachable")
	}
}

// --- Election Tests ---

func TestStartElectionTransitionsToCandidate(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()
	c.running.Store(true)

	c.startElection()

	if c.State() != NodeStateCandidate {
		t.Errorf("Expected candidate state, got %s", c.State())
	}
}

func TestBecomeLeaderTransitionsState(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()
	c.running.Store(true)

	// Set up pre-conditions without holding lock during becomeLeader.
	c.stateMu.Lock()
	c.state = NodeStateCandidate
	c.currentTerm = 1
	c.stateMu.Unlock()

	// becomeLeader internally acquires stateMu.
	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.leaderID = c.config.NodeID
	c.heartbeatTicker = time.NewTicker(c.config.HeartbeatInterval)
	c.stateMu.Unlock()

	if !c.IsLeader() {
		t.Error("Expected node to be leader")
	}
	if c.Leader() != "node-1" {
		t.Errorf("Expected leader node-1, got %s", c.Leader())
	}
}

func TestBecomeFollowerResetsState(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()
	c.running.Store(true)

	// Set leader state directly.
	c.stateMu.Lock()
	c.state = NodeStateFollower
	c.currentTerm = 5
	c.votedFor = ""
	c.stateMu.Unlock()

	if c.State() != NodeStateFollower {
		t.Errorf("Expected follower state, got %s", c.State())
	}
	c.stateMu.RLock()
	if c.currentTerm != 5 {
		t.Errorf("Expected term 5, got %d", c.currentTerm)
	}
	if c.votedFor != "" {
		t.Errorf("Expected votedFor to be empty, got %s", c.votedFor)
	}
	c.stateMu.RUnlock()
}

// --- Heartbeat Tests ---

func TestHeartbeatLoopStopsOnCancel(t *testing.T) {
	c, _ := newTestCluster(t, "leader-1")
	c.running.Store(true)

	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.stateMu.Unlock()
	c.heartbeatTicker = time.NewTicker(10 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		c.heartbeatLoop()
		close(done)
	}()

	c.cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatLoop did not stop")
	}
}

func TestHeartbeatLoopExitsWhenNotLeader(t *testing.T) {
	c, _ := newTestCluster(t, "follower-1")
	defer c.cancel()
	c.running.Store(true)

	c.heartbeatTicker = time.NewTicker(5 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		c.heartbeatLoop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatLoop did not exit for non-leader")
	}
}

// --- RPC Handler Tests ---

func TestHandleVoteRequestGrantsVote(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	req := VoteRequest{
		Term:         1,
		CandidateID:  "candidate-1",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/vote", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	c.handleVoteRequest(rr, httpReq)

	var resp VoteResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if !resp.VoteGranted {
		t.Error("Expected vote to be granted")
	}
}

func TestHandleVoteRequestRejectsStaleTerm(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	c.stateMu.Lock()
	c.currentTerm = 5
	c.stateMu.Unlock()

	req := VoteRequest{Term: 3, CandidateID: "old"}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/vote", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	c.handleVoteRequest(rr, httpReq)

	var resp VoteResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if resp.VoteGranted {
		t.Error("Should not grant vote for stale term")
	}
}

func TestHandleAppendEntriesRejectsStaleTerm(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	c.stateMu.Lock()
	c.currentTerm = 5
	c.stateMu.Unlock()

	req := AppendEntriesRequest{Term: 3, LeaderID: "old-leader"}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/append", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	c.handleAppendEntries(rr, httpReq)

	var resp AppendEntriesResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if resp.Success {
		t.Error("Should reject stale term")
	}
}

func TestHandleAppendEntriesAcceptsValidEntries(t *testing.T) {
	c, pw := newTestCluster(t, "follower-1")
	defer c.cancel()

	c.stateMu.Lock()
	c.currentTerm = 1
	c.stateMu.Unlock()

	p := makeTestPoint()
	pdata, _ := json.Marshal(p)

	req := AppendEntriesRequest{
		Term:     1,
		LeaderID: "leader-1",
		Entries: []LogEntry{
			{Index: 1, Term: 1, Type: LogEntryWrite, Data: pdata},
		},
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/append", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	c.handleAppendEntries(rr, httpReq)

	var resp AppendEntriesResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if !resp.Success {
		t.Error("Expected success for valid entries")
	}
	if pw.pointCount() != 1 {
		t.Errorf("Expected point to be applied, got %d", pw.pointCount())
	}
	if c.Leader() != "leader-1" {
		t.Errorf("Expected leader to be set to leader-1, got %s", c.Leader())
	}
}

func TestHandleReplicateWritesPoint(t *testing.T) {
	c, pw := newTestCluster(t, "follower-1")
	defer c.cancel()

	p := makeTestPoint()
	pdata, _ := json.Marshal(p)

	entry := LogEntry{Index: 1, Term: 1, Type: LogEntryWrite, Data: pdata}
	body, _ := json.Marshal(entry)

	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/replicate", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	c.handleReplicate(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rr.Code)
	}
	if pw.pointCount() != 1 {
		t.Errorf("Expected 1 point, got %d", pw.pointCount())
	}
}

// --- Stats / Nodes ---

func TestStatsReturnsCorrectInfo(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	stats := c.Stats()
	if stats.NodeID != "node-1" {
		t.Errorf("Expected node-1, got %s", stats.NodeID)
	}
	if stats.State != "follower" {
		t.Errorf("Expected follower state, got %s", stats.State)
	}
	if stats.NodeCount != 1 {
		t.Errorf("Expected 1 node (self), got %d", stats.NodeCount)
	}
}

func TestNodesReturnsClusterMembers(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	c.nodesMu.Lock()
	c.nodes["node-2"] = &ClusterNode{ID: "node-2", Addr: "localhost:9001", Healthy: true}
	c.nodesMu.Unlock()

	nodes := c.Nodes()
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}
}

func TestClusterStartStop(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")

	if err := c.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !c.running.Load() {
		t.Error("Expected cluster to be running")
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestClusterDoubleStart(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")

	_ = c.Start()
	defer c.Stop()

	err := c.Start()
	if err == nil {
		t.Error("Expected error on double start")
	}
}
