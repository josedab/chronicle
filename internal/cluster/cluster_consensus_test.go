package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// --- Edge Cases: Leader Step-Down ---

func TestLeaderStepsDownOnHigherTermAppendEntries(t *testing.T) {
	c, _ := newTestCluster(t, "leader-1")
	defer c.cancel()
	c.running.Store(true)

	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.leaderID = "leader-1"
	c.currentTerm = 3
	c.heartbeatTicker = time.NewTicker(time.Hour)
	c.stateMu.Unlock()

	req := AppendEntriesRequest{
		Term:     5, // Higher term
		LeaderID: "new-leader",
	}
	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/append", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	c.handleAppendEntries(rr, httpReq)

	var resp AppendEntriesResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if !resp.Success {
		t.Error("Should accept entries from higher term")
	}

	if c.State() != NodeStateFollower {
		t.Errorf("Leader should step down to follower, got %s", c.State())
	}
	c.stateMu.RLock()
	if c.currentTerm != 5 {
		t.Errorf("Term should update to 5, got %d", c.currentTerm)
	}
	if c.leaderID != "new-leader" {
		t.Errorf("Leader should be updated to new-leader, got %s", c.leaderID)
	}
	c.stateMu.RUnlock()
}

// --- Edge Cases: Write During Election ---

func TestWriteDuringElectionNoLeader(t *testing.T) {
	c, _ := newTestCluster(t, "candidate-1")
	defer c.cancel()
	c.config.EnableLocalWrites = false

	c.stateMu.Lock()
	c.state = NodeStateCandidate
	c.leaderID = "" // No leader during election
	c.stateMu.Unlock()

	err := c.Write(makeTestPoint())
	if err == nil {
		t.Error("Write should fail during election with no leader")
	}
}

// --- Edge Cases: Split-Brain ---

func TestHandleVoteRequestAlreadyVoted(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	c.stateMu.Lock()
	c.currentTerm = 5
	c.votedFor = "other-candidate" // Already voted
	c.stateMu.Unlock()

	req := VoteRequest{
		Term:        5, // Same term
		CandidateID: "candidate-2",
	}
	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/vote", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	c.handleVoteRequest(rr, httpReq)

	var resp VoteResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if resp.VoteGranted {
		t.Error("Should not grant vote when already voted for another candidate in same term")
	}
}

func TestHandleVoteRequestStaleLog(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	c.stateMu.Lock()
	c.currentTerm = 1
	c.votedFor = ""
	c.stateMu.Unlock()

	c.logMu.Lock()
	c.lastLogTerm = 5
	c.lastLogIndex = 100
	c.logMu.Unlock()

	// Candidate has stale log (lower term)
	req := VoteRequest{
		Term:         2,
		CandidateID:  "candidate-1",
		LastLogTerm:  3, // Lower than our lastLogTerm
		LastLogIndex: 200,
	}
	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/vote", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	c.handleVoteRequest(rr, httpReq)

	var resp VoteResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if resp.VoteGranted {
		t.Error("Should not grant vote to candidate with stale log")
	}
}

// --- Edge Cases: Quorum Failure ---

func TestQuorumReplicationSingleNodeCluster(t *testing.T) {
	c, pw := newTestCluster(t, "solo-leader")
	defer c.cancel()
	c.config.ReplicationMode = ReplicationQuorum

	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.leaderID = "solo-leader"
	c.stateMu.Unlock()

	// Only self in cluster, quorum=1, self counts as 1
	err := c.Write(makeTestPoint())
	if err != nil {
		t.Fatalf("Single-node quorum write should succeed: %v", err)
	}
	if pw.pointCount() != 1 {
		t.Errorf("Expected 1 point, got %d", pw.pointCount())
	}
}

// --- Edge Cases: Election not started when not running ---

func TestStartElectionNotRunning(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()
	c.running.Store(false)

	initialTerm := c.Stats().Term

	c.startElection()

	// Should not advance term if not running
	if c.Stats().Term != initialTerm {
		t.Error("Election should not start when node is not running")
	}
}

// --- Edge Cases: Concurrent writes ---

func TestConcurrentWritesAsLeader(t *testing.T) {
	c, pw := newTestCluster(t, "leader-1")
	defer c.cancel()
	c.config.ReplicationMode = ReplicationAsync

	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.leaderID = "leader-1"
	c.stateMu.Unlock()

	const numWrites = 50
	var wg sync.WaitGroup
	errs := make([]error, numWrites)

	for i := 0; i < numWrites; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p := Point{
				Metric:    "cpu.usage",
				Timestamp: time.Now().UnixNano(),
				Value:     float64(idx),
				Tags:      map[string]string{"host": "test"},
			}
			errs[idx] = c.Write(p)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("Write %d failed: %v", i, err)
		}
	}

	if pw.pointCount() != numWrites {
		t.Errorf("Expected %d points, got %d", numWrites, pw.pointCount())
	}
}

// --- Edge Cases: HandleAppendEntries higher term updates state ---

func TestHandleAppendEntriesHigherTermBecomesFollower(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	c.stateMu.Lock()
	c.state = NodeStateCandidate
	c.currentTerm = 3
	c.votedFor = "node-1"
	c.stateMu.Unlock()

	req := AppendEntriesRequest{
		Term:     5,
		LeaderID: "leader-2",
	}
	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/append", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	c.handleAppendEntries(rr, httpReq)

	c.stateMu.RLock()
	state := c.state
	term := c.currentTerm
	voted := c.votedFor
	c.stateMu.RUnlock()

	if state != NodeStateFollower {
		t.Errorf("Should become follower, got %v", state)
	}
	if term != 5 {
		t.Errorf("Term should be 5, got %d", term)
	}
	if voted != "" {
		t.Errorf("votedFor should be cleared, got %s", voted)
	}
}

// --- Edge Cases: HandleGossip with new and existing nodes ---

func TestHandleGossipUpdatesExistingAndAddsNewNodes(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	gossip := GossipMessage{
		Nodes: []*ClusterNode{
			{ID: "node-1", Addr: "localhost:8080", State: NodeStateFollower, Term: 1},
			{ID: "node-2", Addr: "localhost:8081", State: NodeStateLeader, Term: 2},
		},
	}
	body, _ := json.Marshal(gossip)
	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/gossip", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	c.handleGossip(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rr.Code)
	}

	nodes := c.Nodes()
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}
}

// --- Edge Cases: PointWriter error handling ---

func TestWriteAsLeaderWithWriterError(t *testing.T) {
	pw := &mockPointWriter{err: errors.New("storage full")}
	config := DefaultClusterConfig()
	config.NodeID = "leader-1"
	config.BindAddr = ""
	config.GossipInterval = time.Hour
	config.FailureDetectorTimeout = time.Hour
	config.ReplicationMode = ReplicationAsync
	c, err := NewCluster(pw, config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.cancel()

	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.leaderID = "leader-1"
	c.stateMu.Unlock()

	err = c.Write(makeTestPoint())
	if err == nil {
		t.Error("Expected error when PointWriter fails")
	}
}

// --- Edge Cases: HandleReplicate with bad data ---

func TestHandleReplicateBadJSON(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/replicate", bytes.NewReader([]byte("not json")))
	rr := httptest.NewRecorder()

	c.handleReplicate(rr, httpReq)

	if rr.Code == http.StatusOK {
		t.Error("Expected error for bad JSON")
	}
}

func TestHandleVoteRequestBadJSON(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/vote", bytes.NewReader([]byte("not json")))
	rr := httptest.NewRecorder()

	c.handleVoteRequest(rr, httpReq)

	if rr.Code == http.StatusOK {
		t.Error("Expected error for bad JSON vote request")
	}
}

// --- Edge Cases: Listener notifications ---

type testListener struct {
	mu            sync.Mutex
	leaderChanges []string
	nodeJoins     []string
	nodeLeaves    []string
	stateChanges  []NodeState
}

func (l *testListener) OnLeaderChange(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.leaderChanges = append(l.leaderChanges, id)
}
func (l *testListener) OnNodeJoin(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.nodeJoins = append(l.nodeJoins, id)
}
func (l *testListener) OnNodeLeave(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.nodeLeaves = append(l.nodeLeaves, id)
}
func (l *testListener) OnStateChange(s NodeState) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stateChanges = append(l.stateChanges, s)
}

func TestListenerNotificationsOnElection(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()
	c.running.Store(true)

	listener := &testListener{}
	c.AddListener(listener)

	c.startElection()
	time.Sleep(50 * time.Millisecond)

	listener.mu.Lock()
	if len(listener.stateChanges) == 0 {
		t.Error("Expected state change notification on election")
	}
	listener.mu.Unlock()
}

// --- Edge Cases: Detect failures ---

func TestDetectFailuresMarksUnhealthy(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()
	c.config.FailureDetectorTimeout = 100 * time.Millisecond

	c.nodesMu.Lock()
	c.nodes["peer-1"] = &ClusterNode{
		ID:            "peer-1",
		Healthy:       true,
		LastHeartbeat: time.Now().Add(-time.Second), // Old heartbeat
	}
	c.nodesMu.Unlock()

	c.detectFailures()

	c.nodesMu.RLock()
	if c.nodes["peer-1"].Healthy {
		t.Error("Node should be marked unhealthy after timeout")
	}
	c.nodesMu.RUnlock()
}
