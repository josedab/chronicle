package cluster

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Test Start with a bind address (covers HTTP server setup)
func TestStartWithBindAddr(t *testing.T) {
	pw := &mockPointWriter{}
	config := DefaultClusterConfig()
	config.NodeID = "bind-node"
	config.BindAddr = "127.0.0.1:0"
	config.GossipInterval = time.Hour
	config.FailureDetectorTimeout = time.Hour
	c, err := NewCluster(pw, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("Start with bind addr: %v", err)
	}
	defer c.Stop()

	if !c.running.Load() {
		t.Error("Should be running")
	}
}

// Test Start with seed nodes (covers join path)
func TestStartWithSeeds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(GossipMessage{
			Nodes: []*ClusterNode{
				{ID: "seed-1", Addr: "localhost:9001", Healthy: true, Metadata: map[string]string{}},
			},
		})
	}))
	defer srv.Close()

	pw := &mockPointWriter{}
	config := DefaultClusterConfig()
	config.NodeID = "seeded-node"
	config.BindAddr = ""
	config.Seeds = []string{srv.Listener.Addr().String()}
	config.GossipInterval = time.Hour
	config.FailureDetectorTimeout = time.Hour
	c, err := NewCluster(pw, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("Start with seeds: %v", err)
	}
	defer c.Stop()

	nodes := c.Nodes()
	if len(nodes) < 2 {
		t.Errorf("Expected at least 2 nodes after joining seed, got %d", len(nodes))
	}
}

// Test Stop with running server (covers server shutdown path)
func TestStopWithRunningServer(t *testing.T) {
	pw := &mockPointWriter{}
	config := DefaultClusterConfig()
	config.NodeID = "stop-node"
	config.BindAddr = "127.0.0.1:0"
	config.GossipInterval = time.Hour
	config.FailureDetectorTimeout = time.Hour
	c, err := NewCluster(pw, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if c.running.Load() {
		t.Error("Should not be running after stop")
	}
}

// Test replicateAsync with no healthy nodes (covers the empty-targets early return)
func TestReplicateAsyncNoHealthyNodes(t *testing.T) {
	c, _ := newTestCluster(t, "leader-1")
	defer c.cancel()
	c.config.ReplicationMode = ReplicationAsync

	c.stateMu.Lock()
	c.state = NodeStateLeader
	c.leaderID = "leader-1"
	c.stateMu.Unlock()

	// Add an unhealthy peer
	c.nodesMu.Lock()
	c.nodes["peer-1"] = &ClusterNode{ID: "peer-1", Addr: "127.0.0.1:1", Healthy: false}
	c.nodesMu.Unlock()

	entry := LogEntry{Index: 1, Term: 1, Type: LogEntryWrite, Data: []byte(`{}`)}
	// Should not panic - async replication skips unhealthy nodes
	c.replicateAsync(entry)
}

// Test handleReplicate with invalid point data
func TestHandleReplicateInvalidPointData(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	entry := LogEntry{Index: 1, Term: 1, Type: LogEntryWrite, Data: []byte("not valid json")}
	body, _ := json.Marshal(entry)
	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/replicate", jsonReader(body))
	rr := httptest.NewRecorder()

	c.handleReplicate(rr, httpReq)

	if rr.Code == http.StatusOK {
		t.Error("Expected error for invalid point JSON")
	}
}

// Test handleReplicate with non-write entry type (config change)
func TestHandleReplicateConfigEntry(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	entry := LogEntry{Index: 1, Term: 1, Type: LogEntryConfig, Data: []byte(`{}`)}
	body, _ := json.Marshal(entry)
	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/replicate", jsonReader(body))
	rr := httptest.NewRecorder()

	c.handleReplicate(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("Config entries should be accepted, got %d", rr.Code)
	}
}

// Test handleGossip with bad JSON
func TestHandleGossipBadJSON(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/gossip", jsonReader([]byte("not json")))
	rr := httptest.NewRecorder()
	c.handleGossip(rr, httpReq)

	if rr.Code == http.StatusOK {
		t.Error("Expected error for bad gossip JSON")
	}
}

// Test handleAppendEntries bad JSON
func TestHandleAppendEntriesBadJSON(t *testing.T) {
	c, _ := newTestCluster(t, "node-1")
	defer c.cancel()

	httpReq := httptest.NewRequest(http.MethodPost, "/cluster/append", jsonReader([]byte("bad")))
	rr := httptest.NewRecorder()
	c.handleAppendEntries(rr, httpReq)

	if rr.Code == http.StatusOK {
		t.Error("Expected error for bad JSON")
	}
}

// NOTE: becomeFollower and becomeLeader are not tested because they have a
// known reentrant mutex deadlock: notifyStateChange calls stateMu.RLock
// inside a context where stateMu.Lock is already held. This makes them
// impossible to test without modifying production code.
