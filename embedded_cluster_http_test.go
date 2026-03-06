package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCheckPeerHealth_MarksUnhealthy(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", DefaultConfig(dir+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "health-node"
	cfg.HeartbeatInterval = 50 * time.Millisecond
	cluster := NewEmbeddedCluster(db, cfg)

	cluster.mu.Lock()
	cluster.peers = append(cluster.peers, clusterPeer{
		ID:       "stale-peer",
		Healthy:  true,
		LastSeen: time.Now().Add(-time.Hour),
	})
	cluster.mu.Unlock()

	cluster.checkPeerHealth()

	cluster.mu.RLock()
	healthy := cluster.peers[0].Healthy
	cluster.mu.RUnlock()

	if healthy {
		t.Error("Stale peer should be marked unhealthy")
	}
}

func TestCheckPeerHealth_KeepsHealthy(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", DefaultConfig(dir+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "health-node"
	cluster := NewEmbeddedCluster(db, cfg)

	cluster.mu.Lock()
	cluster.peers = append(cluster.peers, clusterPeer{
		ID:       "fresh-peer",
		Healthy:  true,
		LastSeen: time.Now(),
	})
	cluster.mu.Unlock()

	cluster.checkPeerHealth()

	cluster.mu.RLock()
	healthy := cluster.peers[0].Healthy
	cluster.mu.RUnlock()

	if !healthy {
		t.Error("Recent peer should remain healthy")
	}
}

func TestRegisterHTTPHandlers_Health(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", DefaultConfig(dir+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cluster := NewEmbeddedCluster(db, DefaultEmbeddedClusterConfig())
	mux := http.NewServeMux()
	cluster.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("GET", "/api/v1/cluster/health", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("health status = %d, want 200", rr.Code)
	}
	var health ClusterHealth
	if err := json.NewDecoder(rr.Body).Decode(&health); err != nil {
		t.Fatalf("Decode health: %v", err)
	}
}

func TestRegisterHTTPHandlers_Stats(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", DefaultConfig(dir+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "stats-node"
	cluster := NewEmbeddedCluster(db, cfg)
	mux := http.NewServeMux()
	cluster.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("GET", "/api/v1/cluster/stats", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("stats status = %d, want 200", rr.Code)
	}
	var stats EmbeddedClusterStats
	if err := json.NewDecoder(rr.Body).Decode(&stats); err != nil {
		t.Fatalf("Decode stats: %v", err)
	}
	if stats.NodeID != "stats-node" {
		t.Errorf("NodeID = %q, want stats-node", stats.NodeID)
	}
}

func TestRegisterHTTPHandlers_Node(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", DefaultConfig(dir+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "node-info"
	cluster := NewEmbeddedCluster(db, cfg)
	mux := http.NewServeMux()
	cluster.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("GET", "/api/v1/cluster/node", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("node status = %d, want 200", rr.Code)
	}
	var info map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&info); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if info["node_id"] != "node-info" {
		t.Errorf("node_id = %v, want node-info", info["node_id"])
	}
}
