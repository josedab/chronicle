package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestMultiRegionEngine(t *testing.T) *MultiRegionReplicationEngine {
	t.Helper()
	cfg := DefaultMultiRegionReplicationConfig()
	return NewMultiRegionReplicationEngine(nil, cfg)
}

func TestNewMultiRegionReplicationEngine(t *testing.T) {
	cfg := DefaultMultiRegionReplicationConfig()
	if cfg.RegionName != "us-east-1" {
		t.Fatalf("expected region us-east-1, got %s", cfg.RegionName)
	}
	if cfg.ReplicationFactor != 3 {
		t.Fatalf("expected replication factor 3, got %d", cfg.ReplicationFactor)
	}
	if cfg.HeartbeatInterval != 5*time.Second {
		t.Fatalf("expected 5s heartbeat, got %v", cfg.HeartbeatInterval)
	}
	if cfg.ConsistencyLevel != EventualConsistency {
		t.Fatalf("expected eventual consistency, got %v", cfg.ConsistencyLevel)
	}
	if cfg.ConflictResolution != MRLastWriterWins {
		t.Fatalf("expected last_writer_wins, got %v", cfg.ConflictResolution)
	}
	if cfg.MaxReplicationLag != 30*time.Second {
		t.Fatalf("expected 30s max lag, got %v", cfg.MaxReplicationLag)
	}
	if cfg.SyncBatchSize != 1000 {
		t.Fatalf("expected batch size 1000, got %d", cfg.SyncBatchSize)
	}

	engine := NewMultiRegionReplicationEngine(nil, cfg)
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.GetState() != MRInitializing {
		t.Fatalf("expected initializing state, got %s", engine.GetState())
	}
}

func TestMultiRegionReplicationAddRemovePeer(t *testing.T) {
	engine := newTestMultiRegionEngine(t)

	// Add peer
	if err := engine.AddPeer("peer-1", "10.0.0.1:9090", "us-west-2"); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	// Duplicate should fail
	if err := engine.AddPeer("peer-1", "10.0.0.1:9090", "us-west-2"); err == nil {
		t.Fatal("expected error on duplicate peer")
	}

	// Empty ID should fail
	if err := engine.AddPeer("", "10.0.0.1:9090", "us-west-2"); err == nil {
		t.Fatal("expected error on empty ID")
	}

	// Empty address should fail
	if err := engine.AddPeer("peer-2", "", "us-west-2"); err == nil {
		t.Fatal("expected error on empty address")
	}

	// Add second peer
	if err := engine.AddPeer("peer-2", "10.0.0.2:9090", "eu-west-1"); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	peers := engine.ListPeers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(peers))
	}

	// Get specific peer
	p, err := engine.GetPeer("peer-1")
	if err != nil {
		t.Fatalf("GetPeer: %v", err)
	}
	if p.Address != "10.0.0.1:9090" {
		t.Fatalf("expected address 10.0.0.1:9090, got %s", p.Address)
	}
	if p.Region != "us-west-2" {
		t.Fatalf("expected region us-west-2, got %s", p.Region)
	}

	// Get non-existent peer
	if _, err := engine.GetPeer("nonexistent"); err == nil {
		t.Fatal("expected error for non-existent peer")
	}

	// State should be active after adding peers
	if engine.GetState() != MRActive {
		t.Fatalf("expected active state, got %s", engine.GetState())
	}

	// Remove peer
	if err := engine.RemovePeer("peer-1"); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}

	peers = engine.ListPeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}

	// Remove non-existent
	if err := engine.RemovePeer("peer-1"); err == nil {
		t.Fatal("expected error removing non-existent peer")
	}

	// Remove last peer should transition to degraded
	if err := engine.RemovePeer("peer-2"); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}
	if engine.GetState() != MRDegraded {
		t.Fatalf("expected degraded state, got %s", engine.GetState())
	}
}

func TestMultiRegionReplicationReplicateWrite(t *testing.T) {
	engine := newTestMultiRegionEngine(t)
	engine.AddPeer("peer-1", "10.0.0.1:9090", "us-west-2")

	p := Point{
		Metric:    "cpu.usage",
		Value:     75.5,
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "web-1"},
	}

	if err := engine.ReplicateWrite(p); err != nil {
		t.Fatalf("ReplicateWrite: %v", err)
	}

	stats := engine.Stats()
	if stats.TotalEventsReplicated != 1 {
		t.Fatalf("expected 1 event replicated, got %d", stats.TotalEventsReplicated)
	}
	if stats.PeerCount != 1 {
		t.Fatalf("expected 1 peer, got %d", stats.PeerCount)
	}

	// Replicate another write
	if err := engine.ReplicateWrite(Point{Metric: "mem.usage", Value: 60.0, Timestamp: time.Now().UnixNano()}); err != nil {
		t.Fatalf("ReplicateWrite: %v", err)
	}

	stats = engine.Stats()
	if stats.TotalEventsReplicated != 2 {
		t.Fatalf("expected 2 events replicated, got %d", stats.TotalEventsReplicated)
	}
}

func TestMultiRegionReplicationBatchReplicate(t *testing.T) {
	engine := newTestMultiRegionEngine(t)
	engine.AddPeer("peer-1", "10.0.0.1:9090", "us-west-2")

	points := []Point{
		{Metric: "cpu.usage", Value: 70.0, Timestamp: time.Now().UnixNano()},
		{Metric: "cpu.usage", Value: 72.0, Timestamp: time.Now().UnixNano()},
		{Metric: "cpu.usage", Value: 74.0, Timestamp: time.Now().UnixNano()},
	}

	if err := engine.ReplicateBatch(points); err != nil {
		t.Fatalf("ReplicateBatch: %v", err)
	}

	stats := engine.Stats()
	if stats.TotalEventsReplicated != 3 {
		t.Fatalf("expected 3 events replicated, got %d", stats.TotalEventsReplicated)
	}

	// Empty batch should succeed
	if err := engine.ReplicateBatch(nil); err != nil {
		t.Fatalf("ReplicateBatch nil: %v", err)
	}
}

func TestMultiRegionReplicationConflictResolution(t *testing.T) {
	engine := newTestMultiRegionEngine(t)
	engine.AddPeer("peer-1", "10.0.0.1:9090", "us-west-2")

	// Write a local event first
	engine.ReplicateWrite(Point{Metric: "cpu.usage", Value: 50.0, Timestamp: time.Now().UnixNano()})

	// Simulate an incoming concurrent event from a different region
	remoteVC := make(MRVectorClock)
	remoteVC["us-west-2"] = 1

	event := MRReplicationEvent{
		ID:           "remote-event-1",
		Type:         MRReplicationWrite,
		SourceRegion: "us-west-2",
		Timestamp:    time.Now(),
		VectorClock:  remoteVC,
		Data:         []byte(`{"metric":"cpu.usage","value":55.0}`),
	}

	if err := engine.HandleIncomingReplication(event); err != nil {
		t.Fatalf("HandleIncomingReplication: %v", err)
	}

	stats := engine.Stats()
	if stats.ConflictsDetected != 1 {
		t.Fatalf("expected 1 conflict detected, got %d", stats.ConflictsDetected)
	}
	if stats.ConflictsResolved != 1 {
		t.Fatalf("expected 1 conflict resolved, got %d", stats.ConflictsResolved)
	}

	// Event with empty ID should fail
	if err := engine.HandleIncomingReplication(MRReplicationEvent{}); err == nil {
		t.Fatal("expected error on empty event ID")
	}
}

func TestVectorClockMerge(t *testing.T) {
	vc1 := MRVectorClock{"us-east-1": 3, "us-west-2": 1}
	vc2 := MRVectorClock{"us-east-1": 2, "us-west-2": 4, "eu-west-1": 1}

	vc1.Merge(vc2)

	if vc1["us-east-1"] != 3 {
		t.Fatalf("expected us-east-1=3, got %d", vc1["us-east-1"])
	}
	if vc1["us-west-2"] != 4 {
		t.Fatalf("expected us-west-2=4, got %d", vc1["us-west-2"])
	}
	if vc1["eu-west-1"] != 1 {
		t.Fatalf("expected eu-west-1=1, got %d", vc1["eu-west-1"])
	}

	// Copy should be independent
	vc3 := vc1.Copy()
	vc3.Increment("us-east-1")
	if vc1["us-east-1"] != 3 {
		t.Fatal("copy should not affect original")
	}
}

func TestVectorClockHappensBefore(t *testing.T) {
	vc1 := MRVectorClock{"a": 1, "b": 2}
	vc2 := MRVectorClock{"a": 2, "b": 3}

	if !vc1.HappensBefore(vc2) {
		t.Fatal("vc1 should happen before vc2")
	}
	if vc2.HappensBefore(vc1) {
		t.Fatal("vc2 should not happen before vc1")
	}

	// Concurrent clocks (neither happens before the other)
	vc3 := MRVectorClock{"a": 2, "b": 1}
	vc4 := MRVectorClock{"a": 1, "b": 2}
	if vc3.HappensBefore(vc4) {
		t.Fatal("vc3 should not happen before vc4 (concurrent)")
	}
	if vc4.HappensBefore(vc3) {
		t.Fatal("vc4 should not happen before vc3 (concurrent)")
	}

	// Equal clocks
	vc5 := MRVectorClock{"a": 1}
	vc6 := MRVectorClock{"a": 1}
	if vc5.HappensBefore(vc6) {
		t.Fatal("equal clocks should not happen before each other")
	}

	// Empty clock happens before non-empty
	vc7 := MRVectorClock{}
	vc8 := MRVectorClock{"a": 1}
	if !vc7.HappensBefore(vc8) {
		t.Fatal("empty clock should happen before non-empty")
	}
}

func TestMultiRegionReplicationStats(t *testing.T) {
	engine := newTestMultiRegionEngine(t)

	stats := engine.Stats()
	if stats.PeerCount != 0 {
		t.Fatalf("expected 0 peers, got %d", stats.PeerCount)
	}
	if stats.State != MRInitializing {
		t.Fatalf("expected initializing state, got %s", stats.State)
	}
	if stats.Uptime <= 0 {
		t.Fatal("expected positive uptime")
	}

	engine.AddPeer("peer-1", "10.0.0.1:9090", "us-west-2")
	engine.ReplicateWrite(Point{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano()})

	stats = engine.Stats()
	if stats.PeerCount != 1 {
		t.Fatalf("expected 1 peer, got %d", stats.PeerCount)
	}
	if stats.TotalEventsReplicated != 1 {
		t.Fatalf("expected 1 event, got %d", stats.TotalEventsReplicated)
	}
	if stats.State != MRActive {
		t.Fatalf("expected active state, got %s", stats.State)
	}
}

func TestMultiRegionReplicationHTTPHandlers(t *testing.T) {
	engine := newTestMultiRegionEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// POST: add peer
	body := `{"id":"peer-1","address":"10.0.0.1:9090","region":"us-west-2"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/peers", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// GET: list peers
	req = httptest.NewRequest(http.MethodGet, "/api/v1/replication/peers", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var peers []MRPeerNode
	json.NewDecoder(w.Body).Decode(&peers)
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}

	// GET: stats
	req = httptest.NewRequest(http.MethodGet, "/api/v1/replication/stats", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// GET: state
	req = httptest.NewRequest(http.MethodGet, "/api/v1/replication/state", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var stateResp map[string]string
	json.NewDecoder(w.Body).Decode(&stateResp)
	if stateResp["state"] != "active" {
		t.Fatalf("expected active state, got %s", stateResp["state"])
	}

	// GET: lag
	req = httptest.NewRequest(http.MethodGet, "/api/v1/replication/lag?peer_id=peer-1", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// GET: lag without peer_id
	req = httptest.NewRequest(http.MethodGet, "/api/v1/replication/lag", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	// GET: lag with unknown peer
	req = httptest.NewRequest(http.MethodGet, "/api/v1/replication/lag?peer_id=unknown", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}

	// DELETE: remove peer
	body = `{"id":"peer-1"}`
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/replication/peers", strings.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify removal
	peers = engine.ListPeers()
	if len(peers) != 0 {
		t.Fatalf("expected 0 peers after removal, got %d", len(peers))
	}
}
