package chronicle

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestDataMeshAddPeer(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())

	meta := DataMeshPeerMetadata{
		Metrics: []string{"cpu", "mem"},
		Region:  "us-east-1",
		Version: "0.4.0",
	}

	if err := dm.AddPeer("peer-1", "localhost:9095", meta); err != nil {
		t.Fatalf("AddPeer failed: %v", err)
	}

	peers := dm.ListPeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if peers[0].ID != "peer-1" {
		t.Errorf("expected peer-1, got %s", peers[0].ID)
	}
	if peers[0].State != DataMeshPeerHealthy {
		t.Errorf("expected healthy state, got %s", peers[0].State)
	}
}

func TestDataMeshRemovePeer(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())
	dm.AddPeer("peer-1", "localhost:9095", DataMeshPeerMetadata{})
	dm.AddPeer("peer-2", "localhost:9096", DataMeshPeerMetadata{})

	dm.RemovePeer("peer-1")

	peers := dm.ListPeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer after removal, got %d", len(peers))
	}
	if peers[0].ID != "peer-2" {
		t.Errorf("expected peer-2, got %s", peers[0].ID)
	}
}

func TestDataMeshMaxPeers(t *testing.T) {
	cfg := DefaultDataMeshConfig()
	cfg.MaxPeers = 2
	dm := NewDataMesh(nil, cfg)

	dm.AddPeer("peer-1", "localhost:9095", DataMeshPeerMetadata{})
	dm.AddPeer("peer-2", "localhost:9096", DataMeshPeerMetadata{})

	err := dm.AddPeer("peer-3", "localhost:9097", DataMeshPeerMetadata{})
	if err == nil {
		t.Fatal("expected error when exceeding max peers")
	}
}

func TestDataMeshLocalityRouting(t *testing.T) {
	cfg := DefaultDataMeshConfig()
	cfg.EnableLocalityRouting = true
	dm := NewDataMesh(nil, cfg)

	dm.AddPeer("peer-cpu", "localhost:9095", DataMeshPeerMetadata{
		Metrics: []string{"cpu", "load"},
		Region:  "us-east-1",
	})
	dm.AddPeer("peer-mem", "localhost:9096", DataMeshPeerMetadata{
		Metrics: []string{"memory", "swap"},
		Region:  "us-west-2",
	})

	req := DataMeshQueryRequest{Metric: "cpu", Start: time.Now().Add(-time.Hour), End: time.Now()}
	peers := dm.selectPeers(req)
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer for cpu metric, got %d", len(peers))
	}
	if peers[0].ID != "peer-cpu" {
		t.Errorf("expected peer-cpu, got %s", peers[0].ID)
	}
}

func TestDataMeshQuery(t *testing.T) {
	cfg := DefaultDataMeshConfig()
	cfg.NodeID = "local"
	dm := NewDataMesh(nil, cfg)

	dm.AddPeer("peer-1", "localhost:9095", DataMeshPeerMetadata{
		Metrics: []string{"cpu"},
	})

	ctx := context.Background()
	result, err := dm.Query(ctx, DataMeshQueryRequest{
		Metric: "cpu",
		Start:  time.Now().Add(-time.Hour),
		End:    time.Now(),
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.TotalPeers != 1 {
		t.Errorf("expected 1 total peer, got %d", result.TotalPeers)
	}
	if result.RespondedPeers != 1 {
		t.Errorf("expected 1 responded peer, got %d", result.RespondedPeers)
	}
}

func TestDataMeshGossip(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())
	dm.AddPeer("peer-1", "localhost:9095", DataMeshPeerMetadata{})

	dm.Gossip()

	stats := dm.Stats()
	if stats.GossipRounds != 1 {
		t.Errorf("expected 1 gossip round, got %d", stats.GossipRounds)
	}
}

func TestDataMeshTopology(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())
	dm.AddPeer("peer-1", "localhost:9095", DataMeshPeerMetadata{Region: "us-east-1"})
	dm.AddPeer("peer-2", "localhost:9096", DataMeshPeerMetadata{Region: "us-east-1"})
	dm.AddPeer("peer-3", "localhost:9097", DataMeshPeerMetadata{Region: "eu-west-1"})

	topo := dm.Topology()
	if topo.TotalPeers != 3 {
		t.Errorf("expected 3 nodes, got %d", topo.TotalPeers)
	}
	if topo.HealthyPeers != 3 {
		t.Errorf("expected 3 healthy, got %d", topo.HealthyPeers)
	}
	// peer-1 and peer-2 in same region should have an edge
	if len(topo.Edges) < 1 {
		t.Errorf("expected at least 1 edge for same-region peers, got %d", len(topo.Edges))
	}
}

func TestDataMeshStats(t *testing.T) {
	cfg := DefaultDataMeshConfig()
	cfg.NodeID = "test-node"
	dm := NewDataMesh(nil, cfg)
	dm.AddPeer("peer-1", "localhost:9095", DataMeshPeerMetadata{})

	stats := dm.Stats()
	if stats.NodeID != "test-node" {
		t.Errorf("expected node ID test-node, got %s", stats.NodeID)
	}
	if stats.TotalPeers != 1 {
		t.Errorf("expected 1 peer, got %d", stats.TotalPeers)
	}
}

func TestDataMeshUpdatePeerMetadata(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())
	dm.AddPeer("peer-1", "localhost:9095", DataMeshPeerMetadata{Metrics: []string{"cpu"}})

	newMeta := DataMeshPeerMetadata{Metrics: []string{"cpu", "memory"}}
	if err := dm.UpdatePeerMetadata("peer-1", newMeta); err != nil {
		t.Fatalf("UpdatePeerMetadata failed: %v", err)
	}

	peer := dm.GetPeer("peer-1")
	if len(peer.Metadata.Metrics) != 2 {
		t.Errorf("expected 2 metrics after update, got %d", len(peer.Metadata.Metrics))
	}
}

func TestDataMeshMergeResultsDedup(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())

	results := []DataMeshPeerResult{
		{PeerID: "a", Points: []Point{{Metric: "cpu", Timestamp: 100, Value: 1.0}}},
		{PeerID: "b", Points: []Point{{Metric: "cpu", Timestamp: 100, Value: 1.0}, {Metric: "cpu", Timestamp: 200, Value: 2.0}}},
	}

	merged := dm.mergeResults(results, DataMeshQueryRequest{})
	if len(merged.Points) != 2 {
		t.Errorf("expected 2 deduplicated points, got %d", len(merged.Points))
	}
}

func TestDataMesh_DuplicatePeerRegistration(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())
	err := dm.AddPeer("peer-1", "localhost:9095", DataMeshPeerMetadata{})
	if err != nil {
		t.Fatal(err)
	}

	// Second add with same ID - behavior depends on implementation
	err = dm.AddPeer("peer-1", "localhost:9096", DataMeshPeerMetadata{})
	// If no error, it may overwrite. Just verify peers count is still 1
	if err == nil {
		peers := dm.ListPeers()
		if len(peers) != 1 {
			t.Errorf("expected 1 peer after duplicate add, got %d", len(peers))
		}
	}
}

func TestDataMesh_EmptyPeerID(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())
	err := dm.AddPeer("", "localhost:9095", DataMeshPeerMetadata{})
	// AddPeer may or may not validate empty IDs; test it doesn't panic
	_ = err
}

func TestDataMesh_RemoveNonExistentPeer(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())
	// Should not panic
	dm.RemovePeer("nonexistent")
}

func TestDataMesh_GetNonExistentPeer(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())
	peer := dm.GetPeer("nonexistent")
	if peer != nil {
		t.Error("expected nil for nonexistent peer")
	}
}

func TestDataMesh_UpdateNonExistentPeer(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())
	err := dm.UpdatePeerMetadata("nonexistent", DataMeshPeerMetadata{})
	if err == nil {
		t.Error("expected error for nonexistent peer metadata update")
	}
}

func TestDataMesh_QueryNoMatchingPeers(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())
	dm.AddPeer("peer-1", "localhost:9095", DataMeshPeerMetadata{
		Metrics: []string{"memory"},
	})

	ctx := context.Background()
	result, err := dm.Query(ctx, DataMeshQueryRequest{
		Metric: "nonexistent_metric",
		Start:  time.Now().Add(-time.Hour),
		End:    time.Now(),
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Points) != 0 {
		t.Errorf("expected 0 points for non-matching metric, got %d", len(result.Points))
	}
}

func TestDataMesh_StartStop(t *testing.T) {
	cfg := DefaultDataMeshConfig()
	cfg.GossipInterval = 50 * time.Millisecond
	cfg.HeartbeatInterval = 50 * time.Millisecond
	dm := NewDataMesh(nil, cfg)

	dm.Start()
	time.Sleep(100 * time.Millisecond)
	dm.Stop()
}

func TestDataMesh_HealthCheckMarksPeerUnhealthy(t *testing.T) {
	cfg := DefaultDataMeshConfig()
	cfg.HeartbeatTimeout = time.Millisecond
	dm := NewDataMesh(nil, cfg)

	dm.AddPeer("peer-1", "localhost:9095", DataMeshPeerMetadata{})

	// Set LastSeen to the past
	dm.mu.Lock()
	dm.peers["peer-1"].LastSeen = time.Now().Add(-time.Hour)
	dm.mu.Unlock()

	dm.checkPeerHealth()

	// Peer may be removed or marked unhealthy depending on implementation
	peer := dm.GetPeer("peer-1")
	if peer != nil && peer.State == DataMeshPeerHealthy {
		t.Error("expected peer to be marked unhealthy or removed after timeout")
	}
}

func TestDataMesh_MultiRegionRouting(t *testing.T) {
	cfg := DefaultDataMeshConfig()
	cfg.EnableLocalityRouting = true
	dm := NewDataMesh(nil, cfg)

	dm.AddPeer("us-peer", "localhost:9095", DataMeshPeerMetadata{
		Metrics: []string{"cpu", "mem"}, Region: "us-east-1",
	})
	dm.AddPeer("eu-peer", "localhost:9096", DataMeshPeerMetadata{
		Metrics: []string{"cpu", "mem"}, Region: "eu-west-1",
	})

	// Query should route to peers that have the metric
	req := DataMeshQueryRequest{Metric: "cpu", Start: time.Now().Add(-time.Hour), End: time.Now()}
	peers := dm.selectPeers(req)
	if len(peers) != 2 {
		t.Errorf("expected 2 peers for cpu, got %d", len(peers))
	}
}

func TestDataMesh_RegisterHTTPHandlers(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())
	mux := http.NewServeMux()
	dm.RegisterHTTPHandlers(mux)
	// Should not panic
}

func TestDataMesh_PeerStateEnum(t *testing.T) {
	states := []DataMeshPeerState{
		DataMeshPeerHealthy,
		DataMeshPeerSuspect,
		DataMeshPeerUnhealthy,
		DataMeshPeerLeft,
	}
	expected := []string{"healthy", "suspect", "unhealthy", "left"}
	for i, s := range states {
		if string(s) != expected[i] {
			t.Errorf("state %d = %q, want %q", i, s, expected[i])
		}
	}
}

func TestDataMesh_MergeEmptyResults(t *testing.T) {
	dm := NewDataMesh(nil, DefaultDataMeshConfig())
	merged := dm.mergeResults(nil, DataMeshQueryRequest{})
	if len(merged.Points) != 0 {
		t.Errorf("expected 0 points, got %d", len(merged.Points))
	}
}
