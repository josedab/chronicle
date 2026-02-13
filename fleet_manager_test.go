package chronicle

import (
	"encoding/json"
	"testing"
	"time"
)

func TestFleetManager_RegisterNode(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	err := fm.RegisterNode(EdgeNode{
		ID:      "node-1",
		Name:    "edge-us-east-1",
		Address: "10.0.1.1:8080",
		Region:  "us-east",
	})
	if err != nil {
		t.Fatal(err)
	}
	if fm.NodeCount() != 1 {
		t.Errorf("node count = %d, want 1", fm.NodeCount())
	}
}

func TestFleetManager_RegisterValidation(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())

	err := fm.RegisterNode(EdgeNode{})
	if err == nil {
		t.Error("expected error for missing ID")
	}

	err = fm.RegisterNode(EdgeNode{ID: "x"})
	if err == nil {
		t.Error("expected error for missing address")
	}
}

func TestFleetManager_MaxNodes(t *testing.T) {
	cfg := DefaultFleetConfig()
	cfg.MaxNodes = 2
	fm := NewFleetManager(cfg)

	fm.RegisterNode(EdgeNode{ID: "a", Address: "1"})
	fm.RegisterNode(EdgeNode{ID: "b", Address: "2"})
	err := fm.RegisterNode(EdgeNode{ID: "c", Address: "3"})
	if err == nil {
		t.Error("expected error exceeding max nodes")
	}
}

func TestFleetManager_DeregisterNode(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fm.RegisterNode(EdgeNode{ID: "node-1", Address: "1"})

	err := fm.DeregisterNode("node-1")
	if err != nil {
		t.Fatal(err)
	}
	if fm.NodeCount() != 0 {
		t.Error("expected 0 nodes after deregister")
	}

	err = fm.DeregisterNode("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent node")
	}
}

func TestFleetManager_Heartbeat(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fm.RegisterNode(EdgeNode{ID: "n1", Address: "1"})

	err := fm.Heartbeat("n1", NodeResources{CPUUtilization: 0.5, MemUtilization: 0.3})
	if err != nil {
		t.Fatal(err)
	}

	node, _ := fm.GetNode("n1")
	if node.Status != NodeStatusOnline {
		t.Errorf("status = %s, want online", node.Status)
	}
}

func TestFleetManager_HeartbeatDegraded(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fm.RegisterNode(EdgeNode{ID: "n1", Address: "1"})

	var statusChanged bool
	fm.OnNodeStatusChange(func(n *EdgeNode, old, new NodeStatus) {
		statusChanged = true
	})

	fm.Heartbeat("n1", NodeResources{CPUUtilization: 0.95})
	if !statusChanged {
		t.Error("expected status change callback")
	}

	node, _ := fm.GetNode("n1")
	if node.Status != NodeStatusDegraded {
		t.Errorf("status = %s, want degraded", node.Status)
	}
}

func TestFleetManager_ListNodes(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fm.RegisterNode(EdgeNode{ID: "a", Address: "1", Region: "us-east"})
	fm.RegisterNode(EdgeNode{ID: "b", Address: "2", Region: "eu-west"})

	all := fm.ListNodes()
	if len(all) != 2 {
		t.Errorf("all = %d, want 2", len(all))
	}

	online := fm.ListNodes(NodeStatusOnline)
	if len(online) != 2 {
		t.Errorf("online = %d, want 2", len(online))
	}

	offline := fm.ListNodes(NodeStatusOffline)
	if len(offline) != 0 {
		t.Errorf("offline = %d, want 0", len(offline))
	}
}

func TestFleetManager_ListByRegion(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fm.RegisterNode(EdgeNode{ID: "a", Address: "1", Region: "us-east"})
	fm.RegisterNode(EdgeNode{ID: "b", Address: "2", Region: "eu-west"})

	us := fm.ListNodesByRegion("us-east")
	if len(us) != 1 {
		t.Errorf("us-east = %d, want 1", len(us))
	}
}

func TestFleetManager_SendCommand(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fm.RegisterNode(EdgeNode{ID: "n1", Address: "1"})

	cmd, err := fm.SendCommand(FleetCommandSync, "n1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Type != FleetCommandSync {
		t.Error("wrong command type")
	}
	if cmd.Status != CommandStatusPending {
		t.Error("expected pending status")
	}

	// Command to non-existent node
	_, err = fm.SendCommand(FleetCommandSync, "nonexistent", nil)
	if err == nil {
		t.Error("expected error for nonexistent target")
	}
}

func TestFleetManager_BroadcastCommand(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fm.RegisterNode(EdgeNode{ID: "n1", Address: "1"})
	fm.RegisterNode(EdgeNode{ID: "n2", Address: "2"})

	payload, _ := json.Marshal(map[string]string{"version": "1.0"})
	cmd, err := fm.SendCommand(FleetCommandUpgrade, "*", payload)
	if err != nil {
		t.Fatal(err)
	}

	pending1 := fm.PendingCommands("n1")
	pending2 := fm.PendingCommands("n2")
	if len(pending1) != 1 || len(pending2) != 1 {
		t.Error("broadcast command should be pending for all nodes")
	}

	err = fm.AckCommand(cmd.ID, true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFleetManager_AckCommand(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fm.RegisterNode(EdgeNode{ID: "n1", Address: "1"})

	cmd, _ := fm.SendCommand(FleetCommandRestart, "n1", nil)
	fm.AckCommand(cmd.ID, true)

	pending := fm.PendingCommands("n1")
	if len(pending) != 0 {
		t.Error("expected no pending after ack")
	}

	err := fm.AckCommand("nonexistent", true)
	if err == nil {
		t.Error("expected error for nonexistent command")
	}
}

func TestFleetManager_CheckHeartbeats(t *testing.T) {
	cfg := DefaultFleetConfig()
	cfg.HeartbeatTimeout = 1 * time.Millisecond
	fm := NewFleetManager(cfg)

	fm.RegisterNode(EdgeNode{ID: "n1", Address: "1"})
	time.Sleep(5 * time.Millisecond)

	marked := fm.CheckHeartbeats()
	if marked != 1 {
		t.Errorf("marked = %d, want 1", marked)
	}

	node, _ := fm.GetNode("n1")
	if node.Status != NodeStatusOffline {
		t.Errorf("status = %s, want offline", node.Status)
	}
}

func TestFleetManager_Stats(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fm.RegisterNode(EdgeNode{ID: "a", Address: "1", Region: "us"})
	fm.RegisterNode(EdgeNode{ID: "b", Address: "2", Region: "eu"})
	fm.SendCommand(FleetCommandSync, "*", nil)

	stats := fm.Stats()
	if stats.TotalNodes != 2 {
		t.Errorf("total = %d, want 2", stats.TotalNodes)
	}
	if stats.OnlineNodes != 2 {
		t.Errorf("online = %d, want 2", stats.OnlineNodes)
	}
	if len(stats.Regions) != 2 {
		t.Errorf("regions = %d, want 2", len(stats.Regions))
	}
	if stats.PendingCommands != 1 {
		t.Errorf("pending commands = %d, want 1", stats.PendingCommands)
	}
}

func TestNodeStatus_String(t *testing.T) {
	if NodeStatusOnline.String() != "online" {
		t.Error("unexpected string")
	}
	if NodeStatusOffline.String() != "offline" {
		t.Error("unexpected string")
	}
	if NodeStatusUnknown.String() != "unknown" {
		t.Error("unexpected string")
	}
}
