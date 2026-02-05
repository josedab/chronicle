package chronicle

import (
	"testing"
	"time"
)

func TestFleetManagerRegisterAgent(t *testing.T) {
	fm := NewSaaSFleetManager(DefaultSaaSFleetConfig())

	err := fm.RegisterAgent(FleetAgent{
		ID:      "agent-1",
		Name:    "prod-us-east-1",
		Version: "0.5.0",
		Addr:    "10.0.1.1:8086",
		Region:  "us-east-1",
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	agent, ok := fm.GetAgent("agent-1")
	if !ok {
		t.Fatal("expected agent to exist")
	}
	if agent.State != AgentOnline {
		t.Errorf("expected online, got %s", agent.State)
	}
}

func TestFleetManagerHeartbeat(t *testing.T) {
	fm := NewSaaSFleetManager(DefaultSaaSFleetConfig())

	fm.RegisterAgent(FleetAgent{ID: "a1", Name: "test", Version: "0.5.0"})

	err := fm.Heartbeat("a1", AgentUsage{PointsWritten: 1000, QueriesRun: 50})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	agent, _ := fm.GetAgent("a1")
	if agent.Usage.PointsWritten != 1000 {
		t.Errorf("expected 1000 points, got %d", agent.Usage.PointsWritten)
	}
}

func TestFleetManagerStats(t *testing.T) {
	fm := NewSaaSFleetManager(DefaultSaaSFleetConfig())

	fm.RegisterAgent(FleetAgent{ID: "a1", Version: "0.5.0", Region: "us-east-1"})
	fm.RegisterAgent(FleetAgent{ID: "a2", Version: "0.5.0", Region: "eu-west-1"})
	fm.RegisterAgent(FleetAgent{ID: "a3", Version: "0.4.0", Region: "us-east-1"})

	stats := fm.Stats()
	if stats.TotalAgents != 3 {
		t.Errorf("expected 3 agents, got %d", stats.TotalAgents)
	}
	if stats.OnlineAgents != 3 {
		t.Errorf("expected 3 online, got %d", stats.OnlineAgents)
	}
	if stats.VersionDistro["0.5.0"] != 2 {
		t.Errorf("expected 2 on 0.5.0, got %d", stats.VersionDistro["0.5.0"])
	}
	if stats.RegionDistro["us-east-1"] != 2 {
		t.Errorf("expected 2 in us-east-1, got %d", stats.RegionDistro["us-east-1"])
	}
}

func TestFleetManagerUpgrade(t *testing.T) {
	fm := NewSaaSFleetManager(DefaultSaaSFleetConfig())

	fm.RegisterAgent(FleetAgent{ID: "a1", Version: "0.5.0", Region: "us-east-1"})
	fm.RegisterAgent(FleetAgent{ID: "a2", Version: "0.5.0", Region: "us-east-1"})

	status, err := fm.InitiateUpgrade(FleetUpgradeRequest{
		TargetVersion: "0.6.0",
		Strategy:      "rolling",
	})
	if err != nil {
		t.Fatalf("upgrade failed: %v", err)
	}
	if status.TotalAgents != 2 {
		t.Errorf("expected 2 target agents, got %d", status.TotalAgents)
	}
	if status.State != "in_progress" {
		t.Errorf("expected in_progress, got %s", status.State)
	}

	// Complete upgrades
	fm.CompleteAgentUpgrade(status.ID, "a1", "0.6.0")
	fm.CompleteAgentUpgrade(status.ID, "a2", "0.6.0")

	s, _ := fm.GetUpgradeStatus(status.ID)
	if s.State != "completed" {
		t.Errorf("expected completed, got %s", s.State)
	}

	agent, _ := fm.GetAgent("a1")
	if agent.Version != "0.6.0" {
		t.Errorf("expected version 0.6.0, got %s", agent.Version)
	}
}

func TestFleetManagerPushConfig(t *testing.T) {
	fm := NewSaaSFleetManager(DefaultSaaSFleetConfig())

	fm.RegisterAgent(FleetAgent{ID: "a1", Version: "0.5.0"})
	fm.RegisterAgent(FleetAgent{ID: "a2", Version: "0.5.0"})

	err := fm.PushConfig(ConfigUpdate{
		Config: map[string]string{
			"retention_days": "30",
			"max_memory":     "128MB",
		},
	})
	if err != nil {
		t.Fatalf("push config failed: %v", err)
	}

	agent, _ := fm.GetAgent("a1")
	if agent.Config["retention_days"] != "30" {
		t.Errorf("expected retention_days=30, got %s", agent.Config["retention_days"])
	}
}

func TestFleetManagerDecommission(t *testing.T) {
	fm := NewSaaSFleetManager(DefaultSaaSFleetConfig())

	fm.RegisterAgent(FleetAgent{ID: "a1", Version: "0.5.0"})
	fm.DecommissionAgent("a1")

	agent, _ := fm.GetAgent("a1")
	if agent.State != AgentDecommissioned {
		t.Errorf("expected decommissioned, got %s", agent.State)
	}

	stats := fm.Stats()
	if stats.OfflineAgents != 1 {
		t.Errorf("expected 1 offline, got %d", stats.OfflineAgents)
	}
}

func TestFleetManagerListByState(t *testing.T) {
	fm := NewSaaSFleetManager(DefaultSaaSFleetConfig())

	fm.RegisterAgent(FleetAgent{ID: "a1", Version: "0.5.0"})
	fm.RegisterAgent(FleetAgent{ID: "a2", Version: "0.5.0"})
	fm.DecommissionAgent("a2")

	online := fm.ListAgents(AgentOnline)
	if len(online) != 1 {
		t.Errorf("expected 1 online, got %d", len(online))
	}

	all := fm.ListAgents("")
	if len(all) != 2 {
		t.Errorf("expected 2 total, got %d", len(all))
	}
}

func TestFleetManagerUpgradeRegionFilter(t *testing.T) {
	fm := NewSaaSFleetManager(DefaultSaaSFleetConfig())

	fm.RegisterAgent(FleetAgent{ID: "a1", Version: "0.5.0", Region: "us"})
	fm.RegisterAgent(FleetAgent{ID: "a2", Version: "0.5.0", Region: "eu"})

	status, err := fm.InitiateUpgrade(FleetUpgradeRequest{
		TargetVersion: "0.6.0",
		Region:        "us",
		Strategy:      "all-at-once",
	})
	if err != nil {
		t.Fatalf("upgrade failed: %v", err)
	}
	if status.TotalAgents != 1 {
		t.Errorf("expected 1 agent in us region, got %d", status.TotalAgents)
	}
}

func TestFleetManagerExportReport(t *testing.T) {
	fm := NewSaaSFleetManager(DefaultSaaSFleetConfig())
	fm.RegisterAgent(FleetAgent{ID: "a1", Version: "0.5.0"})

	data, err := fm.ExportFleetReport()
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	if len(data) < 10 {
		t.Fatal("expected non-trivial report")
	}
}

func TestFleetManagerMaxAgents(t *testing.T) {
	config := DefaultSaaSFleetConfig()
	config.MaxAgents = 2
	fm := NewSaaSFleetManager(config)

	fm.RegisterAgent(FleetAgent{ID: "a1"})
	fm.RegisterAgent(FleetAgent{ID: "a2"})
	err := fm.RegisterAgent(FleetAgent{ID: "a3"})
	if err == nil {
		t.Fatal("expected max agents error")
	}
}

func TestFleetManagerHealthCheck(t *testing.T) {
	config := DefaultSaaSFleetConfig()
	config.HeartbeatTimeout = 1 * time.Millisecond
	fm := NewSaaSFleetManager(config)

	fm.RegisterAgent(FleetAgent{ID: "a1", Version: "0.5.0"})

	time.Sleep(5 * time.Millisecond)
	fm.checkAgentHealth()

	agent, _ := fm.GetAgent("a1")
	if agent.State != AgentOffline {
		t.Errorf("expected offline after timeout, got %s", agent.State)
	}
}
