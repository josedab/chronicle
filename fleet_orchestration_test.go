package chronicle

import (
	"fmt"
	"testing"
)

func TestFleetOrchestrator_PushConfig(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fo := NewFleetOrchestrator(fm)
	defer fo.Stop()

	// Register nodes
	for i := 0; i < 5; i++ {
		fm.RegisterNode(EdgeNode{
			ID:      fmt.Sprintf("node-%d", i),
			Address: fmt.Sprintf("10.0.0.%d:8080", i),
			Region:  "us-east",
			Labels:  map[string]string{"env": "prod"},
		})
	}

	config := map[string]any{
		"retention_days": 30,
		"batch_size":     1000,
	}

	ver, err := fo.PushConfig(config, nil)
	if err != nil {
		t.Fatalf("push config: %v", err)
	}
	if ver.Version == 0 {
		t.Fatal("expected non-zero version")
	}
	if len(ver.AppliedBy) != 5 {
		t.Fatalf("expected 5 applied, got %d", len(ver.AppliedBy))
	}
}

func TestFleetOrchestrator_PushSchema(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fo := NewFleetOrchestrator(fm)
	defer fo.Stop()

	fm.RegisterNode(EdgeNode{ID: "n1", Address: "10.0.0.1:8080"})

	schema := FleetSchema{
		Name: "cpu_metrics",
		Fields: []FleetSchemaField{
			{Name: "timestamp", Type: "timestamp", Required: true},
			{Name: "value", Type: "float64", Required: true},
			{Name: "host", Type: "string", Tag: true},
		},
	}

	s, err := fo.PushSchema(schema)
	if err != nil {
		t.Fatalf("push schema: %v", err)
	}
	if s.Version != 1 {
		t.Fatalf("expected version 1, got %d", s.Version)
	}

	// Push again should increment version
	s2, err := fo.PushSchema(schema)
	if err != nil {
		t.Fatalf("push schema v2: %v", err)
	}
	if s2.Version != 2 {
		t.Fatalf("expected version 2, got %d", s2.Version)
	}
}

func TestFleetOrchestrator_ScatterGatherQuery(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fo := NewFleetOrchestrator(fm)
	defer fo.Stop()

	// Register nodes across regions
	for i := 0; i < 10; i++ {
		region := "us-east"
		if i >= 5 {
			region = "eu-west"
		}
		fm.RegisterNode(EdgeNode{
			ID:      fmt.Sprintf("node-%d", i),
			Address: fmt.Sprintf("10.0.%d.1:8080", i),
			Region:  region,
		})
	}

	result, err := fo.ScatterGatherQuery(FleetQueryRequest{
		Metric:  "cpu_usage",
		Regions: []string{"us-east"},
	})
	if err != nil {
		t.Fatalf("scatter-gather: %v", err)
	}
	if len(result.NodeResults) != 5 {
		t.Fatalf("expected 5 node results, got %d", len(result.NodeResults))
	}
}

func TestFleetOrchestrator_HealthReport(t *testing.T) {
	fm := NewFleetManager(DefaultFleetConfig())
	fo := NewFleetOrchestrator(fm)
	defer fo.Stop()

	fm.RegisterNode(EdgeNode{
		ID: "n1", Address: "10.0.0.1:8080", Region: "us-east", Version: "1.0",
	})
	fm.RegisterNode(EdgeNode{
		ID: "n2", Address: "10.0.0.2:8080", Region: "eu-west", Version: "1.0",
	})

	report := fo.HealthReport()
	if report.TotalNodes != 2 {
		t.Fatalf("expected 2 nodes, got %d", report.TotalNodes)
	}
	if report.HealthyNodes != 2 {
		t.Fatalf("expected 2 healthy, got %d", report.HealthyNodes)
	}
}

func TestFleetOrchestrator_LargeFleet(t *testing.T) {
	fm := NewFleetManager(FleetConfig{MaxNodes: 2000})
	fo := NewFleetOrchestrator(fm)
	defer fo.Stop()

	// Register 1000 nodes
	for i := 0; i < 1000; i++ {
		region := []string{"us-east", "us-west", "eu-west", "ap-south"}[i%4]
		fm.RegisterNode(EdgeNode{
			ID:      fmt.Sprintf("edge-%04d", i),
			Address: fmt.Sprintf("10.%d.%d.1:8080", i/256, i%256),
			Region:  region,
			Version: "2.0",
			Labels:  map[string]string{"tier": "standard"},
		})
	}

	if fm.NodeCount() != 1000 {
		t.Fatalf("expected 1000 nodes, got %d", fm.NodeCount())
	}

	report := fo.HealthReport()
	if report.TotalNodes != 1000 {
		t.Fatalf("expected 1000 total, got %d", report.TotalNodes)
	}
	if len(report.RegionHealth) != 4 {
		t.Fatalf("expected 4 regions, got %d", len(report.RegionHealth))
	}
}
