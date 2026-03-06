package chronicle

import (
	"net/http"
	"testing"
	"time"
)

func TestTCPTracker(t *testing.T) {
	tracker := NewTCPTracker()

	tracker.RecordConnect(TCPConnectionEvent{
		SrcAddr: "10.0.0.1", SrcPort: 54321,
		DstAddr: "10.0.0.2", DstPort: 80,
		PID: 1234, Comm: "curl",
	})

	stats := tracker.Stats()
	if stats.TotalConnects != 1 {
		t.Fatalf("expected 1 connect, got %d", stats.TotalConnects)
	}
	if stats.ActiveConnections != 1 {
		t.Fatalf("expected 1 active connection, got %d", stats.ActiveConnections)
	}

	active := tracker.ActiveConnections()
	if len(active) != 1 {
		t.Fatalf("expected 1 active connection, got %d", len(active))
	}

	tracker.RecordClose("10.0.0.1", 54321, "10.0.0.2", 80, 1234)
	stats = tracker.Stats()
	if stats.ActiveConnections != 0 {
		t.Fatalf("expected 0 active connections after close, got %d", stats.ActiveConnections)
	}
}

func TestHTTPTracer(t *testing.T) {
	tracer := NewHTTPTracer()

	tracer.RecordTrace(HTTPRequestTrace{
		Method: "GET", URL: "/api/v1/users",
		StatusCode: 200, Duration: 50 * time.Millisecond,
		PID: 1234, Comm: "app",
	})
	tracer.RecordTrace(HTTPRequestTrace{
		Method: "POST", URL: "/api/v1/users",
		StatusCode: 500, Duration: 100 * time.Millisecond,
		PID: 1234, Comm: "app",
	})

	stats := tracer.Stats()
	if stats.TotalRequests != 2 {
		t.Fatalf("expected 2 requests, got %d", stats.TotalRequests)
	}
	if stats.TotalErrors != 1 {
		t.Fatalf("expected 1 error, got %d", stats.TotalErrors)
	}
	if stats.AvgDurationMs <= 0 {
		t.Fatal("expected positive avg duration")
	}
}

func TestDNSTracker(t *testing.T) {
	tracker := NewDNSTracker()

	tracker.RecordQuery(DNSQueryEvent{
		Domain: "example.com", Type: "A",
		Result: []string{"93.184.216.34"},
		Duration: 5 * time.Millisecond,
		PID: 1234, Comm: "curl",
	})

	stats := tracker.Stats()
	if stats.TotalQueries != 1 {
		t.Fatalf("expected 1 query, got %d", stats.TotalQueries)
	}
	if stats.UniqueDomains != 1 {
		t.Fatalf("expected 1 unique domain, got %d", stats.UniqueDomains)
	}
}

func TestProcessDiscovery(t *testing.T) {
	pd := NewProcessDiscovery()

	pd.RegisterProcess(ProcessInfo{
		PID: 1234, Comm: "app", ServiceName: "api-server",
		Container: &EBPFContainerInfo{
			ContainerID: "abc123",
			Runtime:     "docker",
			PodName:     "api-pod-1",
			Namespace:   "default",
		},
	})

	p, ok := pd.GetProcess(1234)
	if !ok {
		t.Fatal("process not found")
	}
	if p.ServiceName != "api-server" {
		t.Fatalf("expected 'api-server', got %s", p.ServiceName)
	}

	containers := pd.ListContainers()
	if len(containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(containers))
	}
}

func TestServiceDependencyGraph(t *testing.T) {
	graph := NewServiceDependencyGraph()

	graph.RecordConnection("api-server", "database", "tcp", 5*time.Millisecond, false)
	graph.RecordConnection("api-server", "cache", "tcp", 1*time.Millisecond, false)
	graph.RecordConnection("web-app", "api-server", "http", 50*time.Millisecond, false)
	graph.RecordConnection("api-server", "database", "tcp", 10*time.Millisecond, true)

	edges := graph.GetEdges()
	if len(edges) != 3 {
		t.Fatalf("expected 3 edges, got %d", len(edges))
	}

	serviceMap := graph.GetServiceMap()
	nodes, ok := serviceMap["nodes"].([]string)
	if !ok || len(nodes) < 3 {
		t.Fatal("expected at least 3 service nodes")
	}

	// Check api-server -> database has 2 requests, 1 error
	for _, e := range edges {
		if e.Source == "api-server" && e.Target == "database" {
			if e.RequestCount != 2 {
				t.Fatalf("expected 2 requests, got %d", e.RequestCount)
			}
			if e.ErrorCount != 1 {
				t.Fatalf("expected 1 error, got %d", e.ErrorCount)
			}
		}
	}
}

func TestAutoInstrumentationAgent(t *testing.T) {
	agent := NewAutoInstrumentationAgent(nil, DefaultEBPFConfig())
	if agent.TCPTracker() == nil {
		t.Fatal("expected non-nil TCP tracker")
	}
	if agent.HTTPTracer() == nil {
		t.Fatal("expected non-nil HTTP tracer")
	}
	if agent.DNSTracker() == nil {
		t.Fatal("expected non-nil DNS tracker")
	}
	if agent.ProcessDiscovery() == nil {
		t.Fatal("expected non-nil process discovery")
	}
	if agent.ServiceGraph() == nil {
		t.Fatal("expected non-nil service graph")
	}

	stats := agent.Stats()
	if stats == nil {
		t.Fatal("expected non-nil stats")
	}
}

func TestAutoInstrumentationAgent_IngestTrackerData(t *testing.T) {
	db := setupTestDB(t)
	agent := NewAutoInstrumentationAgent(db, DefaultEBPFConfig())

	// Record some data
	agent.TCPTracker().RecordConnect(TCPConnectionEvent{
		SrcAddr: "10.0.0.1", SrcPort: 5000, DstAddr: "10.0.0.2", DstPort: 80, PID: 1,
	})
	agent.HTTPTracer().RecordTrace(HTTPRequestTrace{
		Method: "GET", URL: "/api", StatusCode: 200, Duration: 50 * time.Millisecond,
	})
	agent.ServiceGraph().RecordConnection("app", "db", "tcp", 5*time.Millisecond, false)

	// Ingest into Chronicle
	err := agent.IngestTrackerData(db)
	if err != nil {
		t.Fatalf("ingest tracker data: %v", err)
	}

	// Verify data was written
	db.Flush()
	result, err := db.Execute(&Query{Metric: "ebpf_tcp_active_connections"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Points) == 0 {
		t.Error("expected ebpf_tcp_active_connections data")
	}
}

func TestAutoInstrumentationAgent_IngestNilDB(t *testing.T) {
	agent := NewAutoInstrumentationAgent(nil, DefaultEBPFConfig())
	err := agent.IngestTrackerData(nil)
	if err == nil {
		t.Fatal("expected error for nil DB")
	}
}

func TestAutoInstrumentationAgent_HTTPEndpoints(t *testing.T) {
	agent := NewAutoInstrumentationAgent(nil, DefaultEBPFConfig())
	mux := http.NewServeMux()
	agent.RegisterHTTPHandlers(mux)

	// Just verify registration doesn't panic
	if mux == nil {
		t.Fatal("expected non-nil mux")
	}
}
