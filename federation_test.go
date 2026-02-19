package chronicle

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewFederation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fed := NewFederation(db, DefaultFederationConfig())
	if fed == nil {
		t.Fatal("NewFederation returned nil")
	}

	if fed.client == nil {
		t.Error("client should be initialized")
	}
	if fed.remotes == nil {
		t.Error("remotes should be initialized")
	}
}

func TestFederation_AddRemote(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fed := NewFederation(db, DefaultFederationConfig())

	err := fed.AddRemote("test", "http://localhost:8080", 1)
	if err != nil {
		t.Fatalf("AddRemote failed: %v", err)
	}

	remotes := fed.ListRemotes()
	if len(remotes) != 1 {
		t.Errorf("expected 1 remote, got %d", len(remotes))
	}
	if remotes[0].Name != "test" {
		t.Error("remote name mismatch")
	}
}

func TestFederation_AddRemote_Validation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fed := NewFederation(db, DefaultFederationConfig())

	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{"", "http://localhost", true},      // Missing name
		{"test", "", true},                  // Missing URL
		{"test", "http://localhost", false}, // Valid
	}

	for _, tt := range tests {
		err := fed.AddRemote(tt.name, tt.url, 1)
		if (err != nil) != tt.wantErr {
			t.Errorf("AddRemote(%q, %q) error = %v, wantErr %v", tt.name, tt.url, err, tt.wantErr)
		}
	}
}

func TestFederation_RemoveRemote(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fed := NewFederation(db, DefaultFederationConfig())
	fed.AddRemote("test", "http://localhost:8080", 1)

	fed.RemoveRemote("test")

	remotes := fed.ListRemotes()
	if len(remotes) != 0 {
		t.Error("remote should have been removed")
	}
}

func TestFederation_ListRemotes_Priority(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fed := NewFederation(db, DefaultFederationConfig())
	fed.AddRemote("low", "http://localhost:8081", 10)
	fed.AddRemote("high", "http://localhost:8082", 1)
	fed.AddRemote("medium", "http://localhost:8083", 5)

	remotes := fed.ListRemotes()
	if len(remotes) != 3 {
		t.Fatalf("expected 3 remotes, got %d", len(remotes))
	}

	// Should be sorted by priority
	if remotes[0].Name != "high" {
		t.Error("expected high priority first")
	}
	if remotes[1].Name != "medium" {
		t.Error("expected medium priority second")
	}
	if remotes[2].Name != "low" {
		t.Error("expected low priority last")
	}
}

func TestFederation_Query_LocalOnly(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write some data
	db.Write(Point{Metric: "cpu", Value: 1.0, Timestamp: time.Now().UnixNano()})

	fed := NewFederation(db, DefaultFederationConfig())

	ctx := context.Background()
	result, err := fed.Query(ctx, &Query{
		Metric: "cpu",
		Start:  time.Now().Add(-time.Hour).UnixNano(),
		End:    time.Now().Add(time.Hour).UnixNano(),
	})

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Sources) == 0 {
		t.Error("expected at least one source")
	}
}

func TestFederation_Query_WithRemote(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create a mock remote server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/query" {
			result := Result{
				Points: []Point{
					{Metric: "cpu", Value: 2.0, Timestamp: time.Now().UnixNano()},
				},
			}
			json.NewEncoder(w).Encode(result)
		} else if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer mockServer.Close()

	fed := NewFederation(db, DefaultFederationConfig())
	fed.AddRemote("mock", mockServer.URL, 1)

	ctx := context.Background()
	result, err := fed.Query(ctx, &Query{
		Metric: "cpu",
		Start:  time.Now().Add(-time.Hour).UnixNano(),
		End:    time.Now().Add(time.Hour).UnixNano(),
	})

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should have results from both local and remote
	if len(result.Sources) < 2 {
		t.Errorf("expected 2 sources, got %d", len(result.Sources))
	}
}

func TestFederation_CheckHealth(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create healthy mock server
	healthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer healthyServer.Close()

	// Create unhealthy mock server
	unhealthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer unhealthyServer.Close()

	fed := NewFederation(db, DefaultFederationConfig())
	fed.AddRemote("healthy", healthyServer.URL, 1)
	fed.AddRemote("unhealthy", unhealthyServer.URL, 2)

	ctx := context.Background()
	results := fed.CheckHealth(ctx)

	if !results["healthy"] {
		t.Error("healthy server should be marked healthy")
	}
	if results["unhealthy"] {
		t.Error("unhealthy server should be marked unhealthy")
	}
}

func TestFederation_Metrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write data and force flush
	db.Write(Point{Metric: "local_metric", Value: 1.0, Timestamp: time.Now().UnixNano()})

	// Verify local metric exists
	localMetrics := db.Metrics()
	if len(localMetrics) == 0 {
		t.Log("Warning: local metrics empty, may be buffered")
	}

	// Create mock server with metrics
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			json.NewEncoder(w).Encode([]string{"remote_metric"})
		} else if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer mockServer.Close()

	fed := NewFederation(db, DefaultFederationConfig())
	fed.AddRemote("mock", mockServer.URL, 1)

	ctx := context.Background()
	metrics, err := fed.Metrics(ctx)

	if err != nil {
		t.Fatalf("Metrics failed: %v", err)
	}

	// Should include remote metrics at minimum
	hasRemote := false
	for _, m := range metrics {
		if m == "remote_metric" {
			hasRemote = true
		}
	}

	if !hasRemote {
		t.Error("expected remote_metric")
	}
}

func TestDefaultFederationConfig(t *testing.T) {
	config := DefaultFederationConfig()

	if config.Timeout != 30*time.Second {
		t.Error("default timeout should be 30s")
	}
	if config.RetryCount != 3 {
		t.Error("default retry count should be 3")
	}
	if config.HealthCheckInterval != time.Minute {
		t.Error("default health check interval should be 1m")
	}
	if config.MaxConcurrentQueries != 10 {
		t.Error("default max concurrent queries should be 10")
	}
	if config.MergeStrategy != MergeStrategyUnion {
		t.Error("default merge strategy should be union")
	}
}

func TestFederation_DeduplicatePoints(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fed := NewFederation(db, DefaultFederationConfig())

	ts := time.Now().UnixNano()
	points := []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: ts},
		{Metric: "cpu", Value: 1.0, Timestamp: ts}, // Duplicate
		{Metric: "cpu", Value: 2.0, Timestamp: ts + 1000},
	}

	result := fed.deduplicatePoints(points)

	if len(result) != 2 {
		t.Errorf("expected 2 points after deduplication, got %d", len(result))
	}
}

func TestFederation_DeduplicatePoints_Empty(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fed := NewFederation(db, DefaultFederationConfig())

	result := fed.deduplicatePoints(nil)
	if result != nil {
		t.Error("nil input should return nil")
	}

	result = fed.deduplicatePoints([]Point{})
	if len(result) != 0 {
		t.Error("empty input should return empty")
	}
}

func TestFederation_MarkHealthy(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fed := NewFederation(db, DefaultFederationConfig())
	fed.AddRemote("test", "http://localhost:8080", 1)

	// Mark unhealthy first
	fed.markUnhealthy("test")
	remotes := fed.ListRemotes()
	if remotes[0].Healthy {
		t.Error("should be unhealthy")
	}

	// Mark healthy
	fed.markHealthy("test")
	remotes = fed.ListRemotes()
	if !remotes[0].Healthy {
		t.Error("should be healthy")
	}
}

func TestFederation_MergeStrategy(t *testing.T) {
	tests := []struct {
		strategy MergeStrategy
		name     string
	}{
		{MergeStrategyUnion, "union"},
		{MergeStrategyFirst, "first"},
		{MergeStrategyPriority, "priority"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := setupTestDB(t)
			defer db.Close()

			config := DefaultFederationConfig()
			config.MergeStrategy = tt.strategy

			fed := NewFederation(db, config)

			if fed.config.MergeStrategy != tt.strategy {
				t.Errorf("expected strategy %v, got %v", tt.strategy, fed.config.MergeStrategy)
			}
		})
	}
}

func TestFederatedResult_Fields(t *testing.T) {
	result := FederatedResult{
		Points:     []Point{{Metric: "test", Value: 1.0}},
		Sources:    []string{"local", "remote"},
		Errors:     map[string]error{"failed": nil},
		TotalCount: 2,
	}

	if len(result.Points) != 1 {
		t.Error("points mismatch")
	}
	if len(result.Sources) != 2 {
		t.Error("sources mismatch")
	}
	if result.TotalCount != 2 {
		t.Error("total count mismatch")
	}
}
