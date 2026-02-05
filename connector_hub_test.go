package chronicle

import (
	"testing"
	"time"
)

func TestConnectorHubCreateAndList(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	hub := NewConnectorHub(db, DefaultConnectorHubConfig())

	err := hub.CreateConnector(ConnectorConfig{
		Name:   "test-sink",
		Type:   ConnectorTypeSink,
		Driver: "noop",
	})
	if err != nil {
		t.Fatalf("CreateConnector failed: %v", err)
	}

	connectors := hub.ListConnectors()
	if len(connectors) != 1 {
		t.Fatalf("expected 1 connector, got %d", len(connectors))
	}
	if connectors[0].Config.Name != "test-sink" {
		t.Errorf("expected name test-sink, got %s", connectors[0].Config.Name)
	}
}

func TestConnectorHubDuplicate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	hub := NewConnectorHub(db, DefaultConnectorHubConfig())
	hub.CreateConnector(ConnectorConfig{Name: "dup", Driver: "noop"})

	err := hub.CreateConnector(ConnectorConfig{Name: "dup", Driver: "noop"})
	if err == nil {
		t.Error("expected error for duplicate connector")
	}
}

func TestConnectorHubMaxConnectors(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultConnectorHubConfig()
	config.MaxConnectors = 2
	hub := NewConnectorHub(db, config)

	hub.CreateConnector(ConnectorConfig{Name: "c1", Driver: "noop"})
	hub.CreateConnector(ConnectorConfig{Name: "c2", Driver: "noop"})
	err := hub.CreateConnector(ConnectorConfig{Name: "c3", Driver: "noop"})
	if err == nil {
		t.Error("expected max connectors error")
	}
}

func TestConnectorHubStartStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	hub := NewConnectorHub(db, DefaultConnectorHubConfig())
	hub.CreateConnector(ConnectorConfig{Name: "s1", Type: ConnectorTypeSink, Driver: "noop"})

	if err := hub.StartConnector("s1"); err != nil {
		t.Fatalf("StartConnector failed: %v", err)
	}

	inst, found := hub.GetConnector("s1")
	if !found {
		t.Fatal("expected connector to exist")
	}
	if inst.Status != ConnectorStatusRunning {
		t.Errorf("expected running, got %s", inst.Status)
	}

	if err := hub.StopConnector("s1"); err != nil {
		t.Fatalf("StopConnector failed: %v", err)
	}
}

func TestConnectorHubDelete(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	hub := NewConnectorHub(db, DefaultConnectorHubConfig())
	hub.CreateConnector(ConnectorConfig{Name: "del1", Driver: "noop"})

	if err := hub.DeleteConnector("del1"); err != nil {
		t.Fatalf("DeleteConnector failed: %v", err)
	}
	_, found := hub.GetConnector("del1")
	if found {
		t.Error("expected connector to be deleted")
	}
}

func TestConnectorHubDeadLetter(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultConnectorHubConfig()
	config.DeadLetterEnabled = true
	hub := NewConnectorHub(db, config)

	dl := hub.ListDeadLetters(10)
	if len(dl) != 0 {
		t.Errorf("expected empty dead letter queue, got %d", len(dl))
	}

	drained := hub.DrainDeadLetters()
	if len(drained) != 0 {
		t.Errorf("expected empty drain, got %d", len(drained))
	}
}

func TestConnectorHubListDrivers(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	hub := NewConnectorHub(db, DefaultConnectorHubConfig())
	drivers := hub.ListDrivers()
	if len(drivers) < 2 {
		t.Errorf("expected at least 2 built-in drivers, got %d", len(drivers))
	}
}

func TestConnectorHubUnknownDriver(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	hub := NewConnectorHub(db, DefaultConnectorHubConfig())
	hub.CreateConnector(ConnectorConfig{Name: "bad", Driver: "nonexistent", Type: ConnectorTypeSink})

	err := hub.StartConnector("bad")
	if err == nil {
		t.Error("expected error for unknown driver")
	}
}

func TestConnectorHubStartNonExistent(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	hub := NewConnectorHub(db, DefaultConnectorHubConfig())
	err := hub.StartConnector("ghost")
	if err == nil {
		t.Error("expected error for non-existent connector")
	}
}

func TestConnectorHubFilterPoints(t *testing.T) {
	hub := &ConnectorHub{}

	points := []Point{
		{Metric: "cpu", Tags: map[string]string{"host": "a"}, Value: 1},
		{Metric: "mem", Tags: map[string]string{"host": "b"}, Value: 2},
		{Metric: "cpu", Tags: map[string]string{"host": "c"}, Value: 3},
	}

	// Include filter
	filtered := hub.filterPoints(points, ConnectorFilters{IncludeMetrics: []string{"cpu"}})
	if len(filtered) != 2 {
		t.Errorf("expected 2 points after include filter, got %d", len(filtered))
	}

	// Exclude filter
	filtered = hub.filterPoints(points, ConnectorFilters{ExcludeMetrics: []string{"mem"}})
	if len(filtered) != 2 {
		t.Errorf("expected 2 points after exclude filter, got %d", len(filtered))
	}

	// Tag filter
	filtered = hub.filterPoints(points, ConnectorFilters{TagFilters: map[string]string{"host": "a"}})
	if len(filtered) != 1 {
		t.Errorf("expected 1 point after tag filter, got %d", len(filtered))
	}
}

func TestConnectorHubGlobalStartStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultConnectorHubConfig()
	config.Enabled = true
	config.HealthCheckInterval = 50 * time.Millisecond
	hub := NewConnectorHub(db, config)
	hub.Start()
	hub.Stop()
}
