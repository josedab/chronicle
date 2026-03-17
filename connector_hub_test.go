package chronicle

import (
	"os"
	"testing"
	"time"
)

func TestConnectorHubCreateAndList(t *testing.T) {
	db := setupTestDB(t)

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

	hub := NewConnectorHub(db, DefaultConnectorHubConfig())
	hub.CreateConnector(ConnectorConfig{Name: "dup", Driver: "noop"})

	err := hub.CreateConnector(ConnectorConfig{Name: "dup", Driver: "noop"})
	if err == nil {
		t.Error("expected error for duplicate connector")
	}
}

func TestConnectorHubMaxConnectors(t *testing.T) {
	db := setupTestDB(t)

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

	hub := NewConnectorHub(db, DefaultConnectorHubConfig())
	drivers := hub.ListDrivers()
	if len(drivers) < 2 {
		t.Errorf("expected at least 2 built-in drivers, got %d", len(drivers))
	}
}

func TestConnectorHubUnknownDriver(t *testing.T) {
	db := setupTestDB(t)

	hub := NewConnectorHub(db, DefaultConnectorHubConfig())
	hub.CreateConnector(ConnectorConfig{Name: "bad", Driver: "nonexistent", Type: ConnectorTypeSink})

	err := hub.StartConnector("bad")
	if err == nil {
		t.Error("expected error for unknown driver")
	}
}

func TestConnectorHubStartNonExistent(t *testing.T) {
	db := setupTestDB(t)

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

	config := DefaultConnectorHubConfig()
	config.Enabled = true
	config.HealthCheckInterval = 50 * time.Millisecond
	hub := NewConnectorHub(db, config)
	hub.Start()
	hub.Stop()
}

func TestConnectorHubFileDriver(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	hub := NewConnectorHub(db, DefaultConnectorHubConfig())

	outPath := t.TempDir() + "/export.jsonl"

	t.Run("FileDriverRegistered", func(t *testing.T) {
		drivers := hub.ListDrivers()
		found := false
		for _, d := range drivers {
			if d == "file" {
				found = true
			}
		}
		if !found {
			t.Error("expected file driver to be registered")
		}
	})

	t.Run("CreateAndStartFileConnector", func(t *testing.T) {
		err := hub.CreateConnector(ConnectorConfig{
			Name:   "file-sink",
			Type:   ConnectorTypeSink,
			Driver: "file",
			Properties: map[string]string{
				"path": outPath,
			},
		})
		if err != nil {
			t.Fatalf("CreateConnector: %v", err)
		}

		err = hub.StartConnector("file-sink")
		if err != nil {
			t.Fatalf("StartConnector: %v", err)
		}

		inst, ok := hub.GetConnector("file-sink")
		if !ok {
			t.Fatal("connector not found")
		}
		if inst.Status != ConnectorStatusRunning {
			t.Errorf("expected running, got %s", inst.Status)
		}

		hub.StopConnector("file-sink")
	})

	t.Run("FileDriverMissingPath", func(t *testing.T) {
		err := hub.CreateConnector(ConnectorConfig{
			Name:       "file-no-path",
			Type:       ConnectorTypeSink,
			Driver:     "file",
			Properties: map[string]string{},
		})
		if err != nil {
			t.Fatalf("CreateConnector: %v", err)
		}

		err = hub.StartConnector("file-no-path")
		if err == nil {
			t.Error("expected error for missing path")
		}
	})

	t.Run("FileDriverDirectWrite", func(t *testing.T) {
		driver := &fileConnectorDriver{}
		filePath := t.TempDir() + "/direct.jsonl"
		err := driver.Initialize(map[string]string{"path": filePath})
		if err != nil {
			t.Fatalf("Initialize: %v", err)
		}

		points := []Point{
			{Metric: "cpu.usage", Value: 42.5, Timestamp: time.Now().UnixNano()},
			{Metric: "mem.used", Value: 1024, Timestamp: time.Now().UnixNano()},
		}
		if err := driver.Write(points); err != nil {
			t.Fatalf("Write: %v", err)
		}
		driver.Close()

		info, err := os.Stat(filePath)
		if err != nil {
			t.Fatalf("file not created: %v", err)
		}
		if info.Size() == 0 {
			t.Error("expected non-empty file")
		}
	})
}
