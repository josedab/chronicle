package chronicle

import (
	"testing"
)

func TestMetricMetadataStoreEngine(t *testing.T) {
	db := setupTestDB(t)

	e := NewMetricMetadataStoreEngine(db, DefaultMetricMetadataStoreConfig())
	e.Start()
	defer e.Stop()

	t.Run("set and get", func(t *testing.T) {
		e.Set(MetricMetadataEntry{
			Metric:      "cpu_usage",
			Description: "CPU usage percentage",
			Unit:        "percent",
			Type:        "gauge",
			Labels:      map[string]string{"host": "server1"},
		})
		got := e.Get("cpu_usage")
		if got == nil {
			t.Fatal("expected entry, got nil")
		}
		if got.Metric != "cpu_usage" {
			t.Errorf("expected cpu_usage, got %s", got.Metric)
		}
		if got.Description != "CPU usage percentage" {
			t.Errorf("expected CPU usage percentage, got %s", got.Description)
		}
		if got.Type != "gauge" {
			t.Errorf("expected gauge, got %s", got.Type)
		}
	})

	t.Run("get missing returns nil", func(t *testing.T) {
		got := e.Get("nonexistent")
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("update existing", func(t *testing.T) {
		e.Set(MetricMetadataEntry{
			Metric:      "cpu_usage",
			Description: "Updated description",
			Unit:        "percent",
			Type:        "gauge",
		})
		got := e.Get("cpu_usage")
		if got == nil {
			t.Fatal("expected entry, got nil")
		}
		if got.Description != "Updated description" {
			t.Errorf("expected Updated description, got %s", got.Description)
		}
		if got.CreatedAt.IsZero() {
			t.Error("expected CreatedAt to be preserved")
		}
	})

	t.Run("delete", func(t *testing.T) {
		e.Set(MetricMetadataEntry{Metric: "to_delete", Type: "counter"})
		e.Delete("to_delete")
		if got := e.Get("to_delete"); got != nil {
			t.Errorf("expected nil after delete, got %v", got)
		}
	})

	t.Run("list", func(t *testing.T) {
		e.Set(MetricMetadataEntry{Metric: "mem_usage", Type: "gauge"})
		entries := e.List()
		if len(entries) < 2 {
			t.Errorf("expected at least 2 entries, got %d", len(entries))
		}
	})

	t.Run("search by metric name", func(t *testing.T) {
		results := e.Search("cpu")
		if len(results) == 0 {
			t.Error("expected results for 'cpu' search")
		}
	})

	t.Run("search by description", func(t *testing.T) {
		results := e.Search("Updated")
		if len(results) == 0 {
			t.Error("expected results for description search")
		}
	})

	t.Run("search case insensitive", func(t *testing.T) {
		results := e.Search("CPU")
		if len(results) == 0 {
			t.Error("expected case-insensitive search results")
		}
	})

	t.Run("stats", func(t *testing.T) {
		stats := e.Stats()
		if stats.TotalEntries < 2 {
			t.Errorf("expected at least 2 total entries, got %d", stats.TotalEntries)
		}
		if stats.Types["gauge"] < 1 {
			t.Error("expected at least 1 gauge type")
		}
	})
}
