package chronicle

import (
	"testing"
	"time"
)

func TestEdgeCloudFabricEngine(t *testing.T) {
	db, err := Open(t.TempDir()+"/test.db", DefaultConfig(t.TempDir()+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("add and list endpoints", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())

		err := engine.AddEndpoint(FabricEndpoint{ID: "s3-prod", Type: "s3", Region: "us-east-1", Bucket: "my-data"})
		if err != nil {
			t.Fatal(err)
		}
		err = engine.AddEndpoint(FabricEndpoint{ID: "gcs-backup", Type: "gcs", Region: "us-central1", Bucket: "backup"})
		if err != nil {
			t.Fatal(err)
		}

		eps := engine.ListEndpoints()
		if len(eps) != 2 {
			t.Errorf("expected 2 endpoints, got %d", len(eps))
		}
	})

	t.Run("duplicate endpoint", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())
		engine.AddEndpoint(FabricEndpoint{ID: "dup", Type: "s3"})
		if err := engine.AddEndpoint(FabricEndpoint{ID: "dup", Type: "s3"}); err == nil {
			t.Error("expected error for duplicate")
		}
	})

	t.Run("empty endpoint id", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())
		if err := engine.AddEndpoint(FabricEndpoint{}); err == nil {
			t.Error("expected error for empty ID")
		}
	})

	t.Run("remove endpoint", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())
		engine.AddEndpoint(FabricEndpoint{ID: "to-remove", Type: "s3"})

		if err := engine.RemoveEndpoint("to-remove"); err != nil {
			t.Fatal(err)
		}
		if err := engine.RemoveEndpoint("to-remove"); err == nil {
			t.Error("expected error for missing endpoint")
		}
	})

	t.Run("trigger sync", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())
		engine.AddEndpoint(FabricEndpoint{ID: "sync-ep", Type: "s3"})

		job, err := engine.TriggerSync("sync-ep", SyncDirectionUpload)
		if err != nil {
			t.Fatal(err)
		}
		if job.EndpointID != "sync-ep" {
			t.Errorf("expected sync-ep, got %s", job.EndpointID)
		}
		if job.Direction != SyncDirectionUpload {
			t.Errorf("expected upload, got %s", job.Direction)
		}

		// Wait for job to complete
		time.Sleep(100 * time.Millisecond)

		updated := engine.GetJob(job.ID)
		if updated != nil && updated.Status == "completed" {
			if updated.PointsSynced == 0 {
				t.Error("expected points synced > 0")
			}
		}
	})

	t.Run("sync missing endpoint", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())
		_, err := engine.TriggerSync("nope", SyncDirectionUpload)
		if err == nil {
			t.Error("expected error for missing endpoint")
		}
	})

	t.Run("sync disabled endpoint", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())
		engine.AddEndpoint(FabricEndpoint{ID: "disabled", Type: "s3"})
		engine.mu.Lock()
		engine.endpoints["disabled"].Enabled = false
		engine.mu.Unlock()

		_, err := engine.TriggerSync("disabled", SyncDirectionUpload)
		if err == nil {
			t.Error("expected error for disabled endpoint")
		}
	})

	t.Run("list jobs", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())
		engine.AddEndpoint(FabricEndpoint{ID: "jobs-ep", Type: "s3"})

		engine.TriggerSync("jobs-ep", SyncDirectionUpload)
		engine.TriggerSync("jobs-ep", SyncDirectionDownload)

		all := engine.ListJobs("")
		if len(all) < 2 {
			t.Errorf("expected at least 2 jobs, got %d", len(all))
		}
	})

	t.Run("get job not found", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())
		if engine.GetJob("nonexistent") != nil {
			t.Error("expected nil for missing job")
		}
	})

	t.Run("policies", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())

		engine.AddPolicy(SyncPolicy{
			MetricPattern: "cpu.*",
			Direction:     SyncDirectionUpload,
			Priority:      5,
			SampleRate:    1.0,
		})

		policies := engine.ListPolicies()
		if len(policies) != 1 {
			t.Errorf("expected 1 policy, got %d", len(policies))
		}
	})

	t.Run("conflicts", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())
		conflicts := engine.ListConflicts()
		if len(conflicts) != 0 {
			t.Errorf("expected 0 conflicts, got %d", len(conflicts))
		}
	})

	t.Run("stats", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())
		engine.AddEndpoint(FabricEndpoint{ID: "stats-ep", Type: "s3"})

		stats := engine.GetStats()
		if stats.Endpoints != 1 {
			t.Errorf("expected 1 endpoint, got %d", stats.Endpoints)
		}
	})

	t.Run("start stop", func(t *testing.T) {
		engine := NewEdgeCloudFabricEngine(db, DefaultEdgeCloudFabricConfig())
		engine.Start()
		engine.Start()
		engine.Stop()
		engine.Stop()
	})

	t.Run("config defaults", func(t *testing.T) {
		cfg := DefaultEdgeCloudFabricConfig()
		if cfg.SyncInterval != 30*time.Second {
			t.Error("unexpected sync interval")
		}
		if cfg.ConflictResolution != "lww" {
			t.Error("unexpected conflict resolution")
		}
		if cfg.BatchSize != 10000 {
			t.Error("unexpected batch size")
		}
	})
}
