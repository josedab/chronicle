package chronicle

import (
	"testing"
)

func TestIncrementalBackupEngine(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultIncrementalBackupConfig()
	engine := NewIncrementalBackupEngine(db, cfg)
	engine.Start()
	defer engine.Stop()

	t.Run("CreateFull", func(t *testing.T) {
		m, err := engine.CreateFull()
		if err != nil {
			t.Fatalf("CreateFull failed: %v", err)
		}
		if m.Type != "full" {
			t.Errorf("expected type full, got %s", m.Type)
		}
		if m.Status != "completed" {
			t.Errorf("expected status completed, got %s", m.Status)
		}
		if m.Checksum == "" {
			t.Error("expected non-empty checksum")
		}
	})

	t.Run("CreateIncremental", func(t *testing.T) {
		full, err := engine.CreateFull()
		if err != nil {
			t.Fatalf("CreateFull failed: %v", err)
		}

		incr, err := engine.CreateIncremental(full.ID)
		if err != nil {
			t.Fatalf("CreateIncremental failed: %v", err)
		}
		if incr.Type != "incremental" {
			t.Errorf("expected type incremental, got %s", incr.Type)
		}
		if incr.ParentID != full.ID {
			t.Errorf("expected parent %s, got %s", full.ID, incr.ParentID)
		}
	})

	t.Run("CreateIncrementalInvalidParent", func(t *testing.T) {
		_, err := engine.CreateIncremental("nonexistent")
		if err == nil {
			t.Error("expected error for nonexistent parent")
		}
	})

	t.Run("ChainTracking", func(t *testing.T) {
		db2 := setupTestDB(t)
		defer db2.Close()
		e2 := NewIncrementalBackupEngine(db2, cfg)
		e2.Start()
		defer e2.Stop()

		full, _ := e2.CreateFull()
		chainID := "chain-" + full.ID
		e2.CreateIncremental(full.ID)
		e2.CreateIncremental(full.ID)

		chain := e2.GetChain(chainID)
		if chain == nil {
			t.Fatal("expected chain to exist")
		}
		if len(chain.Incrementals) != 2 {
			t.Errorf("expected 2 incrementals, got %d", len(chain.Incrementals))
		}
		if chain.FullBackupID != full.ID {
			t.Errorf("expected full backup ID %s, got %s", full.ID, chain.FullBackupID)
		}
	})

	t.Run("ListBackups", func(t *testing.T) {
		backups := engine.ListBackups()
		if len(backups) == 0 {
			t.Error("expected backups in list")
		}
	})

	t.Run("Verify", func(t *testing.T) {
		full, _ := engine.CreateFull()
		ok, err := engine.Verify(full.ID)
		if err != nil {
			t.Fatalf("Verify failed: %v", err)
		}
		if !ok {
			t.Error("expected checksum verification to pass")
		}
	})

	t.Run("VerifyNotFound", func(t *testing.T) {
		_, err := engine.Verify("nonexistent")
		if err == nil {
			t.Error("expected error for nonexistent backup")
		}
	})

	t.Run("Restore", func(t *testing.T) {
		full, _ := engine.CreateFull()
		err := engine.Restore(full.ID)
		if err != nil {
			t.Fatalf("Restore failed: %v", err)
		}
	})

	t.Run("RestoreNotFound", func(t *testing.T) {
		err := engine.Restore("nonexistent")
		if err == nil {
			t.Error("expected error for nonexistent backup")
		}
	})

	t.Run("MaxChains", func(t *testing.T) {
		db3 := setupTestDB(t)
		defer db3.Close()
		limitCfg := DefaultIncrementalBackupConfig()
		limitCfg.MaxChains = 2
		e3 := NewIncrementalBackupEngine(db3, limitCfg)
		e3.Start()
		defer e3.Stop()

		e3.CreateFull()
		e3.CreateFull()
		e3.CreateFull() // should evict oldest

		// engine should still work
		stats := e3.GetStats()
		if stats.TotalBackups != 3 {
			t.Errorf("expected 3 total backups, got %d", stats.TotalBackups)
		}
	})

	t.Run("Stats", func(t *testing.T) {
		stats := engine.GetStats()
		if stats.TotalBackups == 0 {
			t.Error("expected non-zero total backups")
		}
		if stats.FullBackups == 0 {
			t.Error("expected non-zero full backups")
		}
		if stats.TotalSizeBytes == 0 {
			t.Error("expected non-zero total size")
		}
		if stats.LastBackupAt.IsZero() {
			t.Error("expected non-zero last backup time")
		}
	})
}
