package chronicle

import (
	"testing"
)

func TestHotBackupConfig(t *testing.T) {
	cfg := DefaultHotBackupConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.MaxBackups != 10 {
		t.Errorf("expected 10 max backups, got %d", cfg.MaxBackups)
	}
	if !cfg.CompressionEnabled {
		t.Error("expected compression enabled")
	}
	if cfg.BackupDir != "/var/lib/chronicle/backups" {
		t.Errorf("unexpected backup dir: %s", cfg.BackupDir)
	}
}

func TestHotBackupCreate(t *testing.T) {
	db := setupTestDB(t)

	engine := NewHotBackupEngine(db, DefaultHotBackupConfig())

	manifest, err := engine.CreateBackup(1000, 4096)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if manifest.ID == "" {
		t.Error("expected non-empty ID")
	}
	if manifest.PointCount != 1000 {
		t.Errorf("expected 1000 points, got %d", manifest.PointCount)
	}
	if manifest.SizeBytes != 4096 {
		t.Errorf("expected 4096 bytes, got %d", manifest.SizeBytes)
	}
	if manifest.Status != BackupStatusCompleted {
		t.Errorf("expected completed status, got %s", manifest.Status)
	}
	if manifest.Checksum == "" {
		t.Error("expected non-empty checksum")
	}
	if !manifest.Compressed {
		t.Error("expected compressed")
	}
}

func TestHotBackupListBackups(t *testing.T) {
	db := setupTestDB(t)

	engine := NewHotBackupEngine(db, DefaultHotBackupConfig())

	engine.CreateBackup(100, 1024)
	engine.CreateBackup(200, 2048)
	engine.CreateBackup(300, 3072)

	backups := engine.ListBackups()
	if len(backups) != 3 {
		t.Fatalf("expected 3 backups, got %d", len(backups))
	}

	// Should be sorted by creation time
	for i := 1; i < len(backups); i++ {
		if backups[i].CreatedAt.Before(backups[i-1].CreatedAt) {
			t.Error("backups should be sorted by creation time")
		}
	}
}

func TestHotBackupGetBackup(t *testing.T) {
	db := setupTestDB(t)

	engine := NewHotBackupEngine(db, DefaultHotBackupConfig())

	created, _ := engine.CreateBackup(500, 2048)

	t.Run("existing backup", func(t *testing.T) {
		backup, exists := engine.GetBackup(created.ID)
		if !exists {
			t.Fatal("expected backup to exist")
		}
		if backup.PointCount != 500 {
			t.Errorf("expected 500 points, got %d", backup.PointCount)
		}
	})

	t.Run("nonexistent backup", func(t *testing.T) {
		_, exists := engine.GetBackup("nonexistent")
		if exists {
			t.Error("expected backup to not exist")
		}
	})
}

func TestHotBackupDelete(t *testing.T) {
	db := setupTestDB(t)

	engine := NewHotBackupEngine(db, DefaultHotBackupConfig())

	created, _ := engine.CreateBackup(100, 1024)

	t.Run("delete existing", func(t *testing.T) {
		err := engine.DeleteBackup(created.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_, exists := engine.GetBackup(created.ID)
		if exists {
			t.Error("expected backup to be deleted")
		}
	})

	t.Run("delete nonexistent", func(t *testing.T) {
		err := engine.DeleteBackup("nonexistent")
		if err == nil {
			t.Error("expected error for nonexistent backup")
		}
	})
}

func TestHotBackupMaxBackups(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultHotBackupConfig()
	cfg.MaxBackups = 3
	engine := NewHotBackupEngine(db, cfg)

	for i := 0; i < 3; i++ {
		_, err := engine.CreateBackup(100, 1024)
		if err != nil {
			t.Fatalf("unexpected error on backup %d: %v", i, err)
		}
	}

	_, err := engine.CreateBackup(100, 1024)
	if err == nil {
		t.Error("expected error when exceeding max backups")
	}
}

func TestHotBackupStats(t *testing.T) {
	db := setupTestDB(t)

	engine := NewHotBackupEngine(db, DefaultHotBackupConfig())

	engine.CreateBackup(100, 1024)
	engine.CreateBackup(200, 2048)

	stats := engine.Stats()
	if stats.TotalBackups != 2 {
		t.Errorf("expected 2 backups, got %d", stats.TotalBackups)
	}
	if stats.TotalSizeBytes != 3072 {
		t.Errorf("expected 3072 total bytes, got %d", stats.TotalSizeBytes)
	}
	if stats.TotalPoints != 300 {
		t.Errorf("expected 300 total points, got %d", stats.TotalPoints)
	}
}

func TestHotBackupCompression(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultHotBackupConfig()
	cfg.CompressionEnabled = false
	engine := NewHotBackupEngine(db, cfg)

	manifest, _ := engine.CreateBackup(100, 1024)
	if manifest.Compressed {
		t.Error("expected uncompressed when compression disabled")
	}
}
