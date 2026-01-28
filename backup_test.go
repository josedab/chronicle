package chronicle

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBackupManager_NewBackupManager(t *testing.T) {
	tmpDir := t.TempDir()

	bm, err := NewBackupManager(nil, BackupConfig{
		DestinationPath: tmpDir,
	})
	if err != nil {
		t.Fatalf("NewBackupManager failed: %v", err)
	}

	if bm.config.RetentionCount != 10 {
		t.Error("default retention count should be 10")
	}
}

func TestBackupManager_NewBackupManager_NoDestination(t *testing.T) {
	_, err := NewBackupManager(nil, BackupConfig{})
	if err == nil {
		t.Error("expected error for missing destination")
	}
}

func TestBackupManager_ListBackups_Empty(t *testing.T) {
	tmpDir := t.TempDir()

	bm, _ := NewBackupManager(nil, BackupConfig{
		DestinationPath: tmpDir,
	})

	backups := bm.ListBackups()
	if len(backups) != 0 {
		t.Error("expected empty backup list")
	}
}

func TestBackupManager_IncrementalWithoutFull(t *testing.T) {
	tmpDir := t.TempDir()

	bm, _ := NewBackupManager(nil, BackupConfig{
		DestinationPath:    tmpDir,
		IncrementalEnabled: true,
	})

	_, err := bm.IncrementalBackup()
	if err == nil {
		t.Error("expected error for incremental without full backup")
	}
}

func TestBackupManager_IncrementalDisabled(t *testing.T) {
	tmpDir := t.TempDir()

	bm, _ := NewBackupManager(nil, BackupConfig{
		DestinationPath:    tmpDir,
		IncrementalEnabled: false,
	})

	_, err := bm.IncrementalBackup()
	if err == nil {
		t.Error("expected error when incremental disabled")
	}
}

func TestBackupManager_RestoreLatest_NoBackups(t *testing.T) {
	tmpDir := t.TempDir()

	bm, _ := NewBackupManager(nil, BackupConfig{
		DestinationPath: tmpDir,
	})

	err := bm.RestoreLatest()
	if err == nil {
		t.Error("expected error when no backups available")
	}
}

func TestBackupManager_Restore_NotFound(t *testing.T) {
	tmpDir := t.TempDir()

	bm, _ := NewBackupManager(nil, BackupConfig{
		DestinationPath: tmpDir,
	})

	err := bm.Restore("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent backup")
	}
}

func TestBackupManager_DeleteBackup_NotFound(t *testing.T) {
	tmpDir := t.TempDir()

	bm, _ := NewBackupManager(nil, BackupConfig{
		DestinationPath: tmpDir,
	})

	err := bm.DeleteBackup("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent backup")
	}
}

func TestBackupRecord_Fields(t *testing.T) {
	record := BackupRecord{
		ID:             "full_123",
		Type:           "full",
		Timestamp:      time.Now(),
		Size:           1024,
		Compressed:     true,
		Encrypted:      false,
		WALStart:       0,
		WALEnd:         100,
		PartitionCount: 5,
		FilePath:       "/tmp/backup.chronicle.gz",
	}

	if record.ID != "full_123" {
		t.Error("ID mismatch")
	}
	if record.Type != "full" {
		t.Error("Type mismatch")
	}
	if record.Size != 1024 {
		t.Error("Size mismatch")
	}
	if !record.Compressed {
		t.Error("Compressed mismatch")
	}
}

func TestBackupManifest_Serialization(t *testing.T) {
	tmpDir := t.TempDir()

	bm, _ := NewBackupManager(nil, BackupConfig{
		DestinationPath: tmpDir,
	})

	// Add a record directly
	bm.manifest.Backups = append(bm.manifest.Backups, BackupRecord{
		ID:        "test_backup",
		Type:      "full",
		Timestamp: time.Now(),
		Size:      1024,
	})

	// Save manifest
	if err := bm.saveManifest(); err != nil {
		t.Fatalf("saveManifest failed: %v", err)
	}

	// Load manifest in new manager
	bm2, _ := NewBackupManager(nil, BackupConfig{
		DestinationPath: tmpDir,
	})

	backups := bm2.ListBackups()
	if len(backups) != 1 {
		t.Errorf("expected 1 backup, got %d", len(backups))
	}

	if backups[0].ID != "test_backup" {
		t.Error("backup ID mismatch after reload")
	}
}

func TestBackupConfig_Defaults(t *testing.T) {
	config := BackupConfig{}

	if config.Compression {
		t.Error("compression should default to false")
	}
	if config.Encryption {
		t.Error("encryption should default to false")
	}
	if config.IncrementalEnabled {
		t.Error("incremental should default to false")
	}
}

func TestBytesReader(t *testing.T) {
	data := []byte("hello world")
	reader := newBytesReader(data)

	buf := make([]byte, 5)
	n, err := reader.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes, got %d", n)
	}
	if string(buf) != "hello" {
		t.Errorf("expected 'hello', got '%s'", string(buf))
	}

	// Read rest
	n, err = reader.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes, got %d", n)
	}
	if string(buf) != " worl" {
		t.Errorf("expected ' worl', got '%s'", string(buf))
	}
}

func TestBackupManager_EnforceRetention(t *testing.T) {
	tmpDir := t.TempDir()

	bm, _ := NewBackupManager(nil, BackupConfig{
		DestinationPath: tmpDir,
		RetentionCount:  3,
	})

	// Add more backups than retention allows
	for i := 0; i < 5; i++ {
		bm.manifest.Backups = append(bm.manifest.Backups, BackupRecord{
			ID:        "full_" + string(rune('a'+i)),
			Type:      "full",
			Timestamp: time.Now().Add(time.Duration(i) * time.Hour),
		})
	}

	bm.enforceRetention()

	// Should have retention count backups
	if len(bm.manifest.Backups) > 3 {
		t.Errorf("expected at most 3 backups after retention, got %d", len(bm.manifest.Backups))
	}
}

func TestBackupManager_EnforceRetention_KeepsFullBackup(t *testing.T) {
	tmpDir := t.TempDir()

	bm, _ := NewBackupManager(nil, BackupConfig{
		DestinationPath: tmpDir,
		RetentionCount:  2,
	})

	// Add one full and multiple incrementals
	bm.manifest.Backups = []BackupRecord{
		{ID: "full_1", Type: "full", Timestamp: time.Now().Add(-5 * time.Hour)},
		{ID: "incr_1", Type: "incremental", Timestamp: time.Now().Add(-4 * time.Hour)},
		{ID: "incr_2", Type: "incremental", Timestamp: time.Now().Add(-3 * time.Hour)},
		{ID: "incr_3", Type: "incremental", Timestamp: time.Now().Add(-2 * time.Hour)},
		{ID: "incr_4", Type: "incremental", Timestamp: time.Now().Add(-1 * time.Hour)},
	}

	bm.enforceRetention()

	// Should keep at least the full backup
	hasFullBackup := false
	for _, r := range bm.manifest.Backups {
		if r.Type == "full" {
			hasFullBackup = true
			break
		}
	}

	if !hasFullBackup {
		t.Error("should have kept at least one full backup")
	}
}

func TestBackupResult_Fields(t *testing.T) {
	result := BackupResult{
		Record: BackupRecord{
			ID:   "test",
			Size: 1024,
		},
		Duration:  5 * time.Second,
		BytesRead: 2048,
	}

	if result.Duration != 5*time.Second {
		t.Error("Duration mismatch")
	}
	if result.BytesRead != 2048 {
		t.Error("BytesRead mismatch")
	}
}

func TestBackupManager_ManifestPath(t *testing.T) {
	tmpDir := t.TempDir()

	bm, _ := NewBackupManager(nil, BackupConfig{
		DestinationPath: tmpDir,
	})

	// Save manifest
	bm.saveManifest()

	// Check manifest file exists
	manifestPath := filepath.Join(tmpDir, "manifest.json")
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		t.Error("manifest file should exist")
	}
}
