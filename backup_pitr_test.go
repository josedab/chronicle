package chronicle

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// TestDefaultPITRConfig
// ---------------------------------------------------------------------------

func TestDefaultPITRConfig(t *testing.T) {
	cfg := DefaultPITRConfig()

	if cfg.WALArchiveDir != "/var/lib/chronicle/wal-archive" {
		t.Errorf("WALArchiveDir = %q, want /var/lib/chronicle/wal-archive", cfg.WALArchiveDir)
	}
	if cfg.WALArchiveInterval != 30*time.Second {
		t.Errorf("WALArchiveInterval = %v, want 30s", cfg.WALArchiveInterval)
	}
	if !cfg.EnableDeduplication {
		t.Error("EnableDeduplication should default to true")
	}
	if cfg.EnableEncryption {
		t.Error("EnableEncryption should default to false")
	}
	if cfg.EncryptionKeyPath != "" {
		t.Errorf("EncryptionKeyPath = %q, want empty", cfg.EncryptionKeyPath)
	}
	if cfg.RetentionDays != 7 {
		t.Errorf("RetentionDays = %d, want 7", cfg.RetentionDays)
	}
	if cfg.MaxSegmentSizeMB != 64 {
		t.Errorf("MaxSegmentSizeMB = %d, want 64", cfg.MaxSegmentSizeMB)
	}
	if cfg.CheckpointInterval != 15*time.Minute {
		t.Errorf("CheckpointInterval = %v, want 15m", cfg.CheckpointInterval)
	}
}

// ---------------------------------------------------------------------------
// TestContentAddressableStore
// ---------------------------------------------------------------------------

func TestContentAddressableStore(t *testing.T) {
	t.Run("PutAndGet", func(t *testing.T) {
		store := NewContentAddressableStore()
		data := []byte("hello world")

		hash, err := store.Put(data)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if hash == "" {
			t.Fatal("Put returned empty hash")
		}

		got, err := store.Get(hash)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("Get = %q, want %q", got, data)
		}
	})

	t.Run("PutEmpty", func(t *testing.T) {
		store := NewContentAddressableStore()
		_, err := store.Put([]byte{})
		if err == nil {
			t.Error("expected error for empty data")
		}
	})

	t.Run("Dedup", func(t *testing.T) {
		store := NewContentAddressableStore()
		data := []byte("duplicate data")

		hash1, _ := store.Put(data)
		hash2, _ := store.Put(data)

		if hash1 != hash2 {
			t.Errorf("same data produced different hashes: %s vs %s", hash1, hash2)
		}

		blocks, totalBytes := store.Stats()
		if blocks != 1 {
			t.Errorf("blocks = %d, want 1 (dedup should prevent second block)", blocks)
		}
		if totalBytes != int64(len(data)) {
			t.Errorf("totalBytes = %d, want %d", totalBytes, len(data))
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		store := NewContentAddressableStore()
		_, err := store.Get("nonexistent")
		if err == nil {
			t.Error("expected error for missing block")
		}
	})

	t.Run("ReleaseAndGC", func(t *testing.T) {
		store := NewContentAddressableStore()
		data := []byte("gc me")

		hash, _ := store.Put(data)
		store.Release(hash)

		collected := store.GC()
		if collected != 1 {
			t.Errorf("GC collected %d, want 1", collected)
		}

		_, err := store.Get(hash)
		if err == nil {
			t.Error("expected error after GC removed the block")
		}

		blocks, totalBytes := store.Stats()
		if blocks != 0 {
			t.Errorf("blocks = %d after GC, want 0", blocks)
		}
		if totalBytes != 0 {
			t.Errorf("totalBytes = %d after GC, want 0", totalBytes)
		}
	})

	t.Run("GCKeepsReferenced", func(t *testing.T) {
		store := NewContentAddressableStore()
		data := []byte("keep me")

		hash, _ := store.Put(data)
		// RefCount is 1, so GC should not collect it.
		collected := store.GC()
		if collected != 0 {
			t.Errorf("GC collected %d, want 0", collected)
		}

		got, err := store.Get(hash)
		if err != nil {
			t.Fatalf("Get after GC failed: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Error("data mismatch after GC")
		}
	})
}

// ---------------------------------------------------------------------------
// TestWALArchiver
// ---------------------------------------------------------------------------

func TestWALArchiver(t *testing.T) {
	t.Run("ArchiveAndList", func(t *testing.T) {
		dir := t.TempDir()
		archiveDir := filepath.Join(dir, "archive")
		srcDir := filepath.Join(dir, "src")
		if err := os.MkdirAll(srcDir, 0o750); err != nil {
			t.Fatal(err)
		}

		// Create a temp segment file.
		segPath := filepath.Join(srcDir, "segment-001.wal")
		if err := os.WriteFile(segPath, []byte("wal data 1"), 0o640); err != nil {
			t.Fatal(err)
		}

		archiver, err := NewWALArchiver(archiveDir)
		if err != nil {
			t.Fatalf("NewWALArchiver failed: %v", err)
		}

		seg, err := archiver.Archive(segPath)
		if err != nil {
			t.Fatalf("Archive failed: %v", err)
		}
		if seg.Name != "segment-001.wal" {
			t.Errorf("segment name = %q, want segment-001.wal", seg.Name)
		}
		if seg.Size != int64(len("wal data 1")) {
			t.Errorf("segment size = %d, want %d", seg.Size, len("wal data 1"))
		}

		segs := archiver.ListSegments()
		if len(segs) != 1 {
			t.Fatalf("ListSegments returned %d, want 1", len(segs))
		}
		if segs[0].Name != "segment-001.wal" {
			t.Errorf("listed segment name = %q, want segment-001.wal", segs[0].Name)
		}

		// Verify the archived file exists.
		archivedPath := filepath.Join(archiveDir, "segment-001.wal")
		data, err := os.ReadFile(archivedPath)
		if err != nil {
			t.Fatalf("reading archived file: %v", err)
		}
		if string(data) != "wal data 1" {
			t.Errorf("archived content = %q, want %q", data, "wal data 1")
		}
	})

	t.Run("PurgeOldSegments", func(t *testing.T) {
		dir := t.TempDir()
		archiveDir := filepath.Join(dir, "archive")
		srcDir := filepath.Join(dir, "src")
		if err := os.MkdirAll(srcDir, 0o750); err != nil {
			t.Fatal(err)
		}

		archiver, err := NewWALArchiver(archiveDir)
		if err != nil {
			t.Fatalf("NewWALArchiver failed: %v", err)
		}

		// Archive two segments.
		for i, name := range []string{"seg-old.wal", "seg-new.wal"} {
			path := filepath.Join(srcDir, name)
			if err := os.WriteFile(path, []byte("data"), 0o640); err != nil {
				t.Fatal(err)
			}
			seg, err := archiver.Archive(path)
			if err != nil {
				t.Fatalf("Archive %s failed: %v", name, err)
			}
			// Backdate the first segment so purge will remove it.
			if i == 0 {
				seg.ArchivedAt = time.Now().UTC().Add(-48 * time.Hour)
				archiver.mu.Lock()
				archiver.segments[0].ArchivedAt = seg.ArchivedAt
				archiver.mu.Unlock()
			}
		}

		purged, err := archiver.PurgeOlderThan(24 * time.Hour)
		if err != nil {
			t.Fatalf("PurgeOlderThan failed: %v", err)
		}
		if purged != 1 {
			t.Errorf("purged = %d, want 1", purged)
		}

		segs := archiver.ListSegments()
		if len(segs) != 1 {
			t.Errorf("remaining segments = %d, want 1", len(segs))
		}
	})

	t.Run("ListEmpty", func(t *testing.T) {
		dir := t.TempDir()
		archiver, err := NewWALArchiver(dir)
		if err != nil {
			t.Fatalf("NewWALArchiver failed: %v", err)
		}
		segs := archiver.ListSegments()
		if len(segs) != 0 {
			t.Errorf("expected empty list, got %d", len(segs))
		}
	})
}

// ---------------------------------------------------------------------------
// TestEncryptedWriterReader
// ---------------------------------------------------------------------------

func TestEncryptedWriterReader(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	t.Run("RoundTrip", func(t *testing.T) {
		plaintext := []byte("secret payload for PITR encryption test")

		var buf bytes.Buffer
		ew, err := NewEncryptedWriter(&buf, key)
		if err != nil {
			t.Fatalf("NewEncryptedWriter: %v", err)
		}

		n, err := ew.Write(plaintext)
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
		if n != len(plaintext) {
			t.Errorf("Write returned %d, want %d", n, len(plaintext))
		}

		// The ciphertext should differ from plaintext.
		if bytes.Equal(buf.Bytes(), plaintext) {
			t.Error("ciphertext equals plaintext")
		}

		er, err := NewEncryptedReader(&buf, key)
		if err != nil {
			t.Fatalf("NewEncryptedReader: %v", err)
		}

		out := make([]byte, len(plaintext))
		rn, err := er.Read(out)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if rn != len(plaintext) {
			t.Errorf("Read returned %d, want %d", rn, len(plaintext))
		}
		if !bytes.Equal(out[:rn], plaintext) {
			t.Errorf("decrypted = %q, want %q", out[:rn], plaintext)
		}
	})

	t.Run("WrongKeySize", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := NewEncryptedWriter(&buf, []byte("short"))
		if err == nil {
			t.Error("expected error for wrong key size in writer")
		}
		_, err = NewEncryptedReader(&buf, []byte("short"))
		if err == nil {
			t.Error("expected error for wrong key size in reader")
		}
	})

	t.Run("WrongKeyDecrypt", func(t *testing.T) {
		plaintext := []byte("data to encrypt")

		var buf bytes.Buffer
		ew, _ := NewEncryptedWriter(&buf, key)
		ew.Write(plaintext)

		wrongKey := make([]byte, 32)
		for i := range wrongKey {
			wrongKey[i] = byte(i + 100)
		}
		er, err := NewEncryptedReader(&buf, wrongKey)
		if err != nil {
			t.Fatalf("NewEncryptedReader: %v", err)
		}

		out := make([]byte, len(plaintext))
		_, err = er.Read(out)
		if err == nil {
			t.Error("expected decryption error with wrong key")
		}
	})
}

// ---------------------------------------------------------------------------
// TestNewPITRManager
// ---------------------------------------------------------------------------

func TestNewPITRManager(t *testing.T) {
	t.Run("ValidConfig", func(t *testing.T) {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "test.db")
		cfg := DefaultConfig(dbPath)
		db, err := Open(cfg.Path, cfg)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer db.Close()

		pitrCfg := DefaultPITRConfig()
		pitrCfg.WALArchiveDir = filepath.Join(dir, "wal-archive")

		mgr, err := NewPITRManager(db, pitrCfg)
		if err != nil {
			t.Fatalf("NewPITRManager: %v", err)
		}
		if mgr == nil {
			t.Fatal("manager is nil")
		}
	})

	t.Run("InvalidEncryptionKeyPath", func(t *testing.T) {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "test.db")
		cfg := DefaultConfig(dbPath)
		db, err := Open(cfg.Path, cfg)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer db.Close()

		pitrCfg := DefaultPITRConfig()
		pitrCfg.WALArchiveDir = filepath.Join(dir, "wal-archive")
		pitrCfg.EnableEncryption = true
		pitrCfg.EncryptionKeyPath = filepath.Join(dir, "nonexistent-key")

		_, err = NewPITRManager(db, pitrCfg)
		if err == nil {
			t.Error("expected error for missing encryption key file")
		}
	})
}

// ---------------------------------------------------------------------------
// TestPITRManager_StartStop
// ---------------------------------------------------------------------------

func TestPITRManager_StartStop(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	cfg := DefaultConfig(dbPath)
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	pitrCfg := DefaultPITRConfig()
	pitrCfg.WALArchiveDir = filepath.Join(dir, "wal-archive")
	pitrCfg.WALArchiveInterval = 1 * time.Hour // slow ticker to avoid side effects

	mgr, err := NewPITRManager(db, pitrCfg)
	if err != nil {
		t.Fatalf("NewPITRManager: %v", err)
	}

	status := mgr.Status()
	if status.Running {
		t.Error("manager should not be running before Start")
	}

	mgr.Start()
	status = mgr.Status()
	if !status.Running {
		t.Error("manager should be running after Start")
	}

	// Calling Start again should be safe (idempotent).
	mgr.Start()
	status = mgr.Status()
	if !status.Running {
		t.Error("manager should still be running after double Start")
	}

	mgr.Stop()
	status = mgr.Status()
	if status.Running {
		t.Error("manager should not be running after Stop")
	}
}

// ---------------------------------------------------------------------------
// TestPITRManager_CreateCheckpoint
// ---------------------------------------------------------------------------

func TestPITRManager_CreateCheckpoint(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	cfg := DefaultConfig(dbPath)
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	pitrCfg := DefaultPITRConfig()
	pitrCfg.WALArchiveDir = filepath.Join(dir, "wal-archive")
	pitrCfg.WALArchiveInterval = 1 * time.Hour

	mgr, err := NewPITRManager(db, pitrCfg)
	if err != nil {
		t.Fatalf("NewPITRManager: %v", err)
	}

	// Creating a checkpoint before Start should fail.
	_, err = mgr.CreateCheckpoint(context.Background())
	if err == nil {
		t.Error("expected error when manager is not running")
	}

	mgr.Start()
	defer mgr.Stop()

	// Write a point so the snapshot has data.
	p := Point{
		Metric:    "cpu.usage",
		Timestamp: time.Now().UnixNano(),
		Value:     42.0,
		Tags:      map[string]string{"host": "test"},
	}
	if err := db.Write(p); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	cp, err := mgr.CreateCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}
	if cp.ID == "" {
		t.Error("checkpoint ID is empty")
	}
	if cp.CreatedAt.IsZero() {
		t.Error("checkpoint CreatedAt is zero")
	}
	if cp.SizeBytes <= 0 {
		t.Errorf("checkpoint SizeBytes = %d, want > 0", cp.SizeBytes)
	}

	// Verify it appears in the list.
	cps := mgr.ListCheckpoints()
	if len(cps) != 1 {
		t.Fatalf("ListCheckpoints = %d, want 1", len(cps))
	}
	if cps[0].ID != cp.ID {
		t.Errorf("listed checkpoint ID = %q, want %q", cps[0].ID, cp.ID)
	}
}

// ---------------------------------------------------------------------------
// TestPITRManager_ListCheckpoints
// ---------------------------------------------------------------------------

func TestPITRManager_ListCheckpoints(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	cfg := DefaultConfig(dbPath)
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	pitrCfg := DefaultPITRConfig()
	pitrCfg.WALArchiveDir = filepath.Join(dir, "wal-archive")
	pitrCfg.WALArchiveInterval = 1 * time.Hour

	mgr, err := NewPITRManager(db, pitrCfg)
	if err != nil {
		t.Fatalf("NewPITRManager: %v", err)
	}

	t.Run("Empty", func(t *testing.T) {
		cps := mgr.ListCheckpoints()
		if len(cps) != 0 {
			t.Errorf("expected empty list, got %d", len(cps))
		}
	})

	t.Run("Multiple", func(t *testing.T) {
		mgr.Start()
		defer mgr.Stop()

		for i := 0; i < 3; i++ {
			p := Point{
				Metric:    "cpu.usage",
				Timestamp: time.Now().UnixNano(),
				Value:     float64(i),
				Tags:      map[string]string{"host": "test"},
			}
			if err := db.Write(p); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := db.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			_, err := mgr.CreateCheckpoint(context.Background())
			if err != nil {
				t.Fatalf("CreateCheckpoint %d: %v", i, err)
			}
			// Small sleep to ensure distinct timestamps.
			time.Sleep(5 * time.Millisecond)
		}

		cps := mgr.ListCheckpoints()
		if len(cps) != 3 {
			t.Fatalf("ListCheckpoints = %d, want 3", len(cps))
		}

		// Verify sorted by creation time.
		for i := 1; i < len(cps); i++ {
			if cps[i].CreatedAt.Before(cps[i-1].CreatedAt) {
				t.Errorf("checkpoints not sorted: [%d].CreatedAt=%v before [%d].CreatedAt=%v",
					i, cps[i].CreatedAt, i-1, cps[i-1].CreatedAt)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// TestPITRManager_EstimateRestoreTime
// ---------------------------------------------------------------------------

func TestPITRManager_EstimateRestoreTime(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	cfg := DefaultConfig(dbPath)
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	pitrCfg := DefaultPITRConfig()
	pitrCfg.WALArchiveDir = filepath.Join(dir, "wal-archive")
	pitrCfg.WALArchiveInterval = 1 * time.Hour

	mgr, err := NewPITRManager(db, pitrCfg)
	if err != nil {
		t.Fatalf("NewPITRManager: %v", err)
	}
	mgr.Start()
	defer mgr.Stop()

	// Write data and create a checkpoint.
	p := Point{
		Metric:    "mem.used",
		Timestamp: time.Now().UnixNano(),
		Value:     1024,
		Tags:      map[string]string{"host": "test"},
	}
	if err := db.Write(p); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	cp, err := mgr.CreateCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}

	// Estimate for a time at or after the checkpoint.
	est, err := mgr.EstimateRestoreTime(cp.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("EstimateRestoreTime: %v", err)
	}
	if est <= 0 {
		t.Errorf("estimate = %v, want > 0", est)
	}

	// With no checkpoint before epoch, should error.
	_, err = mgr.EstimateRestoreTime(time.Unix(0, 0))
	if err == nil {
		t.Error("expected error for time before any checkpoint")
	}
}
