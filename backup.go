package chronicle

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// BackupConfig configures backup operations.
type BackupConfig struct {
	// DestinationPath is the local path for backups.
	DestinationPath string

	// StorageBackend is an optional remote storage backend (S3, GCS).
	StorageBackend StorageBackend

	// Compression enables gzip compression for backup files.
	Compression bool

	// Encryption enables encryption for backup files (uses DB encryption key).
	Encryption bool

	// RetentionCount is the number of backups to retain.
	RetentionCount int

	// IncrementalEnabled enables delta backups.
	IncrementalEnabled bool
}

// BackupManager handles database backup and restore operations.
type BackupManager struct {
	db       *DB
	config   BackupConfig
	mu       sync.Mutex
	manifest *BackupManifest
}

// BackupManifest tracks backup history and state.
type BackupManifest struct {
	LastFullBackup        time.Time      `json:"last_full_backup"`
	LastIncrementalBackup time.Time      `json:"last_incremental_backup"`
	Backups               []BackupRecord `json:"backups"`
	LastWALPosition       int64          `json:"last_wal_position"`
}

// BackupRecord represents a single backup.
type BackupRecord struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"` // "full" or "incremental"
	Timestamp      time.Time `json:"timestamp"`
	Size           int64     `json:"size"`
	Compressed     bool      `json:"compressed"`
	Encrypted      bool      `json:"encrypted"`
	WALStart       int64     `json:"wal_start"`
	WALEnd         int64     `json:"wal_end"`
	PartitionCount int       `json:"partition_count"`
	FilePath       string    `json:"file_path"`
}

// BackupResult contains the result of a backup operation.
type BackupResult struct {
	Record    BackupRecord
	Duration  time.Duration
	BytesRead int64
}

// NewBackupManager creates a backup manager.
func NewBackupManager(db *DB, config BackupConfig) (*BackupManager, error) {
	if config.DestinationPath == "" && config.StorageBackend == nil {
		return nil, errors.New("backup destination or storage backend required")
	}

	if config.RetentionCount <= 0 {
		config.RetentionCount = 10
	}

	bm := &BackupManager{
		db:     db,
		config: config,
		manifest: &BackupManifest{
			Backups: make([]BackupRecord, 0),
		},
	}

	// Load existing manifest
	if err := bm.loadManifest(); err != nil {
		// OK if manifest doesn't exist
		if !os.IsNotExist(err) && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("failed to load manifest: %w", err)
		}
	}

	return bm, nil
}

// FullBackup performs a complete database backup.
func (bm *BackupManager) FullBackup() (*BackupResult, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	start := time.Now()
	backupID := fmt.Sprintf("full_%d", start.UnixNano())

	// Create backup file
	filename := backupID + ".chronicle"
	if bm.config.Compression {
		filename += ".gz"
	}

	filePath := filepath.Join(bm.config.DestinationPath, filename)

	var bytesWritten int64
	var err error

	if bm.config.DestinationPath != "" {
		bytesWritten, err = bm.writeFullBackupToFile(filePath)
	} else if bm.config.StorageBackend != nil {
		bytesWritten, err = bm.writeFullBackupToBackend(filename)
	}

	if err != nil {
		return nil, err
	}

	record := BackupRecord{
		ID:             backupID,
		Type:           "full",
		Timestamp:      start,
		Size:           bytesWritten,
		Compressed:     bm.config.Compression,
		Encrypted:      bm.config.Encryption,
		WALStart:       0,
		WALEnd:         bm.getWALPosition(),
		PartitionCount: bm.getPartitionCount(),
		FilePath:       filePath,
	}

	bm.manifest.Backups = append(bm.manifest.Backups, record)
	bm.manifest.LastFullBackup = start
	bm.manifest.LastWALPosition = record.WALEnd

	if err := bm.saveManifest(); err != nil {
		return nil, err
	}

	bm.enforceRetention()

	return &BackupResult{
		Record:    record,
		Duration:  time.Since(start),
		BytesRead: bytesWritten,
	}, nil
}

// IncrementalBackup performs a delta backup since the last backup.
func (bm *BackupManager) IncrementalBackup() (*BackupResult, error) {
	if !bm.config.IncrementalEnabled {
		return nil, errors.New("incremental backups not enabled")
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.manifest.LastFullBackup.IsZero() {
		return nil, errors.New("no full backup exists; perform full backup first")
	}

	start := time.Now()
	backupID := fmt.Sprintf("incr_%d", start.UnixNano())

	filename := backupID + ".chronicle"
	if bm.config.Compression {
		filename += ".gz"
	}

	filePath := filepath.Join(bm.config.DestinationPath, filename)

	walStart := bm.manifest.LastWALPosition
	walEnd := bm.getWALPosition()

	var bytesWritten int64
	var err error

	if bm.config.DestinationPath != "" {
		bytesWritten, err = bm.writeIncrementalBackupToFile(filePath, walStart, walEnd)
	} else if bm.config.StorageBackend != nil {
		bytesWritten, err = bm.writeIncrementalBackupToBackend(filename, walStart, walEnd)
	}

	if err != nil {
		return nil, err
	}

	record := BackupRecord{
		ID:         backupID,
		Type:       "incremental",
		Timestamp:  start,
		Size:       bytesWritten,
		Compressed: bm.config.Compression,
		Encrypted:  bm.config.Encryption,
		WALStart:   walStart,
		WALEnd:     walEnd,
		FilePath:   filePath,
	}

	bm.manifest.Backups = append(bm.manifest.Backups, record)
	bm.manifest.LastIncrementalBackup = start
	bm.manifest.LastWALPosition = walEnd

	if err := bm.saveManifest(); err != nil {
		return nil, err
	}

	bm.enforceRetention()

	return &BackupResult{
		Record:    record,
		Duration:  time.Since(start),
		BytesRead: bytesWritten,
	}, nil
}

// Restore restores the database from a backup.
func (bm *BackupManager) Restore(backupID string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Find backup record
	var record *BackupRecord
	for i := range bm.manifest.Backups {
		if bm.manifest.Backups[i].ID == backupID {
			record = &bm.manifest.Backups[i]
			break
		}
	}

	if record == nil {
		return fmt.Errorf("backup not found: %s", backupID)
	}

	if record.Type == "incremental" {
		return bm.restoreIncremental(record)
	}

	return bm.restoreFull(record)
}

// RestoreLatest restores from the most recent backup.
func (bm *BackupManager) RestoreLatest() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if len(bm.manifest.Backups) == 0 {
		return errors.New("no backups available")
	}

	// Find the most recent full backup and any subsequent incrementals
	var fullBackup *BackupRecord
	var incrementals []*BackupRecord

	// Sort by timestamp descending
	sorted := make([]BackupRecord, len(bm.manifest.Backups))
	copy(sorted, bm.manifest.Backups)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.After(sorted[j].Timestamp)
	})

	for i := range sorted {
		if sorted[i].Type == "full" {
			fullBackup = &sorted[i]
			break
		}
		incrementals = append(incrementals, &sorted[i])
	}

	if fullBackup == nil {
		return errors.New("no full backup found")
	}

	// Restore full backup first
	if err := bm.restoreFull(fullBackup); err != nil {
		return err
	}

	// Apply incrementals in order (oldest first)
	for i := len(incrementals) - 1; i >= 0; i-- {
		if incrementals[i].Timestamp.After(fullBackup.Timestamp) {
			if err := bm.restoreIncremental(incrementals[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

// ListBackups returns all backup records.
func (bm *BackupManager) ListBackups() []BackupRecord {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	result := make([]BackupRecord, len(bm.manifest.Backups))
	copy(result, bm.manifest.Backups)
	return result
}

// DeleteBackup removes a backup.
func (bm *BackupManager) DeleteBackup(backupID string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	ctx := context.Background()
	for i, record := range bm.manifest.Backups {
		if record.ID == backupID {
			// Remove file
			if record.FilePath != "" {
				os.Remove(record.FilePath)
			} else if bm.config.StorageBackend != nil {
				bm.config.StorageBackend.Delete(ctx, record.ID)
			}

			// Remove from manifest
			bm.manifest.Backups = append(bm.manifest.Backups[:i], bm.manifest.Backups[i+1:]...)
			return bm.saveManifest()
		}
	}

	return fmt.Errorf("backup not found: %s", backupID)
}

// Internal methods

func (bm *BackupManager) writeFullBackupToFile(path string) (int64, error) {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return 0, err
	}

	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var w io.WriteCloser = f
	if bm.config.Compression {
		gw := gzip.NewWriter(f)
		defer gw.Close()
		w = gw
	}

	// Write database snapshot
	bm.db.mu.RLock()
	defer bm.db.mu.RUnlock()

	// Copy main database file
	dbFile, err := os.Open(bm.db.path)
	if err != nil {
		return 0, err
	}
	defer dbFile.Close()

	n, err := io.Copy(w, dbFile)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (bm *BackupManager) writeFullBackupToBackend(filename string) (int64, error) {
	bm.db.mu.RLock()
	defer bm.db.mu.RUnlock()

	// Read database file
	data, err := os.ReadFile(bm.db.path)
	if err != nil {
		return 0, err
	}

	ctx := context.Background()
	if err := bm.config.StorageBackend.Write(ctx, filename, data); err != nil {
		return 0, err
	}

	return int64(len(data)), nil
}

func (bm *BackupManager) writeIncrementalBackupToFile(path string, walStart, walEnd int64) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return 0, err
	}

	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var w io.WriteCloser = f
	if bm.config.Compression {
		gw := gzip.NewWriter(f)
		defer gw.Close()
		w = gw
	}

	// Write WAL segment
	if bm.db.wal == nil {
		return 0, errors.New("WAL not available")
	}

	walData, err := bm.db.wal.ReadRange(walStart, walEnd)
	if err != nil {
		return 0, err
	}

	n, err := w.Write(walData)
	return int64(n), err
}

func (bm *BackupManager) writeIncrementalBackupToBackend(filename string, walStart, walEnd int64) (int64, error) {
	if bm.db.wal == nil {
		return 0, errors.New("WAL not available")
	}

	walData, err := bm.db.wal.ReadRange(walStart, walEnd)
	if err != nil {
		return 0, err
	}

	ctx := context.Background()
	if err := bm.config.StorageBackend.Write(ctx, filename, walData); err != nil {
		return 0, err
	}

	return int64(len(walData)), nil
}

func (bm *BackupManager) restoreFull(record *BackupRecord) error {
	var reader io.Reader

	if record.FilePath != "" {
		f, err := os.Open(record.FilePath)
		if err != nil {
			return err
		}
		defer f.Close()
		reader = f

		if record.Compressed {
			gr, err := gzip.NewReader(f)
			if err != nil {
				return err
			}
			defer gr.Close()
			reader = gr
		}
	} else if bm.config.StorageBackend != nil {
		ctx := context.Background()
		data, err := bm.config.StorageBackend.Read(ctx, record.ID)
		if err != nil {
			return err
		}
		reader = newBytesReader(data)
	}

	// Write to database file (after closing current)
	bm.db.mu.Lock()
	defer bm.db.mu.Unlock()

	// Close current file
	if bm.db.file != nil {
		bm.db.file.Close()
	}

	// Write backup data
	f, err := os.Create(bm.db.path)
	if err != nil {
		return err
	}

	_, err = io.Copy(f, reader)
	f.Close()

	if err != nil {
		return err
	}

	// Reopen
	bm.db.file, err = os.OpenFile(bm.db.path, os.O_RDWR|os.O_CREATE, 0644)
	return err
}

func (bm *BackupManager) restoreIncremental(record *BackupRecord) error {
	var data []byte
	var err error

	if record.FilePath != "" {
		f, err := os.Open(record.FilePath)
		if err != nil {
			return err
		}
		defer f.Close()

		var reader io.Reader = f
		if record.Compressed {
			gr, err := gzip.NewReader(f)
			if err != nil {
				return err
			}
			defer gr.Close()
			reader = gr
		}

		data, err = io.ReadAll(reader)
		if err != nil {
			return err
		}
	} else if bm.config.StorageBackend != nil {
		ctx := context.Background()
		data, err = bm.config.StorageBackend.Read(ctx, record.ID)
		if err != nil {
			return err
		}
	}

	// Apply WAL entries
	if bm.db.wal != nil {
		return bm.db.wal.ApplyEntries(data)
	}

	return nil
}

func (bm *BackupManager) loadManifest() error {
	manifestPath := filepath.Join(bm.config.DestinationPath, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, bm.manifest)
}

func (bm *BackupManager) saveManifest() error {
	if bm.config.DestinationPath == "" {
		return nil // No local storage
	}

	if err := os.MkdirAll(bm.config.DestinationPath, 0755); err != nil {
		return err
	}

	manifestPath := filepath.Join(bm.config.DestinationPath, "manifest.json")
	data, err := json.MarshalIndent(bm.manifest, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(manifestPath, data, 0644)
}

func (bm *BackupManager) enforceRetention() {
	if len(bm.manifest.Backups) <= bm.config.RetentionCount {
		return
	}

	// Sort by timestamp
	sort.Slice(bm.manifest.Backups, func(i, j int) bool {
		return bm.manifest.Backups[i].Timestamp.Before(bm.manifest.Backups[j].Timestamp)
	})

	// Remove oldest backups, but keep at least one full backup
	toRemove := len(bm.manifest.Backups) - bm.config.RetentionCount
	removed := 0

	for i := 0; i < len(bm.manifest.Backups) && removed < toRemove; i++ {
		record := bm.manifest.Backups[i]

		// Don't remove the only full backup
		if record.Type == "full" {
			fullCount := 0
			for _, r := range bm.manifest.Backups {
				if r.Type == "full" {
					fullCount++
				}
			}
			if fullCount <= 1 {
				continue
			}
		}

		// Delete backup file
		ctx := context.Background()
		if record.FilePath != "" {
			os.Remove(record.FilePath)
		} else if bm.config.StorageBackend != nil {
			bm.config.StorageBackend.Delete(ctx, record.ID)
		}

		// Mark for removal
		bm.manifest.Backups[i].ID = ""
		removed++
	}

	// Compact slice
	newBackups := make([]BackupRecord, 0, len(bm.manifest.Backups)-removed)
	for _, r := range bm.manifest.Backups {
		if r.ID != "" {
			newBackups = append(newBackups, r)
		}
	}
	bm.manifest.Backups = newBackups
}

func (bm *BackupManager) getWALPosition() int64 {
	if bm.db.wal != nil {
		return bm.db.wal.Position()
	}
	return 0
}

func (bm *BackupManager) getPartitionCount() int {
	if bm.db.index != nil {
		return bm.db.index.Count()
	}
	return 0
}

// bytesReader wraps []byte in an io.Reader
type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
