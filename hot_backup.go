package chronicle

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// HotBackupConfig configures the hot backup manager.
type HotBackupConfig struct {
	Enabled            bool   `json:"enabled"`
	BackupDir          string `json:"backup_dir"`
	MaxBackups         int    `json:"max_backups"`
	CompressionEnabled bool   `json:"compression_enabled"`
}

// DefaultHotBackupConfig returns sensible defaults.
func DefaultHotBackupConfig() HotBackupConfig {
	return HotBackupConfig{
		Enabled:            true,
		BackupDir:          "/var/lib/chronicle/backups",
		MaxBackups:         10,
		CompressionEnabled: true,
	}
}

// BackupStatus represents the status of a backup.
type BackupStatus string

const (
	BackupStatusPending   BackupStatus = "pending"
	BackupStatusCompleted BackupStatus = "completed"
	BackupStatusFailed    BackupStatus = "failed"
)

// HotBackupManifest represents metadata for a single backup.
type HotBackupManifest struct {
	ID          string       `json:"id"`
	CreatedAt   time.Time    `json:"created_at"`
	SizeBytes   int64        `json:"size_bytes"`
	PointCount  int64        `json:"point_count"`
	Checksum    string       `json:"checksum"`
	Status      BackupStatus `json:"status"`
	Compressed  bool         `json:"compressed"`
}

// HotBackupStats tracks aggregate backup statistics.
type HotBackupStats struct {
	TotalBackups   int   `json:"total_backups"`
	TotalSizeBytes int64 `json:"total_size_bytes"`
	TotalPoints    int64 `json:"total_points"`
}

// HotBackupEngine manages hot backups.
type HotBackupEngine struct {
	db     *DB
	config HotBackupConfig

	backups  map[string]*HotBackupManifest
	sequence int64
	running  bool
	stopCh   chan struct{}

	mu sync.RWMutex
}

// NewHotBackupEngine creates a new hot backup engine.
func NewHotBackupEngine(db *DB, cfg HotBackupConfig) *HotBackupEngine {
	return &HotBackupEngine{
		db:      db,
		config:  cfg,
		backups: make(map[string]*HotBackupManifest),
		stopCh:  make(chan struct{}),
	}
}

// Start starts the hot backup engine.
func (e *HotBackupEngine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
}

// Stop stops the hot backup engine.
func (e *HotBackupEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// CreateBackup creates a new backup, recording its metadata.
func (e *HotBackupEngine) CreateBackup(pointCount int64, sizeBytes int64) (*HotBackupManifest, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.backups) >= e.config.MaxBackups {
		return nil, fmt.Errorf("max backups (%d) reached", e.config.MaxBackups)
	}

	e.sequence++
	id := fmt.Sprintf("backup-%d", e.sequence)
	now := time.Now()

	checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s-%d-%d", id, now.UnixNano(), pointCount))))

	manifest := &HotBackupManifest{
		ID:         id,
		CreatedAt:  now,
		SizeBytes:  sizeBytes,
		PointCount: pointCount,
		Checksum:   checksum,
		Status:     BackupStatusCompleted,
		Compressed: e.config.CompressionEnabled,
	}

	e.backups[id] = manifest
	return manifest, nil
}

// ListBackups returns all backups sorted by creation time.
func (e *HotBackupEngine) ListBackups() []HotBackupManifest {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]HotBackupManifest, 0, len(e.backups))
	for _, b := range e.backups {
		out = append(out, *b)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})

	return out
}

// GetBackup returns a backup by ID.
func (e *HotBackupEngine) GetBackup(id string) (HotBackupManifest, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	b, exists := e.backups[id]
	if !exists {
		return HotBackupManifest{}, false
	}
	return *b, true
}

// DeleteBackup removes a backup by ID.
func (e *HotBackupEngine) DeleteBackup(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.backups[id]; !exists {
		return fmt.Errorf("backup %q not found", id)
	}

	// Remove data file if it exists on disk.
	if e.config.BackupDir != "" {
		dataPath := filepath.Join(e.config.BackupDir, id+".json")
		os.Remove(dataPath) // best-effort
		manifestPath := filepath.Join(e.config.BackupDir, id+".manifest.json")
		os.Remove(manifestPath)
	}

	delete(e.backups, id)
	return nil
}

// SnapshotBackup creates a backup by querying all data from the DB, writing
// it to the backup directory, and computing a real content checksum.
func (e *HotBackupEngine) SnapshotBackup() (*HotBackupManifest, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.db == nil {
		return nil, fmt.Errorf("hot backup: database is nil")
	}
	if len(e.backups) >= e.config.MaxBackups {
		return nil, fmt.Errorf("max backups (%d) reached", e.config.MaxBackups)
	}

	metrics := e.db.Metrics()
	var allPoints []Point
	endTime := time.Now().UnixNano()

	for _, metric := range metrics {
		result, err := e.db.Execute(&Query{
			Metric: metric,
			Start:  0,
			End:    endTime,
		})
		if err != nil {
			return nil, fmt.Errorf("hot backup: query %q: %w", metric, err)
		}
		allPoints = append(allPoints, result.Points...)
	}

	data, err := json.Marshal(allPoints)
	if err != nil {
		return nil, fmt.Errorf("hot backup: marshal: %w", err)
	}

	checksum := fmt.Sprintf("%x", sha256.Sum256(data))

	e.sequence++
	id := fmt.Sprintf("backup-%d", e.sequence)
	now := time.Now()

	// Persist to disk if a backup directory is configured.
	if e.config.BackupDir != "" {
		if err := os.MkdirAll(e.config.BackupDir, 0o755); err != nil {
			return nil, fmt.Errorf("hot backup: mkdir: %w", err)
		}
		dataPath := filepath.Join(e.config.BackupDir, id+".json")
		if err := os.WriteFile(dataPath, data, 0o644); err != nil {
			return nil, fmt.Errorf("hot backup: write data: %w", err)
		}
	}

	manifest := &HotBackupManifest{
		ID:         id,
		CreatedAt:  now,
		SizeBytes:  int64(len(data)),
		PointCount: int64(len(allPoints)),
		Checksum:   checksum,
		Status:     BackupStatusCompleted,
		Compressed: false,
	}

	// Persist manifest to disk.
	if e.config.BackupDir != "" {
		mdata, _ := json.Marshal(manifest)
		manifestPath := filepath.Join(e.config.BackupDir, id+".manifest.json")
		if err := os.WriteFile(manifestPath, mdata, 0o644); err != nil {
			return nil, fmt.Errorf("hot backup: write manifest: %w", err)
		}
	}

	e.backups[id] = manifest
	return manifest, nil
}

// RestoreBackup loads a backup by ID from disk and writes all points back to
// the database.
func (e *HotBackupEngine) RestoreBackup(id string) (int, error) {
	e.mu.RLock()
	_, exists := e.backups[id]
	e.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("backup %q not found", id)
	}
	if e.config.BackupDir == "" {
		return 0, fmt.Errorf("no backup directory configured")
	}

	dataPath := filepath.Join(e.config.BackupDir, id+".json")
	data, err := os.ReadFile(dataPath)
	if err != nil {
		return 0, fmt.Errorf("hot backup: read data: %w", err)
	}

	var points []Point
	if err := json.Unmarshal(data, &points); err != nil {
		return 0, fmt.Errorf("hot backup: unmarshal: %w", err)
	}

	if len(points) > 0 {
		if err := e.db.WriteBatch(points); err != nil {
			return 0, fmt.Errorf("hot backup: restore write: %w", err)
		}
	}

	return len(points), nil
}

// Stats returns aggregate backup statistics.
func (e *HotBackupEngine) Stats() HotBackupStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := HotBackupStats{
		TotalBackups: len(e.backups),
	}
	for _, b := range e.backups {
		stats.TotalSizeBytes += b.SizeBytes
		stats.TotalPoints += b.PointCount
	}
	return stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *HotBackupEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/hot-backup/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListBackups())
	})
	mux.HandleFunc("/api/v1/hot-backup/create", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		manifest, err := e.SnapshotBackup()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(manifest)
	})
	mux.HandleFunc("/api/v1/hot-backup/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		var req struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := e.DeleteBackup(req.ID); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
	})
	mux.HandleFunc("/api/v1/hot-backup/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}
