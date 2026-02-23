package chronicle

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
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
	delete(e.backups, id)
	return nil
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
		var req struct {
			PointCount int64 `json:"point_count"`
			SizeBytes  int64 `json:"size_bytes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		manifest, err := e.CreateBackup(req.PointCount, req.SizeBytes)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
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
