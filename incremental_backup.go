package chronicle

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// IncrementalBackupConfig configures the incremental backup engine.
type IncrementalBackupConfig struct {
	Enabled         bool   `json:"enabled"`
	BackupDir       string `json:"backup_dir"`
	MaxChains       int    `json:"max_chains"`
	FullBackupEvery int    `json:"full_backup_every"`
	VerifyChecksum  bool   `json:"verify_checksum"`
}

// DefaultIncrementalBackupConfig returns sensible defaults.
func DefaultIncrementalBackupConfig() IncrementalBackupConfig {
	return IncrementalBackupConfig{
		Enabled:         true,
		BackupDir:       "/var/lib/chronicle/backups",
		MaxChains:       10,
		FullBackupEvery: 5,
		VerifyChecksum:  true,
	}
}

// IncrBackupManifest describes a single backup snapshot.
type IncrBackupManifest struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"` // full or incremental
	ParentID       string    `json:"parent_id,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	SizeBytes      int64     `json:"size_bytes"`
	PartitionCount int       `json:"partition_count"`
	Checksum       string    `json:"checksum"`
	Status         string    `json:"status"` // completed, failed, verified
}

// IncrBackupChain represents a chain of incremental backups rooted at a full backup.
type IncrBackupChain struct {
	ChainID        string    `json:"chain_id"`
	FullBackupID   string    `json:"full_backup_id"`
	Incrementals   []string  `json:"incrementals"`
	TotalSizeBytes int64     `json:"total_size_bytes"`
	CreatedAt      time.Time `json:"created_at"`
}

// IncrementalBackupStats holds engine statistics.
type IncrementalBackupStats struct {
	TotalBackups       int64     `json:"total_backups"`
	FullBackups        int64     `json:"full_backups"`
	IncrementalBackups int64     `json:"incremental_backups"`
	TotalSizeBytes     int64     `json:"total_size_bytes"`
	LastBackupAt       time.Time `json:"last_backup_at"`
}

// IncrementalBackupEngine manages incremental backup and restore.
type IncrementalBackupEngine struct {
	db      *DB
	config  IncrementalBackupConfig
	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}

	backups []IncrBackupManifest
	chains  []IncrBackupChain
	stats   IncrementalBackupStats
	nextID  int
}

// NewIncrementalBackupEngine creates a new incremental backup engine.
func NewIncrementalBackupEngine(db *DB, cfg IncrementalBackupConfig) *IncrementalBackupEngine {
	return &IncrementalBackupEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

func (e *IncrementalBackupEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

func (e *IncrementalBackupEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

func (e *IncrementalBackupEngine) genID() string {
	e.nextID++
	return fmt.Sprintf("bk-%d", e.nextID)
}

func (e *IncrementalBackupEngine) computeChecksum(id string, size int64) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", id, size)))
	return fmt.Sprintf("%x", h[:8])
}

// CreateFull creates a full backup manifest.
func (e *IncrementalBackupEngine) CreateFull() (*IncrBackupManifest, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.chains) >= e.config.MaxChains {
		// evict oldest chain
		e.chains = e.chains[1:]
	}

	id := e.genID()
	size := int64(1024 * 1024) // simulated 1MB
	m := IncrBackupManifest{
		ID:             id,
		Type:           "full",
		CreatedAt:      time.Now(),
		SizeBytes:      size,
		PartitionCount: 1,
		Checksum:       e.computeChecksum(id, size),
		Status:         "completed",
	}
	e.backups = append(e.backups, m)

	chain := IncrBackupChain{
		ChainID:        fmt.Sprintf("chain-%s", id),
		FullBackupID:   id,
		TotalSizeBytes: size,
		CreatedAt:      time.Now(),
	}
	e.chains = append(e.chains, chain)

	e.stats.TotalBackups++
	e.stats.FullBackups++
	e.stats.TotalSizeBytes += size
	e.stats.LastBackupAt = m.CreatedAt

	return &m, nil
}

// CreateIncremental creates an incremental backup from a parent.
func (e *IncrementalBackupEngine) CreateIncremental(parentID string) (*IncrBackupManifest, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// find parent
	found := false
	for _, b := range e.backups {
		if b.ID == parentID {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("parent backup %s not found", parentID)
	}

	id := e.genID()
	size := int64(256 * 1024) // simulated 256KB delta
	m := IncrBackupManifest{
		ID:             id,
		Type:           "incremental",
		ParentID:       parentID,
		CreatedAt:      time.Now(),
		SizeBytes:      size,
		PartitionCount: 1,
		Checksum:       e.computeChecksum(id, size),
		Status:         "completed",
	}
	e.backups = append(e.backups, m)

	// add to chain
	for i := range e.chains {
		if e.chains[i].FullBackupID == parentID || e.containsInChain(&e.chains[i], parentID) {
			e.chains[i].Incrementals = append(e.chains[i].Incrementals, id)
			e.chains[i].TotalSizeBytes += size
			break
		}
	}

	e.stats.TotalBackups++
	e.stats.IncrementalBackups++
	e.stats.TotalSizeBytes += size
	e.stats.LastBackupAt = m.CreatedAt

	return &m, nil
}

func (e *IncrementalBackupEngine) containsInChain(chain *IncrBackupChain, id string) bool {
	for _, inc := range chain.Incrementals {
		if inc == id {
			return true
		}
	}
	return false
}

// ListBackups returns all backup manifests.
func (e *IncrementalBackupEngine) ListBackups() []IncrBackupManifest {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]IncrBackupManifest, len(e.backups))
	copy(result, e.backups)
	return result
}

// GetChain returns a backup chain by ID.
func (e *IncrementalBackupEngine) GetChain(chainID string) *IncrBackupChain {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, c := range e.chains {
		if c.ChainID == chainID {
			ch := c
			return &ch
		}
	}
	return nil
}

// Verify checks the checksum of a backup.
func (e *IncrementalBackupEngine) Verify(backupID string) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, b := range e.backups {
		if b.ID == backupID {
			expected := e.computeChecksum(b.ID, b.SizeBytes)
			if b.Checksum == expected {
				e.backups[i].Status = "verified"
				return true, nil
			}
			return false, nil
		}
	}
	return false, fmt.Errorf("backup %s not found", backupID)
}

// Restore simulates restoring from a backup.
func (e *IncrementalBackupEngine) Restore(backupID string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, b := range e.backups {
		if b.ID == backupID {
			if b.Status == "failed" {
				return fmt.Errorf("cannot restore from failed backup %s", backupID)
			}
			return nil
		}
	}
	return fmt.Errorf("backup %s not found", backupID)
}

// GetStats returns engine statistics.
func (e *IncrementalBackupEngine) GetStats() IncrementalBackupStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *IncrementalBackupEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/incremental-backup/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListBackups())
	})
	mux.HandleFunc("/api/v1/incremental-backup/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
