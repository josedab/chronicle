package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// SmartCompactionConfig configures the smart compaction engine.
type SmartCompactionConfig struct {
	Enabled          bool
	IOBudgetMBps     int
	ColdThreshold    time.Duration
	MinPartitionSize int64
	MaxPartitionSize int64
	CompactInterval  time.Duration
}

// DefaultSmartCompactionConfig returns sensible defaults.
func DefaultSmartCompactionConfig() SmartCompactionConfig {
	return SmartCompactionConfig{
		Enabled:          true,
		IOBudgetMBps:     50,
		ColdThreshold:    24 * time.Hour,
		MinPartitionSize: 1024 * 1024,       // 1MB
		MaxPartitionSize: 256 * 1024 * 1024, // 256MB
		CompactInterval:  5 * time.Minute,
	}
}

// CompactionCandidate represents a partition eligible for compaction.
type CompactionCandidate struct {
	PartitionID  string    `json:"partition_id"`
	SizeBytes    int64     `json:"size_bytes"`
	PointCount   int64     `json:"point_count"`
	LastAccess   time.Time `json:"last_access"`
	AccessCount  int64     `json:"access_count"`
	Temperature  string    `json:"temperature"` // hot, warm, cold, frozen
	Priority     float64   `json:"priority"`
}

// CompactionJob represents a compaction operation.
type CompactionJob struct {
	ID             string    `json:"id"`
	Candidates     []string  `json:"candidates"`
	Status         string    `json:"status"` // pending, running, completed, failed
	InputBytes     int64     `json:"input_bytes"`
	OutputBytes    int64     `json:"output_bytes"`
	CompressionRatio float64 `json:"compression_ratio"`
	StartedAt      time.Time `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	Duration       time.Duration `json:"duration"`
}

// SmartCompactionStats holds engine statistics.
type SmartCompactionStats struct {
	TotalCompactions int64   `json:"total_compactions"`
	BytesReclaimed   int64   `json:"bytes_reclaimed"`
	AvgCompression   float64 `json:"avg_compression_ratio"`
	HotPartitions    int     `json:"hot_partitions"`
	ColdPartitions   int     `json:"cold_partitions"`
	PendingJobs      int     `json:"pending_jobs"`
}

// SmartCompactionEngine provides access-pattern-aware compaction.
type SmartCompactionEngine struct {
	db     *DB
	config SmartCompactionConfig
	mu     sync.RWMutex
	candidates []CompactionCandidate
	jobs       []CompactionJob
	accessLog  map[string]int64 // partition -> access count
	running    bool
	stopCh     chan struct{}
	stats      SmartCompactionStats
	jobSeq     int64
}

// NewSmartCompactionEngine creates a new smart compaction engine.
func NewSmartCompactionEngine(db *DB, cfg SmartCompactionConfig) *SmartCompactionEngine {
	return &SmartCompactionEngine{
		db:        db,
		config:    cfg,
		candidates: make([]CompactionCandidate, 0),
		jobs:      make([]CompactionJob, 0),
		accessLog: make(map[string]int64),
		stopCh:    make(chan struct{}),
	}
}

func (e *SmartCompactionEngine) Start() {
	e.mu.Lock()
	if e.running { e.mu.Unlock(); return }
	e.running = true
	e.mu.Unlock()
	go e.compactLoop()
}

func (e *SmartCompactionEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running { return }
	e.running = false
	close(e.stopCh)
}

// RecordAccess records a partition access for temperature tracking.
func (e *SmartCompactionEngine) RecordAccess(partitionID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.accessLog[partitionID]++
}

// AddCandidate adds a partition as a compaction candidate.
func (e *SmartCompactionEngine) AddCandidate(c CompactionCandidate) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	c.AccessCount = e.accessLog[c.PartitionID]
	if now.Sub(c.LastAccess) > e.config.ColdThreshold {
		c.Temperature = "cold"
	} else if c.AccessCount > 100 {
		c.Temperature = "hot"
	} else {
		c.Temperature = "warm"
	}
	// Priority: cold + small partitions are best candidates
	c.Priority = float64(e.config.ColdThreshold.Hours()) / (float64(now.Sub(c.LastAccess).Hours()) + 1)
	if c.SizeBytes < e.config.MinPartitionSize {
		c.Priority *= 2 // Small partitions get higher priority
	}

	e.candidates = append(e.candidates, c)
}

// TriggerCompaction initiates a compaction job for the given partitions.
func (e *SmartCompactionEngine) TriggerCompaction(partitionIDs []string) (*CompactionJob, error) {
	if len(partitionIDs) == 0 {
		return nil, fmt.Errorf("no partitions specified")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.jobSeq++
	var inputBytes int64
	for _, c := range e.candidates {
		for _, pid := range partitionIDs {
			if c.PartitionID == pid {
				inputBytes += c.SizeBytes
			}
		}
	}

	job := CompactionJob{
		ID:         fmt.Sprintf("compact-%d", e.jobSeq),
		Candidates: partitionIDs,
		Status:     "running",
		InputBytes: inputBytes,
		StartedAt:  time.Now(),
	}

	// Execute real compaction if DB is available
	var compactErr error
	if e.db != nil {
		compactErr = e.db.Compact()
	}

	now := time.Now()
	job.CompletedAt = &now
	job.Duration = time.Since(job.StartedAt)

	if compactErr != nil {
		job.Status = "failed"
		e.jobs = append(e.jobs, job)
		return &job, compactErr
	}

	// Measure actual output size
	job.Status = "completed"
	if e.db != nil && e.db.path != "" {
		if info, err := os.Stat(e.db.path); err == nil {
			job.OutputBytes = info.Size()
		}
	}
	if job.OutputBytes == 0 && inputBytes > 0 {
		job.OutputBytes = inputBytes // fallback: no size change measured
	}
	if inputBytes > 0 {
		job.CompressionRatio = float64(job.OutputBytes) / float64(inputBytes)
	}

	e.jobs = append(e.jobs, job)
	e.stats.TotalCompactions++
	if inputBytes > job.OutputBytes {
		e.stats.BytesReclaimed += inputBytes - job.OutputBytes
	}
	if e.stats.AvgCompression == 0 {
		e.stats.AvgCompression = job.CompressionRatio
	} else {
		e.stats.AvgCompression = (e.stats.AvgCompression + job.CompressionRatio) / 2
	}

	return &job, nil
}

// ListCandidates returns current compaction candidates.
func (e *SmartCompactionEngine) ListCandidates() []CompactionCandidate {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]CompactionCandidate, len(e.candidates))
	copy(result, e.candidates)
	return result
}

// ListJobs returns compaction jobs.
func (e *SmartCompactionEngine) ListJobs() []CompactionJob {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]CompactionJob, len(e.jobs))
	copy(result, e.jobs)
	return result
}

// GetStats returns engine stats.
func (e *SmartCompactionEngine) GetStats() SmartCompactionStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats := e.stats
	for _, c := range e.candidates {
		switch c.Temperature {
		case "hot": stats.HotPartitions++
		case "cold": stats.ColdPartitions++
		}
	}
	return stats
}

func (e *SmartCompactionEngine) compactLoop() {
	ticker := time.NewTicker(e.config.CompactInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.autoCompactCold()
		}
	}
}

// autoCompactCold triggers compaction for cold partitions with low I/O impact.
func (e *SmartCompactionEngine) autoCompactCold() {
	e.mu.RLock()
	var coldIDs []string
	for _, c := range e.candidates {
		if c.Temperature == "cold" {
			coldIDs = append(coldIDs, c.PartitionID)
		}
	}
	e.mu.RUnlock()

	if len(coldIDs) > 0 && e.db != nil {
		e.TriggerCompaction(coldIDs)
	}
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *SmartCompactionEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/compaction/candidates", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListCandidates())
	})
	mux.HandleFunc("/api/v1/compaction/jobs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListJobs())
	})
	mux.HandleFunc("/api/v1/compaction/trigger", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		var req struct { PartitionIDs []string `json:"partition_ids"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { http.Error(w, "bad request", http.StatusBadRequest); return }
		job, err := e.TriggerCompaction(req.PartitionIDs)
		if err != nil { http.Error(w, "bad request", http.StatusBadRequest); return }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(job)
	})
	mux.HandleFunc("/api/v1/compaction/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
