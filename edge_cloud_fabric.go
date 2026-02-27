package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// EdgeCloudFabricConfig configures the edge-to-cloud data fabric.
type EdgeCloudFabricConfig struct {
	Enabled            bool
	SyncInterval       time.Duration
	MaxBandwidthMBps   int
	RetryBackoff       time.Duration
	MaxRetries         int
	ConflictResolution string // "lww" (last writer wins), "crdt", "manual"
	CompressionEnabled bool
	BatchSize          int
	ResumableUploads   bool
}

// DefaultEdgeCloudFabricConfig returns sensible defaults.
func DefaultEdgeCloudFabricConfig() EdgeCloudFabricConfig {
	return EdgeCloudFabricConfig{
		Enabled:            true,
		SyncInterval:       30 * time.Second,
		MaxBandwidthMBps:   10,
		RetryBackoff:       5 * time.Second,
		MaxRetries:         5,
		ConflictResolution: "lww",
		CompressionEnabled: true,
		BatchSize:          10000,
		ResumableUploads:   true,
	}
}

// SyncDirection represents the direction of sync.
type SyncDirection string

const (
	SyncDirectionUpload   SyncDirection = "upload"
	SyncDirectionDownload SyncDirection = "download"
	SyncDirectionBidir    SyncDirection = "bidirectional"
)

// FabricEndpoint represents a cloud storage endpoint.
type FabricEndpoint struct {
	ID       string            `json:"id"`
	Type     string            `json:"type"` // s3, gcs, azure, chronicle-cloud
	Region   string            `json:"region"`
	Bucket   string            `json:"bucket"`
	Prefix   string            `json:"prefix"`
	Config   map[string]string `json:"config,omitempty"`
	Enabled  bool              `json:"enabled"`
	AddedAt  time.Time         `json:"added_at"`
}

// SyncJob represents a sync operation.
type SyncJob struct {
	ID              string        `json:"id"`
	EndpointID      string        `json:"endpoint_id"`
	Direction       SyncDirection `json:"direction"`
	Status          string        `json:"status"` // pending, running, completed, failed, paused
	PointsSynced    int64         `json:"points_synced"`
	BytesSynced     int64         `json:"bytes_synced"`
	PointsTotal     int64         `json:"points_total"`
	StartedAt       time.Time     `json:"started_at"`
	CompletedAt     *time.Time    `json:"completed_at,omitempty"`
	Error           string        `json:"error,omitempty"`
	RetryCount      int           `json:"retry_count"`
	ResumeToken     string        `json:"resume_token,omitempty"`
	Checksum        string        `json:"checksum,omitempty"`
}

// SyncConflict represents a data conflict during sync.
type SyncConflict struct {
	ID         string    `json:"id"`
	Metric     string    `json:"metric"`
	Timestamp  int64     `json:"timestamp"`
	LocalValue float64   `json:"local_value"`
	RemoteValue float64  `json:"remote_value"`
	ResolvedBy string    `json:"resolved_by"`
	ResolvedAt time.Time `json:"resolved_at"`
}

// SyncPolicy defines per-metric sync policies.
type SyncPolicy struct {
	MetricPattern string        `json:"metric_pattern"` // glob pattern
	Direction     SyncDirection `json:"direction"`
	Priority      int           `json:"priority"` // 1-10
	MaxAge        time.Duration `json:"max_age"`
	SampleRate    float64       `json:"sample_rate"` // 0.0-1.0
}

// EdgeCloudFabricStats holds sync engine statistics.
type EdgeCloudFabricStats struct {
	Endpoints        int           `json:"endpoints"`
	ActiveJobs       int           `json:"active_jobs"`
	CompletedJobs    int64         `json:"completed_jobs"`
	FailedJobs       int64         `json:"failed_jobs"`
	TotalPointsSynced int64        `json:"total_points_synced"`
	TotalBytesSynced  int64        `json:"total_bytes_synced"`
	ConflictsResolved int64        `json:"conflicts_resolved"`
	AvgSyncLatency    time.Duration `json:"avg_sync_latency"`
}

// EdgeCloudFabricEngine provides bidirectional sync between edge and cloud.
type EdgeCloudFabricEngine struct {
	db     *DB
	config EdgeCloudFabricConfig

	mu        sync.RWMutex
	endpoints map[string]*FabricEndpoint
	jobs      []*SyncJob
	conflicts []SyncConflict
	policies  []SyncPolicy
	running   bool
	stopCh    chan struct{}
	stats     EdgeCloudFabricStats
	jobSeq    int64
}

// NewEdgeCloudFabricEngine creates a new edge-to-cloud data fabric.
func NewEdgeCloudFabricEngine(db *DB, cfg EdgeCloudFabricConfig) *EdgeCloudFabricEngine {
	return &EdgeCloudFabricEngine{
		db:        db,
		config:    cfg,
		endpoints: make(map[string]*FabricEndpoint),
		jobs:      make([]*SyncJob, 0),
		conflicts: make([]SyncConflict, 0),
		policies:  make([]SyncPolicy, 0),
		stopCh:    make(chan struct{}),
	}
}

// Start starts the sync engine.
func (e *EdgeCloudFabricEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
	go e.syncLoop()
}

// Stop stops the sync engine.
func (e *EdgeCloudFabricEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// AddEndpoint registers a cloud storage endpoint.
func (e *EdgeCloudFabricEngine) AddEndpoint(ep FabricEndpoint) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if ep.ID == "" {
		return fmt.Errorf("endpoint ID required")
	}
	if _, exists := e.endpoints[ep.ID]; exists {
		return fmt.Errorf("endpoint %q already exists", ep.ID)
	}

	ep.AddedAt = time.Now()
	ep.Enabled = true
	e.endpoints[ep.ID] = &ep
	e.stats.Endpoints = len(e.endpoints)
	return nil
}

// RemoveEndpoint removes a cloud endpoint.
func (e *EdgeCloudFabricEngine) RemoveEndpoint(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.endpoints[id]; !exists {
		return fmt.Errorf("endpoint %q not found", id)
	}
	delete(e.endpoints, id)
	e.stats.Endpoints = len(e.endpoints)
	return nil
}

// ListEndpoints returns all registered endpoints.
func (e *EdgeCloudFabricEngine) ListEndpoints() []FabricEndpoint {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]FabricEndpoint, 0, len(e.endpoints))
	for _, ep := range e.endpoints {
		result = append(result, *ep)
	}
	return result
}

// TriggerSync initiates a manual sync job.
func (e *EdgeCloudFabricEngine) TriggerSync(endpointID string, direction SyncDirection) (*SyncJob, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	ep, exists := e.endpoints[endpointID]
	if !exists {
		return nil, fmt.Errorf("endpoint %q not found", endpointID)
	}
	if !ep.Enabled {
		return nil, fmt.Errorf("endpoint %q is disabled", endpointID)
	}

	e.jobSeq++
	job := &SyncJob{
		ID:          fmt.Sprintf("sync-%d", e.jobSeq),
		EndpointID:  endpointID,
		Direction:   direction,
		Status:      "pending",
		StartedAt:   time.Now(),
		PointsTotal: 0,
	}

	e.jobs = append(e.jobs, job)
	e.stats.ActiveJobs++

	// Simulate sync execution
	go e.executeJob(job)

	return job, nil
}

// GetJob returns a sync job by ID.
func (e *EdgeCloudFabricEngine) GetJob(id string) *SyncJob {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, j := range e.jobs {
		if j.ID == id {
			cp := *j
			return &cp
		}
	}
	return nil
}

// ListJobs returns all sync jobs.
func (e *EdgeCloudFabricEngine) ListJobs(status string) []SyncJob {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]SyncJob, 0, len(e.jobs))
	for _, j := range e.jobs {
		if status == "" || j.Status == status {
			result = append(result, *j)
		}
	}
	return result
}

// AddPolicy adds a sync policy.
func (e *EdgeCloudFabricEngine) AddPolicy(policy SyncPolicy) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policies = append(e.policies, policy)
}

// ListPolicies returns all sync policies.
func (e *EdgeCloudFabricEngine) ListPolicies() []SyncPolicy {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]SyncPolicy, len(e.policies))
	copy(result, e.policies)
	return result
}

// ListConflicts returns resolved conflicts.
func (e *EdgeCloudFabricEngine) ListConflicts() []SyncConflict {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]SyncConflict, len(e.conflicts))
	copy(result, e.conflicts)
	return result
}

// GetStats returns engine statistics.
func (e *EdgeCloudFabricEngine) GetStats() EdgeCloudFabricStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

func (e *EdgeCloudFabricEngine) executeJob(job *SyncJob) {
	e.mu.Lock()
	job.Status = "running"
	e.mu.Unlock()

	// Simulate syncing batches of points
	batchCount := 3
	for i := 0; i < batchCount; i++ {
		select {
		case <-e.stopCh:
			e.mu.Lock()
			job.Status = "paused"
			e.stats.ActiveJobs--
			e.mu.Unlock()
			return
		default:
		}

		pointsBatch := int64(e.config.BatchSize)
		bytesBatch := pointsBatch * 24 // ~24 bytes per point

		e.mu.Lock()
		job.PointsSynced += pointsBatch
		job.BytesSynced += bytesBatch
		e.stats.TotalPointsSynced += pointsBatch
		e.stats.TotalBytesSynced += bytesBatch
		e.mu.Unlock()
	}

	e.mu.Lock()
	now := time.Now()
	job.Status = "completed"
	job.CompletedAt = &now
	e.stats.ActiveJobs--
	e.stats.CompletedJobs++
	e.mu.Unlock()
}

func (e *EdgeCloudFabricEngine) syncLoop() {
	ticker := time.NewTicker(e.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			// Auto-sync enabled endpoints
			e.mu.RLock()
			for _, ep := range e.endpoints {
				if ep.Enabled {
					_ = ep // In production, trigger auto-sync
				}
			}
			e.mu.RUnlock()
		}
	}
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *EdgeCloudFabricEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/fabric/endpoints", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e.ListEndpoints())
		case http.MethodPost:
			var ep FabricEndpoint
			r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
			if err := json.NewDecoder(r.Body).Decode(&ep); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if err := e.AddEndpoint(ep); err != nil {
				http.Error(w, "conflict", http.StatusConflict)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "added"})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/fabric/sync", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			EndpointID string        `json:"endpoint_id"`
			Direction  SyncDirection `json:"direction"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		job, err := e.TriggerSync(req.EndpointID, req.Direction)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(job)
	})

	mux.HandleFunc("/api/v1/fabric/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		status := r.URL.Query().Get("status")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListJobs(status))
	})

	mux.HandleFunc("/api/v1/fabric/conflicts", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListConflicts())
	})

	mux.HandleFunc("/api/v1/fabric/policies", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e.ListPolicies())
		case http.MethodPost:
			var policy SyncPolicy
			r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
			if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			e.AddPolicy(policy)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "added"})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/fabric/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
