package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Pipeline Manager — orchestrates multiple ETL pipelines
// ---------------------------------------------------------------------------

// ETLPipelineManagerConfig configures the pipeline manager.
type ETLPipelineManagerConfig struct {
	MaxPipelines     int           `json:"max_pipelines" yaml:"max_pipelines"`
	HealthInterval   time.Duration `json:"health_interval" yaml:"health_interval"`
	MetricsRetention time.Duration `json:"metrics_retention" yaml:"metrics_retention"`
}

// DefaultETLPipelineManagerConfig returns sensible defaults.
func DefaultETLPipelineManagerConfig() ETLPipelineManagerConfig {
	return ETLPipelineManagerConfig{
		MaxPipelines:     64,
		HealthInterval:   10 * time.Second,
		MetricsRetention: 24 * time.Hour,
	}
}

// PipelineState represents the lifecycle state of a managed pipeline.
type PipelineState string

const (
	PipelineStateCreated  PipelineState = "created"
	PipelineStateRunning  PipelineState = "running"
	PipelineStateStopped  PipelineState = "stopped"
	PipelineStateFailed   PipelineState = "failed"
	PipelineStateDraining PipelineState = "draining"
)

// ManagedPipeline wraps an ETLPipeline with management metadata.
type ManagedPipeline struct {
	Name      string            `json:"name"`
	State     PipelineState     `json:"state"`
	Pipeline  *ETLPipeline      `json:"-"`
	Spec      *PipelineSpec     `json:"spec,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	StartedAt *time.Time        `json:"started_at,omitempty"`
	StoppedAt *time.Time        `json:"stopped_at,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Error     string            `json:"error,omitempty"`
	mu        sync.RWMutex
}

// ETLPipelineManager manages the lifecycle of multiple ETL pipelines.
type ETLPipelineManager struct {
	db        *DB
	config    ETLPipelineManagerConfig
	pipelines map[string]*ManagedPipeline
	mu        sync.RWMutex
	running   atomic.Bool
	cancel    context.CancelFunc

	// stats
	totalCreated atomic.Uint64
	totalStarted atomic.Uint64
	totalStopped atomic.Uint64
	totalFailed  atomic.Uint64
}

// NewETLPipelineManager creates a new pipeline manager.
func NewETLPipelineManager(db *DB, config ETLPipelineManagerConfig) *ETLPipelineManager {
	return &ETLPipelineManager{
		db:        db,
		config:    config,
		pipelines: make(map[string]*ManagedPipeline),
	}
}

// Start begins background health monitoring.
func (pm *ETLPipelineManager) Start() error {
	if pm.running.Swap(true) {
		return fmt.Errorf("pipeline manager already running")
	}
	ctx, cancel := context.WithCancel(context.Background())
	pm.cancel = cancel
	go pm.healthLoop(ctx)
	return nil
}

// Stop halts all pipelines and the manager.
func (pm *ETLPipelineManager) Stop() error {
	if !pm.running.Swap(false) {
		return nil
	}
	if pm.cancel != nil {
		pm.cancel()
	}
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for _, mp := range pm.pipelines {
		mp.mu.Lock()
		if mp.State == PipelineStateRunning {
			if mp.Pipeline != nil {
				stopQuietly(mp.Pipeline)
			}
			mp.State = PipelineStateStopped
			now := time.Now()
			mp.StoppedAt = &now
		}
		mp.mu.Unlock()
	}
	return nil
}

// CreatePipeline registers a new pipeline.
func (pm *ETLPipelineManager) CreatePipeline(name string, pipeline *ETLPipeline, labels map[string]string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if len(pm.pipelines) >= pm.config.MaxPipelines {
		return fmt.Errorf("max pipelines (%d) reached", pm.config.MaxPipelines)
	}
	if _, exists := pm.pipelines[name]; exists {
		return fmt.Errorf("pipeline %q already exists", name)
	}
	pm.pipelines[name] = &ManagedPipeline{
		Name:      name,
		State:     PipelineStateCreated,
		Pipeline:  pipeline,
		CreatedAt: time.Now(),
		Labels:    labels,
	}
	pm.totalCreated.Add(1)
	return nil
}

// StartPipeline starts a named pipeline.
func (pm *ETLPipelineManager) StartPipeline(name string) error {
	pm.mu.RLock()
	mp, ok := pm.pipelines[name]
	pm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("pipeline %q not found", name)
	}
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if mp.State == PipelineStateRunning {
		return fmt.Errorf("pipeline %q already running", name)
	}
	if mp.Pipeline != nil {
		if err := mp.Pipeline.Start(); err != nil {
			mp.State = PipelineStateFailed
			mp.Error = err.Error()
			pm.totalFailed.Add(1)
			return err
		}
	}
	mp.State = PipelineStateRunning
	now := time.Now()
	mp.StartedAt = &now
	mp.Error = ""
	pm.totalStarted.Add(1)
	return nil
}

// StopPipeline stops a named pipeline.
func (pm *ETLPipelineManager) StopPipeline(name string) error {
	pm.mu.RLock()
	mp, ok := pm.pipelines[name]
	pm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("pipeline %q not found", name)
	}
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if mp.State != PipelineStateRunning {
		return fmt.Errorf("pipeline %q not running", name)
	}
	if mp.Pipeline != nil {
		stopQuietly(mp.Pipeline)
	}
	mp.State = PipelineStateStopped
	now := time.Now()
	mp.StoppedAt = &now
	pm.totalStopped.Add(1)
	return nil
}

// DeletePipeline removes a pipeline (must be stopped).
func (pm *ETLPipelineManager) DeletePipeline(name string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	mp, ok := pm.pipelines[name]
	if !ok {
		return fmt.Errorf("pipeline %q not found", name)
	}
	mp.mu.RLock()
	state := mp.State
	mp.mu.RUnlock()
	if state == PipelineStateRunning {
		return fmt.Errorf("cannot delete running pipeline %q", name)
	}
	delete(pm.pipelines, name)
	return nil
}

// ListPipelines returns all managed pipelines.
func (pm *ETLPipelineManager) ListPipelines() []*ManagedPipeline {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	result := make([]*ManagedPipeline, 0, len(pm.pipelines))
	for _, mp := range pm.pipelines {
		result = append(result, mp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// GetPipeline returns a specific pipeline by name.
func (pm *ETLPipelineManager) GetPipeline(name string) (*ManagedPipeline, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	mp, ok := pm.pipelines[name]
	if !ok {
		return nil, fmt.Errorf("pipeline %q not found", name)
	}
	return mp, nil
}

// ETLPipelineManagerStats holds aggregate manager stats.
type ETLPipelineManagerStats struct {
	TotalPipelines int    `json:"total_pipelines"`
	Running        int    `json:"running"`
	Stopped        int    `json:"stopped"`
	Failed         int    `json:"failed"`
	TotalCreated   uint64 `json:"total_created"`
	TotalStarted   uint64 `json:"total_started"`
	TotalStopped   uint64 `json:"total_stopped"`
	TotalFailed    uint64 `json:"total_failed"`
}

// Stats returns aggregate statistics.
func (pm *ETLPipelineManager) Stats() ETLPipelineManagerStats {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	stats := ETLPipelineManagerStats{
		TotalPipelines: len(pm.pipelines),
		TotalCreated:   pm.totalCreated.Load(),
		TotalStarted:   pm.totalStarted.Load(),
		TotalStopped:   pm.totalStopped.Load(),
		TotalFailed:    pm.totalFailed.Load(),
	}
	for _, mp := range pm.pipelines {
		mp.mu.RLock()
		switch mp.State {
		case PipelineStateRunning:
			stats.Running++
		case PipelineStateStopped:
			stats.Stopped++
		case PipelineStateFailed:
			stats.Failed++
		}
		mp.mu.RUnlock()
	}
	return stats
}

func (pm *ETLPipelineManager) healthLoop(ctx context.Context) {
	ticker := time.NewTicker(pm.config.HealthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pm.checkHealth()
		}
	}
}

func (pm *ETLPipelineManager) checkHealth() {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for _, mp := range pm.pipelines {
		mp.mu.Lock()
		if mp.State == PipelineStateRunning && mp.Pipeline != nil {
			stats := mp.Pipeline.Stats()
			if stats != nil && stats.PointsErrored > 1000 {
				mp.State = PipelineStateFailed
				mp.Error = "excessive errors detected"
				pm.totalFailed.Add(1)
			}
		}
		mp.mu.Unlock()
	}
}

// RegisterHTTPHandlers registers pipeline manager HTTP endpoints.
func (pm *ETLPipelineManager) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/etl/pipelines", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			pipelines := pm.ListPipelines()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(pipelines)
		case http.MethodPost:
			var req struct {
				Name   string            `json:"name"`
				Labels map[string]string `json:"labels"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			pipeline := NewETLPipeline(DefaultETLPipelineConfig())
			if err := pm.CreatePipeline(req.Name, pipeline, req.Labels); err != nil {
				http.Error(w, "conflict", http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusCreated)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/etl/pipelines/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := pm.Stats()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	})
}

// ---------------------------------------------------------------------------
// Windowed Join — time-based join of two point streams
// ---------------------------------------------------------------------------

// WindowedJoinConfig configures a windowed join.
type WindowedJoinConfig struct {
	LeftMetric   string           `json:"left_metric" yaml:"left_metric"`
	RightMetric  string           `json:"right_metric" yaml:"right_metric"`
	WindowSize   time.Duration    `json:"window_size" yaml:"window_size"`
	OutputMetric string           `json:"output_metric" yaml:"output_metric"`
	JoinType     WindowedJoinType `json:"join_type" yaml:"join_type"`
}

// WindowedJoinType specifies the type of join for windowed operations.
type WindowedJoinType int

const (
	WindowedJoinInner WindowedJoinType = iota
	WindowedJoinLeftOuter
	WindowedJoinRightOuter
	WindowedJoinFullOuter
)

// WindowedJoin performs time-aligned joins between two point streams.
type WindowedJoin struct {
	config    WindowedJoinConfig
	leftBuf   []*Point
	rightBuf  []*Point
	mu        sync.Mutex
	processed atomic.Uint64
	emitted   atomic.Uint64
}

// NewWindowedJoin creates a windowed join operator.
func NewWindowedJoin(config WindowedJoinConfig) *WindowedJoin {
	if config.WindowSize == 0 {
		config.WindowSize = time.Minute
	}
	if config.OutputMetric == "" {
		config.OutputMetric = config.LeftMetric + "_joined_" + config.RightMetric
	}
	return &WindowedJoin{
		config:   config,
		leftBuf:  make([]*Point, 0, 256),
		rightBuf: make([]*Point, 0, 256),
	}
}

// AddLeft adds a point from the left stream.
func (wj *WindowedJoin) AddLeft(p *Point) {
	wj.mu.Lock()
	defer wj.mu.Unlock()
	wj.leftBuf = append(wj.leftBuf, p)
}

// AddRight adds a point from the right stream.
func (wj *WindowedJoin) AddRight(p *Point) {
	wj.mu.Lock()
	defer wj.mu.Unlock()
	wj.rightBuf = append(wj.rightBuf, p)
}

// Emit produces joined points within the window, then flushes matched entries.
