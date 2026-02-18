package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// SQLPipelineConfig configures the SQL-first declarative pipelines engine.
type SQLPipelineConfig struct {
	Enabled            bool          `json:"enabled"`
	MaxConcurrentPipes int           `json:"max_concurrent_pipes"`
	CheckpointInterval time.Duration `json:"checkpoint_interval"`
	BackfillRateLimit  int           `json:"backfill_rate_limit"`
	EnableExactlyOnce  bool          `json:"enable_exactly_once"`
	MaxPipelines       int           `json:"max_pipelines"`
	DefaultBatchSize   int           `json:"default_batch_size"`
}

// DefaultSQLPipelineConfig returns sensible defaults.
func DefaultSQLPipelineConfig() SQLPipelineConfig {
	return SQLPipelineConfig{
		Enabled:            true,
		MaxConcurrentPipes: 10,
		CheckpointInterval: 30 * time.Second,
		BackfillRateLimit:  10000,
		EnableExactlyOnce:  true,
		MaxPipelines:       100,
		DefaultBatchSize:   1000,
	}
}

// PipelineState represents the lifecycle state of a pipeline.
type SQLPipelineState string

const (
	SQLPipelineCreated  SQLPipelineState = "created"
	SQLPipelineRunning  SQLPipelineState = "running"
	SQLPipelinePaused   SQLPipelineState = "paused"
	SQLPipelineStopped  SQLPipelineState = "stopped"
	SQLPipelineFailed   SQLPipelineState = "failed"
	SQLPipelineBackfill SQLPipelineState = "backfilling"
)

// SQLPipelineDefinition defines a declarative SQL pipeline.
type SQLPipelineDefinition struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	Description    string            `json:"description"`
	SourceQuery    string            `json:"source_query"`
	TransformSQL   string            `json:"transform_sql"`
	DestMetric     string            `json:"dest_metric"`
	Schedule       string            `json:"schedule"` // cron expression or "continuous"
	WindowSize     time.Duration     `json:"window_size"`
	WatermarkDelay time.Duration     `json:"watermark_delay"`
	EmitMode       PipelineEmitMode  `json:"emit_mode"`
	Tags           map[string]string `json:"tags,omitempty"`
	BatchSize      int               `json:"batch_size"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
}

// PipelineEmitMode controls when results are emitted.
type PipelineEmitMode string

const (
	EmitOnWatermark PipelineEmitMode = "on_watermark"
	EmitOnComplete  PipelineEmitMode = "on_complete"
	EmitPeriodic    PipelineEmitMode = "periodic"
)

// PipelineCheckpoint records processing progress.
type PipelineCheckpoint struct {
	PipelineID    string    `json:"pipeline_id"`
	LastProcessed time.Time `json:"last_processed"`
	RowsProcessed int64     `json:"rows_processed"`
	RowsEmitted   int64     `json:"rows_emitted"`
	Watermark     time.Time `json:"watermark"`
	CheckpointAt  time.Time `json:"checkpoint_at"`
	State         []byte    `json:"state,omitempty"`
}

// PipelineMetrics tracks pipeline execution metrics.
type PipelineMetrics struct {
	PipelineID       string           `json:"pipeline_id"`
	State            SQLPipelineState `json:"state"`
	TotalRuns        int64            `json:"total_runs"`
	SuccessfulRuns   int64            `json:"successful_runs"`
	FailedRuns       int64            `json:"failed_runs"`
	RowsIn           int64            `json:"rows_in"`
	RowsOut          int64            `json:"rows_out"`
	LastRunDuration  time.Duration    `json:"last_run_duration"`
	AvgRunDuration   time.Duration    `json:"avg_run_duration"`
	LastRunAt        time.Time        `json:"last_run_at"`
	LastError        string           `json:"last_error,omitempty"`
	BackfillProgress float64          `json:"backfill_progress"`
}

// SQLPipelineStats contains global pipeline engine statistics.
type SQLPipelineStats struct {
	TotalPipelines   int   `json:"total_pipelines"`
	RunningPipelines int   `json:"running_pipelines"`
	TotalRuns        int64 `json:"total_runs"`
	TotalRowsIn      int64 `json:"total_rows_in"`
	TotalRowsOut     int64 `json:"total_rows_out"`
	TotalErrors      int64 `json:"total_errors"`
}

// SQLPipelineEngine manages declarative SQL-based ETL pipelines.
type SQLPipelineEngine struct {
	db     *DB
	config SQLPipelineConfig

	pipelines map[string]*sqlPipelineInstance
	sem       chan struct{}
	stopCh    chan struct{}

	mu sync.RWMutex
}

type sqlPipelineInstance struct {
	def        SQLPipelineDefinition
	state      SQLPipelineState
	checkpoint PipelineCheckpoint
	metrics    PipelineMetrics
	stopCh     chan struct{}
	totalRunNs int64
}

// NewSQLPipelineEngine creates a new SQL pipeline engine.
func NewSQLPipelineEngine(db *DB, cfg SQLPipelineConfig) *SQLPipelineEngine {
	return &SQLPipelineEngine{
		db:        db,
		config:    cfg,
		pipelines: make(map[string]*sqlPipelineInstance),
		sem:       make(chan struct{}, cfg.MaxConcurrentPipes),
		stopCh:    make(chan struct{}),
	}
}

// CreatePipeline registers a new SQL pipeline.
func (spe *SQLPipelineEngine) CreatePipeline(def SQLPipelineDefinition) error {
	spe.mu.Lock()
	defer spe.mu.Unlock()

	if def.ID == "" {
		return fmt.Errorf("sql_pipeline: pipeline ID is required")
	}
	if def.TransformSQL == "" {
		return fmt.Errorf("sql_pipeline: transform SQL is required")
	}
	if len(spe.pipelines) >= spe.config.MaxPipelines {
		return fmt.Errorf("sql_pipeline: max pipelines (%d) reached", spe.config.MaxPipelines)
	}

	def.CreatedAt = time.Now()
	def.UpdatedAt = time.Now()
	if def.BatchSize <= 0 {
		def.BatchSize = spe.config.DefaultBatchSize
	}

	spe.pipelines[def.ID] = &sqlPipelineInstance{
		def:    def,
		state:  SQLPipelineCreated,
		stopCh: make(chan struct{}),
		metrics: PipelineMetrics{
			PipelineID: def.ID,
			State:      SQLPipelineCreated,
		},
		checkpoint: PipelineCheckpoint{
			PipelineID: def.ID,
		},
	}
	return nil
}

// GetPipeline returns a pipeline definition by ID.
func (spe *SQLPipelineEngine) GetPipeline(id string) *SQLPipelineDefinition {
	spe.mu.RLock()
	defer spe.mu.RUnlock()

	if inst, ok := spe.pipelines[id]; ok {
		cp := inst.def
		return &cp
	}
	return nil
}

// ListPipelines returns all registered pipelines.
func (spe *SQLPipelineEngine) ListPipelines() []SQLPipelineDefinition {
	spe.mu.RLock()
	defer spe.mu.RUnlock()

	result := make([]SQLPipelineDefinition, 0, len(spe.pipelines))
	for _, inst := range spe.pipelines {
		result = append(result, inst.def)
	}
	return result
}

// StartPipeline starts a pipeline's continuous execution.
func (spe *SQLPipelineEngine) StartPipeline(id string) error {
	spe.mu.Lock()
	defer spe.mu.Unlock()

	inst, ok := spe.pipelines[id]
	if !ok {
		return fmt.Errorf("sql_pipeline: pipeline %q not found", id)
	}
	if inst.state == SQLPipelineRunning {
		return fmt.Errorf("sql_pipeline: pipeline %q already running", id)
	}

	inst.state = SQLPipelineRunning
	inst.metrics.State = SQLPipelineRunning
	inst.stopCh = make(chan struct{})

	go spe.runPipeline(inst)
	return nil
}

// StopPipeline stops a running pipeline.
func (spe *SQLPipelineEngine) StopPipeline(id string) error {
	spe.mu.Lock()
	defer spe.mu.Unlock()

	inst, ok := spe.pipelines[id]
	if !ok {
		return fmt.Errorf("sql_pipeline: pipeline %q not found", id)
	}
	if inst.state != SQLPipelineRunning && inst.state != SQLPipelineBackfill {
		return fmt.Errorf("sql_pipeline: pipeline %q not running", id)
	}

	close(inst.stopCh)
	inst.state = SQLPipelineStopped
	inst.metrics.State = SQLPipelineStopped
	return nil
}

// PausePipeline pauses a running pipeline.
func (spe *SQLPipelineEngine) PausePipeline(id string) error {
	spe.mu.Lock()
	defer spe.mu.Unlock()

	inst, ok := spe.pipelines[id]
	if !ok {
		return fmt.Errorf("sql_pipeline: pipeline %q not found", id)
	}
	if inst.state != SQLPipelineRunning {
		return fmt.Errorf("sql_pipeline: pipeline %q not running", id)
	}

	close(inst.stopCh)
	inst.state = SQLPipelinePaused
	inst.metrics.State = SQLPipelinePaused
	return nil
}

// DeletePipeline removes a pipeline.
func (spe *SQLPipelineEngine) DeletePipeline(id string) error {
	spe.mu.Lock()
	defer spe.mu.Unlock()

	inst, ok := spe.pipelines[id]
	if !ok {
		return fmt.Errorf("sql_pipeline: pipeline %q not found", id)
	}
	if inst.state == SQLPipelineRunning {
		close(inst.stopCh)
	}
	delete(spe.pipelines, id)
	return nil
}

// GetMetrics returns metrics for a specific pipeline.
func (spe *SQLPipelineEngine) GetMetrics(id string) *PipelineMetrics {
	spe.mu.RLock()
	defer spe.mu.RUnlock()

	if inst, ok := spe.pipelines[id]; ok {
		cp := inst.metrics
		return &cp
	}
	return nil
}

// GetCheckpoint returns the latest checkpoint for a pipeline.
func (spe *SQLPipelineEngine) GetCheckpoint(id string) *PipelineCheckpoint {
	spe.mu.RLock()
	defer spe.mu.RUnlock()

	if inst, ok := spe.pipelines[id]; ok {
		cp := inst.checkpoint
		return &cp
	}
	return nil
}

// ExecuteOnce runs a pipeline once (for batch/manual execution).
func (spe *SQLPipelineEngine) ExecuteOnce(ctx context.Context, id string) (*PipelineMetrics, error) {
	spe.mu.RLock()
	inst, ok := spe.pipelines[id]
	spe.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("sql_pipeline: pipeline %q not found", id)
	}

	spe.sem <- struct{}{}
	defer func() { <-spe.sem }()

	start := time.Now()
	rowsIn, rowsOut, err := spe.executePipelineStep(ctx, inst)

	spe.mu.Lock()
	inst.metrics.TotalRuns++
	inst.metrics.RowsIn += rowsIn
	inst.metrics.RowsOut += rowsOut
	inst.metrics.LastRunAt = time.Now()
	inst.metrics.LastRunDuration = time.Since(start)
	inst.totalRunNs += int64(inst.metrics.LastRunDuration)
	if inst.metrics.TotalRuns > 0 {
		inst.metrics.AvgRunDuration = time.Duration(inst.totalRunNs / inst.metrics.TotalRuns)
	}
	if err != nil {
		inst.metrics.FailedRuns++
		inst.metrics.LastError = err.Error()
	} else {
		inst.metrics.SuccessfulRuns++
		inst.metrics.LastError = ""
	}
	result := inst.metrics
	spe.mu.Unlock()

	return &result, err
}

// Backfill runs a pipeline over historical data.
func (spe *SQLPipelineEngine) Backfill(ctx context.Context, id string, start, end time.Time) error {
	spe.mu.Lock()
	inst, ok := spe.pipelines[id]
	if !ok {
		spe.mu.Unlock()
		return fmt.Errorf("sql_pipeline: pipeline %q not found", id)
	}
	inst.state = SQLPipelineBackfill
	inst.metrics.State = SQLPipelineBackfill
	inst.metrics.BackfillProgress = 0
	spe.mu.Unlock()

	totalDuration := end.Sub(start)
	windowSize := inst.def.WindowSize
	if windowSize <= 0 {
		windowSize = time.Hour
	}

	current := start
	for current.Before(end) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		windowEnd := current.Add(windowSize)
		if windowEnd.After(end) {
			windowEnd = end
		}

		// Execute the pipeline step for this window
		spe.executePipelineStep(ctx, inst)

		spe.mu.Lock()
		progress := float64(current.Sub(start)) / float64(totalDuration) * 100
		inst.metrics.BackfillProgress = progress
		inst.checkpoint.LastProcessed = windowEnd
		inst.checkpoint.CheckpointAt = time.Now()
		spe.mu.Unlock()

		current = windowEnd
	}

	spe.mu.Lock()
	inst.state = SQLPipelineStopped
	inst.metrics.State = SQLPipelineStopped
	inst.metrics.BackfillProgress = 100
	spe.mu.Unlock()

	return nil
}

// Stats returns global pipeline engine statistics.
func (spe *SQLPipelineEngine) Stats() SQLPipelineStats {
	spe.mu.RLock()
	defer spe.mu.RUnlock()

	stats := SQLPipelineStats{
		TotalPipelines: len(spe.pipelines),
	}
	for _, inst := range spe.pipelines {
		if inst.state == SQLPipelineRunning {
			stats.RunningPipelines++
		}
		stats.TotalRuns += inst.metrics.TotalRuns
		stats.TotalRowsIn += inst.metrics.RowsIn
		stats.TotalRowsOut += inst.metrics.RowsOut
		stats.TotalErrors += inst.metrics.FailedRuns
	}
	return stats
}

// Stop stops all pipelines.
func (spe *SQLPipelineEngine) Stop() {
	close(spe.stopCh)
	spe.mu.Lock()
	defer spe.mu.Unlock()
	for _, inst := range spe.pipelines {
		if inst.state == SQLPipelineRunning {
			close(inst.stopCh)
			inst.state = SQLPipelineStopped
			inst.metrics.State = SQLPipelineStopped
		}
	}
}

func (spe *SQLPipelineEngine) runPipeline(inst *sqlPipelineInstance) {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			spe.sem <- struct{}{}
			_, _, err := spe.executePipelineStep(ctx, inst)
			<-spe.sem
			cancel()

			spe.mu.Lock()
			inst.metrics.TotalRuns++
			atomic.AddInt64(&inst.metrics.TotalRuns, 0) // memory barrier
			if err != nil {
				inst.metrics.FailedRuns++
				inst.metrics.LastError = err.Error()
			} else {
				inst.metrics.SuccessfulRuns++
			}
			inst.metrics.LastRunAt = time.Now()
			spe.mu.Unlock()
		case <-inst.stopCh:
			return
		case <-spe.stopCh:
			return
		}
	}
}

func (spe *SQLPipelineEngine) executePipelineStep(ctx context.Context, inst *sqlPipelineInstance) (int64, int64, error) {
	// Simulated execution: in production, this would parse and execute the SQL transform
	// against the source data and write results to the destination metric
	var rowsIn, rowsOut int64

	if spe.db != nil && inst.def.SourceQuery != "" {
		// Query source data via Execute
		q := &Query{Metric: inst.def.SourceQuery}
		if inst.def.WindowSize > 0 {
			q.Start = time.Now().Add(-inst.def.WindowSize).UnixNano()
			q.End = time.Now().UnixNano()
		}
		result, err := spe.db.Execute(q)
		if err != nil {
			return 0, 0, fmt.Errorf("source query failed: %w", err)
		}
		rowsIn = int64(len(result.Points))

		// Apply transforms and write results
		for _, p := range result.Points {
			if inst.def.DestMetric != "" {
				p.Metric = inst.def.DestMetric
			}
			if err := spe.db.Write(p); err != nil {
				return rowsIn, rowsOut, fmt.Errorf("write failed: %w", err)
			}
			rowsOut++
		}
	}

	spe.mu.Lock()
	inst.checkpoint.RowsProcessed += rowsIn
	inst.checkpoint.RowsEmitted += rowsOut
	inst.checkpoint.CheckpointAt = time.Now()
	inst.checkpoint.LastProcessed = time.Now()
	spe.mu.Unlock()

	return rowsIn, rowsOut, nil
}

// RegisterHTTPHandlers registers SQL pipeline HTTP endpoints.
func (spe *SQLPipelineEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/pipelines", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(spe.ListPipelines())
		case http.MethodPost:
			var def SQLPipelineDefinition
			if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			if err := spe.CreatePipeline(def); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(spe.GetPipeline(def.ID))
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/pipelines/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(spe.Stats())
	})
}
