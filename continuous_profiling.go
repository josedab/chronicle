package chronicle

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

// PyroscopeIngestRequest represents a Pyroscope push protocol request.
type PyroscopeIngestRequest struct {
	Name            string            `json:"name"`
	SpyName         string            `json:"spyName"`
	SampleRate      int               `json:"sampleRate"`
	Units           string            `json:"units"`
	AggregationType string            `json:"aggregationType"`
	From            int64             `json:"from"`
	Until           int64             `json:"until"`
	Labels          map[string]string `json:"labels,omitempty"`
	RawProfile      []byte            `json:"profile"`
}

// PyroscopeQueryResponse represents a Pyroscope query protocol response.
type PyroscopeQueryResponse struct {
	Name         string                 `json:"name"`
	Metadata     map[string]interface{} `json:"metadata"`
	FlameBearer  *FlameBearer           `json:"flamebearer"`
	Timeline     *ProfileTimeline       `json:"timeline,omitempty"`
}

// FlameBearer represents flame graph data in Pyroscope format.
type FlameBearer struct {
	Names    []string    `json:"names"`
	Levels   [][]int     `json:"levels"`
	NumTicks int64       `json:"numTicks"`
	MaxSelf  int64       `json:"maxSelf"`
	Format   string      `json:"format"`
}

// ProfileTimeline represents a timeline of profile samples.
type ProfileTimeline struct {
	StartTime  int64   `json:"startTime"`
	Samples    []int64 `json:"samples"`
	DurationMs int64   `json:"durationDelta"`
}

// FlameGraphDiff represents differences between two flame graphs.
type FlameGraphDiff struct {
	Baseline  *FlameBearer `json:"baseline"`
	Comparison *FlameBearer `json:"comparison"`
	Added     []string     `json:"added_functions"`
	Removed   []string     `json:"removed_functions"`
	Changed   []FlameGraphDiffEntry `json:"changed"`
}

// FlameGraphDiffEntry represents a changed function between two profiles.
type FlameGraphDiffEntry struct {
	Function     string  `json:"function"`
	BaselineSelf int64   `json:"baseline_self"`
	CompSelf     int64   `json:"comparison_self"`
	DiffPct      float64 `json:"diff_pct"`
}

// ContinuousProfilingConfig configures the continuous profiling integration.
type ContinuousProfilingConfig struct {
	Enabled             bool          `json:"enabled"`
	RetentionDuration   time.Duration `json:"retention_duration"`
	MaxProfileSize      int64         `json:"max_profile_size"`
	PyroscopeCompat     bool          `json:"pyroscope_compat"`
	AutoCorrelate       bool          `json:"auto_correlate"`
	CorrelationWindowMs int64         `json:"correlation_window_ms"`
}

// DefaultContinuousProfilingConfig returns sensible defaults.
func DefaultContinuousProfilingConfig() ContinuousProfilingConfig {
	return ContinuousProfilingConfig{
		Enabled:             true,
		RetentionDuration:   7 * 24 * time.Hour,
		MaxProfileSize:      50 * 1024 * 1024,
		PyroscopeCompat:     true,
		AutoCorrelate:       true,
		CorrelationWindowMs: 60000,
	}
}

// ContinuousProfilingEngine extends ProfileStore with Pyroscope compatibility
// and temporal correlation features.
type ContinuousProfilingEngine struct {
	store  *ProfileStore
	db     *DB
	config ContinuousProfilingConfig
	mu     sync.RWMutex

	// Index: service+profile_type -> timestamps for quick lookup
	timeIndex map[string][]int64

	// Stats
	totalIngested   int64
	totalQueried    int64
	pyroscopePushes int64
}

// NewContinuousProfilingEngine creates a new continuous profiling engine.
func NewContinuousProfilingEngine(db *DB, store *ProfileStore, config ContinuousProfilingConfig) *ContinuousProfilingEngine {
	return &ContinuousProfilingEngine{
		store:     store,
		db:        db,
		config:    config,
		timeIndex: make(map[string][]int64),
	}
}

// IngestProfile ingests a pprof-format profile.
func (e *ContinuousProfilingEngine) IngestProfile(p Profile) error {
	if len(p.Data) == 0 {
		return fmt.Errorf("continuous_profiling: empty profile data")
	}
	if int64(len(p.Data)) > e.config.MaxProfileSize {
		return fmt.Errorf("continuous_profiling: profile too large (%d > %d)", len(p.Data), e.config.MaxProfileSize)
	}

	if err := e.store.Write(p); err != nil {
		return fmt.Errorf("continuous_profiling: write failed: %w", err)
	}

	e.mu.Lock()
	e.totalIngested++
	// Update time index
	key := profileIndexKey(p.Labels["service"], p.Type)
	e.timeIndex[key] = append(e.timeIndex[key], p.Timestamp)
	// Cap index size
	if len(e.timeIndex[key]) > 100000 {
		e.timeIndex[key] = e.timeIndex[key][len(e.timeIndex[key])-100000:]
	}
	e.mu.Unlock()

	return nil
}

func profileIndexKey(service string, profileType ProfileType) string {
	return fmt.Sprintf("%s:%d", service, profileType)
}

// IngestPyroscope processes a Pyroscope-format push request.
func (e *ContinuousProfilingEngine) IngestPyroscope(req PyroscopeIngestRequest) error {
	if !e.config.PyroscopeCompat {
		return fmt.Errorf("continuous_profiling: pyroscope compatibility disabled")
	}

	profileType := pyroscopeToProfileType(req.SpyName)
	labels := req.Labels
	if labels == nil {
		labels = make(map[string]string)
	}
	labels["__name__"] = req.Name
	labels["spy_name"] = req.SpyName

	p := Profile{
		Type:        profileType,
		Timestamp:   req.From * 1e9, // seconds to nanos
		Duration:    time.Duration(req.Until-req.From) * time.Second,
		Labels:      labels,
		Data:        req.RawProfile,
		SampleCount: int64(req.SampleRate),
	}

	e.mu.Lock()
	e.pyroscopePushes++
	e.mu.Unlock()

	return e.IngestProfile(p)
}

func pyroscopeToProfileType(spyName string) ProfileType {
	switch spyName {
	case "gospy", "ebpfspy":
		return ProfileTypeCPU
	case "rbspy", "pyspy":
		return ProfileTypeCPU
	default:
		return ProfileTypeCPU
	}
}

// QueryProfiles queries profiles with temporal correlation support.
func (e *ContinuousProfilingEngine) QueryProfiles(
	profileType ProfileType,
	labels map[string]string,
	start, end int64,
) ([]Profile, error) {
	e.mu.Lock()
	e.totalQueried++
	e.mu.Unlock()

	return e.store.Query(profileType, labels, start, end)
}

// CorrelateWithMetric finds profiles near timestamps where a metric exceeds a threshold.
// Example: "show CPU profile when p99_latency > 500ms"
func (e *ContinuousProfilingEngine) CorrelateWithMetric(
	metric string,
	threshold float64,
	profileType ProfileType,
	start, end int64,
) ([]ProfileMetricCorrelation, error) {
	result, err := e.db.Execute(&Query{
		Metric: metric,
		Start:  start,
		End:    end,
	})
	if err != nil {
		return nil, fmt.Errorf("continuous_profiling: metric query failed: %w", err)
	}

	windowNs := e.config.CorrelationWindowMs * int64(time.Millisecond)

	var correlations []ProfileMetricCorrelation

	for _, point := range result.Points {
		if point.Value < threshold {
			continue
		}

		profiles, err := e.store.Query(profileType, nil, point.Timestamp-windowNs, point.Timestamp+windowNs)
		if err != nil {
			continue
		}

		for _, p := range profiles {
			correlations = append(correlations, ProfileMetricCorrelation{
				MetricName:      metric,
				MetricValue:     point.Value,
				MetricTimestamp:  point.Timestamp,
				ProfileID:       p.ID,
				ProfileType:     p.Type,
				ProfileTimestamp: p.Timestamp,
				TimeDiffMs:      (p.Timestamp - point.Timestamp) / int64(time.Millisecond),
			})
		}
	}

	// Sort by time difference (closest first)
	sort.Slice(correlations, func(i, j int) bool {
		return profAbs64(correlations[i].TimeDiffMs) < profAbs64(correlations[j].TimeDiffMs)
	})

	return correlations, nil
}

func profAbs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// ProfileMetricCorrelation links a profile to a metric data point.
type ProfileMetricCorrelation struct {
	MetricName      string      `json:"metric_name"`
	MetricValue     float64     `json:"metric_value"`
	MetricTimestamp  int64       `json:"metric_timestamp"`
	ProfileID       string      `json:"profile_id"`
	ProfileType     ProfileType `json:"profile_type"`
	ProfileTimestamp int64       `json:"profile_timestamp"`
	TimeDiffMs      int64       `json:"time_diff_ms"`
}

// ComputeFlameGraphDiff computes the diff between two flame graphs.
func (e *ContinuousProfilingEngine) ComputeFlameGraphDiff(baseline, comparison *FlameBearer) *FlameGraphDiff {
	if baseline == nil || comparison == nil {
		return nil
	}

	baseNames := make(map[string]bool)
	for _, n := range baseline.Names {
		baseNames[n] = true
	}
	compNames := make(map[string]bool)
	for _, n := range comparison.Names {
		compNames[n] = true
	}

	var added, removed []string
	for _, n := range comparison.Names {
		if !baseNames[n] {
			added = append(added, n)
		}
	}
	for _, n := range baseline.Names {
		if !compNames[n] {
			removed = append(removed, n)
		}
	}

	return &FlameGraphDiff{
		Baseline:   baseline,
		Comparison: comparison,
		Added:      added,
		Removed:    removed,
	}
}

// GetStats returns profiling engine statistics.
func (e *ContinuousProfilingEngine) GetStats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return map[string]interface{}{
		"total_ingested":    e.totalIngested,
		"total_queried":     e.totalQueried,
		"pyroscope_pushes":  e.pyroscopePushes,
		"indexed_series":    len(e.timeIndex),
		"pyroscope_compat":  e.config.PyroscopeCompat,
	}
}

// RegisterHTTPHandlers registers profiling HTTP endpoints.
func (e *ContinuousProfilingEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	// Ingest endpoint: accepts pprof-format profiles
	mux.HandleFunc("/api/v1/profiles", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var p Profile
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, "invalid profile", http.StatusBadRequest)
			return
		}
		if err := e.IngestProfile(p); err != nil {
			log.Printf("profile ingest error: %v", err)
			http.Error(w, "failed to ingest profile", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})

	// Pyroscope push compatibility
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req PyroscopeIngestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := e.IngestPyroscope(req); err != nil {
			log.Printf("pyroscope ingest error: %v", err)
			http.Error(w, "failed to ingest profile", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	// Flame graph diff endpoint
	mux.HandleFunc("/api/v1/profiles/diff", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Baseline   *FlameBearer `json:"baseline"`
			Comparison *FlameBearer `json:"comparison"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		diff := e.ComputeFlameGraphDiff(req.Baseline, req.Comparison)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(diff)
	})

	// Stats endpoint
	mux.HandleFunc("/api/v1/profiles/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
