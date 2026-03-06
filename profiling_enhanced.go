package chronicle

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// --- Profile Types ---
// ProfileType and its constants (ProfileTypeCPU, etc.) are defined in profile.go.

// parseProfileType converts a string to ProfileType.
func parseProfileType(s string) ProfileType {
	switch s {
	case "cpu":
		return ProfileTypeCPU
	case "memory", "heap":
		return ProfileTypeHeap
	case "goroutine":
		return ProfileTypeGoroutine
	case "block":
		return ProfileTypeBlock
	case "mutex":
		return ProfileTypeMutex
	case "allocs":
		return ProfileTypeAllocs
	default:
		return ProfileTypeCPU
	}
}

// ProfileSample represents a single profile sample with stack trace.
type ProfileSample struct {
	StackTrace  []string          `json:"stack_trace"`
	Value       int64             `json:"value"`
	Labels      map[string]string `json:"labels,omitempty"`
	SpanID      string            `json:"span_id,omitempty"`
	TraceID     string            `json:"trace_id,omitempty"`
	Timestamp   int64             `json:"timestamp"`
}

// StoredProfile represents a profile stored in dedicated partitions.
type StoredProfile struct {
	ID          string            `json:"id"`
	Service     string            `json:"service"`
	ProfileType ProfileType       `json:"profile_type"`
	Samples     []ProfileSample   `json:"samples"`
	Duration    time.Duration     `json:"duration"`
	StartTime   int64             `json:"start_time"`
	EndTime     int64             `json:"end_time"`
	Labels      map[string]string `json:"labels,omitempty"`
	SizeBytes   int64             `json:"size_bytes"`
}

// ProfilePartitionStore stores profiles in dedicated partitions.
type ProfilePartitionStore struct {
	profiles  map[string]*StoredProfile // keyed by ID
	byService map[string][]string       // service -> profile IDs
	byType    map[ProfileType][]string  // type -> profile IDs
	bySpan    map[string][]string       // span_id -> profile IDs
	timeIndex []profileTimeEntry
	mu        sync.RWMutex
	config    ProfilePartitionConfig
}

type profileTimeEntry struct {
	startTime int64
	profileID string
}

// ProfilePartitionConfig configures the profile partition store.
type ProfilePartitionConfig struct {
	MaxProfiles       int           `json:"max_profiles"`
	RetentionPeriod   time.Duration `json:"retention_period"`
	MaxProfileSize    int64         `json:"max_profile_size"` // bytes
	EnableSpanLinking bool          `json:"enable_span_linking"`
}

// DefaultProfilePartitionConfig returns sensible defaults.
func DefaultProfilePartitionConfig() ProfilePartitionConfig {
	return ProfilePartitionConfig{
		MaxProfiles:       10000,
		RetentionPeriod:   7 * 24 * time.Hour,
		MaxProfileSize:    10 * 1024 * 1024, // 10 MB
		EnableSpanLinking: true,
	}
}

// NewProfilePartitionStore creates a new profile partition store.
func NewProfilePartitionStore(config ProfilePartitionConfig) *ProfilePartitionStore {
	return &ProfilePartitionStore{
		profiles:  make(map[string]*StoredProfile),
		byService: make(map[string][]string),
		byType:    make(map[ProfileType][]string),
		bySpan:    make(map[string][]string),
		config:    config,
	}
}

// Store adds a profile to the partition store.
func (s *ProfilePartitionStore) Store(profile StoredProfile) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.profiles) >= s.config.MaxProfiles {
		// Evict oldest profile
		if len(s.timeIndex) > 0 {
			oldest := s.timeIndex[0]
			delete(s.profiles, oldest.profileID)
			s.timeIndex = s.timeIndex[1:]
		}
	}

	if profile.ID == "" {
		profile.ID = fmt.Sprintf("prof-%d", time.Now().UnixNano())
	}

	s.profiles[profile.ID] = &profile
	s.byService[profile.Service] = append(s.byService[profile.Service], profile.ID)
	s.byType[profile.ProfileType] = append(s.byType[profile.ProfileType], profile.ID)

	// Index span links
	if s.config.EnableSpanLinking {
		for _, sample := range profile.Samples {
			if sample.SpanID != "" {
				s.bySpan[sample.SpanID] = append(s.bySpan[sample.SpanID], profile.ID)
			}
		}
	}

	s.timeIndex = append(s.timeIndex, profileTimeEntry{
		startTime: profile.StartTime,
		profileID: profile.ID,
	})

	return nil
}

// QueryByService returns profiles for a service within a time range.
func (s *ProfilePartitionStore) QueryByService(service string, profileType ProfileType, startTime, endTime int64) []*StoredProfile {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids, ok := s.byService[service]
	if !ok {
		return nil
	}

	var result []*StoredProfile
	for _, id := range ids {
		p := s.profiles[id]
		if p == nil {
			continue
		}
		if p.ProfileType != profileType {
			continue
		}
		if p.StartTime >= startTime && p.StartTime <= endTime {
			result = append(result, p)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].StartTime < result[j].StartTime
	})
	return result
}

// QueryBySpan returns profiles linked to a specific trace span.
func (s *ProfilePartitionStore) QueryBySpan(spanID string) []*StoredProfile {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids, ok := s.bySpan[spanID]
	if !ok {
		return nil
	}

	var result []*StoredProfile
	for _, id := range ids {
		if p := s.profiles[id]; p != nil {
			result = append(result, p)
		}
	}
	return result
}

// Stats returns partition store statistics.
func (s *ProfilePartitionStore) Stats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	serviceCount := len(s.byService)
	spanLinks := 0
	for _, ids := range s.bySpan {
		spanLinks += len(ids)
	}

	typeCounts := make(map[string]int)
	for pt, ids := range s.byType {
		typeCounts[fmt.Sprintf("%d", pt)] = len(ids)
	}

	return map[string]interface{}{
		"total_profiles": len(s.profiles),
		"services":       serviceCount,
		"span_links":     spanLinks,
		"by_type":        typeCounts,
	}
}

// --- pprof Binary Ingestion ---

// PprofIngestHandler provides HTTP handler for pprof binary ingestion.
type PprofIngestHandler struct {
	store  *ProfilePartitionStore
	mu     sync.RWMutex
	stats  PprofIngestStats
}

// PprofIngestStats tracks pprof ingestion statistics.
type PprofIngestStats struct {
	TotalIngested   int64 `json:"total_ingested"`
	TotalBytes      int64 `json:"total_bytes"`
	TotalErrors     int64 `json:"total_errors"`
	AvgSizeBytes    int64 `json:"avg_size_bytes"`
}

// NewPprofIngestHandler creates a new pprof ingestion handler.
func NewPprofIngestHandler(store *ProfilePartitionStore) *PprofIngestHandler {
	return &PprofIngestHandler{store: store}
}

// RegisterHTTPHandlers registers pprof ingestion HTTP endpoints.
func (h *PprofIngestHandler) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/profiles/ingest", h.handleIngest)
	mux.HandleFunc("/api/v1/profiles/query", h.handleQuery)
	mux.HandleFunc("/api/v1/profiles/span", h.handleSpanProfiles)
	mux.HandleFunc("/api/v1/profiles/stats", h.handleStats)
}

func (h *PprofIngestHandler) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read raw profile data
	body, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	// Extract metadata from headers/query params
	service := r.URL.Query().Get("service")
	if service == "" {
		service = r.Header.Get("X-Service-Name")
	}
	if service == "" {
		service = "unknown"
	}

	profileType := parseProfileType(r.URL.Query().Get("type"))

	spanID := r.URL.Query().Get("span_id")
	traceID := r.URL.Query().Get("trace_id")

	now := time.Now().UnixNano()
	profile := StoredProfile{
		Service:     service,
		ProfileType: profileType,
		StartTime:   now,
		EndTime:     now,
		SizeBytes:   int64(len(body)),
		Labels:      map[string]string{"format": "pprof"},
	}

	// Store profile samples (in production, would parse pprof protobuf)
	if spanID != "" || traceID != "" {
		profile.Samples = []ProfileSample{
			{
				StackTrace: []string{"(pprof binary data)"},
				Value:      int64(len(body)),
				SpanID:     spanID,
				TraceID:    traceID,
				Timestamp:  now,
			},
		}
	}

	if err := h.store.Store(profile); err != nil {
		h.mu.Lock()
		h.stats.TotalErrors++
		h.mu.Unlock()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.mu.Lock()
	h.stats.TotalIngested++
	h.stats.TotalBytes += int64(len(body))
	if h.stats.TotalIngested > 0 {
		h.stats.AvgSizeBytes = h.stats.TotalBytes / h.stats.TotalIngested
	}
	h.mu.Unlock()

	w.WriteHeader(http.StatusAccepted)
}

func (h *PprofIngestHandler) handleQuery(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	profileType := parseProfileType(r.URL.Query().Get("type"))

	// Default to last hour
	endTime := time.Now().UnixNano()
	startTime := endTime - int64(time.Hour)

	profiles := h.store.QueryByService(service, profileType, startTime, endTime)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(profiles)
}

func (h *PprofIngestHandler) handleSpanProfiles(w http.ResponseWriter, r *http.Request) {
	spanID := r.URL.Query().Get("span_id")
	if spanID == "" {
		http.Error(w, "span_id required", http.StatusBadRequest)
		return
	}

	profiles := h.store.QueryBySpan(spanID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(profiles)
}

func (h *PprofIngestHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	stats := h.stats
	h.mu.RUnlock()

	storeStats := h.store.Stats()

	response := map[string]interface{}{
		"ingest": stats,
		"store":  storeStats,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// --- Metric-to-Profile Correlation ---

// MetricProfileCorrelation links metric spikes to top functions in profiles.
type MetricProfileCorrelation struct {
	MetricName    string          `json:"metric_name"`
	SpikeTime     int64           `json:"spike_time"`
	SpikeValue    float64         `json:"spike_value"`
	TopFunctions  []FunctionUsage `json:"top_functions"`
	ProfileID     string          `json:"profile_id"`
	Service       string          `json:"service"`
}

// FunctionUsage describes a function's resource usage in a profile.
type FunctionUsage struct {
	Function  string  `json:"function"`
	Self      int64   `json:"self"`
	Total     int64   `json:"total"`
	Percentage float64 `json:"percentage"`
}

// CorrelateMetricToProfiles finds profiles that overlap with a metric spike.
func (s *ProfilePartitionStore) CorrelateMetricToProfiles(service string, spikeTime int64, windowNs int64) []*StoredProfile {
	s.mu.RLock()
	defer s.mu.RUnlock()

	startTime := spikeTime - windowNs
	endTime := spikeTime + windowNs

	var result []*StoredProfile
	for _, p := range s.profiles {
		if p.Service != service {
			continue
		}
		if p.StartTime <= endTime && p.EndTime >= startTime {
			result = append(result, p)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		// Sort by proximity to spike time
		di := profileAbs64(result[i].StartTime - spikeTime)
		dj := profileAbs64(result[j].StartTime - spikeTime)
		return di < dj
	})

	return result
}

func profileAbs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// --- Span-to-Profile Linking ---

// SpanProfileLink links a trace span to its associated profile data.
type SpanProfileLink struct {
	SpanID      string          `json:"span_id"`
	TraceID     string          `json:"trace_id"`
	Service     string          `json:"service"`
	Profiles    []*StoredProfile `json:"profiles"`
	FlameGraph  *FlameBearer    `json:"flame_graph,omitempty"`
}

// GetSpanProfile retrieves profiles linked to a span and generates a flame graph.
func (s *ProfilePartitionStore) GetSpanProfile(spanID string) *SpanProfileLink {
	profiles := s.QueryBySpan(spanID)
	if len(profiles) == 0 {
		return nil
	}

	link := &SpanProfileLink{
		SpanID:   spanID,
		Service:  profiles[0].Service,
		Profiles: profiles,
	}

	// Extract trace ID from first profile's samples
	for _, sample := range profiles[0].Samples {
		if sample.TraceID != "" {
			link.TraceID = sample.TraceID
			break
		}
	}

	// Build a flame graph from the profiles
	link.FlameGraph = buildFlameGraphFromProfiles(profiles)

	return link
}

func buildFlameGraphFromProfiles(profiles []*StoredProfile) *FlameBearer {
	nameSet := make(map[string]bool)
	var names []string

	for _, p := range profiles {
		for _, s := range p.Samples {
			for _, fn := range s.StackTrace {
				if !nameSet[fn] {
					nameSet[fn] = true
					names = append(names, fn)
				}
			}
		}
	}

	if len(names) == 0 {
		return nil
	}

	return &FlameBearer{
		Names:    names,
		Levels:   [][]int{{0, len(names), 0, 0}},
		NumTicks: int64(len(names)),
		MaxSelf:  1,
		Format:   "single",
	}
}

// --- Signal Correlation Integration ---

// ProfileCorrelationResult connects a metric anomaly to related profiles and top functions.
type ProfileCorrelationResult struct {
	Metric        string          `json:"metric"`
	SpikeTime     int64           `json:"spike_time"`
	Service       string          `json:"service"`
	Profiles      []*StoredProfile `json:"profiles"`
	TopFunctions  []string        `json:"top_functions"`
	FlameGraph    *FlameBearer    `json:"flame_graph,omitempty"`
	LinkedSpans   []string        `json:"linked_spans,omitempty"`
}

// CorrelateMetricSpikeToProfiles finds profiles correlated with a metric spike
// and extracts top functions for drill-down analysis.
func (s *ProfilePartitionStore) CorrelateMetricSpikeToProfiles(
	service string,
	spikeTimeNs int64,
	profileType ProfileType,
	windowNs int64,
) *ProfileCorrelationResult {
	startTime := spikeTimeNs - windowNs
	endTime := spikeTimeNs + windowNs

	profiles := s.QueryByService(service, profileType, startTime, endTime)
	if len(profiles) == 0 {
		return nil
	}

	result := &ProfileCorrelationResult{
		Service:  service,
		SpikeTime: spikeTimeNs,
		Profiles: profiles,
	}

	// Extract top functions from profiles
	funcCounts := make(map[string]int64)
	spanSet := make(map[string]bool)

	for _, p := range profiles {
		for _, sample := range p.Samples {
			for _, fn := range sample.StackTrace {
				funcCounts[fn] += sample.Value
			}
			if sample.SpanID != "" {
				spanSet[sample.SpanID] = true
			}
		}
	}

	// Sort functions by value
	type funcEntry struct {
		name  string
		value int64
	}
	var entries []funcEntry
	for name, value := range funcCounts {
		entries = append(entries, funcEntry{name, value})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].value > entries[j].value
	})

	topN := 10
	if len(entries) < topN {
		topN = len(entries)
	}
	for _, e := range entries[:topN] {
		result.TopFunctions = append(result.TopFunctions, e.name)
	}

	for span := range spanSet {
		result.LinkedSpans = append(result.LinkedSpans, span)
	}

	result.FlameGraph = buildFlameGraphFromProfiles(profiles)

	return result
}
