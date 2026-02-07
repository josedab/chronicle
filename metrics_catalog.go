package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// MetricsCatalogConfig configures the embedded metrics catalog.
type MetricsCatalogConfig struct {
	Enabled                bool          `json:"enabled"`
	ScanInterval           time.Duration `json:"scan_interval"`
	MaxMetrics             int           `json:"max_metrics"`
	TrackLineage           bool          `json:"track_lineage"`
	TrackQueryUsage        bool          `json:"track_query_usage"`
	DeprecationGracePeriod time.Duration `json:"deprecation_grace_period"`
}

// DefaultMetricsCatalogConfig returns sensible defaults.
func DefaultMetricsCatalogConfig() MetricsCatalogConfig {
	return MetricsCatalogConfig{
		Enabled:                true,
		ScanInterval:           time.Hour,
		MaxMetrics:             10000,
		TrackLineage:           true,
		TrackQueryUsage:        true,
		DeprecationGracePeriod: 7 * 24 * time.Hour,
	}
}

// CatalogMetricType identifies the type of a metric (gauge, counter, etc.).
type CatalogMetricType int

const (
	CatalogMetricTypeUnknown   CatalogMetricType = iota
	CatalogMetricTypeGauge                       // values go up and down
	CatalogMetricTypeCounter                     // monotonically increasing
	CatalogMetricTypeHistogram                   // distribution of values
	CatalogMetricTypeSummary                     // pre-computed quantiles
)

// String returns the string representation of a CatalogMetricType.
func (t CatalogMetricType) String() string {
	switch t {
	case CatalogMetricTypeGauge:
		return "gauge"
	case CatalogMetricTypeCounter:
		return "counter"
	case CatalogMetricTypeHistogram:
		return "histogram"
	case CatalogMetricTypeSummary:
		return "summary"
	default:
		return "unknown"
	}
}

// CatalogMetricStatus represents the lifecycle status of a metric.
type CatalogMetricStatus int

const (
	CatalogMetricStatusActive     CatalogMetricStatus = iota
	CatalogMetricStatusInactive                       // no recent data
	CatalogMetricStatusDeprecated                     // scheduled for removal
	CatalogMetricStatusArchived                       // no longer in use
)

// String returns the string representation of a CatalogMetricStatus.
func (s CatalogMetricStatus) String() string {
	switch s {
	case CatalogMetricStatusActive:
		return "active"
	case CatalogMetricStatusInactive:
		return "inactive"
	case CatalogMetricStatusDeprecated:
		return "deprecated"
	case CatalogMetricStatusArchived:
		return "archived"
	default:
		return "unknown"
	}
}

// CatalogEntry represents a discovered or registered metric in the catalog.
type CatalogEntry struct {
	Name               string              `json:"name"`
	Description        string              `json:"description,omitempty"`
	Type               CatalogMetricType   `json:"type"`
	Unit               string              `json:"unit,omitempty"`
	Tags               []CatalogTagInfo    `json:"tags,omitempty"`
	FirstSeen          time.Time           `json:"first_seen"`
	LastSeen           time.Time           `json:"last_seen"`
	PointCount         int64               `json:"point_count"`
	Cardinality        int                 `json:"cardinality"`
	Owner              string              `json:"owner,omitempty"`
	Status             CatalogMetricStatus `json:"status"`
	Deprecated         bool                `json:"deprecated"`
	DeprecatedAt       time.Time           `json:"deprecated_at,omitempty"`
	DeprecationMessage string              `json:"deprecation_message,omitempty"`
	Labels             map[string]string   `json:"labels,omitempty"`
}

// CatalogTagInfo describes a tag key used by a metric.
type CatalogTagInfo struct {
	Key          string   `json:"key"`
	Cardinality  int      `json:"cardinality"`
	SampleValues []string `json:"sample_values,omitempty"`
	Required     bool     `json:"required"`
}

// MetricLineage tracks how a metric is produced and consumed.
type MetricLineage struct {
	MetricName   string           `json:"metric_name"`
	Producers    []LineageLink    `json:"producers,omitempty"`
	Consumers    []LineageLink    `json:"consumers,omitempty"`
	Queries      []CatalogQueryUsage `json:"queries,omitempty"`
	Dashboards   []string         `json:"dashboards,omitempty"`
	AlertRules   []string         `json:"alert_rules,omitempty"`
	Dependencies int              `json:"dependencies"`
	Dependents   int              `json:"dependents"`
}

// LineageLink describes a producer or consumer of a metric.
type LineageLink struct {
	Source     string    `json:"source"`
	Type       string    `json:"type"` // "raw", "derived", "aggregated", "continuous_query"
	Expression string    `json:"expression,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// CatalogQueryUsage records how a metric is queried.
type CatalogQueryUsage struct {
	Query      string        `json:"query"`
	Count      int64         `json:"count"`
	LastUsed   time.Time     `json:"last_used"`
	AvgLatency time.Duration `json:"avg_latency"`
}

// CatalogSearchQuery defines search criteria for the catalog.
type CatalogSearchQuery struct {
	Pattern    string            `json:"pattern,omitempty"`
	Type       CatalogMetricType `json:"type,omitempty"`
	Status     CatalogMetricStatus `json:"status,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	Owner      string            `json:"owner,omitempty"`
	MinPoints  int64             `json:"min_points,omitempty"`
	MaxAge     time.Duration     `json:"max_age,omitempty"`
	HasLineage bool              `json:"has_lineage,omitempty"`
	Limit      int               `json:"limit,omitempty"`
	Offset     int               `json:"offset,omitempty"`
}

// CatalogSearchResult holds the results of a catalog search.
type CatalogSearchResult struct {
	Entries  []*CatalogEntry    `json:"entries"`
	Total    int                `json:"total"`
	Limit    int                `json:"limit"`
	Offset   int                `json:"offset"`
	Query    CatalogSearchQuery `json:"query"`
	Duration time.Duration      `json:"duration"`
}

// DeprecationRequest is a request to deprecate a metric.
type DeprecationRequest struct {
	MetricName  string        `json:"metric_name"`
	Reason      string        `json:"reason"`
	Replacement string        `json:"replacement,omitempty"`
	GracePeriod time.Duration `json:"grace_period,omitempty"`
	NotifyOwner bool          `json:"notify_owner"`
}

// DeprecationReport summarizes a metric deprecation.
type DeprecationReport struct {
	Metric             string    `json:"metric"`
	Status             string    `json:"status"` // "pending", "grace_period", "deprecated", "archived"
	Replacement        string    `json:"replacement,omitempty"`
	AffectedQueries    int       `json:"affected_queries"`
	AffectedAlerts     int       `json:"affected_alerts"`
	AffectedDashboards int       `json:"affected_dashboards"`
	GraceEndsAt        time.Time `json:"grace_ends_at"`
}

// MetricsCatalogStats holds operational statistics for the catalog.
type MetricsCatalogStats struct {
	TotalMetrics      int           `json:"total_metrics"`
	ActiveMetrics     int           `json:"active_metrics"`
	DeprecatedMetrics int           `json:"deprecated_metrics"`
	TotalTags         int           `json:"total_tags"`
	UniqueOwners      int           `json:"unique_owners"`
	TotalQueries      int64         `json:"total_queries"`
	ScanCount         int64         `json:"scan_count"`
	LastScan          time.Time     `json:"last_scan"`
	ScanDuration      time.Duration `json:"scan_duration"`
}

// MetricsCatalog provides auto-discovery, lineage tracking, deprecation
// workflows, and search for metrics stored in a Chronicle database.
type MetricsCatalog struct {
	db           *DB
	config       MetricsCatalogConfig
	mu           sync.RWMutex
	entries      map[string]*CatalogEntry
	lineage      map[string]*MetricLineage
	queryLog     map[string][]CatalogQueryUsage
	deprecations map[string]*DeprecationReport
	stopCh       chan struct{}
	running      bool
	stats        MetricsCatalogStats
}

// NewMetricsCatalog creates a new MetricsCatalog.
func NewMetricsCatalog(db *DB, cfg MetricsCatalogConfig) *MetricsCatalog {
	return &MetricsCatalog{
		db:           db,
		config:       cfg,
		entries:      make(map[string]*CatalogEntry),
		lineage:      make(map[string]*MetricLineage),
		queryLog:     make(map[string][]CatalogQueryUsage),
		deprecations: make(map[string]*DeprecationReport),
		stopCh:       make(chan struct{}),
	}
}

// Start begins the periodic background scanning of metrics.
func (mc *MetricsCatalog) Start() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.running {
		return
	}
	mc.running = true
	go mc.scanLoop()
}

// Stop halts the background scanning goroutine.
func (mc *MetricsCatalog) Stop() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if !mc.running {
		return
	}
	mc.running = false
	close(mc.stopCh)
}

func (mc *MetricsCatalog) scanLoop() {
	ticker := time.NewTicker(mc.config.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-mc.stopCh:
			return
		case <-ticker.C:
			_ = mc.Scan()
		}
	}
}

// Scan discovers metrics from the database and updates the catalog.
func (mc *MetricsCatalog) Scan() error {
	start := time.Now()

	if mc.db == nil {
		mc.mu.Lock()
		mc.stats.ScanCount++
		mc.stats.LastScan = time.Now()
		mc.stats.ScanDuration = time.Since(start)
		mc.mu.Unlock()
		return nil
	}

	metrics := mc.db.Metrics()
	now := time.Now()

	mc.mu.Lock()
	defer mc.mu.Unlock()

	for _, name := range metrics {
		if len(mc.entries) >= mc.config.MaxMetrics {
			break
		}

		entry, exists := mc.entries[name]
		if !exists {
			entry = &CatalogEntry{
				Name:      name,
				FirstSeen: now,
				Status:    CatalogMetricStatusActive,
				Labels:    make(map[string]string),
			}
			mc.entries[name] = entry
		}

		entry.LastSeen = now

		// Query recent data to infer type
		result, err := mc.db.Execute(&Query{
			Metric: name,
			Limit:  100,
		})
		if err == nil && len(result.Points) > 0 {
			values := make([]float64, len(result.Points))
			for i, p := range result.Points {
				values[i] = p.Value
			}
			if entry.Type == CatalogMetricTypeUnknown {
				entry.Type = mc.inferMetricType(values)
			}
			entry.PointCount = int64(len(result.Points))

			// Collect tag info
			tagKeys := mc.db.TagKeysForMetric(name)
			entry.Tags = make([]CatalogTagInfo, 0, len(tagKeys))
			for _, key := range tagKeys {
				vals := mc.db.TagValuesForMetric(name, key)
				sample := vals
				if len(sample) > 5 {
					sample = sample[:5]
				}
				entry.Tags = append(entry.Tags, CatalogTagInfo{
					Key:          key,
					Cardinality:  len(vals),
					SampleValues: sample,
				})
			}
			entry.Cardinality = len(tagKeys)
		}

		if entry.Unit == "" {
			entry.Unit = mc.inferUnit(name)
		}

		// Initialize lineage if tracking enabled
		if mc.config.TrackLineage {
			if _, ok := mc.lineage[name]; !ok {
				mc.lineage[name] = &MetricLineage{
					MetricName: name,
				}
			}
		}
	}

	mc.stats.ScanCount++
	mc.stats.LastScan = now
	mc.stats.ScanDuration = time.Since(start)

	return nil
}

// inferMetricType detects whether values look like a gauge or counter.
func (mc *MetricsCatalog) inferMetricType(values []float64) CatalogMetricType {
	if len(values) < 2 {
		return CatalogMetricTypeUnknown
	}

	// Check for monotonically increasing (counter)
	monotonic := true
	allNonNeg := true
	for i := 1; i < len(values); i++ {
		if values[i] < values[i-1] {
			monotonic = false
		}
		if values[i] < 0 {
			allNonNeg = false
		}
	}
	if monotonic && allNonNeg {
		return CatalogMetricTypeCounter
	}

	// Check for histogram-like distribution (wide spread, many distinct values)
	if len(values) >= 10 {
		min, max := values[0], values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		spread := max - min
		if spread > 0 {
			mean := 0.0
			for _, v := range values {
				mean += v
			}
			mean /= float64(len(values))
			variance := 0.0
			for _, v := range values {
				diff := v - mean
				variance += diff * diff
			}
			variance /= float64(len(values))
			cv := math.Sqrt(variance) / math.Abs(mean+0.0001)
			if cv > 1.5 {
				return CatalogMetricTypeHistogram
			}
		}
	}

	return CatalogMetricTypeGauge
}

// inferUnit attempts to detect the unit from the metric name.
func (mc *MetricsCatalog) inferUnit(name string) string {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "bytes") || strings.HasSuffix(lower, "_bytes"):
		return "bytes"
	case strings.Contains(lower, "seconds") || strings.HasSuffix(lower, "_seconds") ||
		strings.HasSuffix(lower, "_duration") || strings.Contains(lower, "latency"):
		return "seconds"
	case strings.Contains(lower, "percent") || strings.HasSuffix(lower, "_percent") ||
		strings.HasSuffix(lower, "_ratio") || strings.Contains(lower, "usage"):
		return "percent"
	case strings.HasSuffix(lower, "_total") || strings.HasSuffix(lower, "_count") ||
		strings.Contains(lower, "requests") || strings.Contains(lower, "errors"):
		return "count"
	case strings.Contains(lower, "temperature") || strings.HasSuffix(lower, "_temp"):
		return "celsius"
	default:
		return ""
	}
}

// RegisterMetric manually registers a metric in the catalog.
func (mc *MetricsCatalog) RegisterMetric(entry CatalogEntry) error {
	if entry.Name == "" {
		return fmt.Errorf("metrics_catalog: metric name is required")
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	if len(mc.entries) >= mc.config.MaxMetrics {
		if _, exists := mc.entries[entry.Name]; !exists {
			return fmt.Errorf("metrics_catalog: maximum metrics limit reached (%d)", mc.config.MaxMetrics)
		}
	}

	if entry.FirstSeen.IsZero() {
		entry.FirstSeen = time.Now()
	}
	if entry.LastSeen.IsZero() {
		entry.LastSeen = entry.FirstSeen
	}
	if entry.Labels == nil {
		entry.Labels = make(map[string]string)
	}

	mc.entries[entry.Name] = &entry

	if mc.config.TrackLineage {
		if _, ok := mc.lineage[entry.Name]; !ok {
			mc.lineage[entry.Name] = &MetricLineage{
				MetricName: entry.Name,
			}
		}
	}

	return nil
}

// UpdateMetric updates fields on an existing catalog entry.
func (mc *MetricsCatalog) UpdateMetric(name string, updates map[string]string) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	entry, ok := mc.entries[name]
	if !ok {
		return fmt.Errorf("metrics_catalog: metric %q not found", name)
	}

	for k, v := range updates {
		switch k {
		case "description":
			entry.Description = v
		case "owner":
			entry.Owner = v
		case "unit":
			entry.Unit = v
		}
	}
	if entry.Labels == nil {
		entry.Labels = make(map[string]string)
	}
	for k, v := range updates {
		if k != "description" && k != "owner" && k != "unit" {
			entry.Labels[k] = v
		}
	}

	return nil
}

// Search finds catalog entries matching the given query criteria.
func (mc *MetricsCatalog) Search(query CatalogSearchQuery) (*CatalogSearchResult, error) {
	start := time.Now()

	mc.mu.RLock()
	defer mc.mu.RUnlock()

	var matches []*CatalogEntry
	for _, entry := range mc.entries {
		if !mc.matchesSearch(entry, query) {
			continue
		}
		matches = append(matches, entry)
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Name < matches[j].Name
	})

	total := len(matches)
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	offset := query.Offset
	if offset > len(matches) {
		offset = len(matches)
	}
	end := offset + limit
	if end > len(matches) {
		end = len(matches)
	}
	page := matches[offset:end]

	return &CatalogSearchResult{
		Entries:  page,
		Total:    total,
		Limit:    limit,
		Offset:   offset,
		Query:    query,
		Duration: time.Since(start),
	}, nil
}

func (mc *MetricsCatalog) matchesSearch(entry *CatalogEntry, q CatalogSearchQuery) bool {
	if q.Pattern != "" {
		if !matchGlob(q.Pattern, entry.Name) {
			return false
		}
	}
	if q.Type != CatalogMetricTypeUnknown && entry.Type != q.Type {
		return false
	}
	if q.Status != 0 && entry.Status != q.Status {
		return false
	}
	if q.Owner != "" && entry.Owner != q.Owner {
		return false
	}
	if q.MinPoints > 0 && entry.PointCount < q.MinPoints {
		return false
	}
	if q.MaxAge > 0 {
		age := time.Since(entry.LastSeen)
		if age > q.MaxAge {
			return false
		}
	}
	if q.HasLineage {
		lineage, ok := mc.lineage[entry.Name]
		if !ok || (len(lineage.Producers) == 0 && len(lineage.Consumers) == 0) {
			return false
		}
	}
	if len(q.Tags) > 0 {
		for tk, tv := range q.Tags {
			found := false
			for _, t := range entry.Tags {
				if t.Key == tk {
					if tv == "" {
						found = true
					} else {
						for _, sv := range t.SampleValues {
							if sv == tv {
								found = true
								break
							}
						}
					}
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

// matchGlob performs simple glob matching supporting * wildcards.
func matchGlob(pattern, s string) bool {
	if pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == s
	}
	parts := strings.Split(pattern, "*")
	if len(parts) == 2 {
		return strings.HasPrefix(s, parts[0]) && strings.HasSuffix(s, parts[1])
	}
	// General multi-* glob
	pos := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.Index(s[pos:], part)
		if idx < 0 {
			return false
		}
		if i == 0 && idx != 0 {
			return false
		}
		pos += idx + len(part)
	}
	if parts[len(parts)-1] != "" {
		return strings.HasSuffix(s, parts[len(parts)-1])
	}
	return true
}

// GetEntry returns a single catalog entry by name, or nil if not found.
func (mc *MetricsCatalog) GetEntry(name string) *CatalogEntry {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.entries[name]
}

// GetLineage returns the lineage information for a metric.
func (mc *MetricsCatalog) GetLineage(metricName string) *MetricLineage {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.lineage[metricName]
}

// RecordQuery records a query execution against a metric for usage tracking.
func (mc *MetricsCatalog) RecordQuery(metricName string, query string, latency time.Duration) {
	if !mc.config.TrackQueryUsage {
		return
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.stats.TotalQueries++

	usages := mc.queryLog[metricName]
	for i, u := range usages {
		if u.Query == query {
			usages[i].Count++
			usages[i].LastUsed = time.Now()
			totalLatency := time.Duration(u.AvgLatency.Nanoseconds()*u.Count) + latency
			usages[i].AvgLatency = time.Duration(totalLatency.Nanoseconds() / usages[i].Count)
			mc.queryLog[metricName] = usages
			// Also update lineage
			if lineage, ok := mc.lineage[metricName]; ok {
				lineage.Queries = usages
			}
			return
		}
	}

	usage := CatalogQueryUsage{
		Query:      query,
		Count:      1,
		LastUsed:   time.Now(),
		AvgLatency: latency,
	}
	usages = append(usages, usage)
	mc.queryLog[metricName] = usages

	if lineage, ok := mc.lineage[metricName]; ok {
		lineage.Queries = usages
	}
}

// DeprecateMetric marks a metric as deprecated with an optional grace period.
func (mc *MetricsCatalog) DeprecateMetric(req DeprecationRequest) (*DeprecationReport, error) {
	if req.MetricName == "" {
		return nil, fmt.Errorf("metrics_catalog: metric name is required")
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	entry, ok := mc.entries[req.MetricName]
	if !ok {
		return nil, fmt.Errorf("metrics_catalog: metric %q not found", req.MetricName)
	}

	gracePeriod := req.GracePeriod
	if gracePeriod == 0 {
		gracePeriod = mc.config.DeprecationGracePeriod
	}

	now := time.Now()
	entry.Deprecated = true
	entry.DeprecatedAt = now
	entry.DeprecationMessage = req.Reason
	entry.Status = CatalogMetricStatusDeprecated

	report := &DeprecationReport{
		Metric:      req.MetricName,
		Status:      "grace_period",
		Replacement: req.Replacement,
		GraceEndsAt: now.Add(gracePeriod),
	}

	// Count affected resources from lineage
	if lineage, ok := mc.lineage[req.MetricName]; ok {
		report.AffectedQueries = len(lineage.Queries)
		report.AffectedAlerts = len(lineage.AlertRules)
		report.AffectedDashboards = len(lineage.Dashboards)
	}

	mc.deprecations[req.MetricName] = report

	return report, nil
}

// UndeprecateMetric reverses a deprecation.
func (mc *MetricsCatalog) UndeprecateMetric(name string) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	entry, ok := mc.entries[name]
	if !ok {
		return fmt.Errorf("metrics_catalog: metric %q not found", name)
	}

	entry.Deprecated = false
	entry.DeprecatedAt = time.Time{}
	entry.DeprecationMessage = ""
	entry.Status = CatalogMetricStatusActive

	delete(mc.deprecations, name)

	return nil
}

// ListDeprecated returns all deprecation reports.
func (mc *MetricsCatalog) ListDeprecated() []*DeprecationReport {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	reports := make([]*DeprecationReport, 0, len(mc.deprecations))
	for _, r := range mc.deprecations {
		reports = append(reports, r)
	}
	sort.Slice(reports, func(i, j int) bool {
		return reports[i].Metric < reports[j].Metric
	})
	return reports
}

// SetOwner assigns an owner to a metric.
func (mc *MetricsCatalog) SetOwner(metricName string, owner string) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	entry, ok := mc.entries[metricName]
	if !ok {
		return fmt.Errorf("metrics_catalog: metric %q not found", metricName)
	}
	entry.Owner = owner
	return nil
}

// Export returns all catalog entries as a slice.
func (mc *MetricsCatalog) Export() []*CatalogEntry {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	result := make([]*CatalogEntry, 0, len(mc.entries))
	for _, e := range mc.entries {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// Stats returns a snapshot of catalog statistics.
func (mc *MetricsCatalog) Stats() MetricsCatalogStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	stats := mc.stats
	stats.TotalMetrics = len(mc.entries)

	owners := make(map[string]struct{})
	tagCount := 0
	for _, e := range mc.entries {
		switch e.Status {
		case CatalogMetricStatusActive:
			stats.ActiveMetrics++
		case CatalogMetricStatusDeprecated:
			stats.DeprecatedMetrics++
		}
		tagCount += len(e.Tags)
		if e.Owner != "" {
			owners[e.Owner] = struct{}{}
		}
	}
	stats.TotalTags = tagCount
	stats.UniqueOwners = len(owners)

	return stats
}

// RegisterHTTPHandlers registers catalog HTTP API endpoints.
func (mc *MetricsCatalog) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/catalog/metrics", mc.handleMetrics)
	mux.HandleFunc("/api/v1/catalog/metrics/", mc.handleMetricByName)
	mux.HandleFunc("/api/v1/catalog/scan", mc.handleScan)
	mux.HandleFunc("/api/v1/catalog/deprecate", mc.handleDeprecate)
	mux.HandleFunc("/api/v1/catalog/deprecated", mc.handleListDeprecated)
	mux.HandleFunc("/api/v1/catalog/stats", mc.handleStats)
	mux.HandleFunc("/api/v1/catalog/export", mc.handleExport)
}

func (mc *MetricsCatalog) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	query := CatalogSearchQuery{
		Pattern: r.URL.Query().Get("pattern"),
		Owner:   r.URL.Query().Get("owner"),
		Limit:   100,
	}
	result, err := mc.Search(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (mc *MetricsCatalog) handleMetricByName(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/catalog/metrics/")
	if path == "" {
		http.Error(w, "metric name required", http.StatusBadRequest)
		return
	}

	// Check for lineage sub-path
	if strings.HasSuffix(path, "/lineage") {
		name := strings.TrimSuffix(path, "/lineage")
		lineage := mc.GetLineage(name)
		if lineage == nil {
			http.Error(w, "lineage not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(lineage)
		return
	}

	entry := mc.GetEntry(path)
	if entry == nil {
		http.Error(w, "metric not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entry)
}

func (mc *MetricsCatalog) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := mc.Scan(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (mc *MetricsCatalog) handleDeprecate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req DeprecationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	report, err := mc.DeprecateMetric(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(report)
}

func (mc *MetricsCatalog) handleListDeprecated(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(mc.ListDeprecated())
}

func (mc *MetricsCatalog) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(mc.Stats())
}

func (mc *MetricsCatalog) handleExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(mc.Export())
}
