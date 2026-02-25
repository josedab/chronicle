package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Search, filtering, and glob matching for the metrics catalog.

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
		internalError(w, err, "internal error")
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
		internalError(w, err, "internal error")
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
		http.Error(w, "bad request", http.StatusBadRequest)
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
