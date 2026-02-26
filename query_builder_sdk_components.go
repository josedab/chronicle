package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Internal query generation, component parsing, and HTTP handlers for the visual query builder SDK.

func (b *VisualQueryBuilder) generateInternalQuery(vq *VisualQuerySpec) (*Query, error) {
	q := &Query{}

	for _, comp := range vq.Components {
		switch comp.Type {
		case ComponentMetric:
			m := parseMetricComponent(comp.Properties)
			if m != nil {
				q.Metric = m.Name
				if q.Tags == nil && len(m.Tags) > 0 {
					q.Tags = m.Tags
				}
			}

		case ComponentTimeRange:
			tr := parseTimeRangeComponent(comp.Properties)
			if tr != nil {
				if tr.Start != nil {
					q.Start = tr.Start.UnixNano()
				}
				if tr.End != nil {
					q.End = tr.End.UnixNano()
				}
				if tr.Relative != "" {
					dur := parseRelativeDuration(tr.Relative)
					q.Start = time.Now().Add(-dur).UnixNano()
					q.End = time.Now().UnixNano()
				}
			}

		case ComponentAggregation:
			a := parseAggregationComponent(comp.Properties)
			if a != nil {
				q.Aggregation = &Aggregation{
					Function: vqbStringToAggFunc(a.Function),
				}
				if a.Interval != "" {
					q.Aggregation.Window = parseRelativeDuration(a.Interval)
				}
			}

		case ComponentGroupBy:
			g := parseGroupByComponent(comp.Properties)
			if g != nil {
				q.GroupBy = g.Fields
			}

		case ComponentLimit:
			limit := parseLimitComponent(comp.Properties)
			if limit > 0 {
				q.Limit = limit
			}
		}
	}

	return q, nil
}

// CreateComponent creates a new query component.
func (b *VisualQueryBuilder) CreateComponent(compType ComponentType, properties any) *QueryComponent {
	genID1, _ := generateID()
	return &QueryComponent{
		ID:         genID1,
		Type:       compType,
		Properties: properties,
	}
}

// CreateVisualQuerySpec creates a new visual query.
func (b *VisualQueryBuilder) CreateVisualQuery(name string) *VisualQuerySpec {
	now := time.Now()
	genID2, _ := generateID()
	return &VisualQuerySpec{
		ID:        genID2,
		Name:      name,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// ParseFromSQL parses SQL into a visual query.
func (b *VisualQueryBuilder) ParseFromSQL(sql string) (*VisualQuerySpec, error) {
	vq := b.CreateVisualQuery("")

	sql = strings.TrimSpace(sql)
	upperSQL := strings.ToUpper(sql)

	// Extract SELECT fields
	selectIdx := strings.Index(upperSQL, "SELECT")
	fromIdx := strings.Index(upperSQL, "FROM")
	if selectIdx == -1 || fromIdx == -1 {
		return nil, fmt.Errorf("invalid SQL: missing SELECT or FROM")
	}

	// Extract FROM table (metric)
	whereIdx := strings.Index(upperSQL, "WHERE")
	var tablePart string
	if whereIdx != -1 {
		tablePart = strings.TrimSpace(sql[fromIdx+4 : whereIdx])
	} else {
		groupIdx := strings.Index(upperSQL, "GROUP")
		if groupIdx != -1 {
			tablePart = strings.TrimSpace(sql[fromIdx+4 : groupIdx])
		} else {
			limitIdx := strings.Index(upperSQL, "LIMIT")
			if limitIdx != -1 {
				tablePart = strings.TrimSpace(sql[fromIdx+4 : limitIdx])
			} else {
				tablePart = strings.TrimSpace(sql[fromIdx+4:])
			}
		}
	}

	metricComp := b.CreateComponent(ComponentMetric, MetricComponent{
		Name: tablePart,
	})
	vq.Components = append(vq.Components, metricComp)

	return vq, nil
}

// Handler functions for HTTP API

// HandleGetSchema handles GET /api/query-builder/schema
func (b *VisualQueryBuilder) HandleGetSchema(w http.ResponseWriter, r *http.Request) {
	schema, err := b.GetSchema(r.Context())
	if err != nil {
		internalError(w, err, "schema retrieval failed")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(schema)
}

// HandleAutocomplete handles POST /api/query-builder/autocomplete
func (b *VisualQueryBuilder) HandleAutocomplete(w http.ResponseWriter, r *http.Request) {
	var req AutocompleteRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	resp, err := b.Autocomplete(r.Context(), &req)
	if err != nil {
		internalError(w, err, "autocomplete failed")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// HandleGenerateQuery handles POST /api/query-builder/generate
func (b *VisualQueryBuilder) HandleGenerateQuery(w http.ResponseWriter, r *http.Request) {
	var vq VisualQuerySpec
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&vq); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	result, err := b.GenerateQuery(r.Context(), &vq)
	if err != nil {
		internalError(w, err, "query generation failed")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// HandleValidateQuery handles POST /api/query-builder/validate
func (b *VisualQueryBuilder) HandleValidateQuery(w http.ResponseWriter, r *http.Request) {
	var vq VisualQuerySpec
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&vq); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	errors, warnings := b.ValidateQuery(&vq)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"valid":    len(errors) == 0,
		"errors":   errors,
		"warnings": warnings,
	})
}

// HandleParseSQL handles POST /api/query-builder/parse-sql
func (b *VisualQueryBuilder) HandleParseSQL(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SQL string `json:"sql"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	vq, err := b.ParseFromSQL(req.SQL)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(vq)
}

// RegisterRoutes registers query builder routes.
func (b *VisualQueryBuilder) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/query-builder/schema", b.HandleGetSchema)
	mux.HandleFunc("/api/query-builder/autocomplete", b.HandleAutocomplete)
	mux.HandleFunc("/api/query-builder/generate", b.HandleGenerateQuery)
	mux.HandleFunc("/api/query-builder/validate", b.HandleValidateQuery)
	mux.HandleFunc("/api/query-builder/parse-sql", b.HandleParseSQL)
}

// Helper functions

func (b *VisualQueryBuilder) getMetrics(ctx context.Context) ([]string, error) {
	b.cacheMu.RLock()
	if b.config.CacheMetadata && time.Since(b.cacheTime) < b.config.MetadataCacheTTL && len(b.metricsCache) > 0 {
		metrics := b.metricsCache
		b.cacheMu.RUnlock()
		return metrics, nil
	}
	b.cacheMu.RUnlock()

	// Use db.Metrics() which returns all metric names
	metrics := b.db.Metrics()

	b.cacheMu.Lock()
	b.metricsCache = metrics
	b.cacheTime = time.Now()
	b.cacheMu.Unlock()

	return metrics, nil
}

func (b *VisualQueryBuilder) getTagKeys(ctx context.Context, metric string) ([]string, error) {
	b.cacheMu.RLock()
	if cached, ok := b.tagKeysCache[metric]; ok && b.config.CacheMetadata && time.Since(b.cacheTime) < b.config.MetadataCacheTTL {
		b.cacheMu.RUnlock()
		return cached, nil
	}
	b.cacheMu.RUnlock()

	// Query actual tag keys from index
	var keys []string
	if metric != "" {
		keys = b.db.TagKeysForMetric(metric)
	} else {
		keys = b.db.TagKeys()
	}

	b.cacheMu.Lock()
	b.tagKeysCache[metric] = keys
	b.cacheMu.Unlock()

	return keys, nil
}

func (b *VisualQueryBuilder) getTagValues(ctx context.Context, metric, key string) ([]string, error) {
	cacheKey := metric + ":" + key

	b.cacheMu.RLock()
	if metricCache, ok := b.tagValuesCache[metric]; ok {
		if cached, ok := metricCache[key]; ok && b.config.CacheMetadata && time.Since(b.cacheTime) < b.config.MetadataCacheTTL {
			b.cacheMu.RUnlock()
			return cached, nil
		}
	}
	b.cacheMu.RUnlock()

	// Query actual tag values from index
	var values []string
	if metric != "" {
		values = b.db.TagValuesForMetric(metric, key)
	} else {
		values = b.db.TagValues(key)
	}

	b.cacheMu.Lock()
	if b.tagValuesCache[cacheKey] == nil {
		b.tagValuesCache[cacheKey] = make(map[string][]string)
	}
	b.tagValuesCache[cacheKey][key] = values
	b.cacheMu.Unlock()

	return values, nil
}

func (b *VisualQueryBuilder) getAvailableFunctions() []VQBFunctionInfo {
	return []VQBFunctionInfo{
		{Name: "rate", Description: "Calculate rate of change", Arguments: []ArgInfo{{Name: "metric", Type: "metric", Required: true}}, ReturnType: "metric", Category: "transform"},
		{Name: "irate", Description: "Calculate instant rate of change", Arguments: []ArgInfo{{Name: "metric", Type: "metric", Required: true}}, ReturnType: "metric", Category: "transform"},
		{Name: "increase", Description: "Calculate increase over time", Arguments: []ArgInfo{{Name: "metric", Type: "metric", Required: true}}, ReturnType: "metric", Category: "transform"},
		{Name: "delta", Description: "Calculate delta between first and last", Arguments: []ArgInfo{{Name: "metric", Type: "metric", Required: true}}, ReturnType: "metric", Category: "transform"},
		{Name: "abs", Description: "Absolute value", Arguments: []ArgInfo{{Name: "value", Type: "number", Required: true}}, ReturnType: "number", Category: "math"},
		{Name: "ceil", Description: "Round up to integer", Arguments: []ArgInfo{{Name: "value", Type: "number", Required: true}}, ReturnType: "number", Category: "math"},
		{Name: "floor", Description: "Round down to integer", Arguments: []ArgInfo{{Name: "value", Type: "number", Required: true}}, ReturnType: "number", Category: "math"},
		{Name: "round", Description: "Round to nearest integer", Arguments: []ArgInfo{{Name: "value", Type: "number", Required: true}}, ReturnType: "number", Category: "math"},
		{Name: "ln", Description: "Natural logarithm", Arguments: []ArgInfo{{Name: "value", Type: "number", Required: true}}, ReturnType: "number", Category: "math"},
		{Name: "log2", Description: "Base 2 logarithm", Arguments: []ArgInfo{{Name: "value", Type: "number", Required: true}}, ReturnType: "number", Category: "math"},
		{Name: "log10", Description: "Base 10 logarithm", Arguments: []ArgInfo{{Name: "value", Type: "number", Required: true}}, ReturnType: "number", Category: "math"},
		{Name: "exp", Description: "Exponential", Arguments: []ArgInfo{{Name: "value", Type: "number", Required: true}}, ReturnType: "number", Category: "math"},
		{Name: "sqrt", Description: "Square root", Arguments: []ArgInfo{{Name: "value", Type: "number", Required: true}}, ReturnType: "number", Category: "math"},
		{Name: "deriv", Description: "Derivative", Arguments: []ArgInfo{{Name: "metric", Type: "metric", Required: true}}, ReturnType: "metric", Category: "transform"},
		{Name: "predict_linear", Description: "Linear prediction", Arguments: []ArgInfo{{Name: "metric", Type: "metric", Required: true}, {Name: "seconds", Type: "number", Required: true}}, ReturnType: "metric", Category: "predict"},
		{Name: "histogram_quantile", Description: "Calculate histogram quantile", Arguments: []ArgInfo{{Name: "quantile", Type: "number", Required: true}, {Name: "metric", Type: "metric", Required: true}}, ReturnType: "number", Category: "aggregation"},
	}
}

func (b *VisualQueryBuilder) getAvailableAggregations() []AggregationInfo {
	return []AggregationInfo{
		{Name: "avg", Description: "Average value", SupportsTime: true},
		{Name: "sum", Description: "Sum of values", SupportsTime: true},
		{Name: "min", Description: "Minimum value", SupportsTime: true},
		{Name: "max", Description: "Maximum value", SupportsTime: true},
		{Name: "count", Description: "Count of values", SupportsTime: true},
		{Name: "first", Description: "First value", SupportsTime: true},
		{Name: "last", Description: "Last value", SupportsTime: true},
		{Name: "stddev", Description: "Standard deviation", SupportsTime: true},
		{Name: "variance", Description: "Variance", SupportsTime: true},
		{Name: "median", Description: "Median value", SupportsTime: true},
		{Name: "percentile_90", Description: "90th percentile", SupportsTime: true},
		{Name: "percentile_95", Description: "95th percentile", SupportsTime: true},
		{Name: "percentile_99", Description: "99th percentile", SupportsTime: true},
	}
}

func (b *VisualQueryBuilder) getAvailableOperators() []OperatorInfo {
	return []OperatorInfo{
		{Symbol: "=", Name: "equals", Description: "Equal to", Types: []string{"string", "number"}},
		{Symbol: "!=", Name: "not_equals", Description: "Not equal to", Types: []string{"string", "number"}},
		{Symbol: ">", Name: "greater_than", Description: "Greater than", Types: []string{"number"}},
		{Symbol: ">=", Name: "greater_or_equal", Description: "Greater than or equal", Types: []string{"number"}},
		{Symbol: "<", Name: "less_than", Description: "Less than", Types: []string{"number"}},
		{Symbol: "<=", Name: "less_or_equal", Description: "Less than or equal", Types: []string{"number"}},
		{Symbol: "=~", Name: "regex_match", Description: "Regex match", Types: []string{"string"}},
		{Symbol: "!~", Name: "regex_not_match", Description: "Regex not match", Types: []string{"string"}},
		{Symbol: "IN", Name: "in", Description: "In list", Types: []string{"string", "number"}},
		{Symbol: "NOT IN", Name: "not_in", Description: "Not in list", Types: []string{"string", "number"}},
		{Symbol: "LIKE", Name: "like", Description: "Pattern match", Types: []string{"string"}},
		{Symbol: "IS NULL", Name: "is_null", Description: "Is null", Types: []string{"any"}},
		{Symbol: "IS NOT NULL", Name: "is_not_null", Description: "Is not null", Types: []string{"any"}},
	}
}

func parseMetricComponent(props any) *MetricComponent {
	if props == nil {
		return nil
	}

	data, err := json.Marshal(props)
	if err != nil {
		return nil
	}

	var m MetricComponent
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return &m
}

func parseFilterComponent(props any) *FilterComponent {
	if props == nil {
		return nil
	}

	data, err := json.Marshal(props)
	if err != nil {
		return nil
	}

	var f FilterComponent
	if err := json.Unmarshal(data, &f); err != nil {
		return nil
	}
	return &f
}

func parseAggregationComponent(props any) *AggregationComponent {
	if props == nil {
		return nil
	}

	data, err := json.Marshal(props)
	if err != nil {
		return nil
	}

	var a AggregationComponent
	if err := json.Unmarshal(data, &a); err != nil {
		return nil
	}
	return &a
}

func parseGroupByComponent(props any) *GroupByComponent {
	if props == nil {
		return nil
	}

	data, err := json.Marshal(props)
	if err != nil {
		return nil
	}

	var g GroupByComponent
	if err := json.Unmarshal(data, &g); err != nil {
		return nil
	}
	return &g
}

func parseTimeRangeComponent(props any) *TimeRangeComponent {
	if props == nil {
		return nil
	}

	data, err := json.Marshal(props)
	if err != nil {
		return nil
	}

	var t TimeRangeComponent
	if err := json.Unmarshal(data, &t); err != nil {
		return nil
	}
	return &t
}

func parseLimitComponent(props any) int {
	if props == nil {
		return 0
	}

	switch v := props.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case map[string]any:
		if limit, ok := v["limit"]; ok {
			if l, ok := limit.(float64); ok {
				return int(l)
			}
		}
	}
	return 0
}

func formatFilterClause(f *FilterComponent) string {
	if f == nil {
		return ""
	}

	// Whitelist allowed SQL operators
	allowedOps := map[string]bool{
		"=": true, "!=": true, ">": true, "<": true,
		">=": true, "<=": true, "LIKE": true, "like": true,
	}
	op := f.Operator
	if !allowedOps[op] {
		op = "="
	}

	field := sanitizeIdentifier(f.Field)
	value := f.Value
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("%s %s '%s'", field, op, escapeSQL(v))
	case float64:
		return fmt.Sprintf("%s %s %v", field, op, v)
	default:
		return fmt.Sprintf("%s %s %v", field, op, v)
	}
}

func formatPromQLMatcher(f *FilterComponent) string {
	if f == nil {
		return ""
	}

	op := "="
	switch f.Operator {
	case "=", "==":
		op = "="
	case "!=", "<>":
		op = "!="
	case "=~":
		op = "=~"
	case "!~":
		op = "!~"
	default:
		return ""
	}

	return fmt.Sprintf(`%s%s"%v"`, f.Field, op, f.Value)
}

func mapAggToPromQL(agg string) string {
	mapping := map[string]string{
		"avg":      "avg",
		"sum":      "sum",
		"min":      "min",
		"max":      "max",
		"count":    "count",
		"stddev":   "stddev",
		"variance": "stdvar",
	}
	if mapped, ok := mapping[strings.ToLower(agg)]; ok {
		return mapped
	}
	return agg
}

func parseRelativeDuration(s string) time.Duration {
	if s == "" {
		return 0
	}

	// Parse formats like "1h", "24h", "7d"
	d, err := time.ParseDuration(s)
	if err == nil {
		return d
	}

	// Handle day format
	if strings.HasSuffix(s, "d") {
		days := strings.TrimSuffix(s, "d")
		if n, err := fmt.Sscanf(days, "%d", new(int)); err == nil && n == 1 {
			var d int
			fmt.Sscanf(days, "%d", &d)
			return time.Duration(d) * 24 * time.Hour
		}
	}

	return 0
}

func vqbStringToAggFunc(s string) AggFunc {
	mapping := map[string]AggFunc{
		"avg":    AggMean,
		"sum":    AggSum,
		"min":    AggMin,
		"max":    AggMax,
		"count":  AggCount,
		"first":  AggFirst,
		"last":   AggLast,
		"stddev": AggStddev,
	}
	if f, ok := mapping[strings.ToLower(s)]; ok {
		return f
	}
	return AggNone
}
