package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// VisualQueryBuilderConfig configures the visual query builder SDK.
type VisualQueryBuilderConfig struct {
	// Enabled turns on the query builder API
	Enabled bool

	// AutocompleteLimit for suggestions
	AutocompleteLimit int

	// CacheMetadata caches schema for performance
	CacheMetadata bool

	// MetadataCacheTTL for cached metadata
	MetadataCacheTTL time.Duration

	// MaxQueryComplexity limits query depth
	MaxQueryComplexity int

	// EnableValidation validates queries before execution
	EnableValidation bool
}

// DefaultVisualQueryBuilderConfig returns default configuration.
func DefaultVisualQueryBuilderConfig() VisualQueryBuilderConfig {
	return VisualQueryBuilderConfig{
		Enabled:            true,
		AutocompleteLimit:  100,
		CacheMetadata:      true,
		MetadataCacheTTL:   5 * time.Minute,
		MaxQueryComplexity: 10,
		EnableValidation:   true,
	}
}

// VisualQueryBuilder provides APIs for building queries visually.
type VisualQueryBuilder struct {
	db     *DB
	config VisualQueryBuilderConfig

	// Metadata cache
	metricsCache    []string
	tagKeysCache    map[string][]string
	tagValuesCache  map[string]map[string][]string
	cacheTime       time.Time
	cacheMu         sync.RWMutex
}

// QueryComponent represents a building block of a visual query.
type QueryComponent struct {
	Type       ComponentType `json:"type"`
	ID         string        `json:"id"`
	Properties interface{}   `json:"properties"`
	Children   []string      `json:"children,omitempty"`
}

// ComponentType defines the type of query component.
type ComponentType string

const (
	ComponentMetric      ComponentType = "metric"
	ComponentFilter      ComponentType = "filter"
	ComponentAggregation ComponentType = "aggregation"
	ComponentTimeRange   ComponentType = "time_range"
	ComponentGroupBy     ComponentType = "group_by"
	ComponentOrderBy     ComponentType = "order_by"
	ComponentLimit       ComponentType = "limit"
	ComponentFunction    ComponentType = "function"
	ComponentMath        ComponentType = "math"
	ComponentSubquery    ComponentType = "subquery"
)

// MetricComponent represents a metric selection.
type MetricComponent struct {
	Name  string            `json:"name"`
	Alias string            `json:"alias,omitempty"`
	Tags  map[string]string `json:"tags,omitempty"`
}

// FilterComponent represents a filter condition.
type FilterComponent struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
	Logic    string      `json:"logic,omitempty"` // AND, OR
}

// AggregationComponent represents an aggregation function.
type AggregationComponent struct {
	Function string   `json:"function"` // avg, sum, min, max, count, etc.
	Field    string   `json:"field,omitempty"`
	Interval string   `json:"interval,omitempty"` // For time aggregations
	Fill     string   `json:"fill,omitempty"`     // none, null, previous, linear
}

// TimeRangeComponent represents a time range selection.
type TimeRangeComponent struct {
	Start    *time.Time `json:"start,omitempty"`
	End      *time.Time `json:"end,omitempty"`
	Relative string     `json:"relative,omitempty"` // 1h, 24h, 7d, etc.
}

// GroupByComponent represents grouping configuration.
type GroupByComponent struct {
	Fields   []string `json:"fields"`
	Interval string   `json:"interval,omitempty"` // For time grouping
}

// FunctionComponent represents a built-in function.
type FunctionComponent struct {
	Name       string        `json:"name"`
	Arguments  []interface{} `json:"arguments"`
}

// VisualQuerySpec represents a complete visual query structure.
type VisualQuerySpec struct {
	ID          string            `json:"id"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	Components  []*QueryComponent `json:"components"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// QuerySchema describes the schema for query building.
type QuerySchema struct {
	Metrics       []MetricInfo       `json:"metrics"`
	TagKeys       []string           `json:"tag_keys"`
	Functions     []VQBFunctionInfo     `json:"functions"`
	Aggregations  []AggregationInfo  `json:"aggregations"`
	Operators     []OperatorInfo     `json:"operators"`
	TimeIntervals []string           `json:"time_intervals"`
}

// MetricInfo describes a metric.
type MetricInfo struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Type        string            `json:"type,omitempty"` // gauge, counter, histogram
	Unit        string            `json:"unit,omitempty"`
}

// VQBFunctionInfo describes a function.
type VQBFunctionInfo struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Arguments   []ArgInfo  `json:"arguments"`
	ReturnType  string     `json:"return_type"`
	Category    string     `json:"category"`
}

// AggregationInfo describes an aggregation function.
type AggregationInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	SupportsTime bool  `json:"supports_time"`
}

// OperatorInfo describes an operator.
type OperatorInfo struct {
	Symbol      string `json:"symbol"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Types       []string `json:"types"` // Applicable types
}

// ArgInfo describes a function argument.
type ArgInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
	Default  interface{} `json:"default,omitempty"`
}

// GeneratedQuery represents a generated query in various formats.
type GeneratedQuery struct {
	SQL       string `json:"sql,omitempty"`
	PromQL    string `json:"promql,omitempty"`
	Internal  *Query `json:"internal,omitempty"`
	Validated bool   `json:"validated"`
	Errors    []string `json:"errors,omitempty"`
	Warnings  []string `json:"warnings,omitempty"`
}

// AutocompleteRequest for suggestions.
type AutocompleteRequest struct {
	Type    string `json:"type"`    // metric, tag_key, tag_value, function
	Prefix  string `json:"prefix"`  // Search prefix
	Context *VisualQuerySpec `json:"context,omitempty"` // Current query context
	TagKey  string `json:"tag_key,omitempty"` // For tag value completion
}

// AutocompleteResponse contains suggestions.
type AutocompleteResponse struct {
	Suggestions []Suggestion `json:"suggestions"`
	HasMore     bool         `json:"has_more"`
}

// Suggestion represents an autocomplete suggestion.
type Suggestion struct {
	Value       string `json:"value"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
	Type        string `json:"type"`
	Score       float64 `json:"score,omitempty"`
}

// NewVisualQueryBuilder creates a new visual query builder.
func NewVisualQueryBuilder(db *DB, config VisualQueryBuilderConfig) *VisualQueryBuilder {
	return &VisualQueryBuilder{
		db:             db,
		config:         config,
		tagKeysCache:   make(map[string][]string),
		tagValuesCache: make(map[string]map[string][]string),
	}
}

// GetSchema returns the query building schema.
func (b *VisualQueryBuilder) GetSchema(ctx context.Context) (*QuerySchema, error) {
	schema := &QuerySchema{
		Functions:     b.getAvailableFunctions(),
		Aggregations:  b.getAvailableAggregations(),
		Operators:     b.getAvailableOperators(),
		TimeIntervals: []string{"1s", "5s", "10s", "30s", "1m", "5m", "10m", "30m", "1h", "6h", "12h", "1d", "7d", "30d"},
	}

	// Get metrics
	metrics, err := b.getMetrics(ctx)
	if err != nil {
		return nil, err
	}
	
	for _, m := range metrics {
		schema.Metrics = append(schema.Metrics, MetricInfo{Name: m})
	}

	// Get tag keys
	schema.TagKeys, _ = b.getTagKeys(ctx, "")

	return schema, nil
}

// Autocomplete provides suggestions for query building.
func (b *VisualQueryBuilder) Autocomplete(ctx context.Context, req *AutocompleteRequest) (*AutocompleteResponse, error) {
	var suggestions []Suggestion

	switch req.Type {
	case "metric":
		metrics, err := b.getMetrics(ctx)
		if err != nil {
			return nil, err
		}
		for _, m := range metrics {
			if strings.HasPrefix(strings.ToLower(m), strings.ToLower(req.Prefix)) {
				suggestions = append(suggestions, Suggestion{
					Value: m,
					Label: m,
					Type:  "metric",
				})
			}
		}

	case "tag_key":
		keys, err := b.getTagKeys(ctx, "")
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			if strings.HasPrefix(strings.ToLower(k), strings.ToLower(req.Prefix)) {
				suggestions = append(suggestions, Suggestion{
					Value: k,
					Label: k,
					Type:  "tag_key",
				})
			}
		}

	case "tag_value":
		if req.TagKey == "" {
			return nil, fmt.Errorf("tag_key required for tag_value autocomplete")
		}
		values, err := b.getTagValues(ctx, "", req.TagKey)
		if err != nil {
			return nil, err
		}
		for _, v := range values {
			if strings.HasPrefix(strings.ToLower(v), strings.ToLower(req.Prefix)) {
				suggestions = append(suggestions, Suggestion{
					Value: v,
					Label: v,
					Type:  "tag_value",
				})
			}
		}

	case "function":
		for _, f := range b.getAvailableFunctions() {
			if strings.HasPrefix(strings.ToLower(f.Name), strings.ToLower(req.Prefix)) {
				suggestions = append(suggestions, Suggestion{
					Value:       f.Name,
					Label:       f.Name,
					Description: f.Description,
					Type:        "function",
				})
			}
		}

	case "aggregation":
		for _, a := range b.getAvailableAggregations() {
			if strings.HasPrefix(strings.ToLower(a.Name), strings.ToLower(req.Prefix)) {
				suggestions = append(suggestions, Suggestion{
					Value:       a.Name,
					Label:       a.Name,
					Description: a.Description,
					Type:        "aggregation",
				})
			}
		}
	}

	// Sort by relevance
	sort.Slice(suggestions, func(i, j int) bool {
		return suggestions[i].Value < suggestions[j].Value
	})

	hasMore := len(suggestions) > b.config.AutocompleteLimit
	if hasMore {
		suggestions = suggestions[:b.config.AutocompleteLimit]
	}

	return &AutocompleteResponse{
		Suggestions: suggestions,
		HasMore:     hasMore,
	}, nil
}

// GenerateQuery converts a visual query to executable query formats.
func (b *VisualQueryBuilder) GenerateQuery(ctx context.Context, vq *VisualQuerySpec) (*GeneratedQuery, error) {
	result := &GeneratedQuery{
		Validated: true,
	}

	// Validate query
	if b.config.EnableValidation {
		errors, warnings := b.validateQuery(vq)
		result.Errors = errors
		result.Warnings = warnings
		if len(errors) > 0 {
			result.Validated = false
			return result, nil
		}
	}

	// Generate SQL
	sql, err := b.generateSQL(vq)
	if err != nil {
		result.Errors = append(result.Errors, err.Error())
		result.Validated = false
		return result, nil
	}
	result.SQL = sql

	// Generate PromQL
	promql, err := b.generatePromQL(vq)
	if err == nil {
		result.PromQL = promql
	}

	// Generate internal query
	internal, err := b.generateInternalQuery(vq)
	if err == nil {
		result.Internal = internal
	}

	return result, nil
}

// ValidateQuery validates a visual query.
func (b *VisualQueryBuilder) ValidateQuery(vq *VisualQuerySpec) ([]string, []string) {
	return b.validateQuery(vq)
}

func (b *VisualQueryBuilder) validateQuery(vq *VisualQuerySpec) ([]string, []string) {
	var errors, warnings []string

	if len(vq.Components) == 0 {
		errors = append(errors, "query must have at least one component")
		return errors, warnings
	}

	hasMetric := false
	hasTimeRange := false
	complexity := 0

	for _, comp := range vq.Components {
		complexity++
		
		switch comp.Type {
		case ComponentMetric:
			hasMetric = true
			props, ok := comp.Properties.(map[string]interface{})
			if ok {
				if _, exists := props["name"]; !exists {
					errors = append(errors, "metric component must have a name")
				}
			}

		case ComponentTimeRange:
			hasTimeRange = true

		case ComponentFilter:
			props, ok := comp.Properties.(map[string]interface{})
			if ok {
				if _, exists := props["field"]; !exists {
					errors = append(errors, "filter must have a field")
				}
				if _, exists := props["operator"]; !exists {
					errors = append(errors, "filter must have an operator")
				}
			}

		case ComponentAggregation:
			props, ok := comp.Properties.(map[string]interface{})
			if ok {
				if _, exists := props["function"]; !exists {
					errors = append(errors, "aggregation must have a function")
				}
			}

		case ComponentSubquery:
			complexity += 2 // Subqueries add more complexity
		}
	}

	if !hasMetric {
		errors = append(errors, "query must include at least one metric")
	}

	if !hasTimeRange {
		warnings = append(warnings, "no time range specified, will use default")
	}

	if complexity > b.config.MaxQueryComplexity {
		errors = append(errors, fmt.Sprintf("query complexity %d exceeds maximum %d", complexity, b.config.MaxQueryComplexity))
	}

	return errors, warnings
}

func (b *VisualQueryBuilder) generateSQL(vq *VisualQuerySpec) (string, error) {
	var builder strings.Builder
	builder.WriteString("SELECT ")

	// Extract components by type
	var metrics []*MetricComponent
	var filters []*FilterComponent
	var aggregations []*AggregationComponent
	var groupBys []*GroupByComponent
	var timeRange *TimeRangeComponent
	var limit int

	for _, comp := range vq.Components {
		switch comp.Type {
		case ComponentMetric:
			m := parseMetricComponent(comp.Properties)
			if m != nil {
				metrics = append(metrics, m)
			}
		case ComponentFilter:
			f := parseFilterComponent(comp.Properties)
			if f != nil {
				filters = append(filters, f)
			}
		case ComponentAggregation:
			a := parseAggregationComponent(comp.Properties)
			if a != nil {
				aggregations = append(aggregations, a)
			}
		case ComponentGroupBy:
			g := parseGroupByComponent(comp.Properties)
			if g != nil {
				groupBys = append(groupBys, g)
			}
		case ComponentTimeRange:
			timeRange = parseTimeRangeComponent(comp.Properties)
		case ComponentLimit:
			limit = parseLimitComponent(comp.Properties)
		}
	}

	// Build SELECT clause
	selectClauses := []string{"time"}
	for _, m := range metrics {
		field := "value"
		if m.Alias != "" {
			field = fmt.Sprintf("value AS %s", m.Alias)
		}
		if len(aggregations) > 0 {
			for _, agg := range aggregations {
				field = fmt.Sprintf("%s(value) AS %s_%s", agg.Function, m.Name, agg.Function)
			}
		}
		selectClauses = append(selectClauses, field)
	}
	builder.WriteString(strings.Join(selectClauses, ", "))

	// Build FROM clause
	if len(metrics) > 0 {
		builder.WriteString(fmt.Sprintf(" FROM %s", metrics[0].Name))
	}

	// Build WHERE clause
	var whereClauses []string
	if timeRange != nil {
		if timeRange.Start != nil {
			whereClauses = append(whereClauses, fmt.Sprintf("time >= '%s'", timeRange.Start.Format(time.RFC3339)))
		}
		if timeRange.End != nil {
			whereClauses = append(whereClauses, fmt.Sprintf("time <= '%s'", timeRange.End.Format(time.RFC3339)))
		}
		if timeRange.Relative != "" {
			whereClauses = append(whereClauses, fmt.Sprintf("time >= now() - %s", timeRange.Relative))
		}
	}

	for _, m := range metrics {
		for k, v := range m.Tags {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = '%s'", k, v))
		}
	}

	for _, f := range filters {
		clause := formatFilterClause(f)
		if clause != "" {
			whereClauses = append(whereClauses, clause)
		}
	}

	if len(whereClauses) > 0 {
		builder.WriteString(" WHERE ")
		builder.WriteString(strings.Join(whereClauses, " AND "))
	}

	// Build GROUP BY clause
	if len(groupBys) > 0 || (len(aggregations) > 0 && aggregations[0].Interval != "") {
		var groupFields []string
		for _, g := range groupBys {
			groupFields = append(groupFields, g.Fields...)
			if g.Interval != "" {
				groupFields = append(groupFields, fmt.Sprintf("time(%s)", g.Interval))
			}
		}
		if len(aggregations) > 0 && aggregations[0].Interval != "" {
			groupFields = append(groupFields, fmt.Sprintf("time(%s)", aggregations[0].Interval))
		}
		if len(groupFields) > 0 {
			builder.WriteString(" GROUP BY ")
			builder.WriteString(strings.Join(groupFields, ", "))
		}
	}

	// Build LIMIT clause
	if limit > 0 {
		builder.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	}

	return builder.String(), nil
}

func (b *VisualQueryBuilder) generatePromQL(vq *VisualQuerySpec) (string, error) {
	var builder strings.Builder

	// Extract components
	var metrics []*MetricComponent
	var filters []*FilterComponent
	var aggregations []*AggregationComponent
	var timeRange *TimeRangeComponent

	for _, comp := range vq.Components {
		switch comp.Type {
		case ComponentMetric:
			m := parseMetricComponent(comp.Properties)
			if m != nil {
				metrics = append(metrics, m)
			}
		case ComponentFilter:
			f := parseFilterComponent(comp.Properties)
			if f != nil {
				filters = append(filters, f)
			}
		case ComponentAggregation:
			a := parseAggregationComponent(comp.Properties)
			if a != nil {
				aggregations = append(aggregations, a)
			}
		case ComponentTimeRange:
			timeRange = parseTimeRangeComponent(comp.Properties)
		}
	}

	if len(metrics) == 0 {
		return "", fmt.Errorf("no metric specified")
	}

	// Build metric selector
	metric := metrics[0]
	builder.WriteString(metric.Name)

	// Build label matchers
	var matchers []string
	for k, v := range metric.Tags {
		matchers = append(matchers, fmt.Sprintf(`%s="%s"`, k, v))
	}
	for _, f := range filters {
		matcher := formatPromQLMatcher(f)
		if matcher != "" {
			matchers = append(matchers, matcher)
		}
	}
	if len(matchers) > 0 {
		builder.WriteString("{")
		builder.WriteString(strings.Join(matchers, ", "))
		builder.WriteString("}")
	}

	// Add time range
	if timeRange != nil && timeRange.Relative != "" {
		builder.WriteString(fmt.Sprintf("[%s]", timeRange.Relative))
	}

	// Wrap with aggregations
	result := builder.String()
	for _, agg := range aggregations {
		result = fmt.Sprintf("%s(%s)", mapAggToPromQL(agg.Function), result)
	}

	return result, nil
}

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
func (b *VisualQueryBuilder) CreateComponent(compType ComponentType, properties interface{}) *QueryComponent {
	return &QueryComponent{
		ID:         generateID(),
		Type:       compType,
		Properties: properties,
	}
}

// CreateVisualQuerySpec creates a new visual query.
func (b *VisualQueryBuilder) CreateVisualQuery(name string) *VisualQuerySpec {
	now := time.Now()
	return &VisualQuerySpec{
		ID:        generateID(),
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(schema)
}

// HandleAutocomplete handles POST /api/query-builder/autocomplete
func (b *VisualQueryBuilder) HandleAutocomplete(w http.ResponseWriter, r *http.Request) {
	var req AutocompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := b.Autocomplete(r.Context(), &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// HandleGenerateQuery handles POST /api/query-builder/generate
func (b *VisualQueryBuilder) HandleGenerateQuery(w http.ResponseWriter, r *http.Request) {
	var vq VisualQuerySpec
	if err := json.NewDecoder(r.Body).Decode(&vq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := b.GenerateQuery(r.Context(), &vq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// HandleValidateQuery handles POST /api/query-builder/validate
func (b *VisualQueryBuilder) HandleValidateQuery(w http.ResponseWriter, r *http.Request) {
	var vq VisualQuerySpec
	if err := json.NewDecoder(r.Body).Decode(&vq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	errors, warnings := b.ValidateQuery(&vq)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	vq, err := b.ParseFromSQL(req.SQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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

func parseMetricComponent(props interface{}) *MetricComponent {
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

func parseFilterComponent(props interface{}) *FilterComponent {
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

func parseAggregationComponent(props interface{}) *AggregationComponent {
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

func parseGroupByComponent(props interface{}) *GroupByComponent {
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

func parseTimeRangeComponent(props interface{}) *TimeRangeComponent {
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

func parseLimitComponent(props interface{}) int {
	if props == nil {
		return 0
	}
	
	switch v := props.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case map[string]interface{}:
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

	value := f.Value
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("%s %s '%s'", f.Field, f.Operator, v)
	case float64:
		return fmt.Sprintf("%s %s %v", f.Field, f.Operator, v)
	default:
		return fmt.Sprintf("%s %s %v", f.Field, f.Operator, v)
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
		"avg":     "avg",
		"sum":     "sum",
		"min":     "min",
		"max":     "max",
		"count":   "count",
		"stddev":  "stddev",
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
		"avg":   AggMean,
		"sum":   AggSum,
		"min":   AggMin,
		"max":   AggMax,
		"count": AggCount,
		"first": AggFirst,
		"last":  AggLast,
		"stddev": AggStddev,
	}
	if f, ok := mapping[strings.ToLower(s)]; ok {
		return f
	}
	return AggNone
}
