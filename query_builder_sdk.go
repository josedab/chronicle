package chronicle

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// validRelativeTimeSDKRe matches safe relative time expressions like "1h", "30m", "7d", "2w".
var validRelativeTimeSDKRe = regexp.MustCompile(`^[0-9]+[smhdw]$`)

// allowedAggFunctionsSDK is the whitelist of permitted SQL aggregation function names for the query builder.
var allowedAggFunctionsSDK = map[string]bool{
	"mean": true, "avg": true, "sum": true, "count": true,
	"min": true, "max": true, "first": true, "last": true,
	"stddev": true, "median": true, "percentile": true,
	"rate": true, "irate": true, "increase": true,
	"derivative": true, "non_negative_derivative": true,
	"mode": true, "spread": true,
}

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
	metricsCache   []string
	tagKeysCache   map[string][]string
	tagValuesCache map[string]map[string][]string
	cacheTime      time.Time
	cacheMu        sync.RWMutex
}

// QueryComponent represents a building block of a visual query.
type QueryComponent struct {
	Type       ComponentType `json:"type"`
	ID         string        `json:"id"`
	Properties any           `json:"properties"`
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
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    any    `json:"value"`
	Logic    string `json:"logic,omitempty"` // AND, OR
}

// AggregationComponent represents an aggregation function.
type AggregationComponent struct {
	Function string `json:"function"` // avg, sum, min, max, count, etc.
	Field    string `json:"field,omitempty"`
	Interval string `json:"interval,omitempty"` // For time aggregations
	Fill     string `json:"fill,omitempty"`     // none, null, previous, linear
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
	Name      string `json:"name"`
	Arguments []any  `json:"arguments"`
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
	Metrics       []MetricInfo      `json:"metrics"`
	TagKeys       []string          `json:"tag_keys"`
	Functions     []VQBFunctionInfo `json:"functions"`
	Aggregations  []AggregationInfo `json:"aggregations"`
	Operators     []OperatorInfo    `json:"operators"`
	TimeIntervals []string          `json:"time_intervals"`
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
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Arguments   []ArgInfo `json:"arguments"`
	ReturnType  string    `json:"return_type"`
	Category    string    `json:"category"`
}

// AggregationInfo describes an aggregation function.
type AggregationInfo struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	SupportsTime bool   `json:"supports_time"`
}

// OperatorInfo describes an operator.
type OperatorInfo struct {
	Symbol      string   `json:"symbol"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Types       []string `json:"types"` // Applicable types
}

// ArgInfo describes a function argument.
type ArgInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
	Default  any    `json:"default,omitempty"`
}

// GeneratedQuery represents a generated query in various formats.
type GeneratedQuery struct {
	SQL       string   `json:"sql,omitempty"`
	PromQL    string   `json:"promql,omitempty"`
	Internal  *Query   `json:"internal,omitempty"`
	Validated bool     `json:"validated"`
	Errors    []string `json:"errors,omitempty"`
	Warnings  []string `json:"warnings,omitempty"`
}

// AutocompleteRequest for suggestions.
type AutocompleteRequest struct {
	Type    string           `json:"type"`              // metric, tag_key, tag_value, function
	Prefix  string           `json:"prefix"`            // Search prefix
	Context *VisualQuerySpec `json:"context,omitempty"` // Current query context
	TagKey  string           `json:"tag_key,omitempty"` // For tag value completion
}

// AutocompleteResponse contains suggestions.
type AutocompleteResponse struct {
	Suggestions []Suggestion `json:"suggestions"`
	HasMore     bool         `json:"has_more"`
}

// Suggestion represents an autocomplete suggestion.
type Suggestion struct {
	Value       string  `json:"value"`
	Label       string  `json:"label"`
	Description string  `json:"description,omitempty"`
	Type        string  `json:"type"`
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
	schema.TagKeys, _ = b.getTagKeys(ctx, "") //nolint:errcheck // tag keys are optional metadata

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
			props, ok := comp.Properties.(map[string]any)
			if ok {
				if _, exists := props["name"]; !exists {
					errors = append(errors, "metric component must have a name")
				}
			}

		case ComponentTimeRange:
			hasTimeRange = true

		case ComponentFilter:
			props, ok := comp.Properties.(map[string]any)
			if ok {
				if _, exists := props["field"]; !exists {
					errors = append(errors, "filter must have a field")
				}
				if _, exists := props["operator"]; !exists {
					errors = append(errors, "filter must have an operator")
				}
			}

		case ComponentAggregation:
			props, ok := comp.Properties.(map[string]any)
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
			field = fmt.Sprintf("value AS %s", sanitizeIdentifier(m.Alias))
		}
		if len(aggregations) > 0 {
			for _, agg := range aggregations {
				fn := strings.ToLower(agg.Function)
				if !allowedAggFunctionsSDK[fn] {
					fn = "mean"
				}
				field = fmt.Sprintf("%s(value) AS %s_%s", fn, sanitizeIdentifier(m.Name), fn)
			}
		}
		selectClauses = append(selectClauses, field)
	}
	builder.WriteString(strings.Join(selectClauses, ", "))

	// Build FROM clause
	if len(metrics) > 0 {
		builder.WriteString(fmt.Sprintf(" FROM %s", sanitizeIdentifier(metrics[0].Name)))
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
			if validRelativeTimeSDKRe.MatchString(timeRange.Relative) {
				whereClauses = append(whereClauses, fmt.Sprintf("time >= now() - %s", timeRange.Relative))
			}
		}
	}

	for _, m := range metrics {
		for k, v := range m.Tags {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = '%s'", sanitizeIdentifier(k), escapeSQL(v)))
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
