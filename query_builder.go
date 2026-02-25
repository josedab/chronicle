package chronicle

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// QueryBuilderConfig configures the visual query builder.
type QueryBuilderConfig struct {
	// Enabled enables the query builder
	Enabled bool `json:"enabled"`

	// MaxSavedQueries maximum saved queries per user
	MaxSavedQueries int `json:"max_saved_queries"`

	// EnableSharing allows query sharing
	EnableSharing bool `json:"enable_sharing"`

	// AutocompleteLimit suggestions limit
	AutocompleteLimit int `json:"autocomplete_limit"`

	// QueryHistorySize queries to keep in history
	QueryHistorySize int `json:"query_history_size"`
}

// DefaultQueryBuilderConfig returns default configuration.
func DefaultQueryBuilderConfig() QueryBuilderConfig {
	return QueryBuilderConfig{
		Enabled:           true,
		MaxSavedQueries:   100,
		EnableSharing:     true,
		AutocompleteLimit: 50,
		QueryHistorySize:  500,
	}
}

// QueryBuilder provides a visual query building interface.
type QueryBuilder struct {
	db     *DB
	config QueryBuilderConfig
	mu     sync.RWMutex

	// Saved queries
	savedQueries map[string]*SavedQuery

	// Query history
	history []*QueryHistoryEntry

	// Query templates
	templates map[string]*QueryTemplate
}

// SavedQuery represents a saved query.
type SavedQuery struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Query       *VisualQuery `json:"query"`
	Owner       string       `json:"owner"`
	IsPublic    bool         `json:"is_public"`
	Tags        []string     `json:"tags"`
	Created     time.Time    `json:"created"`
	Updated     time.Time    `json:"updated"`
	UsageCount  int          `json:"usage_count"`
}

// QueryHistoryEntry represents a query execution.
type QueryHistoryEntry struct {
	ID         string       `json:"id"`
	Query      *VisualQuery `json:"query"`
	SQL        string       `json:"sql"`
	ExecutedAt time.Time    `json:"executed_at"`
	Duration   int64        `json:"duration_ms"`
	RowCount   int          `json:"row_count"`
	User       string       `json:"user"`
	Success    bool         `json:"success"`
	ErrorMsg   string       `json:"error_msg,omitempty"`
}

// QueryTemplate provides reusable query patterns.
type QueryTemplate struct {
	ID          string              `json:"id"`
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Category    string              `json:"category"`
	Query       *VisualQuery        `json:"query"`
	Parameters  []TemplateParameter `json:"parameters"`
}

// TemplateParameter defines a template parameter.
type TemplateParameter struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Description  string `json:"description"`
	DefaultValue any    `json:"default_value"`
	Required     bool   `json:"required"`
}

// VisualQuery represents a query built visually.
type VisualQuery struct {
	// Data source
	Source QuerySource `json:"source"`

	// Selected fields/metrics
	Select []SelectItem `json:"select"`

	// Filters
	Filters []QueryFilter `json:"filters"`

	// Grouping
	GroupBy []string `json:"group_by,omitempty"`

	// Ordering
	OrderBy []OrderItem `json:"order_by,omitempty"`

	// Time range
	TimeRange *QueryTimeRange `json:"time_range,omitempty"`

	// Limit
	Limit int `json:"limit,omitempty"`

	// Joins
	Joins []QueryJoin `json:"joins,omitempty"`

	// Subqueries
	Subqueries map[string]*VisualQuery `json:"subqueries,omitempty"`
}

// QuerySource identifies the data source.
type QuerySource struct {
	Type  string `json:"type"` // "metric", "logs", "traces", "events"
	Name  string `json:"name"`
	Alias string `json:"alias,omitempty"`
}

// SelectItem represents a selected field or aggregation.
type SelectItem struct {
	Field       string `json:"field"`
	Alias       string `json:"alias,omitempty"`
	Aggregation string `json:"aggregation,omitempty"` // sum, avg, count, min, max, etc.
	Distinct    bool   `json:"distinct,omitempty"`
}

// QueryFilter represents a filter condition.
type QueryFilter struct {
	Field    string `json:"field"`
	Operator string `json:"operator"` // =, !=, >, <, >=, <=, in, not_in, like, regex
	Value    any    `json:"value"`
	AndOr    string `json:"and_or,omitempty"` // "AND" or "OR"
}

// OrderItem represents ordering.
type OrderItem struct {
	Field     string `json:"field"`
	Direction string `json:"direction"` // "asc" or "desc"
}

// QueryTimeRange represents time range.
type QueryTimeRange struct {
	Type     string `json:"type"` // "relative" or "absolute"
	Start    int64  `json:"start,omitempty"`
	End      int64  `json:"end,omitempty"`
	Relative string `json:"relative,omitempty"` // "1h", "24h", "7d", etc.
}

// QueryJoin represents a join.
type QueryJoin struct {
	Type      string      `json:"type"` // "inner", "left", "right", "outer"
	Source    QuerySource `json:"source"`
	Condition string      `json:"condition"`
}

// NewQueryBuilder creates a new query builder.
func NewQueryBuilder(db *DB, config QueryBuilderConfig) *QueryBuilder {
	qb := &QueryBuilder{
		db:           db,
		config:       config,
		savedQueries: make(map[string]*SavedQuery),
		history:      make([]*QueryHistoryEntry, 0),
		templates:    make(map[string]*QueryTemplate),
	}

	// Initialize built-in templates
	qb.initTemplates()

	return qb
}

func (qb *QueryBuilder) initTemplates() {
	qb.templates["avg-by-time"] = &QueryTemplate{
		ID:          "avg-by-time",
		Name:        "Average Over Time",
		Description: "Calculate average of a metric over time windows",
		Category:    "aggregation",
		Query: &VisualQuery{
			Source: QuerySource{Type: "metric", Name: "{{metric}}"},
			Select: []SelectItem{
				{Field: "value", Aggregation: "avg", Alias: "avg_value"},
			},
			GroupBy:   []string{"time_bucket(1m)"},
			TimeRange: &QueryTimeRange{Type: "relative", Relative: "1h"},
		},
		Parameters: []TemplateParameter{
			{Name: "metric", Type: "string", Description: "Metric name", Required: true},
		},
	}

	qb.templates["top-n"] = &QueryTemplate{
		ID:          "top-n",
		Name:        "Top N by Value",
		Description: "Find top N items by value",
		Category:    "ranking",
		Query: &VisualQuery{
			Source: QuerySource{Type: "metric", Name: "{{metric}}"},
			Select: []SelectItem{
				{Field: "{{group_field}}"},
				{Field: "value", Aggregation: "sum", Alias: "total"},
			},
			GroupBy: []string{"{{group_field}}"},
			OrderBy: []OrderItem{{Field: "total", Direction: "desc"}},
			Limit:   10,
		},
		Parameters: []TemplateParameter{
			{Name: "metric", Type: "string", Description: "Metric name", Required: true},
			{Name: "group_field", Type: "string", Description: "Field to group by", Required: true},
			{Name: "limit", Type: "integer", Description: "Number of results", DefaultValue: 10},
		},
	}

	qb.templates["error-rate"] = &QueryTemplate{
		ID:          "error-rate",
		Name:        "Error Rate",
		Description: "Calculate error rate percentage",
		Category:    "monitoring",
		Query: &VisualQuery{
			Source: QuerySource{Type: "metric", Name: "http_requests"},
			Select: []SelectItem{
				{Field: "count(*) FILTER (WHERE status >= 500)", Alias: "errors"},
				{Field: "count(*)", Alias: "total"},
			},
			TimeRange: &QueryTimeRange{Type: "relative", Relative: "1h"},
		},
		Parameters: []TemplateParameter{},
	}

	qb.templates["percentile"] = &QueryTemplate{
		ID:          "percentile",
		Name:        "Percentile Analysis",
		Description: "Calculate percentiles for a metric",
		Category:    "statistics",
		Query: &VisualQuery{
			Source: QuerySource{Type: "metric", Name: "{{metric}}"},
			Select: []SelectItem{
				{Field: "value", Aggregation: "p50", Alias: "p50"},
				{Field: "value", Aggregation: "p90", Alias: "p90"},
				{Field: "value", Aggregation: "p99", Alias: "p99"},
			},
			TimeRange: &QueryTimeRange{Type: "relative", Relative: "1h"},
		},
		Parameters: []TemplateParameter{
			{Name: "metric", Type: "string", Description: "Metric name", Required: true},
		},
	}
}

// BuildSQL converts a visual query to SQL.
func (qb *QueryBuilder) BuildSQL(vq *VisualQuery) (string, error) {
	if vq == nil {
		return "", errors.New("query cannot be nil")
	}

	var sql strings.Builder

	// SELECT clause
	sql.WriteString("SELECT ")
	if len(vq.Select) == 0 {
		sql.WriteString("*")
	} else {
		selectParts := make([]string, len(vq.Select))
		for i, s := range vq.Select {
			selectParts[i] = qb.buildSelectItem(s)
		}
		sql.WriteString(strings.Join(selectParts, ", "))
	}

	// FROM clause
	sql.WriteString("\nFROM ")
	sql.WriteString(vq.Source.Name)
	if vq.Source.Alias != "" {
		sql.WriteString(" AS ")
		sql.WriteString(vq.Source.Alias)
	}

	// JOIN clauses
	for _, join := range vq.Joins {
		sql.WriteString("\n")
		sql.WriteString(strings.ToUpper(join.Type))
		sql.WriteString(" JOIN ")
		sql.WriteString(join.Source.Name)
		if join.Source.Alias != "" {
			sql.WriteString(" AS ")
			sql.WriteString(join.Source.Alias)
		}
		sql.WriteString(" ON ")
		sql.WriteString(join.Condition)
	}

	// WHERE clause
	if len(vq.Filters) > 0 || vq.TimeRange != nil {
		sql.WriteString("\nWHERE ")
		conditions := qb.buildFilters(vq.Filters)

		// Add time range condition
		if vq.TimeRange != nil {
			timeCondition := qb.buildTimeCondition(vq.TimeRange)
			if timeCondition != "" {
				if len(conditions) > 0 {
					conditions = append(conditions, "AND")
				}
				conditions = append(conditions, timeCondition)
			}
		}

		sql.WriteString(strings.Join(conditions, " "))
	}

	// GROUP BY clause
	if len(vq.GroupBy) > 0 {
		sql.WriteString("\nGROUP BY ")
		sql.WriteString(strings.Join(vq.GroupBy, ", "))
	}

	// ORDER BY clause
	if len(vq.OrderBy) > 0 {
		sql.WriteString("\nORDER BY ")
		orderParts := make([]string, len(vq.OrderBy))
		for i, o := range vq.OrderBy {
			orderParts[i] = fmt.Sprintf("%s %s", o.Field, strings.ToUpper(o.Direction))
		}
		sql.WriteString(strings.Join(orderParts, ", "))
	}

	// LIMIT clause
	if vq.Limit > 0 {
		sql.WriteString(fmt.Sprintf("\nLIMIT %d", vq.Limit))
	}

	return sql.String(), nil
}

func (qb *QueryBuilder) buildSelectItem(s SelectItem) string {
	var result string

	if s.Aggregation != "" {
		if s.Distinct {
			result = fmt.Sprintf("%s(DISTINCT %s)", strings.ToUpper(s.Aggregation), s.Field)
		} else {
			result = fmt.Sprintf("%s(%s)", strings.ToUpper(s.Aggregation), s.Field)
		}
	} else {
		result = s.Field
	}

	if s.Alias != "" {
		result += " AS " + s.Alias
	}

	return result
}

func (qb *QueryBuilder) buildFilters(filters []QueryFilter) []string {
	var conditions []string

	for i, f := range filters {
		condition := qb.buildFilterCondition(f)

		if i > 0 && f.AndOr != "" {
			conditions = append(conditions, f.AndOr)
		}
		conditions = append(conditions, condition)
	}

	return conditions
}

func (qb *QueryBuilder) buildFilterCondition(f QueryFilter) string {
	valueStr := formatValue(f.Value)

	switch strings.ToLower(f.Operator) {
	case "=", "==":
		return fmt.Sprintf("%s = %s", f.Field, valueStr)
	case "!=", "<>":
		return fmt.Sprintf("%s != %s", f.Field, valueStr)
	case ">":
		return fmt.Sprintf("%s > %s", f.Field, valueStr)
	case ">=":
		return fmt.Sprintf("%s >= %s", f.Field, valueStr)
	case "<":
		return fmt.Sprintf("%s < %s", f.Field, valueStr)
	case "<=":
		return fmt.Sprintf("%s <= %s", f.Field, valueStr)
	case "in":
		return fmt.Sprintf("%s IN (%s)", f.Field, valueStr)
	case "not_in":
		return fmt.Sprintf("%s NOT IN (%s)", f.Field, valueStr)
	case "like":
		return fmt.Sprintf("%s LIKE %s", f.Field, valueStr)
	case "regex":
		return fmt.Sprintf("%s ~ %s", f.Field, valueStr)
	case "is_null":
		return fmt.Sprintf("%s IS NULL", f.Field)
	case "is_not_null":
		return fmt.Sprintf("%s IS NOT NULL", f.Field)
	default:
		return fmt.Sprintf("%s %s %s", f.Field, f.Operator, valueStr)
	}
}

func formatValue(v any) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''"))
	case []any:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = formatValue(item)
		}
		return strings.Join(parts, ", ")
	case []string:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = fmt.Sprintf("'%s'", item)
		}
		return strings.Join(parts, ", ")
	default:
		return fmt.Sprintf("%v", val)
	}
}

func (qb *QueryBuilder) buildTimeCondition(tr *QueryTimeRange) string {
	if tr.Type == "relative" {
		return fmt.Sprintf("timestamp >= NOW() - INTERVAL '%s'", tr.Relative)
	}

	if tr.Start > 0 && tr.End > 0 {
		return fmt.Sprintf("timestamp BETWEEN %d AND %d", tr.Start, tr.End)
	} else if tr.Start > 0 {
		return fmt.Sprintf("timestamp >= %d", tr.Start)
	} else if tr.End > 0 {
		return fmt.Sprintf("timestamp <= %d", tr.End)
	}

	return ""
}

// ValidateQuery validates a visual query.
func (qb *QueryBuilder) ValidateQuery(vq *VisualQuery) []QueryValidationError {
	var errs []QueryValidationError

	if vq.Source.Name == "" {
		errs = append(errs, QueryValidationError{
			Field:   "source.name",
			Message: "Source name is required",
		})
	}

	// Validate filters
	for i, f := range vq.Filters {
		if f.Field == "" {
			errs = append(errs, QueryValidationError{
				Field:   fmt.Sprintf("filters[%d].field", i),
				Message: "Filter field is required",
			})
		}
		if f.Operator == "" {
			errs = append(errs, QueryValidationError{
				Field:   fmt.Sprintf("filters[%d].operator", i),
				Message: "Filter operator is required",
			})
		}
	}

	// Validate limit
	if vq.Limit < 0 {
		errs = append(errs, QueryValidationError{
			Field:   "limit",
			Message: "Limit cannot be negative",
		})
	}

	return errs
}
