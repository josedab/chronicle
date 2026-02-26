package chronicle

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ClickHouseServer provides ClickHouse-compatible HTTP interface.
// This enables Chronicle to work with existing ClickHouse clients, visualization tools,
// and BI integrations without modification.
type ClickHouseServer struct {
	db     *DB
	config ClickHouseConfig
}

// ClickHouseConfig configures the ClickHouse-compatible interface.
type ClickHouseConfig struct {
	// Enabled enables the ClickHouse HTTP interface.
	Enabled bool

	// DefaultDatabase is the default database name returned to clients.
	DefaultDatabase string

	// DefaultFormat is the default output format (TabSeparated, JSON, JSONEachRow).
	DefaultFormat ClickHouseFormat

	// MaxQuerySize is the maximum query size in bytes.
	MaxQuerySize int64

	// MaxResultRows limits the number of rows returned.
	MaxResultRows int
}

// ClickHouseFormat represents output formats supported by ClickHouse protocol.
type ClickHouseFormat int

const (
	// ClickHouseFormatTabSeparated outputs data as tab-separated values.
	ClickHouseFormatTabSeparated ClickHouseFormat = iota
	// ClickHouseFormatJSON outputs data as a JSON object with metadata.
	ClickHouseFormatJSON
	// ClickHouseFormatJSONEachRow outputs each row as a separate JSON object.
	ClickHouseFormatJSONEachRow
	// ClickHouseFormatJSONCompact outputs JSON with arrays instead of objects.
	ClickHouseFormatJSONCompact
	// ClickHouseFormatCSV outputs comma-separated values.
	ClickHouseFormatCSV
)

// DefaultClickHouseConfig returns default ClickHouse configuration.
func DefaultClickHouseConfig() ClickHouseConfig {
	return ClickHouseConfig{
		Enabled:         false,
		DefaultDatabase: "default",
		DefaultFormat:   ClickHouseFormatTabSeparated,
		MaxQuerySize:    1024 * 1024, // 1MB
		MaxResultRows:   100000,
	}
}

// NewClickHouseServer creates a new ClickHouse-compatible server.
func NewClickHouseServer(db *DB, config ClickHouseConfig) *ClickHouseServer {
	return &ClickHouseServer{
		db:     db,
		config: config,
	}
}

// Handler returns the HTTP handler for ClickHouse protocol.
func (s *ClickHouseServer) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.handleRequest(w, r)
	}
}

// handleRequest processes ClickHouse HTTP protocol requests.
func (s *ClickHouseServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	// Extract query from URL parameter or body
	query := r.URL.Query().Get("query")
	format := s.parseFormat(r.URL.Query().Get("default_format"))

	if r.Method == http.MethodPost {
		body, err := io.ReadAll(io.LimitReader(r.Body, s.config.MaxQuerySize))
		if err != nil {
			s.writeError(w, err, http.StatusBadRequest)
			return
		}
		if query == "" {
			query = string(body)
		} else {
			// Append body as data for INSERT
			query = query + " " + string(body)
		}
	}

	query = strings.TrimSpace(query)
	if query == "" {
		s.writeError(w, fmt.Errorf("query is empty"), http.StatusBadRequest)
		return
	}

	// Handle special system queries
	if s.handleSystemQuery(w, query, format) {
		return
	}

	// Parse and execute
	result, columns, err := s.executeQuery(r.Context(), query)
	if err != nil {
		s.writeError(w, err, http.StatusBadRequest)
		return
	}

	s.writeResult(w, result, columns, format)
}

// ClickHouseColumn represents column metadata.
type ClickHouseColumn struct {
	Name string
	Type string
}

// handleSystemQuery handles ClickHouse system queries that don't touch data.
func (s *ClickHouseServer) handleSystemQuery(w http.ResponseWriter, query string, format ClickHouseFormat) bool {
	queryLower := strings.ToLower(strings.TrimSpace(query))

	// Ping check
	if queryLower == "select 1" || queryLower == "select 1;" {
		s.writeResult(w, [][]any{{1}}, []ClickHouseColumn{{Name: "1", Type: "UInt8"}}, format)
		return true
	}

	// Version query
	if strings.Contains(queryLower, "version()") {
		s.writeResult(w, [][]any{{"Chronicle/1.0.0 (ClickHouse-compatible)"}},
			[]ClickHouseColumn{{Name: "version()", Type: "String"}}, format)
		return true
	}

	// Database list
	if strings.HasPrefix(queryLower, "show databases") {
		s.writeResult(w, [][]any{{s.config.DefaultDatabase}},
			[]ClickHouseColumn{{Name: "name", Type: "String"}}, format)
		return true
	}

	// Tables list (returns metrics)
	if strings.HasPrefix(queryLower, "show tables") {
		metrics := s.db.Metrics()
		rows := make([][]any, len(metrics))
		for i, m := range metrics {
			rows[i] = []any{m}
		}
		s.writeResult(w, rows, []ClickHouseColumn{{Name: "name", Type: "String"}}, format)
		return true
	}

	// Database selection
	if strings.HasPrefix(queryLower, "use ") {
		w.WriteHeader(http.StatusOK)
		return true
	}

	// System settings
	if strings.HasPrefix(queryLower, "set ") {
		w.WriteHeader(http.StatusOK)
		return true
	}

	// System queries for ClickHouse clients
	if strings.Contains(queryLower, "system.") {
		return s.handleSystemTableQuery(w, queryLower, format)
	}

	return false
}

// handleSystemTableQuery handles queries to system tables.
func (s *ClickHouseServer) handleSystemTableQuery(w http.ResponseWriter, query string, format ClickHouseFormat) bool {
	switch {
	case strings.Contains(query, "system.databases"):
		s.writeResult(w, [][]any{{s.config.DefaultDatabase}},
			[]ClickHouseColumn{{Name: "name", Type: "String"}}, format)
		return true

	case strings.Contains(query, "system.tables"):
		metrics := s.db.Metrics()
		rows := make([][]any, len(metrics))
		for i, m := range metrics {
			rows[i] = []any{s.config.DefaultDatabase, m, "MergeTree"}
		}
		s.writeResult(w, rows, []ClickHouseColumn{
			{Name: "database", Type: "String"},
			{Name: "name", Type: "String"},
			{Name: "engine", Type: "String"},
		}, format)
		return true

	case strings.Contains(query, "system.columns"):
		// Return standard time-series columns for all metrics
		rows := [][]any{
			{s.config.DefaultDatabase, "", "timestamp", "DateTime64(9)"},
			{s.config.DefaultDatabase, "", "metric", "String"},
			{s.config.DefaultDatabase, "", "value", "Float64"},
			{s.config.DefaultDatabase, "", "tags", "Map(String, String)"},
		}
		s.writeResult(w, rows, []ClickHouseColumn{
			{Name: "database", Type: "String"},
			{Name: "table", Type: "String"},
			{Name: "name", Type: "String"},
			{Name: "type", Type: "String"},
		}, format)
		return true

	case strings.Contains(query, "system.settings"):
		s.writeResult(w, [][]any{},
			[]ClickHouseColumn{{Name: "name", Type: "String"}, {Name: "value", Type: "String"}}, format)
		return true
	}

	return false
}

// executeQuery translates and executes a ClickHouse SQL query.
func (s *ClickHouseServer) executeQuery(ctx context.Context, query string) ([][]any, []ClickHouseColumn, error) {
	// Parse ClickHouse SQL to Chronicle query
	chronicleQuery, selectFields, err := s.translateQuery(query)
	if err != nil {
		return nil, nil, fmt.Errorf("query translation failed: %w", err)
	}

	// Execute query
	result, err := s.db.ExecuteContext(ctx, chronicleQuery)
	if err != nil {
		return nil, nil, err
	}

	// Convert results to ClickHouse format
	return s.convertResult(result, selectFields, chronicleQuery)
}

// ClickHouseSelectField represents a field in a SELECT clause.
type ClickHouseSelectField struct {
	Name      string
	Alias     string
	IsAgg     bool
	AggFunc   string
	FieldName string
}

// translateQuery converts ClickHouse SQL to Chronicle query.
func (s *ClickHouseServer) translateQuery(sql string) (*Query, []ClickHouseSelectField, error) {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimSuffix(sql, ";")

	// Only support SELECT queries for now
	if !strings.HasPrefix(strings.ToUpper(sql), "SELECT") {
		return nil, nil, fmt.Errorf("only SELECT queries are supported")
	}

	// Parse SELECT ... FROM ... WHERE ... GROUP BY ... ORDER BY ... LIMIT ...
	parser := &clickHouseQueryParser{sql: sql, maxResultRows: s.config.MaxResultRows}
	return parser.parse()
}

// clickHouseQueryParser parses ClickHouse SQL queries.
type clickHouseQueryParser struct {
	sql           string
	maxResultRows int
}

func (p *clickHouseQueryParser) parse() (*Query, []ClickHouseSelectField, error) {
	sql := p.sql

	// Extract major clauses using regex
	selectPattern := regexp.MustCompile(`(?i)^SELECT\s+(.+?)\s+FROM\s+`)
	fromPattern := regexp.MustCompile(`(?i)\s+FROM\s+(\w+)`)
	wherePattern := regexp.MustCompile(`(?i)\s+WHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s*$)`)
	groupByPattern := regexp.MustCompile(`(?i)\s+GROUP\s+BY\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s*$)`)
	limitPattern := regexp.MustCompile(`(?i)\s+LIMIT\s+(\d+)`)

	// Extract SELECT fields
	selectMatch := selectPattern.FindStringSubmatch(sql)
	if selectMatch == nil {
		return nil, nil, fmt.Errorf("invalid SELECT clause")
	}
	selectFields := p.parseSelectFields(selectMatch[1])

	// Extract FROM (metric name)
	fromMatch := fromPattern.FindStringSubmatch(sql)
	if fromMatch == nil {
		return nil, nil, fmt.Errorf("missing FROM clause")
	}
	metric := fromMatch[1]

	query := &Query{
		Metric: metric,
		Start:  time.Now().Add(-time.Hour).UnixNano(),
		End:    time.Now().UnixNano(),
	}

	// Extract WHERE conditions
	if whereMatch := wherePattern.FindStringSubmatch(sql); whereMatch != nil {
		if err := p.parseWhere(whereMatch[1], query); err != nil {
			return nil, nil, err
		}
	}

	// Extract GROUP BY
	if groupMatch := groupByPattern.FindStringSubmatch(sql); groupMatch != nil {
		p.parseGroupBy(groupMatch[1], query, selectFields)
	}

	// Extract LIMIT
	if limitMatch := limitPattern.FindStringSubmatch(sql); limitMatch != nil {
		limit, _ := strconv.Atoi(limitMatch[1])
		query.Limit = limit
	}

	// Apply max result rows limit
	if p.maxResultRows > 0 && (query.Limit == 0 || query.Limit > p.maxResultRows) {
		query.Limit = p.maxResultRows
	}

	// Set aggregation from select fields
	for _, f := range selectFields {
		if f.IsAgg {
			aggFunc := p.translateAggFunc(f.AggFunc)
			if aggFunc != AggNone {
				query.Aggregation = &Aggregation{
					Function: aggFunc,
					Window:   time.Minute, // Default window
				}
				break
			}
		}
	}

	return query, selectFields, nil
}

func (p *clickHouseQueryParser) parseSelectFields(fieldsStr string) []ClickHouseSelectField {
	var fields []ClickHouseSelectField

	// Handle * case
	if strings.TrimSpace(fieldsStr) == "*" {
		return []ClickHouseSelectField{
			{Name: "timestamp", FieldName: "timestamp"},
			{Name: "value", FieldName: "value"},
			{Name: "tags", FieldName: "tags"},
		}
	}

	// Split by comma, handling parentheses
	parts := splitClickHouseFields(fieldsStr)

	aggPattern := regexp.MustCompile(`(?i)^(\w+)\s*\(\s*(\w+|\*)\s*\)(?:\s+AS\s+(\w+))?$`)
	aliasPattern := regexp.MustCompile(`(?i)^(\w+)\s+AS\s+(\w+)$`)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		field := ClickHouseSelectField{Name: part, FieldName: part}

		// Check for aggregation function
		if aggMatch := aggPattern.FindStringSubmatch(part); aggMatch != nil {
			field.IsAgg = true
			field.AggFunc = strings.ToLower(aggMatch[1])
			field.FieldName = aggMatch[2]
			if aggMatch[3] != "" {
				field.Alias = aggMatch[3]
				field.Name = aggMatch[3]
			} else {
				field.Name = part
			}
		} else if aliasMatch := aliasPattern.FindStringSubmatch(part); aliasMatch != nil {
			field.FieldName = aliasMatch[1]
			field.Alias = aliasMatch[2]
			field.Name = aliasMatch[2]
		}

		fields = append(fields, field)
	}

	return fields
}

func splitClickHouseFields(s string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, ch := range s {
		switch ch {
		case '(':
			depth++
			current.WriteRune(ch)
		case ')':
			depth--
			current.WriteRune(ch)
		case ',':
			if depth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}
