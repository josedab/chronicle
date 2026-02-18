package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
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

func (p *clickHouseQueryParser) parseWhere(whereStr string, query *Query) error {
	// Parse time conditions and tag filters
	conditions := strings.Split(whereStr, " AND ")

	timePattern := regexp.MustCompile(`(?i)(\w+)\s*(>=?|<=?|=)\s*(.+)`)

	for _, cond := range conditions {
		cond = strings.TrimSpace(cond)
		if cond == "" {
			continue
		}

		match := timePattern.FindStringSubmatch(cond)
		if match == nil {
			continue
		}

		field := strings.ToLower(match[1])
		op := match[2]
		value := strings.TrimSpace(match[3])

		// Handle time conditions
		if field == "timestamp" || field == "time" || field == "ts" {
			ts, err := p.parseTimeValue(value)
			if err != nil {
				continue
			}
			switch op {
			case ">=", ">":
				query.Start = ts
			case "<=", "<":
				query.End = ts
			case "=":
				query.Start = ts
				query.End = ts + int64(time.Millisecond)
			}
		} else {
			// Tag filter
			value = strings.Trim(value, "'\"")
			if query.Tags == nil {
				query.Tags = make(map[string]string)
			}
			query.Tags[field] = value
		}
	}

	return nil
}

func (p *clickHouseQueryParser) parseTimeValue(value string) (int64, error) {
	value = strings.TrimSpace(value)

	// Handle now() - interval
	if strings.HasPrefix(strings.ToLower(value), "now()") {
		now := time.Now().UnixNano()
		remaining := strings.TrimPrefix(strings.ToLower(value), "now()")
		remaining = strings.TrimSpace(remaining)

		if remaining == "" {
			return now, nil
		}

		// Parse interval subtraction: now() - INTERVAL 1 HOUR
		intervalPattern := regexp.MustCompile(`(?i)^-\s*(?:INTERVAL\s+)?(\d+)\s*(\w+)$`)
		if match := intervalPattern.FindStringSubmatch(remaining); match != nil {
			amount, _ := strconv.ParseInt(match[1], 10, 64)
			unit := strings.ToLower(match[2])
			duration := p.parseDurationUnit(amount, unit)
			return now - int64(duration), nil
		}
	}

	// Try parsing as Unix timestamp (seconds or nanoseconds)
	if ts, err := strconv.ParseInt(value, 10, 64); err == nil {
		if ts < 1e12 {
			return ts * int64(time.Second), nil
		}
		return ts, nil
	}

	// Try parsing as ISO date/time
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, value); err == nil {
			return t.UnixNano(), nil
		}
	}

	return 0, fmt.Errorf("cannot parse time value: %s", value)
}

func (p *clickHouseQueryParser) parseDurationUnit(amount int64, unit string) time.Duration {
	switch unit {
	case "second", "seconds", "s":
		return time.Duration(amount) * time.Second
	case "minute", "minutes", "m":
		return time.Duration(amount) * time.Minute
	case "hour", "hours", "h":
		return time.Duration(amount) * time.Hour
	case "day", "days", "d":
		return time.Duration(amount) * 24 * time.Hour
	case "week", "weeks", "w":
		return time.Duration(amount) * 7 * 24 * time.Hour
	default:
		return time.Duration(amount) * time.Second
	}
}

func (p *clickHouseQueryParser) parseGroupBy(groupStr string, query *Query, selectFields []ClickHouseSelectField) {
	parts := strings.Split(groupStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Handle time grouping: toStartOfMinute(timestamp), toStartOfHour(timestamp), etc.
		timeGroupPattern := regexp.MustCompile(`(?i)toStartOf(\w+)\s*\(\s*\w+\s*\)`)
		if match := timeGroupPattern.FindStringSubmatch(part); match != nil {
			interval := strings.ToLower(match[1])
			switch interval {
			case "minute":
				if query.Aggregation != nil {
					query.Aggregation.Window = time.Minute
				}
			case "fiveminutes", "fiveminute":
				if query.Aggregation != nil {
					query.Aggregation.Window = 5 * time.Minute
				}
			case "tenminutes", "tenminute":
				if query.Aggregation != nil {
					query.Aggregation.Window = 10 * time.Minute
				}
			case "hour":
				if query.Aggregation != nil {
					query.Aggregation.Window = time.Hour
				}
			case "day":
				if query.Aggregation != nil {
					query.Aggregation.Window = 24 * time.Hour
				}
			}
			continue
		}

		// Regular group by field
		query.GroupBy = append(query.GroupBy, part)
	}
}

func (p *clickHouseQueryParser) translateAggFunc(funcName string) AggFunc {
	switch strings.ToLower(funcName) {
	case "count":
		return AggCount
	case "sum":
		return AggSum
	case "avg", "mean":
		return AggMean
	case "min":
		return AggMin
	case "max":
		return AggMax
	case "stddev", "stddevpop", "stddevsamp":
		return AggStddev
	case "first":
		return AggFirst
	case "last":
		return AggLast
	default:
		return AggNone
	}
}

// convertResult converts Chronicle result to ClickHouse row format.
func (s *ClickHouseServer) convertResult(result *Result, selectFields []ClickHouseSelectField, query *Query) ([][]any, []ClickHouseColumn, error) {
	if result == nil || len(result.Points) == 0 {
		// Return empty result with proper columns
		columns := s.buildColumns(selectFields)
		return [][]any{}, columns, nil
	}

	columns := s.buildColumns(selectFields)
	rows := make([][]any, 0, len(result.Points))

	for _, point := range result.Points {
		row := make([]any, len(selectFields))
		for i, field := range selectFields {
			row[i] = s.extractFieldValue(point, field)
		}
		rows = append(rows, row)
	}

	return rows, columns, nil
}

func (s *ClickHouseServer) buildColumns(fields []ClickHouseSelectField) []ClickHouseColumn {
	columns := make([]ClickHouseColumn, len(fields))
	for i, f := range fields {
		colType := "String"
		switch strings.ToLower(f.FieldName) {
		case "timestamp", "time", "ts":
			colType = "DateTime64(9)"
		case "value":
			colType = "Float64"
		case "*":
			colType = "String"
		}
		if f.IsAgg {
			colType = "Float64"
		}

		name := f.Name
		if f.Alias != "" {
			name = f.Alias
		}
		columns[i] = ClickHouseColumn{Name: name, Type: colType}
	}
	return columns
}

func (s *ClickHouseServer) extractFieldValue(point Point, field ClickHouseSelectField) any {
	switch strings.ToLower(field.FieldName) {
	case "timestamp", "time", "ts":
		return time.Unix(0, point.Timestamp).Format("2006-01-02 15:04:05.000000000")
	case "value", "*":
		return point.Value
	case "metric":
		return point.Metric
	case "tags":
		if data, err := json.Marshal(point.Tags); err == nil {
			return string(data)
		}
		return "{}"
	default:
		// Check if it's a tag
		if v, ok := point.Tags[field.FieldName]; ok {
			return v
		}
		// For aggregations, return the value
		if field.IsAgg {
			return point.Value
		}
		return nil
	}
}

// parseFormat parses output format from string.
func (s *ClickHouseServer) parseFormat(format string) ClickHouseFormat {
	switch strings.ToLower(format) {
	case "json":
		return ClickHouseFormatJSON
	case "jsoneachrow":
		return ClickHouseFormatJSONEachRow
	case "jsoncompact":
		return ClickHouseFormatJSONCompact
	case "csv":
		return ClickHouseFormatCSV
	case "tabseparated", "tsv", "":
		return ClickHouseFormatTabSeparated
	default:
		return s.config.DefaultFormat
	}
}

// writeResult writes query results in the specified format.
func (s *ClickHouseServer) writeResult(w http.ResponseWriter, rows [][]any, columns []ClickHouseColumn, format ClickHouseFormat) {
	switch format {
	case ClickHouseFormatJSON:
		s.writeJSON(w, rows, columns)
	case ClickHouseFormatJSONEachRow:
		s.writeJSONEachRow(w, rows, columns)
	case ClickHouseFormatJSONCompact:
		s.writeJSONCompact(w, rows, columns)
	case ClickHouseFormatCSV:
		s.writeCSV(w, rows, columns)
	default:
		s.writeTabSeparated(w, rows, columns)
	}
}

func (s *ClickHouseServer) writeTabSeparated(w http.ResponseWriter, rows [][]any, columns []ClickHouseColumn) {
	w.Header().Set("Content-Type", "text/tab-separated-values; charset=UTF-8")
	w.Header().Set("X-ClickHouse-Format", "TabSeparated")

	var buf bytes.Buffer
	for _, row := range rows {
		for i, val := range row {
			if i > 0 {
				buf.WriteByte('\t')
			}
			buf.WriteString(fmt.Sprintf("%v", val))
		}
		buf.WriteByte('\n')
	}
	_, _ = w.Write(buf.Bytes())
}

func (s *ClickHouseServer) writeJSON(w http.ResponseWriter, rows [][]any, columns []ClickHouseColumn) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("X-ClickHouse-Format", "JSON")

	meta := make([]map[string]string, len(columns))
	for i, col := range columns {
		meta[i] = map[string]string{"name": col.Name, "type": col.Type}
	}

	data := make([]map[string]any, len(rows))
	for i, row := range rows {
		rowMap := make(map[string]any)
		for j, col := range columns {
			if j < len(row) {
				rowMap[col.Name] = row[j]
			}
		}
		data[i] = rowMap
	}

	response := map[string]any{
		"meta":                       meta,
		"data":                       data,
		"rows":                       len(rows),
		"statistics":                 map[string]any{"elapsed": 0.001, "rows_read": len(rows), "bytes_read": 0},
		"rows_before_limit_at_least": len(rows),
	}

	_ = json.NewEncoder(w).Encode(response)
}

func (s *ClickHouseServer) writeJSONEachRow(w http.ResponseWriter, rows [][]any, columns []ClickHouseColumn) {
	w.Header().Set("Content-Type", "application/x-ndjson; charset=UTF-8")
	w.Header().Set("X-ClickHouse-Format", "JSONEachRow")

	for _, row := range rows {
		rowMap := make(map[string]any)
		for j, col := range columns {
			if j < len(row) {
				rowMap[col.Name] = row[j]
			}
		}
		data, _ := json.Marshal(rowMap)
		_, _ = w.Write(data)
		_, _ = w.Write([]byte("\n"))
	}
}

func (s *ClickHouseServer) writeJSONCompact(w http.ResponseWriter, rows [][]any, columns []ClickHouseColumn) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("X-ClickHouse-Format", "JSONCompact")

	meta := make([]map[string]string, len(columns))
	for i, col := range columns {
		meta[i] = map[string]string{"name": col.Name, "type": col.Type}
	}

	response := map[string]any{
		"meta": meta,
		"data": rows,
		"rows": len(rows),
		"statistics": map[string]any{
			"elapsed":    0.001,
			"rows_read":  len(rows),
			"bytes_read": 0,
		},
	}

	_ = json.NewEncoder(w).Encode(response)
}

func (s *ClickHouseServer) writeCSV(w http.ResponseWriter, rows [][]any, columns []ClickHouseColumn) {
	w.Header().Set("Content-Type", "text/csv; charset=UTF-8")
	w.Header().Set("X-ClickHouse-Format", "CSV")

	var buf bytes.Buffer
	for _, row := range rows {
		for i, val := range row {
			if i > 0 {
				buf.WriteByte(',')
			}
			// Quote strings
			switch v := val.(type) {
			case string:
				buf.WriteByte('"')
				buf.WriteString(strings.ReplaceAll(v, "\"", "\"\""))
				buf.WriteByte('"')
			default:
				buf.WriteString(fmt.Sprintf("%v", val))
			}
		}
		buf.WriteByte('\n')
	}
	_, _ = w.Write(buf.Bytes())
}

func (s *ClickHouseServer) writeError(w http.ResponseWriter, err error, status int) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.Header().Set("X-ClickHouse-Exception-Code", "62")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(fmt.Sprintf("Code: 62. DB::Exception: %s", err.Error())))
}

// setupClickHouseRoutes configures ClickHouse-compatible endpoints.
func setupClickHouseRoutes(mux *http.ServeMux, db *DB, config ClickHouseConfig, wrap func(http.HandlerFunc) http.HandlerFunc) {
	if !config.Enabled {
		return
	}

	server := NewClickHouseServer(db, config)

	// Main query endpoint
	mux.HandleFunc("/", wrap(server.Handler()))

	// ClickHouse-specific endpoints
	mux.HandleFunc("/ping", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("Ok.\n"))
	}))

	mux.HandleFunc("/replicas_status", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("Ok.\n"))
	}))

	// Query endpoint (explicit)
	mux.HandleFunc("/query", wrap(server.Handler()))

	// Play interface placeholder
	mux.HandleFunc("/play", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<!DOCTYPE html>
<html>
<head><title>Chronicle ClickHouse Interface</title></head>
<body>
<h1>Chronicle - ClickHouse Compatible Interface</h1>
<p>This endpoint provides ClickHouse HTTP protocol compatibility.</p>
<p>Use any ClickHouse client to connect.</p>
</body>
</html>`))
	}))
}
