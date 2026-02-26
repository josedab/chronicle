// clickhouse_parser.go contains extended clickhouse functionality.
package chronicle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

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
	if _, err := w.Write(buf.Bytes()); err != nil {
		log.Printf("[WARN] chronicle: clickhouse writeTabSeparated: %v", err)
	}
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

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[WARN] chronicle: clickhouse writeJSON encode: %v", err)
	}
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
		if _, err := w.Write(data); err != nil {
			log.Printf("[WARN] chronicle: clickhouse writeJSONEachRow: %v", err)
			return
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			log.Printf("[WARN] chronicle: clickhouse writeJSONEachRow: %v", err)
			return
		}
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

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[WARN] chronicle: clickhouse writeJSONCompact encode: %v", err)
	}
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
	if _, err := w.Write(buf.Bytes()); err != nil {
		log.Printf("[WARN] chronicle: clickhouse writeCSV: %v", err)
	}
}

func (s *ClickHouseServer) writeError(w http.ResponseWriter, err error, status int) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.Header().Set("X-ClickHouse-Exception-Code", "62")
	w.WriteHeader(status)
	log.Printf("[ERROR] chronicle: clickhouse query error: %v", err)
	if _, werr := w.Write([]byte("Code: 62. DB::Exception: internal server error")); werr != nil {
		log.Printf("[WARN] chronicle: clickhouse writeError: %v", werr)
	}
}

// setupClickHouseRoutes configures ClickHouse-compatible endpoints.
func setupClickHouseRoutes(mux *http.ServeMux, db *DB, config ClickHouseConfig, wrap func(http.HandlerFunc) http.HandlerFunc) {
	if !config.Enabled {
		return
	}

	server := NewClickHouseServer(db, config)

	// Main query endpoint
	mux.HandleFunc("/api/v1/clickhouse/", wrap(server.Handler()))

	// ClickHouse-specific endpoints
	mux.HandleFunc("/api/v1/clickhouse/ping", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		if _, err := w.Write([]byte("Ok.\n")); err != nil {
			log.Printf("[WARN] chronicle: clickhouse ping write: %v", err)
		}
	}))

	mux.HandleFunc("/replicas_status", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		if _, err := w.Write([]byte("Ok.\n")); err != nil {
			log.Printf("[WARN] chronicle: clickhouse replicas_status write: %v", err)
		}
	}))

	// Query endpoint (explicit)
	mux.HandleFunc("/api/v1/clickhouse/query", wrap(server.Handler()))

	// Play interface placeholder
	mux.HandleFunc("/play", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		if _, err := w.Write([]byte(`<!DOCTYPE html>
<html>
<head><title>Chronicle ClickHouse Interface</title></head>
<body>
<h1>Chronicle - ClickHouse Compatible Interface</h1>
<p>This endpoint provides ClickHouse HTTP protocol compatibility.</p>
<p>Use any ClickHouse client to connect.</p>
</body>
</html>`)); err != nil {
			log.Printf("[WARN] chronicle: clickhouse play write: %v", err)
		}
	}))
}
