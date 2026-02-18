package pgwire

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Execute translates and executes a SQL statement against Chronicle.
func (t *PGQueryTranslator) Execute(stmt string) (*PGQueryResult, error) {
	upper := strings.ToUpper(strings.TrimSpace(stmt))

	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return t.executeSelect(stmt)
	case strings.HasPrefix(upper, "INSERT"):
		return t.executeInsert(stmt)
	default:
		return nil, fmt.Errorf("unsupported SQL: %s", stmt)
	}
}

var selectRegex = regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+FROM\s+(\S+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+?))?(?:\s+LIMIT\s+(\d+))?\s*$`)

func (t *PGQueryTranslator) executeSelect(stmt string) (*PGQueryResult, error) {
	m := selectRegex.FindStringSubmatch(strings.TrimRight(stmt, ";"))
	if m == nil {
		return t.handleSpecialSelect(stmt)
	}

	columns := strings.TrimSpace(m[1])
	table := strings.TrimSpace(m[2])
	whereClause := strings.TrimSpace(m[3])
	limitStr := strings.TrimSpace(m[5])

	// Table name is the metric name
	metric := strings.Trim(table, `"`)

	start := int64(0)
	end := time.Now().UnixNano()
	var tags map[string]string
	limit := 0

	// Parse WHERE for time range
	if whereClause != "" {
		start, end, tags = parseWhereClause(whereClause, start, end)
	}

	if limitStr != "" {
		if n, err := strconv.Atoi(limitStr); err == nil {
			limit = n
		}
	}

	result, err := t.db.Execute(metric, start, end, tags, limit)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	// Build columns from SELECT list
	colNames := parseColumnList(columns)
	pgCols := buildPGColumns(colNames)

	// Build rows
	rows := make([][]any, 0, len(result.Points))
	for _, p := range result.Points {
		row := buildRow(colNames, p)
		rows = append(rows, row)
	}

	return &PGQueryResult{
		Columns:  pgCols,
		Rows:     rows,
		RowCount: len(rows),
		Tag:      fmt.Sprintf("SELECT %d", len(rows)),
	}, nil
}

func (t *PGQueryTranslator) handleSpecialSelect(stmt string) (*PGQueryResult, error) {
	upper := strings.ToUpper(stmt)

	// Handle SELECT * FROM metrics (list all metrics)
	if strings.Contains(upper, "FROM METRICS") || strings.Contains(upper, "FROM TABLES") || strings.Contains(upper, "FROM PG_TABLES") {
		metrics := t.db.Metrics()
		cols := []PGColumn{{Name: "metric_name", TypeOID: PGTypeText, TypeLen: -1, TypeMod: -1}}
		rows := make([][]any, len(metrics))
		for i, m := range metrics {
			rows[i] = []any{m}
		}
		return &PGQueryResult{
			Columns:  cols,
			Rows:     rows,
			RowCount: len(rows),
			Tag:      fmt.Sprintf("SELECT %d", len(rows)),
		}, nil
	}

	return nil, fmt.Errorf("unsupported SELECT syntax: %s", stmt)
}

func parseWhereClause(clause string, start, end int64) (int64, int64, map[string]string) {
	var tags map[string]string
	parts := strings.Split(clause, "AND")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		upper := strings.ToUpper(part)

		if strings.Contains(upper, "TIMESTAMP") || strings.Contains(upper, "TIME") {
			if strings.Contains(part, ">") || strings.Contains(upper, ">=") {
				if val := extractTimestamp(part); val > 0 {
					start = val
				}
			}
			if strings.Contains(part, "<") || strings.Contains(upper, "<=") {
				if val := extractTimestamp(part); val > 0 {
					end = val
				}
			}
		} else if strings.Contains(part, "=") {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(strings.Trim(kv[0], `"'`))
				val := strings.TrimSpace(strings.Trim(kv[1], `"'`))
				if tags == nil {
					tags = make(map[string]string)
				}
				tags[key] = val
			}
		}
	}
	return start, end, tags
}

func extractTimestamp(expr string) int64 {
	// Look for numeric value (Unix timestamp in seconds or nanoseconds)
	re := regexp.MustCompile(`(\d{10,19})`)
	m := re.FindString(expr)
	if m == "" {
		return 0
	}
	val, err := strconv.ParseInt(m, 10, 64)
	if err != nil {
		return 0
	}
	// If it looks like seconds (10 digits), convert to nanoseconds
	if val < 1e12 {
		val *= 1e9
	}
	return val
}

func (t *PGQueryTranslator) executeInsert(stmt string) (*PGQueryResult, error) {
	re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\S+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)`)
	m := re.FindStringSubmatch(stmt)
	if m == nil {
		return nil, fmt.Errorf("unsupported INSERT syntax")
	}

	metric := strings.Trim(m[1], `"`)
	cols := splitTrimmed(m[2], ",")
	vals := splitTrimmed(m[3], ",")

	if len(cols) != len(vals) {
		return nil, fmt.Errorf("column/value count mismatch")
	}

	p := Point{
		Metric:    metric,
		Timestamp: time.Now().UnixNano(),
		Tags:      make(map[string]string),
	}

	for i, col := range cols {
		col = strings.Trim(col, `"`)
		val := strings.Trim(vals[i], `"' `)
		switch strings.ToLower(col) {
		case "value", "val":
			if f, err := strconv.ParseFloat(val, 64); err == nil {
				p.Value = f
			}
		case "timestamp", "time", "ts":
			if ts, err := strconv.ParseInt(val, 10, 64); err == nil {
				p.Timestamp = ts
			}
		default:
			p.Tags[col] = val
		}
	}

	if err := t.db.Write(p); err != nil {
		return nil, fmt.Errorf("write failed: %w", err)
	}

	return &PGQueryResult{Tag: "INSERT 0 1", RowCount: 1}, nil
}

func parseColumnList(columns string) []string {
	if strings.TrimSpace(columns) == "*" {
		return []string{"timestamp", "value", "metric", "tags"}
	}
	return splitTrimmed(columns, ",")
}

func buildPGColumns(names []string) []PGColumn {
	cols := make([]PGColumn, len(names))
	for i, name := range names {
		name = strings.Trim(strings.TrimSpace(name), `"`)
		col := PGColumn{Name: name, TypeOID: PGTypeText, TypeLen: -1, TypeMod: -1}
		switch strings.ToLower(name) {
		case "timestamp", "time", "ts":
			col.TypeOID = PGTypeTimestamp
		case "value", "val":
			col.TypeOID = PGTypeFloat8
			col.TypeLen = 8
		}
		cols[i] = col
	}
	return cols
}

func buildRow(colNames []string, p Point) []any {
	row := make([]any, len(colNames))
	for i, col := range colNames {
		col = strings.Trim(strings.TrimSpace(col), `"`)
		switch strings.ToLower(col) {
		case "timestamp", "time", "ts":
			row[i] = time.Unix(0, p.Timestamp).UTC()
		case "value", "val":
			row[i] = p.Value
		case "metric", "name":
			row[i] = p.Metric
		case "tags":
			row[i] = fmt.Sprintf("%v", p.Tags)
		case "*":
			// handled in parseColumnList
		default:
			if p.Tags != nil {
				if v, ok := p.Tags[col]; ok {
					row[i] = v
				}
			}
		}
	}
	return row
}

func splitTrimmed(s, sep string) []string {
	parts := strings.Split(s, sep)
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
