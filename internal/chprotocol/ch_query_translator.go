package chprotocol

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

// Execute parses and executes a ClickHouse SQL query
func (t *CHQueryTranslator) Execute(ctx context.Context, query string) (*CHQueryResult, error) {
	query = strings.TrimSpace(query)
	queryLower := strings.ToLower(query)

	if strings.HasPrefix(queryLower, "select 1") || query == "SELECT 1" {
		return t.selectOne()
	}

	if strings.Contains(queryLower, "system.databases") {
		return t.showDatabases()
	}

	if strings.Contains(queryLower, "system.tables") {
		return t.showTables()
	}

	if strings.Contains(queryLower, "system.columns") {
		return t.showColumns(query)
	}

	if strings.HasPrefix(queryLower, "show databases") {
		return t.showDatabases()
	}

	if strings.HasPrefix(queryLower, "show tables") {
		return t.showTables()
	}

	if strings.HasPrefix(queryLower, "describe") || strings.HasPrefix(queryLower, "desc ") {
		return t.describeTable(query)
	}

	if strings.HasPrefix(queryLower, "select") {
		return t.executeSelect(ctx, query)
	}

	if strings.HasPrefix(queryLower, "insert") {
		return t.executeInsert(ctx, query)
	}

	return nil, fmt.Errorf("unsupported query: %s", query)
}

func (t *CHQueryTranslator) selectOne() (*CHQueryResult, error) {
	return &CHQueryResult{
		Columns:  []CHColumn{{Name: "1", Type: CHTypeUInt8}},
		Rows:     [][]any{{uint8(1)}},
		RowCount: 1,
	}, nil
}

func (t *CHQueryTranslator) showDatabases() (*CHQueryResult, error) {
	return &CHQueryResult{
		Columns:  []CHColumn{{Name: "name", Type: CHTypeString}},
		Rows:     [][]any{{"chronicle"}, {"system"}},
		RowCount: 2,
	}, nil
}

func (t *CHQueryTranslator) showTables() (*CHQueryResult, error) {

	metrics := t.db.Metrics()

	rows := make([][]any, 0, len(metrics))
	for _, m := range metrics {
		rows = append(rows, []any{m})
	}

	return &CHQueryResult{
		Columns:  []CHColumn{{Name: "name", Type: CHTypeString}},
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

func (t *CHQueryTranslator) showColumns(query string) (*CHQueryResult, error) {

	re := regexp.MustCompile(`table\s*=\s*'([^']+)'`)
	matches := re.FindStringSubmatch(query)

	_ = "unknown"
	if len(matches) > 1 {
		_ = matches[1]
	}

	return &CHQueryResult{
		Columns: []CHColumn{
			{Name: "name", Type: CHTypeString},
			{Name: "type", Type: CHTypeString},
			{Name: "default_type", Type: CHTypeString},
			{Name: "default_expression", Type: CHTypeString},
		},
		Rows: [][]any{
			{"timestamp", CHTypeDateTime, "", ""},
			{"value", CHTypeFloat64, "", ""},
			{"series", CHTypeString, "", ""},
		},
		RowCount: 3,
	}, nil
}

func (t *CHQueryTranslator) describeTable(query string) (*CHQueryResult, error) {

	parts := strings.Fields(query)
	tableName := "unknown"
	if len(parts) >= 2 {
		tableName = strings.Trim(parts[len(parts)-1], "`\"")
	}

	return &CHQueryResult{
		Columns: []CHColumn{
			{Name: "name", Type: CHTypeString},
			{Name: "type", Type: CHTypeString},
			{Name: "default_type", Type: CHTypeString},
			{Name: "default_expression", Type: CHTypeString},
			{Name: "comment", Type: CHTypeString},
			{Name: "codec_expression", Type: CHTypeString},
			{Name: "ttl_expression", Type: CHTypeString},
		},
		Rows: [][]any{
			{"timestamp", CHTypeDateTime, "", "", "Event timestamp", "", ""},
			{"value", CHTypeFloat64, "", "", "Metric value", "", ""},
			{"series", CHTypeString, "", "", "Series name: " + tableName, "", ""},
		},
		RowCount: 3,
	}, nil
}

func (t *CHQueryTranslator) executeSelect(ctx context.Context, query string) (*CHQueryResult, error) {

	parsed, err := t.parseSelect(query)
	if err != nil {
		return nil, err
	}

	points, err := t.executeChronicleQuery(ctx, parsed)
	if err != nil {
		return nil, err
	}

	return t.buildSelectResult(parsed, points)
}

func (t *CHQueryTranslator) parseSelect(query string) (*parsedSelect, error) {
	parsed := &parsedSelect{
		aggregates: make(map[string]string),
		limit:      1000,
		endTime:    time.Now().UnixNano(),
		startTime:  time.Now().Add(-time.Hour).UnixNano(),
	}

	queryLower := strings.ToLower(query)

	selectMatch := regexp.MustCompile(`(?i)select\s+(.+?)\s+from`).FindStringSubmatch(query)
	if len(selectMatch) > 1 {
		cols := strings.Split(selectMatch[1], ",")
		for _, col := range cols {
			col = strings.TrimSpace(col)
			parsed.columns = append(parsed.columns, col)

			for _, agg := range []string{"sum", "avg", "min", "max", "count"} {
				if strings.HasPrefix(strings.ToLower(col), agg+"(") {
					innerCol := regexp.MustCompile(`\(([^)]+)\)`).FindStringSubmatch(col)
					if len(innerCol) > 1 {
						parsed.aggregates[col] = agg
					}
				}
			}
		}
	}

	fromMatch := regexp.MustCompile(`(?i)from\s+([^\s,;]+)`).FindStringSubmatch(query)
	if len(fromMatch) > 1 {
		parsed.table = strings.Trim(fromMatch[1], "`\"")
	}

	if strings.Contains(queryLower, "where") {

		timeMatch := regexp.MustCompile(`(?i)timestamp\s*>=?\s*(\d+)`).FindStringSubmatch(query)
		if len(timeMatch) > 1 {
			if ts, err := strconv.ParseInt(timeMatch[1], 10, 64); err == nil {
				parsed.startTime = ts
			}
		}

		timeMatch = regexp.MustCompile(`(?i)timestamp\s*<=?\s*(\d+)`).FindStringSubmatch(query)
		if len(timeMatch) > 1 {
			if ts, err := strconv.ParseInt(timeMatch[1], 10, 64); err == nil {
				parsed.endTime = ts
			}
		}

		dateMatch := regexp.MustCompile(`(?i)timestamp\s*>=?\s*toDateTime\('([^']+)'\)`).FindStringSubmatch(query)
		if len(dateMatch) > 1 {
			if t, err := time.Parse("2006-01-02 15:04:05", dateMatch[1]); err == nil {
				parsed.startTime = t.UnixNano()
			}
		}

		dateMatch = regexp.MustCompile(`(?i)timestamp\s*<=?\s*toDateTime\('([^']+)'\)`).FindStringSubmatch(query)
		if len(dateMatch) > 1 {
			if t, err := time.Parse("2006-01-02 15:04:05", dateMatch[1]); err == nil {
				parsed.endTime = t.UnixNano()
			}
		}
	}

	groupMatch := regexp.MustCompile(`(?i)group\s+by\s+([^\s;]+)`).FindStringSubmatch(query)
	if len(groupMatch) > 1 {
		parsed.groupBy = groupMatch[1]
	}

	orderMatch := regexp.MustCompile(`(?i)order\s+by\s+([^\s;]+)`).FindStringSubmatch(query)
	if len(orderMatch) > 1 {
		parsed.orderBy = orderMatch[1]
		parsed.orderDesc = strings.Contains(strings.ToLower(query), "desc")
	}

	limitMatch := regexp.MustCompile(`(?i)limit\s+(\d+)`).FindStringSubmatch(query)
	if len(limitMatch) > 1 {
		if l, err := strconv.Atoi(limitMatch[1]); err == nil {
			parsed.limit = l
		}
	}

	return parsed, nil
}

func (t *CHQueryTranslator) executeChronicleQuery(ctx context.Context, parsed *parsedSelect) ([]chronicle.Point, error) {
	q := &chronicle.Query{
		Metric: parsed.table,
		Start:  parsed.startTime,
		End:    parsed.endTime,
		Limit:  parsed.limit,
	}

	result, err := t.db.ExecuteContext(ctx, q)
	if err != nil {
		return nil, err
	}
	return result.Points, nil
}

func (t *CHQueryTranslator) buildSelectResult(parsed *parsedSelect, points []chronicle.Point) (*CHQueryResult, error) {

	columns := make([]CHColumn, 0)
	_ = false
	_ = false

	for _, col := range parsed.columns {
		colLower := strings.ToLower(col)
		if col == "*" {
			columns = append(columns,
				CHColumn{Name: "timestamp", Type: CHTypeDateTime},
				CHColumn{Name: "value", Type: CHTypeFloat64},
				CHColumn{Name: "metric", Type: CHTypeString},
			)
		} else if strings.Contains(colLower, "timestamp") || strings.Contains(colLower, "time") {
			columns = append(columns, CHColumn{Name: col, Type: CHTypeDateTime})
		} else if strings.Contains(colLower, "value") || parsed.aggregates[col] != "" {
			columns = append(columns, CHColumn{Name: col, Type: CHTypeFloat64})
		} else {
			columns = append(columns, CHColumn{Name: col, Type: CHTypeString})
		}
	}

	if len(parsed.aggregates) > 0 {
		return t.buildAggregateResult(parsed, points, columns)
	}

	rows := make([][]any, 0, len(points))
	for _, p := range points {
		row := make([]any, 0, len(columns))
		for _, col := range columns {
			switch strings.ToLower(col.Name) {
			case "timestamp", "time":
				row = append(row, p.Timestamp/int64(time.Second))
			case "value":
				row = append(row, p.Value)
			case "metric", "series":
				row = append(row, p.Metric)
			case "*":
				row = append(row, p.Timestamp/int64(time.Second), p.Value, p.Metric)
			default:
				if v, ok := p.Tags[col.Name]; ok {
					row = append(row, v)
				} else {
					row = append(row, "")
				}
			}
		}
		rows = append(rows, row)
	}

	return &CHQueryResult{
		Columns:  columns,
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

func (t *CHQueryTranslator) buildAggregateResult(parsed *parsedSelect, points []chronicle.Point, columns []CHColumn) (*CHQueryResult, error) {
	if len(points) == 0 {
		return &CHQueryResult{
			Columns:  columns,
			Rows:     [][]any{},
			RowCount: 0,
		}, nil
	}

	values := make([]float64, len(points))
	for i, p := range points {
		values[i] = p.Value
	}

	row := make([]any, 0, len(columns))
	for _, col := range parsed.columns {
		colLower := strings.ToLower(col)
		if agg, ok := parsed.aggregates[col]; ok {
			var result float64
			switch agg {
			case "sum":
				for _, v := range values {
					result += v
				}
			case "avg":
				for _, v := range values {
					result += v
				}
				result /= float64(len(values))
			case "min":
				result = values[0]
				for _, v := range values[1:] {
					if v < result {
						result = v
					}
				}
			case "max":
				result = values[0]
				for _, v := range values[1:] {
					if v > result {
						result = v
					}
				}
			case "count":
				result = float64(len(values))
			}
			row = append(row, result)
		} else if strings.Contains(colLower, "timestamp") {
			row = append(row, points[0].Timestamp/int64(time.Second))
		} else {
			row = append(row, points[0].Metric)
		}
	}

	return &CHQueryResult{
		Columns:  columns,
		Rows:     [][]any{row},
		RowCount: 1,
	}, nil
}

func (t *CHQueryTranslator) executeInsert(ctx context.Context, query string) (*CHQueryResult, error) {

	re := regexp.MustCompile(`(?i)insert\s+into\s+([^\s(]+)\s*(?:\([^)]+\))?\s*values\s*(.+)`)
	matches := re.FindStringSubmatch(query)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid INSERT syntax")
	}

	table := strings.Trim(matches[1], "`\"")
	valuesStr := matches[2]

	valueRe := regexp.MustCompile(`\(([^)]+)\)`)
	valueMatches := valueRe.FindAllStringSubmatch(valuesStr, -1)

	points := make([]chronicle.Point, 0, len(valueMatches))
	for _, vm := range valueMatches {
		if len(vm) < 2 {
			continue
		}

		parts := strings.Split(vm[1], ",")
		if len(parts) < 2 {
			continue
		}

		timestamp := time.Now().UnixNano()
		if ts, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64); err == nil {
			timestamp = ts * int64(time.Second)
		}

		value := 0.0
		if v, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64); err == nil {
			value = v
		}

		points = append(points, chronicle.Point{
			Metric:    table,
			Timestamp: timestamp,
			Value:     value,
		})
	}

	for _, p := range points {
		if err := t.db.Write(p); err != nil {
			return nil, fmt.Errorf("write failed: %w", err)
		}
	}

	return &CHQueryResult{
		Columns:  []CHColumn{},
		Rows:     [][]any{},
		RowCount: 0,
	}, nil
}
