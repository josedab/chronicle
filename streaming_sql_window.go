package chronicle

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Window emission, state management, result formatting, and HTTP handlers for streaming SQL.

func (e *StreamingSQLEngine) emitWindowResults(query *StreamingQuery, windowStates map[string]*WindowState) {
	now := time.Now().UnixNano()

	for key, state := range windowStates {
		// Check if window has closed
		if state.End <= now {
			result := &StreamingResult{
				QueryID:     query.ID,
				Timestamp:   now,
				WindowStart: state.Start,
				WindowEnd:   state.End,
				GroupKey:    state.Key,
				Values:      make(map[string]any),
				Type:        ResultTypeFinal,
			}

			// Calculate final aggregations
			for _, sel := range query.Parsed.Select {
				switch sel.Function {
				case "COUNT":
					result.Values[sel.Alias] = state.Count
				case "SUM":
					result.Values[sel.Alias] = state.Sum
				case "AVG", "MEAN":
					if state.Count > 0 {
						result.Values[sel.Alias] = state.Sum / float64(state.Count)
					}
				case "MIN":
					result.Values[sel.Alias] = state.Min
				case "MAX":
					result.Values[sel.Alias] = state.Max
				default:
					if v, ok := state.Values[sel.Field]; ok {
						result.Values[sel.Alias] = v
					}
				}
			}

			// Apply HAVING
			if query.Parsed.Having == nil || e.matchesHaving(result, query.Parsed.Having) {
				select {
				case query.Results <- result:
					query.mu.Lock()
					query.LastEmit = time.Now()
					query.mu.Unlock()
				default:
				}
			}

			// Remove closed window
			delete(windowStates, key)
		}
	}
}

func (e *StreamingSQLEngine) matchesHaving(result *StreamingResult, having *HavingClause) bool {
	for _, cond := range having.Conditions {
		value, ok := result.Values[cond.Field]
		if !ok {
			return false
		}

		condValue, _ := cond.Value.(float64)
		resultValue, _ := value.(float64)

		switch cond.Operator {
		case ">":
			if resultValue <= condValue {
				return false
			}
		case ">=":
			if resultValue < condValue {
				return false
			}
		case "<":
			if resultValue >= condValue {
				return false
			}
		case "<=":
			if resultValue > condValue {
				return false
			}
		case "=", "==":
			if resultValue != condValue {
				return false
			}
		}
	}
	return true
}

func (e *StreamingSQLEngine) projectPoint(query *StreamingQuery, point Point) *StreamingResult {
	result := &StreamingResult{
		QueryID:   query.ID,
		Timestamp: point.Timestamp,
		Values:    make(map[string]any),
		Type:      ResultTypeUpdate,
	}

	for _, sel := range query.Parsed.Select {
		alias := sel.Alias
		if alias == "" {
			alias = sel.Expression
		}

		switch sel.Expression {
		case "*":
			result.Values["metric"] = point.Metric
			result.Values["value"] = point.Value
			for k, v := range point.Tags {
				result.Values[k] = v
			}
		case "value":
			result.Values[alias] = point.Value
		case "metric":
			result.Values[alias] = point.Metric
		case "timestamp":
			result.Values[alias] = point.Timestamp
		default:
			if v, ok := point.Tags[sel.Expression]; ok {
				result.Values[alias] = v
			}
		}
	}

	return result
}

// ParseSQL parses a streaming SQL statement.
func (e *StreamingSQLEngine) ParseSQL(sql string) (*ParsedStreamingSQL, error) {
	sql = strings.TrimSpace(sql)
	upper := strings.ToUpper(sql)

	parsed := &ParsedStreamingSQL{}

	// Determine statement type
	if strings.HasPrefix(upper, "CREATE STREAM") {
		parsed.Type = StreamingSQLTypeCreateStream
		return e.parseCreateStream(sql, parsed)
	} else if strings.HasPrefix(upper, "SELECT") {
		parsed.Type = StreamingSQLTypeSelect
		return e.parseSelect(sql, parsed)
	} else if strings.HasPrefix(upper, "INSERT INTO") {
		parsed.Type = StreamingSQLTypeInsertInto
		return e.parseInsertInto(sql, parsed)
	}

	return nil, errors.New("unsupported SQL statement")
}

func (e *StreamingSQLEngine) parseSelect(sql string, parsed *ParsedStreamingSQL) (*ParsedStreamingSQL, error) {
	upper := strings.ToUpper(sql)

	// Extract SELECT fields
	selectIdx := strings.Index(upper, "SELECT") + 6
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx == -1 {
		return nil, errors.New("missing FROM clause")
	}

	selectPart := strings.TrimSpace(sql[selectIdx:fromIdx])
	parsed.Select = e.parseSelectFields(selectPart)

	// Extract FROM (source stream)
	remaining := sql[fromIdx+4:]
	parts := strings.Fields(remaining)
	if len(parts) == 0 {
		return nil, errors.New("missing source stream")
	}
	parsed.Source = parts[0]

	// Parse WHERE
	if whereIdx := strings.Index(upper, "WHERE"); whereIdx != -1 {
		whereEnd := len(sql)
		for _, keyword := range []string{"GROUP BY", "WINDOW", "HAVING", "EMIT"} {
			if idx := strings.Index(upper[whereIdx:], keyword); idx != -1 {
				if whereIdx+idx < whereEnd {
					whereEnd = whereIdx + idx
				}
			}
		}
		wherePart := strings.TrimSpace(sql[whereIdx+5 : whereEnd])
		parsed.Where = e.parseWhere(wherePart)
	}

	// Parse GROUP BY
	if groupIdx := strings.Index(upper, "GROUP BY"); groupIdx != -1 {
		groupEnd := len(sql)
		for _, keyword := range []string{"WINDOW", "HAVING", "EMIT"} {
			if idx := strings.Index(upper[groupIdx:], keyword); idx != -1 {
				if groupIdx+idx < groupEnd {
					groupEnd = groupIdx + idx
				}
			}
		}
		groupPart := strings.TrimSpace(sql[groupIdx+8 : groupEnd])
		parsed.GroupBy = &GroupByClause{
			Fields: strings.Split(groupPart, ","),
		}
		for i := range parsed.GroupBy.Fields {
			parsed.GroupBy.Fields[i] = strings.TrimSpace(parsed.GroupBy.Fields[i])
		}
	}

	// Parse WINDOW
	if windowIdx := strings.Index(upper, "WINDOW"); windowIdx != -1 {
		windowEnd := len(sql)
		for _, keyword := range []string{"HAVING", "EMIT"} {
			if idx := strings.Index(upper[windowIdx:], keyword); idx != -1 {
				if windowIdx+idx < windowEnd {
					windowEnd = windowIdx + idx
				}
			}
		}
		windowPart := strings.TrimSpace(sql[windowIdx+6 : windowEnd])
		parsed.Window = e.parseWindow(windowPart)
	}

	// Parse HAVING
	if havingIdx := strings.Index(upper, "HAVING"); havingIdx != -1 {
		havingEnd := len(sql)
		if emitIdx := strings.Index(upper[havingIdx:], "EMIT"); emitIdx != -1 {
			havingEnd = havingIdx + emitIdx
		}
		havingPart := strings.TrimSpace(sql[havingIdx+6 : havingEnd])
		parsed.Having = &HavingClause{
			Conditions: e.parseConditions(havingPart),
		}
	}

	// Parse EMIT
	if emitIdx := strings.Index(upper, "EMIT"); emitIdx != -1 {
		emitPart := strings.TrimSpace(sql[emitIdx+4:])
		parsed.Emit = e.parseEmit(emitPart)
	}

	return parsed, nil
}

func (e *StreamingSQLEngine) parseSelectFields(selectPart string) []SelectField {
	var fields []SelectField

	// Split by comma, handling functions
	parts := splitSelectFields(selectPart)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		field := SelectField{Expression: part}

		// Check for alias
		upperPart := strings.ToUpper(part)
		if asIdx := strings.Index(upperPart, " AS "); asIdx != -1 {
			field.Alias = strings.TrimSpace(part[asIdx+4:])
			part = strings.TrimSpace(part[:asIdx])
			field.Expression = part
		}

		// Check for function
		if parenIdx := strings.Index(part, "("); parenIdx != -1 {
			field.Function = strings.ToUpper(part[:parenIdx])
			endParen := strings.Index(part, ")")
			if endParen > parenIdx {
				field.Field = strings.TrimSpace(part[parenIdx+1 : endParen])
			}
		} else {
			field.Field = part
		}

		if field.Alias == "" {
			if field.Function != "" {
				field.Alias = strings.ToLower(field.Function) + "_" + field.Field
			} else {
				field.Alias = field.Field
			}
		}

		fields = append(fields, field)
	}

	return fields
}

func splitSelectFields(s string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, c := range s {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
		} else if c == ',' && depth == 0 {
			result = append(result, current.String())
			current.Reset()
			continue
		}
		current.WriteRune(c)
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

func (e *StreamingSQLEngine) parseWhere(wherePart string) *WhereClause {
	return &WhereClause{
		Conditions: e.parseConditions(wherePart),
	}
}

func (e *StreamingSQLEngine) parseConditions(conditionStr string) []Condition {
	var conditions []Condition

	// Simple parsing: split by AND
	parts := strings.Split(strings.ToUpper(conditionStr), " AND ")
	for i, part := range parts {
		_ = i //nolint:errcheck // index consumed by iteration
		part = strings.TrimSpace(part)
		cond := e.parseCondition(part)
		if cond != nil {
			conditions = append(conditions, *cond)
		}
	}

	return conditions
}

func (e *StreamingSQLEngine) parseCondition(condStr string) *Condition {
	condStr = strings.TrimSpace(condStr)

	operators := []string{">=", "<=", "!=", "<>", "=", ">", "<", "LIKE"}

	for _, op := range operators {
		if idx := strings.Index(strings.ToUpper(condStr), op); idx != -1 {
			field := strings.TrimSpace(condStr[:idx])
			valueStr := strings.TrimSpace(condStr[idx+len(op):])
			valueStr = strings.Trim(valueStr, "'\"")

			var value any = valueStr
			// Try to parse as number
			if f, ok := parseFloat(valueStr); ok {
				value = f
			}

			return &Condition{
				Field:    field,
				Operator: op,
				Value:    value,
			}
		}
	}

	return nil
}

func parseFloat(s string) (float64, bool) {
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	return f, err == nil
}

func (e *StreamingSQLEngine) parseWindow(windowPart string) *WindowClause {
	window := &WindowClause{}
	upper := strings.ToUpper(windowPart)

	if strings.Contains(upper, "TUMBLING") {
		window.Type = StreamingWindowTumbling
	} else if strings.Contains(upper, "HOPPING") {
		window.Type = StreamingWindowHopping
	} else if strings.Contains(upper, "SESSION") {
		window.Type = StreamingWindowSession
	} else if strings.Contains(upper, "SLIDING") {
		window.Type = StreamingWindowSliding
	}

	// Parse SIZE
	if sizeIdx := strings.Index(upper, "SIZE"); sizeIdx != -1 {
		window.Size = e.parseDuration(windowPart[sizeIdx+4:])
	}

	// Parse ADVANCE (for hopping)
	if advanceIdx := strings.Index(upper, "ADVANCE"); advanceIdx != -1 {
		window.Advance = e.parseDuration(windowPart[advanceIdx+7:])
	}

	// Parse GRACE
	if graceIdx := strings.Index(upper, "GRACE"); graceIdx != -1 {
		window.Grace = e.parseDuration(windowPart[graceIdx+5:])
	}

	if window.Size == 0 {
		window.Size = time.Minute // Default
	}

	return window
}

func (e *StreamingSQLEngine) parseDuration(s string) time.Duration {
	s = strings.TrimSpace(s)
	s = strings.ToUpper(s)

	// Extract number and unit
	var num int
	var unit string
	fmt.Sscanf(s, "%d %s", &num, &unit)
	if num == 0 {
		fmt.Sscanf(s, "%d%s", &num, &unit)
	}

	switch {
	case strings.HasPrefix(unit, "SEC"):
		return time.Duration(num) * time.Second
	case strings.HasPrefix(unit, "MIN"):
		return time.Duration(num) * time.Minute
	case strings.HasPrefix(unit, "HOUR"):
		return time.Duration(num) * time.Hour
	case strings.HasPrefix(unit, "DAY"):
		return time.Duration(num) * 24 * time.Hour
	default:
		return time.Duration(num) * time.Second
	}
}

func (e *StreamingSQLEngine) parseEmit(emitPart string) *EmitClause {
	emit := &EmitClause{}
	upper := strings.ToUpper(emitPart)

	if strings.Contains(upper, "CHANGES") {
		emit.Type = EmitTypeChanges
	} else if strings.Contains(upper, "FINAL") {
		emit.Type = EmitTypeFinal
	} else if strings.Contains(upper, "EVERY") {
		emit.Type = EmitTypeInterval
		emit.Interval = e.parseDuration(emitPart)
	}

	return emit
}

func (e *StreamingSQLEngine) parseCreateStream(sql string, parsed *ParsedStreamingSQL) (*ParsedStreamingSQL, error) {
	// CREATE STREAM name AS SELECT ...
	upper := strings.ToUpper(sql)

	asIdx := strings.Index(upper, " AS ")
	if asIdx == -1 {
		return nil, errors.New("CREATE STREAM requires AS clause")
	}

	// Extract stream name
	namePart := strings.TrimSpace(sql[14:asIdx]) // After "CREATE STREAM "
	parsed.Sink = namePart

	// Parse the SELECT part
	selectSQL := strings.TrimSpace(sql[asIdx+4:])
	return e.parseSelect(selectSQL, parsed)
}

func (e *StreamingSQLEngine) parseInsertInto(sql string, parsed *ParsedStreamingSQL) (*ParsedStreamingSQL, error) {
	// INSERT INTO sink SELECT ... FROM source
	upper := strings.ToUpper(sql)

	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx == -1 {
		return nil, errors.New("INSERT INTO requires SELECT clause")
	}

	// Extract sink name
	sinkPart := strings.TrimSpace(sql[11:selectIdx]) // After "INSERT INTO "
	parsed.Sink = sinkPart

	// Parse the SELECT part
	selectSQL := strings.TrimSpace(sql[selectIdx:])
	return e.parseSelect(selectSQL, parsed)
}

// StopQuery stops a running query.
func (e *StreamingSQLEngine) StopQuery(queryID string) error {
	e.queryMu.Lock()
	query, ok := e.queries[queryID]
	if !ok {
		e.queryMu.Unlock()
		return errors.New("query not found")
	}
	delete(e.queries, queryID)
	e.queryMu.Unlock()

	query.cancel()
	query.mu.Lock()
	query.State = StreamingQueryStateStopped
	query.mu.Unlock()

	return nil
}

// GetQuery returns a query by ID.
func (e *StreamingSQLEngine) GetQuery(queryID string) (*StreamingQuery, bool) {
	e.queryMu.RLock()
	defer e.queryMu.RUnlock()
	q, ok := e.queries[queryID]
	return q, ok
}

// ListQueries returns all active queries.
func (e *StreamingSQLEngine) ListQueries() []*StreamingQuery {
	e.queryMu.RLock()
	defer e.queryMu.RUnlock()

	queries := make([]*StreamingQuery, 0, len(e.queries))
	for _, q := range e.queries {
		queries = append(queries, q)
	}

	sort.Slice(queries, func(i, j int) bool {
		return queries[i].Created.Before(queries[j].Created)
	})

	return queries
}

// GetStats returns streaming SQL engine statistics.
func (e *StreamingSQLEngine) GetStats() StreamingSQLStats {
	e.queryMu.RLock()
	queryCount := len(e.queries)
	e.queryMu.RUnlock()

	e.stateStore.mu.RLock()
	windowCount := len(e.stateStore.windows)
	e.stateStore.mu.RUnlock()

	return StreamingSQLStats{
		ActiveQueries:  queryCount,
		ActiveWindows:  windowCount,
		MaxConcurrent:  e.config.MaxConcurrentQueries,
		StateStoreSize: e.config.StateStoreSize,
	}
}

// StreamingSQLStats contains engine statistics.
type StreamingSQLStats struct {
	ActiveQueries  int `json:"active_queries"`
	ActiveWindows  int `json:"active_windows"`
	MaxConcurrent  int `json:"max_concurrent"`
	StateStoreSize int `json:"state_store_size"`
}
