package chronicle

// arrow_flight_sql_protocol.go contains wire protocol helpers and SQL translation
// for the Arrow Flight SQL server. See arrow_flight_sql.go for the core server.

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
)

// --- Wire protocol helpers ---

func (s *FlightSQLServer) sendResponseMsg(conn net.Conn, msgType uint32, data any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return s.sendMessage(conn, msgType, body)
}

func (s *FlightSQLServer) sendErrorMsg(conn net.Conn, err error) {
	resp := map[string]string{"error": err.Error()}
	body, _ := json.Marshal(resp)                        //nolint:errcheck // simple map marshal cannot fail
	_ = s.sendMessage(conn, flightSQLMessageError, body) //nolint:errcheck // error path: best-effort send
}

func (s *FlightSQLServer) sendMessage(conn net.Conn, msgType uint32, body []byte) error {
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], msgType)
	binary.BigEndian.PutUint32(header[4:], uint32(len(body)))

	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(body)
	return err
}

// --- SQLToQueryTranslator ---

// SQLToQueryTranslator translates basic SQL SELECT statements to Chronicle queries.
type SQLToQueryTranslator struct{}

// Translate parses a SQL SELECT statement and converts it to a Chronicle Query.
// Supports: SELECT ... FROM metric WHERE time > X AND tag = 'val' ORDER BY timestamp LIMIT N
func (t *SQLToQueryTranslator) Translate(sql string) (*Query, error) {
	normalized := strings.TrimSpace(sql)
	if normalized == "" {
		return nil, fmt.Errorf("empty SQL query")
	}

	upper := strings.ToUpper(normalized)
	if !strings.HasPrefix(upper, "SELECT") {
		return nil, fmt.Errorf("only SELECT statements are supported")
	}

	query := &Query{}

	// Extract FROM clause to get metric name.
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return nil, fmt.Errorf("missing FROM clause")
	}

	afterFrom := strings.TrimSpace(normalized[fromIdx+4:])
	metric, rest := extractIdentifier(afterFrom)
	if metric == "" {
		return nil, fmt.Errorf("missing table name in FROM clause")
	}
	// Strip surrounding quotes if present.
	metric = strings.Trim(metric, "\"'`")
	query.Metric = metric

	// Default time range: last hour.
	query.Start = time.Now().Add(-time.Hour).UnixNano()
	query.End = time.Now().UnixNano()

	restUpper := strings.ToUpper(rest)

	// Parse WHERE clause.
	if whereIdx := strings.Index(restUpper, "WHERE"); whereIdx >= 0 {
		wherePart := rest[whereIdx+5:]
		// Find end of WHERE clause (ORDER BY or LIMIT).
		endIdx := len(wherePart)
		for _, kw := range []string{"ORDER BY", "LIMIT"} {
			idx := strings.Index(strings.ToUpper(wherePart), kw)
			if idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		conditions := strings.TrimSpace(wherePart[:endIdx])
		rest = wherePart[endIdx:]
		restUpper = strings.ToUpper(rest)

		if err := parseWhereConditions(conditions, query); err != nil {
			return nil, err
		}
	}

	// Parse ORDER BY clause.
	if orderIdx := strings.Index(restUpper, "ORDER BY"); orderIdx >= 0 {
		afterOrder := rest[orderIdx+8:]
		limitIdx := strings.Index(strings.ToUpper(afterOrder), "LIMIT")
		if limitIdx >= 0 {
			rest = afterOrder[limitIdx:]
			restUpper = strings.ToUpper(rest)
		} else {
			rest = ""
			restUpper = ""
		}
	}

	// Parse LIMIT clause.
	if limitIdx := strings.Index(restUpper, "LIMIT"); limitIdx >= 0 {
		limitPart := strings.TrimSpace(rest[limitIdx+5:])
		var limit int
		if _, err := fmt.Sscanf(limitPart, "%d", &limit); err == nil && limit > 0 {
			query.Limit = limit
		}
	}

	return query, nil
}

func extractIdentifier(s string) (string, string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}

	// Handle quoted identifiers.
	if s[0] == '"' || s[0] == '\'' || s[0] == '`' {
		quote := s[0]
		end := strings.IndexByte(s[1:], quote)
		if end >= 0 {
			return s[1 : end+1], strings.TrimSpace(s[end+2:])
		}
	}

	// Unquoted: ends at whitespace or keywords.
	for i, ch := range s {
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == ';' {
			return s[:i], strings.TrimSpace(s[i:])
		}
	}
	return s, ""
}

func parseWhereConditions(conditions string, query *Query) error {
	if strings.TrimSpace(conditions) == "" {
		return nil
	}

	// Split on AND (case-insensitive).
	parts := splitOnAND(conditions)
	if query.Tags == nil {
		query.Tags = make(map[string]string)
	}

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		partUpper := strings.ToUpper(part)

		// Time range conditions.
		if strings.Contains(partUpper, "TIME") || strings.Contains(partUpper, "TIMESTAMP") {
			if ts, ok := parseTimeCondition(part); ok {
				if strings.Contains(part, ">") {
					query.Start = ts
				} else if strings.Contains(part, "<") {
					query.End = ts
				}
				continue
			}
		}

		// Tag equality: key = 'value'
		if eqIdx := strings.Index(part, "="); eqIdx > 0 && !strings.Contains(part, "!=") {
			key := strings.TrimSpace(part[:eqIdx])
			val := strings.TrimSpace(part[eqIdx+1:])
			val = strings.Trim(val, "'\"")
			key = strings.Trim(key, "\"'`")
			query.Tags[key] = val
		}
	}

	return nil
}

func splitOnAND(s string) []string {
	var parts []string
	upper := strings.ToUpper(s)
	for {
		idx := strings.Index(upper, " AND ")
		if idx < 0 {
			parts = append(parts, strings.TrimSpace(s))
			break
		}
		parts = append(parts, strings.TrimSpace(s[:idx]))
		s = s[idx+5:]
		upper = upper[idx+5:]
	}
	return parts
}

func parseTimeCondition(cond string) (int64, bool) {
	// Try to find a numeric timestamp.
	for _, op := range []string{">=", "<=", ">", "<", "="} {
		idx := strings.Index(cond, op)
		if idx < 0 {
			continue
		}
		val := strings.TrimSpace(cond[idx+len(op):])
		val = strings.Trim(val, "'\"")

		// Try parsing as unix nanoseconds.
		var ts int64
		if _, err := fmt.Sscanf(val, "%d", &ts); err == nil {
			return ts, true
		}

		// Try RFC3339.
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t.UnixNano(), true
		}

		// Try common SQL timestamp format.
		if t, err := time.Parse("2006-01-02 15:04:05", val); err == nil {
			return t.UnixNano(), true
		}

		break
	}
	return 0, false
}
