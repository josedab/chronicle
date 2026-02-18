package chronicle

import (
	"fmt"
	"strings"
)

// CorrelationQuery represents a parsed cross-signal correlation query.
// Syntax: CORRELATE <signal_type> WITH <signal_type> WHERE <conditions>
// Example: CORRELATE metrics WITH traces WHERE service='api' AND status='error'
type CorrelationQuery struct {
	SourceSignal SignalType         `json:"source_signal"`
	TargetSignal SignalType         `json:"target_signal"`
	Conditions   []CorrelationCondition `json:"conditions"`
	Limit        int                `json:"limit,omitempty"`
}

// CorrelationCondition is a single condition in a correlation query.
type CorrelationCondition struct {
	Key      string `json:"key"`
	Operator string `json:"operator"` // "=", "!=", "~" (regex)
	Value    string `json:"value"`
}

// ParseCorrelationQuery parses a CORRELATE query string.
func ParseCorrelationQuery(input string) (*CorrelationQuery, error) {
	input = strings.TrimSpace(input)

	if !strings.HasPrefix(strings.ToUpper(input), "CORRELATE") {
		return nil, fmt.Errorf("query must start with CORRELATE")
	}

	// Remove CORRELATE prefix
	rest := strings.TrimSpace(input[len("CORRELATE"):])

	// Find WITH keyword
	withIdx := strings.Index(strings.ToUpper(rest), " WITH ")
	if withIdx < 0 {
		return nil, fmt.Errorf("missing WITH keyword")
	}

	sourceStr := strings.TrimSpace(rest[:withIdx])
	afterWith := strings.TrimSpace(rest[withIdx+len(" WITH "):])

	// Parse source signal type
	source, err := parseSignalType(sourceStr)
	if err != nil {
		return nil, fmt.Errorf("invalid source signal: %w", err)
	}

	// Find WHERE keyword (optional)
	var targetStr string
	var conditions []CorrelationCondition

	whereIdx := strings.Index(strings.ToUpper(afterWith), " WHERE ")
	if whereIdx >= 0 {
		targetStr = strings.TrimSpace(afterWith[:whereIdx])
		whereClause := strings.TrimSpace(afterWith[whereIdx+len(" WHERE "):])
		conditions, err = parseConditions(whereClause)
		if err != nil {
			return nil, fmt.Errorf("invalid WHERE clause: %w", err)
		}
	} else {
		targetStr = afterWith
	}

	target, err := parseSignalType(targetStr)
	if err != nil {
		return nil, fmt.Errorf("invalid target signal: %w", err)
	}

	return &CorrelationQuery{
		SourceSignal: source,
		TargetSignal: target,
		Conditions:   conditions,
		Limit:        100,
	}, nil
}

func parseSignalType(s string) (SignalType, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "metrics", "metric":
		return SignalMetric, nil
	case "traces", "trace":
		return SignalTrace, nil
	case "logs", "log":
		return SignalLog, nil
	default:
		return "", fmt.Errorf("unknown signal type %q (use metrics, traces, or logs)", s)
	}
}

func parseConditions(whereClause string) ([]CorrelationCondition, error) {
	var conditions []CorrelationCondition

	// Split on AND
	parts := splitOnAnd(whereClause)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		cond, err := parseCondition(part)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)
	}

	return conditions, nil
}

func splitOnAnd(s string) []string {
	upper := strings.ToUpper(s)
	var parts []string
	start := 0

	for i := 0; i < len(upper)-4; i++ {
		if upper[i:i+5] == " AND " {
			parts = append(parts, s[start:i])
			start = i + 5
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func parseCondition(s string) (CorrelationCondition, error) {
	// Try != first (before =)
	if idx := strings.Index(s, "!="); idx >= 0 {
		return CorrelationCondition{
			Key:      strings.TrimSpace(s[:idx]),
			Operator: "!=",
			Value:    trimQuotes(strings.TrimSpace(s[idx+2:])),
		}, nil
	}

	// Try =~  (regex match)
	if idx := strings.Index(s, "=~"); idx >= 0 {
		return CorrelationCondition{
			Key:      strings.TrimSpace(s[:idx]),
			Operator: "~",
			Value:    trimQuotes(strings.TrimSpace(s[idx+2:])),
		}, nil
	}

	// Try =
	if idx := strings.Index(s, "="); idx >= 0 {
		return CorrelationCondition{
			Key:      strings.TrimSpace(s[:idx]),
			Operator: "=",
			Value:    trimQuotes(strings.TrimSpace(s[idx+1:])),
		}, nil
	}

	return CorrelationCondition{}, fmt.Errorf("invalid condition %q (expected key=value)", s)
}

func trimQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') ||
			(s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// ExecuteCorrelation executes a parsed correlation query against the engine.
func (e *SignalCorrelationEngine) ExecuteCorrelation(query *CorrelationQuery) CorrelatedResult {
	result := CorrelatedResult{}

	// Extract key conditions for correlation
	var service, traceID, labelKey, labelValue string
	for _, c := range query.Conditions {
		if c.Operator != "=" {
			continue
		}
		switch c.Key {
		case "service", "service_name":
			service = c.Value
		case "trace_id":
			traceID = c.Value
		default:
			labelKey = c.Key
			labelValue = c.Value
		}
	}

	// Route to appropriate correlation method
	switch {
	case traceID != "":
		result = e.CorrelateTraceWithLogs(traceID)
		if spans, ok := e.traceStore.GetTrace(traceID); ok {
			result.Traces = spans
		}
	case service != "":
		result = e.CorrelateService(service)
	case labelKey != "" && labelValue != "":
		result = e.CorrelateByLabel(labelKey, labelValue)
	}

	return result
}
