package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocket streaming and live data support for the Grafana backend.

func (g *GrafanaBackend) handleStream(w http.ResponseWriter, r *http.Request) {
	if g.hub == nil {
		http.Error(w, "streaming not enabled", http.StatusServiceUnavailable)
		return
	}

	conn, err := newUpgrader(g.config.AllowedOrigins).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer closeQuietly(conn)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Track subscriptions for this connection
	connSubs := make(map[string]*Subscription)
	var connMu sync.Mutex

	// Read commands from client
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}

			var cmd StreamMessage
			if err := json.Unmarshal(msg, &cmd); err != nil {
				resp, _ := json.Marshal(StreamMessage{Type: "error", Error: "invalid message format"})
				if err := conn.WriteMessage(1, resp); err != nil {
					slog.Debug("stream write error", "err", err)
				}
				continue
			}

			switch cmd.Type {
			case "subscribe":
				sub := g.hub.Subscribe(cmd.Metric, cmd.Tags)
				connMu.Lock()
				connSubs[sub.ID] = sub
				connMu.Unlock()

				resp, _ := json.Marshal(StreamMessage{Type: "subscribed", SubID: sub.ID})
				_ = conn.WriteMessage(1, resp) //nolint:errcheck // best-effort response encoding

				go g.forwardStreamPoints(ctx, conn, sub)

			case "unsubscribe":
				connMu.Lock()
				if sub, ok := connSubs[cmd.SubID]; ok {
					delete(connSubs, cmd.SubID)
					g.hub.Unsubscribe(sub.ID)
				}
				connMu.Unlock()

				resp, _ := json.Marshal(StreamMessage{Type: "unsubscribed", SubID: cmd.SubID})
				_ = conn.WriteMessage(1, resp) //nolint:errcheck // best-effort response encoding

			default:
				resp, _ := json.Marshal(StreamMessage{Type: "error", Error: "unknown command: " + cmd.Type})
				_ = conn.WriteMessage(1, resp) //nolint:errcheck // best-effort response encoding
			}
		}
	}()

	<-ctx.Done()
	wg.Wait()

	// Cleanup all subscriptions
	connMu.Lock()
	for _, sub := range connSubs {
		g.hub.Unsubscribe(sub.ID)
	}
	connMu.Unlock()
}

func (g *GrafanaBackend) forwardStreamPoints(ctx context.Context, conn *websocket.Conn, sub *Subscription) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.done:
			return
		case p, ok := <-sub.ch:
			if !ok {
				return
			}
			msg, _ := json.Marshal(StreamMessage{
				Type:  "point",
				SubID: sub.ID,
				Point: &p,
			})
			if err := conn.WriteMessage(1, msg); err != nil {
				return
			}
		}
	}
}

func (g *GrafanaBackend) handleAlerts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		g.listAlertRules(w, r)
	case http.MethodPost:
		g.createAlertRule(w, r)
	case http.MethodDelete:
		g.deleteAlertRule(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (g *GrafanaBackend) createAlertRule(w http.ResponseWriter, r *http.Request) {
	var rule GrafanaAlertRule
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if rule.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	if rule.ID == "" {
		rule.ID = fmt.Sprintf("alert-%d", time.Now().UnixNano())
	}

	// Register with AlertManager if available
	if g.alertMgr != nil {
		alertRule := AlertRule{
			Name:        rule.Name,
			Metric:      rule.Query.Metric,
			Tags:        rule.Query.Tags,
			Condition:   g.parseAlertCondition(rule.Condition),
			Threshold:   rule.Threshold,
			Labels:      rule.Labels,
			Annotations: rule.Annotations,
		}
		if rule.For != "" {
			alertRule.ForDuration, _ = g.parseDuration(rule.For) //nolint:errcheck // best-effort response encoding
		}
		if err := g.alertMgr.AddRule(alertRule); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
	}

	g.alertMu.Lock()
	g.alertRules = append(g.alertRules, rule)
	g.alertMu.Unlock()

	g.writeJSON(w, rule)
}

func (g *GrafanaBackend) listAlertRules(w http.ResponseWriter, r *http.Request) {
	g.alertMu.RLock()
	rules := g.alertRules
	g.alertMu.RUnlock()

	if rules == nil {
		rules = []GrafanaAlertRule{}
	}

	g.writeJSON(w, rules)
}

func (g *GrafanaBackend) deleteAlertRule(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		// Try to extract from path
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/alerts/"), "/")
		if len(parts) > 0 && parts[0] != "" {
			id = parts[0]
		}
	}

	if id == "" {
		http.Error(w, "id parameter required", http.StatusBadRequest)
		return
	}

	g.alertMu.Lock()
	found := false
	for i, rule := range g.alertRules {
		if rule.ID == id {
			g.alertRules = append(g.alertRules[:i], g.alertRules[i+1:]...)
			found = true
			// Remove from AlertManager if available
			if g.alertMgr != nil {
				g.alertMgr.RemoveRule(rule.Name)
			}
			break
		}
	}
	g.alertMu.Unlock()

	if !found {
		http.Error(w, "alert rule not found", http.StatusNotFound)
		return
	}

	g.writeJSON(w, map[string]string{"status": "deleted", "id": id})
}

func (g *GrafanaBackend) parseAlertCondition(s string) AlertCondition {
	switch strings.ToLower(s) {
	case "above", "gt", ">":
		return AlertConditionAbove
	case "below", "lt", "<":
		return AlertConditionBelow
	case "equal", "eq", "=":
		return AlertConditionEqual
	case "not_equal", "ne", "!=":
		return AlertConditionNotEqual
	case "absent", "no_data":
		return AlertConditionAbsent
	default:
		return AlertConditionAbove
	}
}

// Helper methods

func (g *GrafanaBackend) parseTimeRange(from, to string) (int64, int64, error) {
	// Handle relative time strings like "now-1h"
	fromTime, err := g.parseTime(from)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid from time: %w", err)
	}

	toTime, err := g.parseTime(to)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid to time: %w", err)
	}

	return fromTime.UnixNano(), toTime.UnixNano(), nil
}

func (g *GrafanaBackend) parseTime(s string) (time.Time, error) {
	// Try numeric timestamp first
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMilli(ts), nil
	}

	// Handle "now" and relative time
	if strings.HasPrefix(s, "now") {
		now := time.Now()
		if s == "now" {
			return now, nil
		}

		// Parse relative offset (now-1h, now-30m, etc.)
		offset := strings.TrimPrefix(s, "now")
		if strings.HasPrefix(offset, "-") || strings.HasPrefix(offset, "+") {
			duration, err := g.parseDuration(offset[1:])
			if err != nil {
				return time.Time{}, err
			}
			if offset[0] == '-' {
				return now.Add(-duration), nil
			}
			return now.Add(duration), nil
		}
	}

	// Try parsing as RFC3339
	return time.Parse(time.RFC3339, s)
}

func (g *GrafanaBackend) parseDuration(s string) (time.Duration, error) {
	// Handle common formats: 1h, 30m, 1d, 1w
	if len(s) < 2 {
		return 0, errors.New("invalid duration")
	}

	unit := s[len(s)-1]
	value, err := strconv.Atoi(s[:len(s)-1])
	if err != nil {
		return 0, err
	}

	switch unit {
	case 's':
		return time.Duration(value) * time.Second, nil
	case 'm':
		return time.Duration(value) * time.Minute, nil
	case 'h':
		return time.Duration(value) * time.Hour, nil
	case 'd':
		return time.Duration(value) * 24 * time.Hour, nil
	case 'w':
		return time.Duration(value) * 7 * 24 * time.Hour, nil
	default:
		return time.ParseDuration(s)
	}
}

func (g *GrafanaBackend) parseWindow(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, _ := g.parseDuration(s)
	return d
}

func (g *GrafanaBackend) parseAggFunc(s string) AggFunc {
	switch strings.ToLower(s) {
	case "mean", "avg", "average":
		return AggMean
	case "sum":
		return AggSum
	case "count":
		return AggCount
	case "min":
		return AggMin
	case "max":
		return AggMax
	case "first":
		return AggFirst
	case "last":
		return AggLast
	default:
		return AggMean
	}
}

func (g *GrafanaBackend) parseQueryText(text string) (*Query, error) {
	// Basic SQL-like query parsing
	// Format: SELECT agg(field) FROM metric WHERE tags GROUP BY time(window)
	text = strings.TrimSpace(text)

	query := &Query{}

	// Extract metric name (FROM clause)
	fromIdx := strings.Index(strings.ToUpper(text), "FROM ")
	if fromIdx == -1 {
		return nil, errors.New("missing FROM clause")
	}

	afterFrom := text[fromIdx+5:]
	endIdx := strings.IndexAny(afterFrom, " \t\n")
	if endIdx == -1 {
		query.Metric = strings.TrimSpace(afterFrom)
	} else {
		query.Metric = strings.TrimSpace(afterFrom[:endIdx])
	}

	// Extract aggregation (SELECT clause)
	selectIdx := strings.Index(strings.ToUpper(text), "SELECT ")
	if selectIdx != -1 {
		selectClause := text[selectIdx+7 : fromIdx]
		// Parse aggregation function
		if parenIdx := strings.Index(selectClause, "("); parenIdx != -1 {
			funcName := strings.TrimSpace(selectClause[:parenIdx])
			query.Aggregation = &Aggregation{
				Function: g.parseAggFunc(funcName),
				Window:   time.Minute, // Default
			}
		}
	}

	// Extract GROUP BY time window
	groupByIdx := strings.Index(strings.ToUpper(text), "GROUP BY ")
	if groupByIdx != -1 && query.Aggregation != nil {
		groupByClause := text[groupByIdx+9:]
		if timeIdx := strings.Index(strings.ToLower(groupByClause), "time("); timeIdx != -1 {
			start := timeIdx + 5
			end := strings.Index(groupByClause[start:], ")")
			if end != -1 {
				windowStr := groupByClause[start : start+end]
				if d, err := g.parseDuration(windowStr); err == nil {
					query.Aggregation.Window = d
				}
			}
		}
	}

	return query, nil
}

func (g *GrafanaBackend) getMetricsCached() []string {
	g.metricCacheMu.RLock()
	if time.Since(g.metricCacheAt) < g.config.CacheDuration && g.metricCache != nil {
		defer g.metricCacheMu.RUnlock()
		return g.metricCache
	}
	g.metricCacheMu.RUnlock()

	// Refresh cache
	metrics := g.db.Metrics()
	sort.Strings(metrics)

	g.metricCacheMu.Lock()
	g.metricCache = metrics
	g.metricCacheAt = time.Now()
	g.metricCacheMu.Unlock()

	return metrics
}

func (g *GrafanaBackend) getTagKeys() []string {
	if g.db == nil {
		return []string{}
	}
	keys := g.db.TagKeys()
	if len(keys) == 0 {
		return []string{}
	}
	sort.Strings(keys)
	return keys
}

func (g *GrafanaBackend) getTagValues(key string) []string {
	if g.db == nil {
		return []string{}
	}
	values := g.db.TagValues(key)
	if len(values) == 0 {
		return []string{}
	}
	sort.Strings(values)
	return values
}

func (g *GrafanaBackend) downsamplePoints(points []Point, maxPoints int) []Point {
	if len(points) <= maxPoints {
		return points
	}

	// Simple downsampling by taking every Nth point
	step := len(points) / maxPoints
	if step < 1 {
		step = 1
	}

	result := make([]Point, 0, maxPoints)
	for i := 0; i < len(points); i += step {
		if len(result) >= maxPoints {
			break
		}
		result = append(result, points[i])
	}

	return result
}

func (g *GrafanaBackend) writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// GrafanaAlertRule represents a Grafana alerting rule.
type GrafanaAlertRule struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Query       GrafanaQuery      `json:"query"`
	Condition   string            `json:"condition"`
	Threshold   float64           `json:"threshold"`
	For         string            `json:"for"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// GrafanaAlertState represents alert state.
type GrafanaAlertState string

const (
	GrafanaAlertStateOK       GrafanaAlertState = "ok"
	GrafanaAlertStatePending  GrafanaAlertState = "pending"
	GrafanaAlertStateAlerting GrafanaAlertState = "alerting"
)

// AddAnnotation adds an annotation programmatically.
func (g *GrafanaBackend) AddAnnotation(ann GrafanaAnnotation) {
	g.annotationsMu.Lock()
	defer g.annotationsMu.Unlock()

	ann.ID = int64(len(g.annotations) + 1)
	if ann.Time == 0 {
		ann.Time = time.Now().UnixMilli()
	}
	g.annotations = append(g.annotations, ann)
}

// GetAnnotations returns annotations in a time range.
func (g *GrafanaBackend) GetAnnotations(from, to int64) []GrafanaAnnotation {
	g.annotationsMu.RLock()
	defer g.annotationsMu.RUnlock()

	var result []GrafanaAnnotation
	for _, ann := range g.annotations {
		if ann.Time >= from && ann.Time <= to {
			result = append(result, ann)
		}
	}
	return result
}
