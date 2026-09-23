package chronicle

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

// setupPrometheusRoutes configures Prometheus-compatible endpoints
func setupPrometheusRoutes(mux *http.ServeMux, db *DB, wrap middlewareWrapper) {
	mux.HandleFunc("/prometheus/write", wrap(func(w http.ResponseWriter, r *http.Request) {
		if !db.config.HTTP.PrometheusRemoteWriteEnabled {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodPost {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, "bad request", http.StatusBadRequest)
			return
		}
		decoded, err := snappy.Decode(nil, body)
		if err != nil {
			writeError(w, "bad request", http.StatusBadRequest)
			return
		}
		var req prompb.WriteRequest
		if err := req.Unmarshal(decoded); err != nil {
			writeError(w, "bad request", http.StatusBadRequest)
			return
		}
		points := convertPromWrite(&req)
		if err := db.WriteBatchContext(r.Context(), points); err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))

	mux.HandleFunc("/api/v1/query", wrap(func(w http.ResponseWriter, r *http.Request) {
		handlePromQuery(db, w, r, false)
	}))

	mux.HandleFunc("/api/v1/query_range", wrap(func(w http.ResponseWriter, r *http.Request) {
		handlePromQuery(db, w, r, true)
	}))

	// Expose internal metrics in Prometheus exposition format
	mux.HandleFunc("/metrics", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if db.metrics != nil {
			snap := db.metrics.Snapshot()
			writePrometheusMetrics(w, snap)
		} else {
			w.Write([]byte("# Chronicle Metrics\n"))
		}
	}))
}

func writePrometheusMetrics(w io.Writer, snap *MetricsSnapshot) {
	if snap == nil {
		return
	}
	var sb strings.Builder
	sb.WriteString("# HELP chronicle_uptime_seconds Process uptime in seconds\n")
	sb.WriteString("# TYPE chronicle_uptime_seconds gauge\n")
	sb.WriteString(fmt.Sprintf("chronicle_uptime_seconds %f\n\n", snap.Uptime.Seconds()))

	for k, v := range snap.Counters {
		metricName := sanitizeMetricName("chronicle_counter_" + k)
		sb.WriteString(fmt.Sprintf("# TYPE %s counter\n", metricName))
		sb.WriteString(fmt.Sprintf("%s %d\n", metricName, v))
	}
	for k, v := range snap.Gauges {
		metricName := sanitizeMetricName("chronicle_gauge_" + k)
		sb.WriteString(fmt.Sprintf("# TYPE %s gauge\n", metricName))
		sb.WriteString(fmt.Sprintf("%s %d\n", metricName, v))
	}
	for k, h := range snap.Histograms {
		metricName := sanitizeMetricName("chronicle_histogram_" + k)
		sb.WriteString(fmt.Sprintf("# TYPE %s_count counter\n", metricName))
		sb.WriteString(fmt.Sprintf("%s_count %d\n", metricName, h.Count))
		sb.WriteString(fmt.Sprintf("# TYPE %s_sum counter\n", metricName))
		sb.WriteString(fmt.Sprintf("%s_sum %f\n", metricName, h.Sum))
		sb.WriteString(fmt.Sprintf("# TYPE %s gauge\n", metricName))
		sb.WriteString(fmt.Sprintf("%s{quantile=\"0.5\"} %f\n", metricName, h.P50))
		sb.WriteString(fmt.Sprintf("%s{quantile=\"0.9\"} %f\n", metricName, h.P90))
		sb.WriteString(fmt.Sprintf("%s{quantile=\"0.95\"} %f\n", metricName, h.P95))
		sb.WriteString(fmt.Sprintf("%s{quantile=\"0.99\"} %f\n", metricName, h.P99))
	}
	w.Write([]byte(sb.String()))
}

func sanitizeMetricName(name string) string {
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "/", "_")
	return name
}

func convertPromWrite(req *prompb.WriteRequest) []Point {
	points := make([]Point, 0, len(req.Timeseries))
	for i := range req.Timeseries {
		ts := &req.Timeseries[i]
		metric := ""
		tags := make(map[string]string)
		for _, label := range ts.Labels {
			if label.Name == "__name__" {
				metric = label.Value
			} else {
				tags[label.Name] = label.Value
			}
		}
		if metric == "" {
			continue // skip timeseries without __name__ label
		}
		for _, sample := range ts.Samples {
			points = append(points, Point{
				Metric:    metric,
				Tags:      tags,
				Value:     sample.Value,
				Timestamp: sample.Timestamp * int64(time.Millisecond),
			})
		}
	}
	return points
}

// handlePromQuery handles Prometheus-compatible /api/v1/query and /api/v1/query_range
func handlePromQuery(db *DB, w http.ResponseWriter, r *http.Request, isRange bool) {
	var query string
	var start, end int64
	var step time.Duration

	if r.Method == http.MethodGet {
		query = r.URL.Query().Get("query")
		if s := r.URL.Query().Get("time"); s != "" {
			if t, err := strconv.ParseFloat(s, 64); err == nil {
				start = int64(t * 1e9)
				end = start
			}
		}
		if s := r.URL.Query().Get("start"); s != "" {
			if t, err := strconv.ParseFloat(s, 64); err == nil {
				start = int64(t * 1e9)
			}
		}
		if s := r.URL.Query().Get("end"); s != "" {
			if t, err := strconv.ParseFloat(s, 64); err == nil {
				end = int64(t * 1e9)
			}
		}
		if s := r.URL.Query().Get("step"); s != "" {
			if d, err := time.ParseDuration(s); err == nil {
				step = d
			} else if f, err := strconv.ParseFloat(s, 64); err == nil {
				step = time.Duration(f * float64(time.Second))
			}
		}
	} else if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			promError(w, "bad_data", err.Error())
			return
		}
		query = r.FormValue("query")
		if s := r.FormValue("time"); s != "" {
			if t, err := strconv.ParseFloat(s, 64); err == nil {
				start = int64(t * 1e9)
				end = start
			}
		}
		if s := r.FormValue("start"); s != "" {
			if t, err := strconv.ParseFloat(s, 64); err == nil {
				start = int64(t * 1e9)
			}
		}
		if s := r.FormValue("end"); s != "" {
			if t, err := strconv.ParseFloat(s, 64); err == nil {
				end = int64(t * 1e9)
			}
		}
		if s := r.FormValue("step"); s != "" {
			if d, err := time.ParseDuration(s); err == nil {
				step = d
			}
		}
	} else {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if query == "" {
		promError(w, "bad_data", "query parameter is required")
		return
	}

	// Parse PromQL
	parser := &PromQLParser{}
	pq, err := parser.Parse(query)
	if err != nil {
		promError(w, "bad_data", err.Error())
		return
	}

	// Set defaults
	if start == 0 {
		start = time.Now().Add(-time.Hour).UnixNano()
	}
	if end == 0 {
		end = time.Now().UnixNano()
	}
	if step == 0 {
		step = time.Minute
	}

	// Convert to Chronicle query
	cq := pq.ToChronicleQuery(start, end)
	if cq.Aggregation != nil && cq.Aggregation.Window <= 0 {
		cq.Aggregation.Window = step
	}

	result, err := db.ExecuteContext(r.Context(), cq)
	if err != nil {
		promError(w, "execution", err.Error())
		return
	}

	// Format response
	resp := promResponse(result, pq.Metric, isRange)
	writeJSON(w, resp)
}

func promError(w http.ResponseWriter, errorType, message string) {
	jsonError(w, http.StatusBadRequest, errorType, message)
}

func promResponse(result *Result, metric string, isRange bool) map[string]any {
	data := make([]map[string]any, 0)

	// Group points by tags
	groups := make(map[string][]Point)
	for _, p := range result.Points {
		key := formatTags(p.Tags)
		groups[key] = append(groups[key], p)
	}

	for _, points := range groups {
		if len(points) == 0 {
			continue
		}

		labels := make(map[string]string)
		labels["__name__"] = metric
		for k, v := range points[0].Tags {
			labels[k] = v
		}

		entry := map[string]any{
			"metric": labels,
		}

		if isRange {
			values := make([][]any, len(points))
			for i, p := range points {
				values[i] = []any{
					float64(p.Timestamp) / 1e9,
					strconv.FormatFloat(p.Value, 'f', -1, 64),
				}
			}
			entry["values"] = values
		} else {
			if len(points) > 0 {
				p := points[len(points)-1]
				entry["value"] = []any{
					float64(p.Timestamp) / 1e9,
					strconv.FormatFloat(p.Value, 'f', -1, 64),
				}
			}
		}

		data = append(data, entry)
	}

	resultType := "vector"
	if isRange {
		resultType = "matrix"
	}

	return map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": resultType,
			"result":     data,
		},
	}
}

func formatTags(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	parts := make([]string, 0, len(tags))
	for k, v := range tags {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}
