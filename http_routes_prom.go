package chronicle

import (
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
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		decoded, err := snappy.Decode(nil, body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req prompb.WriteRequest
		if err := req.Unmarshal(decoded); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		points := convertPromWrite(&req)
		if err := db.WriteBatch(points); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
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
		w.WriteHeader(http.StatusMethodNotAllowed)
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

	result, err := db.Execute(cq)
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

func promResponse(result *Result, metric string, isRange bool) map[string]interface{} {
	data := make([]map[string]interface{}, 0)

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

		entry := map[string]interface{}{
			"metric": labels,
		}

		if isRange {
			values := make([][]interface{}, len(points))
			for i, p := range points {
				values[i] = []interface{}{
					float64(p.Timestamp) / 1e9,
					strconv.FormatFloat(p.Value, 'f', -1, 64),
				}
			}
			entry["values"] = values
		} else {
			if len(points) > 0 {
				p := points[len(points)-1]
				entry["value"] = []interface{}{
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

	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
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
