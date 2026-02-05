package chronicle

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func setupWriteRoutes(mux *http.ServeMux, db *DB, wrap middlewareWrapper) {
	mux.HandleFunc("/write", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		var reader io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer func() { _ = gz.Close() }()
			reader = io.LimitReader(gz, maxBodySize)
		}

		body, err := io.ReadAll(reader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(body) == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		var req writeRequest
		if err := json.NewDecoder(bytes.NewReader(body)).Decode(&req); err == nil && len(req.Points) > 0 {
			for i := range req.Points {
				if err := ValidatePoint(&req.Points[i]); err != nil {
					http.Error(w, fmt.Sprintf("invalid point %d: %v", i, err), http.StatusBadRequest)
					return
				}
			}
			if err := db.WriteBatch(req.Points); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			return
		}

		points, err := parseLineProtocol(string(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(points) == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		for i := range points {
			if err := ValidatePoint(&points[i]); err != nil {
				http.Error(w, fmt.Sprintf("invalid point %d: %v", i, err), http.StatusBadRequest)
				return
			}
		}
		if err := db.WriteBatch(points); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
}

// setupQueryRoutes configures query-related endpoints
func setupQueryRoutes(mux *http.ServeMux, db *DB, wrap middlewareWrapper) {
	mux.HandleFunc("/query", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		var req queryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var q *Query
		if req.Query != "" {
			parser := &QueryParser{}
			parsed, err := parser.Parse(req.Query)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			q = parsed
		} else {
			q = &Query{
				Metric: req.Metric,
				Start:  req.Start,
				End:    req.End,
				Tags:   req.Tags,
			}
			if req.Aggregation != "" {
				agg := parseAggFunc(req.Aggregation)
				if agg == AggNone {
					http.Error(w, "invalid aggregation function", http.StatusBadRequest)
					return
				}
				window := time.Minute
				if req.Window != "" {
					parsedWindow, err := parseDuration(req.Window)
					if err != nil {
						http.Error(w, "invalid aggregation window", http.StatusBadRequest)
						return
					}
					if parsedWindow > 0 {
						window = parsedWindow
					}
				}
				q.Aggregation = &Aggregation{
					Function: agg,
					Window:   window,
				}
			}
		}

		result, err := db.Execute(q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeJSON(w, queryResponse{Points: result.Points})
	}))
}

func parseLineProtocol(body string) ([]Point, error) {
	lines := strings.Split(strings.TrimSpace(body), "\n")
	points := make([]Point, 0, len(lines))
	now := time.Now().UnixNano()

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, fmt.Errorf("invalid line protocol")
		}

		measurementAndTags := fields[0]
		fieldSet := fields[1]
		timestamp := now
		if len(fields) >= 3 {
			ts, err := strconv.ParseInt(fields[2], 10, 64)
			if err == nil {
				timestamp = ts
			}
		}

		metric, tags := parseMeasurementTags(measurementAndTags)
		fieldKey, fieldValue, err := parseFieldSet(fieldSet)
		if err != nil {
			return nil, err
		}

		_ = fieldKey
		points = append(points, Point{
			Metric:    metric,
			Tags:      tags,
			Value:     fieldValue,
			Timestamp: timestamp,
		})
	}

	return points, nil
}

func parseMeasurementTags(input string) (metric string, tags map[string]string) {
	parts := strings.Split(input, ",")
	metric = parts[0]
	tags = make(map[string]string)
	for _, part := range parts[1:] {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			tags[kv[0]] = kv[1]
		}
	}
	return metric, tags
}

func parseFieldSet(input string) (key string, value float64, err error) {
	kv := strings.SplitN(input, "=", 2)
	if len(kv) != 2 {
		return "", 0, fmt.Errorf("invalid field set")
	}
	val := strings.TrimSuffix(kv[1], "i")
	floatVal, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return "", 0, err
	}
	return kv[0], floatVal, nil
}
