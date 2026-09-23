package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/chronicle-db/chronicle"
)

// RemoteWriteTimeSeries represents a basic Prometheus time series payload
type RemoteWriteTimeSeries struct {
	Labels  map[string]string `json:"labels"`
	Samples []struct {
		Value     float64 `json:"value"`
		Timestamp int64   `json:"timestamp"`
	} `json:"samples"`
}

// RemoteWriteRequest represents a Prometheus remote write payload
type RemoteWriteRequest struct {
	Timeseries []RemoteWriteTimeSeries `json:"timeseries"`
}

// HandleRemoteWrite handles Prometheus remote write ingestion
func (s *Server) HandleRemoteWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read body: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req RemoteWriteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON/Protobuf payload: %v", err), http.StatusBadRequest)
		return
	}

	for _, ts := range req.Timeseries {
		metricName := ts.Labels["__name__"]
		if metricName == "" {
			metricName = "unnamed_metric"
		}
		for _, sample := range ts.Samples {
			t := time.UnixMilli(sample.Timestamp)
			if sample.Timestamp == 0 {
				t = time.Now()
			}
			_ = s.engine.Write(metricName, ts.Labels, sample.Value, t)
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
