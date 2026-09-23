package chronicle

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestHttpRoutesProm(t *testing.T) {
	db := setupTestDB(t)

	tests := []struct {
		name string
	}{
		{name: "setup_prometheus_routes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
			setupPrometheusRoutes(mux, db, wrap)
		})
	}
}

func TestPrometheusMetricsExposition(t *testing.T) {
	db := setupTestDB(t)
	mux := http.NewServeMux()
	wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
	setupPrometheusRoutes(mux, db, wrap)

	snap := &MetricsSnapshot{
		Uptime:     10 * time.Second,
		Counters:   map[string]int64{"writes": 100},
		Gauges:     map[string]int64{"memory": 2048},
		Histograms: map[string]HistogramSnapshot{"latency": {Count: 5, Sum: 12.5, P50: 2.0, P90: 3.0, P95: 3.5, P99: 4.0}},
	}
	var buf strings.Builder
	writePrometheusMetrics(&buf, snap)
	output := buf.String()
	if !strings.Contains(output, "chronicle_counter_writes 100") {
		t.Errorf("expected counter in output, got %s", output)
	}
	if !strings.Contains(output, "chronicle_uptime_seconds") {
		t.Errorf("expected uptime in output, got %s", output)
	}
}
