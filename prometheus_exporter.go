package chronicle

import (
	"fmt"
	"io"
	"runtime"
	"sort"
	"time"
)

var (
	globalHTTPMetrics *HTTPMetrics
	globalStartTime   = time.Now()
)

// SetGlobalHTTPMetrics registers the active HTTP metrics tracker for Prometheus export.
func SetGlobalHTTPMetrics(m *HTTPMetrics, startTime time.Time) {
	globalHTTPMetrics = m
	if !startTime.IsZero() {
		globalStartTime = startTime
	}
}

// WritePrometheusMetrics writes Chronicle database and runtime internals in Prometheus text exposition format.
func WritePrometheusMetrics(w io.Writer, db *DB, httpMetrics *HTTPMetrics, startTime time.Time) {
	if httpMetrics == nil {
		httpMetrics = globalHTTPMetrics
	}
	if startTime.IsZero() {
		startTime = globalStartTime
	}

	// 1. Build and runtime information
	fmt.Fprintf(w, "# HELP chronicle_build_info Information about the Chronicle build and runtime.\n")
	fmt.Fprintf(w, "# TYPE chronicle_build_info gauge\n")
	fmt.Fprintf(w, "chronicle_build_info{goversion=\"%s\",os=\"%s\",arch=\"%s\"} 1\n",
		runtime.Version(), runtime.GOOS, runtime.GOARCH)

	// 2. Uptime
	uptime := time.Since(startTime).Seconds()
	if uptime < 0 {
		uptime = 0
	}
	fmt.Fprintf(w, "# HELP chronicle_uptime_seconds Process uptime in seconds.\n")
	fmt.Fprintf(w, "# TYPE chronicle_uptime_seconds gauge\n")
	fmt.Fprintf(w, "chronicle_uptime_seconds %.3f\n", uptime)

	// 3. Database series and metric counts
	if db != nil {
		fmt.Fprintf(w, "# HELP chronicle_series_total Total number of active time series stored in the database.\n")
		fmt.Fprintf(w, "# TYPE chronicle_series_total gauge\n")
		fmt.Fprintf(w, "chronicle_series_total %d\n", db.SeriesCount())

		metrics := db.Metrics()
		fmt.Fprintf(w, "# HELP chronicle_metrics_total Total number of unique metric names registered.\n")
		fmt.Fprintf(w, "# TYPE chronicle_metrics_total gauge\n")
		fmt.Fprintf(w, "chronicle_metrics_total %d\n", len(metrics))

		// Configuration indicators
		if db.config.Storage.MaxMemory > 0 {
			fmt.Fprintf(w, "# HELP chronicle_config_max_memory_bytes Configured maximum memory limit for buffers.\n")
			fmt.Fprintf(w, "# TYPE chronicle_config_max_memory_bytes gauge\n")
			fmt.Fprintf(w, "chronicle_config_max_memory_bytes %d\n", db.config.Storage.MaxMemory)
		}
		if db.config.Storage.BufferSize > 0 {
			fmt.Fprintf(w, "# HELP chronicle_config_buffer_size Configured point buffer size.\n")
			fmt.Fprintf(w, "# TYPE chronicle_config_buffer_size gauge\n")
			fmt.Fprintf(w, "chronicle_config_buffer_size %d\n", db.config.Storage.BufferSize)
		}
	}

	// 4. HTTP Server metrics
	if httpMetrics != nil {
		snap := httpMetrics.Snapshot()
		fmt.Fprintf(w, "# HELP chronicle_http_requests_total Total number of HTTP requests processed.\n")
		fmt.Fprintf(w, "# TYPE chronicle_http_requests_total counter\n")
		fmt.Fprintf(w, "chronicle_http_requests_total %d\n", snap.TotalRequests)

		fmt.Fprintf(w, "# HELP chronicle_http_errors_total Total number of HTTP requests that resulted in error responses.\n")
		fmt.Fprintf(w, "# TYPE chronicle_http_errors_total counter\n")
		fmt.Fprintf(w, "chronicle_http_errors_total %d\n", snap.TotalErrors)

		if len(snap.StatusCounts) > 0 {
			fmt.Fprintf(w, "# HELP chronicle_http_requests_by_status_total HTTP requests partitioned by status code.\n")
			fmt.Fprintf(w, "# TYPE chronicle_http_requests_by_status_total counter\n")
			// Deterministic sort by status code
			statuses := make([]int, 0, len(snap.StatusCounts))
			for s := range snap.StatusCounts {
				statuses = append(statuses, s)
			}
			sort.Ints(statuses)
			for _, s := range statuses {
				fmt.Fprintf(w, "chronicle_http_requests_by_status_total{status=\"%d\"} %d\n", s, snap.StatusCounts[s])
			}
		}

		if len(snap.RouteStats) > 0 {
			fmt.Fprintf(w, "# HELP chronicle_http_route_requests_total Number of requests per HTTP route.\n")
			fmt.Fprintf(w, "# TYPE chronicle_http_route_requests_total counter\n")
			routes := make([]string, 0, len(snap.RouteStats))
			for r := range snap.RouteStats {
				routes = append(routes, r)
			}
			sort.Strings(routes)
			for _, r := range routes {
				stat := snap.RouteStats[r]
				fmt.Fprintf(w, "chronicle_http_route_requests_total{path=\"%s\"} %d\n", r, stat.Count)
			}

			fmt.Fprintf(w, "# HELP chronicle_http_route_duration_seconds Average latency per HTTP route in seconds.\n")
			fmt.Fprintf(w, "# TYPE chronicle_http_route_duration_seconds gauge\n")
			for _, r := range routes {
				stat := snap.RouteStats[r]
				fmt.Fprintf(w, "chronicle_http_route_duration_seconds{path=\"%s\"} %.6f\n", r, stat.AvgMs/1000.0)
			}
		}
	}

	// 5. Go runtime memory and goroutines
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Fprintf(w, "# HELP chronicle_goroutines Number of active Go goroutines.\n")
	fmt.Fprintf(w, "# TYPE chronicle_goroutines gauge\n")
	fmt.Fprintf(w, "chronicle_goroutines %d\n", runtime.NumGoroutine())

	fmt.Fprintf(w, "# HELP chronicle_memory_alloc_bytes Bytes of allocated heap memory currently in use.\n")
	fmt.Fprintf(w, "# TYPE chronicle_memory_alloc_bytes gauge\n")
	fmt.Fprintf(w, "chronicle_memory_alloc_bytes %d\n", m.Alloc)

	fmt.Fprintf(w, "# HELP chronicle_memory_total_alloc_bytes Cumulative bytes allocated for heap objects.\n")
	fmt.Fprintf(w, "# TYPE chronicle_memory_total_alloc_bytes counter\n")
	fmt.Fprintf(w, "chronicle_memory_total_alloc_bytes %d\n", m.TotalAlloc)

	fmt.Fprintf(w, "# HELP chronicle_memory_sys_bytes Total bytes of memory obtained from the OS.\n")
	fmt.Fprintf(w, "# TYPE chronicle_memory_sys_bytes gauge\n")
	fmt.Fprintf(w, "chronicle_memory_sys_bytes %d\n", m.Sys)

	fmt.Fprintf(w, "# HELP chronicle_memory_heap_objects Number of allocated heap objects.\n")
	fmt.Fprintf(w, "# TYPE chronicle_memory_heap_objects gauge\n")
	fmt.Fprintf(w, "chronicle_memory_heap_objects %d\n", m.HeapObjects)

	fmt.Fprintf(w, "# HELP chronicle_gc_runs_total Total number of completed garbage collection cycles.\n")
	fmt.Fprintf(w, "# TYPE chronicle_gc_runs_total counter\n")
	fmt.Fprintf(w, "chronicle_gc_runs_total %d\n", m.NumGC)
}
