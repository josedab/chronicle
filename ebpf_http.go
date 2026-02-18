package chronicle

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// EBPFHTTPEvent represents an HTTP request captured via eBPF uprobe/kprobe.
type EBPFHTTPEvent struct {
	Timestamp    int64             `json:"timestamp"`
	Method       string            `json:"method"`
	Path         string            `json:"path"`
	StatusCode   int               `json:"status_code"`
	LatencyNs    int64             `json:"latency_ns"`
	RequestSize  int64             `json:"request_size"`
	ResponseSize int64             `json:"response_size"`
	SourceIP     string            `json:"source_ip,omitempty"`
	DestIP       string            `json:"dest_ip,omitempty"`
	DestPort     int               `json:"dest_port,omitempty"`
	PID          int               `json:"pid"`
	ProcessName  string            `json:"process_name,omitempty"`
	ContainerID  string            `json:"container_id,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
}

// EBPFHTTPConfig configures HTTP request tracing.
type EBPFHTTPConfig struct {
	Enabled          bool          `json:"enabled"`
	CaptureHeaders   bool          `json:"capture_headers"`
	PathNormalization bool         `json:"path_normalization"` // Normalize paths like /api/v1/users/123 → /api/v1/users/:id
	MaxPathDepth     int           `json:"max_path_depth"`
	SampleRate       float64       `json:"sample_rate"` // 0.0-1.0
	LatencyBuckets   []float64     `json:"latency_buckets"`
}

// DefaultEBPFHTTPConfig returns sensible defaults.
func DefaultEBPFHTTPConfig() EBPFHTTPConfig {
	return EBPFHTTPConfig{
		Enabled:           true,
		PathNormalization: true,
		MaxPathDepth:      10,
		SampleRate:        1.0,
		LatencyBuckets:    []float64{1e6, 5e6, 10e6, 25e6, 50e6, 100e6, 250e6, 500e6, 1e9, 5e9},
	}
}

// EBPFHTTPCollector collects HTTP request metrics via eBPF or fallback.
type EBPFHTTPCollector struct {
	config  EBPFHTTPConfig
	events  []EBPFHTTPEvent
	stats   EBPFHTTPStats

	mu      sync.RWMutex
	stopCh  chan struct{}
	running bool
}

// EBPFHTTPStats provides aggregated HTTP collection statistics.
type EBPFHTTPStats struct {
	TotalRequests    int64              `json:"total_requests"`
	TotalErrors      int64              `json:"total_errors"` // 5xx responses
	AvgLatencyNs     int64              `json:"avg_latency_ns"`
	P50LatencyNs     int64              `json:"p50_latency_ns"`
	P99LatencyNs     int64              `json:"p99_latency_ns"`
	RequestsByMethod map[string]int64   `json:"requests_by_method"`
	RequestsByStatus map[int]int64      `json:"requests_by_status"`
	TopPaths         map[string]int64   `json:"top_paths"`
}

// NewEBPFHTTPCollector creates a new HTTP request collector.
func NewEBPFHTTPCollector(config EBPFHTTPConfig) *EBPFHTTPCollector {
	return &EBPFHTTPCollector{
		config: config,
		events: make([]EBPFHTTPEvent, 0),
		stats: EBPFHTTPStats{
			RequestsByMethod: make(map[string]int64),
			RequestsByStatus: make(map[int]int64),
			TopPaths:         make(map[string]int64),
		},
		stopCh: make(chan struct{}),
	}
}

// RecordEvent records an HTTP event (used when eBPF feeds data in).
func (c *EBPFHTTPCollector) RecordEvent(event EBPFHTTPEvent) {
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixNano()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Keep bounded event buffer
	if len(c.events) >= 10000 {
		c.events = c.events[1:]
	}
	c.events = append(c.events, event)

	// Update stats
	c.stats.TotalRequests++
	if event.StatusCode >= 500 {
		c.stats.TotalErrors++
	}
	c.stats.RequestsByMethod[event.Method]++
	c.stats.RequestsByStatus[event.StatusCode]++

	path := event.Path
	if c.config.PathNormalization {
		path = normalizePath(path)
	}
	c.stats.TopPaths[path]++
}

// GetStats returns collected HTTP statistics.
func (c *EBPFHTTPCollector) GetStats() EBPFHTTPStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

// GetRecentEvents returns recent HTTP events.
func (c *EBPFHTTPCollector) GetRecentEvents(limit int) []EBPFHTTPEvent {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if limit <= 0 || limit > len(c.events) {
		limit = len(c.events)
	}

	start := len(c.events) - limit
	result := make([]EBPFHTTPEvent, limit)
	copy(result, c.events[start:])
	return result
}

// ToPoints converts recent HTTP events to time-series points for storage.
func (c *EBPFHTTPCollector) ToPoints() []Point {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var points []Point
	for method, count := range c.stats.RequestsByMethod {
		points = append(points, Point{
			Metric:    "ebpf_http_requests_total",
			Value:     float64(count),
			Timestamp: time.Now().UnixNano(),
			Tags:      map[string]string{"method": method},
		})
	}

	for status, count := range c.stats.RequestsByStatus {
		points = append(points, Point{
			Metric:    "ebpf_http_responses_total",
			Value:     float64(count),
			Timestamp: time.Now().UnixNano(),
			Tags:      map[string]string{"status": fmt.Sprintf("%d", status)},
		})
	}

	return points
}

// HTTPMiddleware returns an http.Handler middleware that records request metrics.
// This is an alternative to eBPF for Go applications.
func (c *EBPFHTTPCollector) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, statusCode: 200}

		next.ServeHTTP(recorder, r)

		c.RecordEvent(EBPFHTTPEvent{
			Timestamp:    start.UnixNano(),
			Method:       r.Method,
			Path:         r.URL.Path,
			StatusCode:   recorder.statusCode,
			LatencyNs:    time.Since(start).Nanoseconds(),
			RequestSize:  r.ContentLength,
			ResponseSize: int64(recorder.bytesWritten),
			SourceIP:     r.RemoteAddr,
		})
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.bytesWritten += n
	return n, err
}

func normalizePath(path string) string {
	// Simple normalization: replace numeric segments with :id
	parts := splitPath(path)
	for i, p := range parts {
		if isNumeric(p) {
			parts[i] = ":id"
		}
	}
	result := "/" + joinPath(parts)
	return result
}

func splitPath(path string) []string {
	var parts []string
	current := ""
	for _, c := range path {
		if c == '/' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(c)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

func joinPath(parts []string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += "/"
		}
		result += p
	}
	return result
}

func isNumeric(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
