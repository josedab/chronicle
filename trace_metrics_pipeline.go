package chronicle

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// TraceMetricsPipelineConfig configures the trace-to-metrics pipeline.
type TraceMetricsPipelineConfig struct {
	Enabled              bool          `json:"enabled"`
	CollectionInterval   time.Duration `json:"collection_interval"`
	HistogramBuckets     []float64     `json:"histogram_buckets"`
	AutoGenerateSLIs     bool          `json:"auto_generate_slis"`
	EnableCorrelation    bool          `json:"enable_correlation"`
	MaxServicesTracked   int           `json:"max_services_tracked"`
}

// DefaultTraceMetricsPipelineConfig returns sensible defaults.
func DefaultTraceMetricsPipelineConfig() TraceMetricsPipelineConfig {
	return TraceMetricsPipelineConfig{
		Enabled:            true,
		CollectionInterval: 30 * time.Second,
		HistogramBuckets:   []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		AutoGenerateSLIs:   true,
		EnableCorrelation:  true,
		MaxServicesTracked: 1000,
	}
}

// REDMetrics holds Rate, Errors, Duration metrics for a service/operation.
type REDMetrics struct {
	Service       string             `json:"service"`
	Operation     string             `json:"operation"`
	RequestCount  int64              `json:"request_count"`
	ErrorCount    int64              `json:"error_count"`
	ErrorRate     float64            `json:"error_rate"`
	DurationSum   float64            `json:"duration_sum_ms"`
	DurationMin   float64            `json:"duration_min_ms"`
	DurationMax   float64            `json:"duration_max_ms"`
	DurationP50   float64            `json:"duration_p50_ms"`
	DurationP95   float64            `json:"duration_p95_ms"`
	DurationP99   float64            `json:"duration_p99_ms"`
	Histogram     map[float64]int64  `json:"duration_histogram"`
	LastUpdated   time.Time          `json:"last_updated"`
}

// ServiceTopology represents the discovered service dependency graph.
type ServiceTopology struct {
	Services    map[string]*ServiceNode    `json:"services"`
	Edges       []ServiceEdge              `json:"edges"`
	DiscoveredAt time.Time                 `json:"discovered_at"`
}

// ServiceNode represents a service in the topology.
type ServiceNode struct {
	Name          string   `json:"name"`
	Operations    []string `json:"operations"`
	SpanCount     int64    `json:"span_count"`
	ErrorCount    int64    `json:"error_count"`
	AvgDurationMs float64  `json:"avg_duration_ms"`
}

// ServiceEdge represents a dependency between services.
type ServiceEdge struct {
	From      string  `json:"from"`
	To        string  `json:"to"`
	CallCount int64   `json:"call_count"`
	ErrorRate float64 `json:"error_rate"`
}

// TraceMetricsPipeline derives RED metrics from stored traces and generates SLI definitions.
type TraceMetricsPipeline struct {
	config       TraceMetricsPipelineConfig
	traceStore   *TraceStore
	db           *DB
	sloEngine    *SLOEngine
	correlation  *SignalCorrelationEngine

	mu           sync.RWMutex
	redMetrics   map[string]*REDMetrics // key: service:operation
	topology     *ServiceTopology
	durations    map[string][]float64   // key: service:operation -> duration samples

	running      bool
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewTraceMetricsPipeline creates a new trace-to-metrics pipeline.
func NewTraceMetricsPipeline(
	db *DB,
	traceStore *TraceStore,
	sloEngine *SLOEngine,
	correlation *SignalCorrelationEngine,
	config TraceMetricsPipelineConfig,
) *TraceMetricsPipeline {
	return &TraceMetricsPipeline{
		config:      config,
		traceStore:  traceStore,
		db:          db,
		sloEngine:   sloEngine,
		correlation: correlation,
		redMetrics:  make(map[string]*REDMetrics),
		topology: &ServiceTopology{
			Services: make(map[string]*ServiceNode),
		},
		durations: make(map[string][]float64),
		stopCh:    make(chan struct{}),
	}
}

// Start starts the background pipeline processing.
func (p *TraceMetricsPipeline) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return fmt.Errorf("trace_metrics: already running")
	}
	p.running = true
	p.stopCh = make(chan struct{})

	p.wg.Add(1)
	go p.processLoop()

	return nil
}

// Stop stops the pipeline.
func (p *TraceMetricsPipeline) Stop() error {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return nil
	}
	p.running = false
	close(p.stopCh)
	p.mu.Unlock()
	p.wg.Wait()
	return nil
}

func (p *TraceMetricsPipeline) processLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.config.CollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.collectREDMetrics()
		}
	}
}

// ProcessSpan processes a single span and updates RED metrics.
func (p *TraceMetricsPipeline) ProcessSpan(span Span) {
	key := span.Service + ":" + span.Name

	p.mu.Lock()
	defer p.mu.Unlock()

	red, exists := p.redMetrics[key]
	if !exists {
		red = &REDMetrics{
			Service:     span.Service,
			Operation:   span.Name,
			Histogram:   make(map[float64]int64),
			DurationMin: math.Inf(1),
			DurationMax: math.Inf(-1),
		}
		p.redMetrics[key] = red
	}

	red.RequestCount++
	durationMs := float64(span.Duration.Milliseconds())
	red.DurationSum += durationMs

	if durationMs < red.DurationMin {
		red.DurationMin = durationMs
	}
	if durationMs > red.DurationMax {
		red.DurationMax = durationMs
	}

	// Histogram bucketing
	for _, boundary := range p.config.HistogramBuckets {
		if durationMs <= boundary {
			red.Histogram[boundary]++
			break
		}
	}

	// Track if error - classify by status code and span events
	isError := span.Status.Code == StatusError
	if !isError {
		for _, evt := range span.Events {
			if evt.Name == "exception" {
				isError = true
				break
			}
		}
	}
	if isError {
		red.ErrorCount++
	}
	if red.RequestCount > 0 {
		red.ErrorRate = float64(red.ErrorCount) / float64(red.RequestCount)
	}

	red.LastUpdated = time.Now()

	// Collect duration samples for percentile computation
	p.durations[key] = append(p.durations[key], durationMs)
	// Cap sample size
	if len(p.durations[key]) > 10000 {
		p.durations[key] = p.durations[key][len(p.durations[key])-10000:]
	}

	// Update topology
	p.updateTopology(span)
}

func (p *TraceMetricsPipeline) updateTopology(span Span) {
	node, exists := p.topology.Services[span.Service]
	if !exists {
		node = &ServiceNode{Name: span.Service}
		p.topology.Services[span.Service] = node
	}
	node.SpanCount++
	if span.Status.Code == StatusError {
		node.ErrorCount++
	}

	// Track operations
	found := false
	for _, op := range node.Operations {
		if op == span.Name {
			found = true
			break
		}
	}
	if !found {
		node.Operations = append(node.Operations, span.Name)
	}

	// Track cross-service edges via parent span's service attribute
	if span.ParentSpanID != "" {
		parentService := span.Attributes["parent.service"]
		if parentService != "" && parentService != span.Service {
			edgeFound := false
			for i := range p.topology.Edges {
				if p.topology.Edges[i].From == parentService && p.topology.Edges[i].To == span.Service {
					p.topology.Edges[i].CallCount++
					if span.Status.Code == StatusError {
						total := p.topology.Edges[i].CallCount
						p.topology.Edges[i].ErrorRate = float64(1) / float64(total) // simplified
					}
					edgeFound = true
					break
				}
			}
			if !edgeFound {
				errRate := 0.0
				if span.Status.Code == StatusError {
					errRate = 1.0
				}
				p.topology.Edges = append(p.topology.Edges, ServiceEdge{
					From:      parentService,
					To:        span.Service,
					CallCount: 1,
					ErrorRate: errRate,
				})
			}
		}
	}
}

// collectREDMetrics writes accumulated RED metrics to the DB.
func (p *TraceMetricsPipeline) collectREDMetrics() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now().UnixNano()

	for key, red := range p.redMetrics {
		// Compute percentiles from samples
		if samples, ok := p.durations[key]; ok && len(samples) > 0 {
			red.DurationP50 = tracePercentile(samples, 0.50)
			red.DurationP95 = tracePercentile(samples, 0.95)
			red.DurationP99 = tracePercentile(samples, 0.99)
			if red.RequestCount > 0 {
				node := p.topology.Services[red.Service]
				if node != nil {
					node.AvgDurationMs = red.DurationSum / float64(red.RequestCount)
				}
			}
		}

		tags := map[string]string{
			"service":   red.Service,
			"operation": red.Operation,
		}

		// Write trace_request_count
		p.db.Write(Point{
			Metric:    "trace_request_count",
			Value:     float64(red.RequestCount),
			Timestamp: now,
			Tags:      tags,
		})

		// Write trace_error_count
		p.db.Write(Point{
			Metric:    "trace_error_count",
			Value:     float64(red.ErrorCount),
			Timestamp: now,
			Tags:      tags,
		})

		// Write trace_duration_bucket histogram
		for boundary, count := range red.Histogram {
			bucketTags := map[string]string{
				"service":   red.Service,
				"operation": red.Operation,
				"le":        fmt.Sprintf("%.0f", boundary),
			}
			p.db.Write(Point{
				Metric:    "trace_duration_bucket",
				Value:     float64(count),
				Timestamp: now,
				Tags:      bucketTags,
			})
		}

		// Write percentile metrics
		for _, pct := range []struct {
			name string
			val  float64
		}{
			{"trace_duration_p50", red.DurationP50},
			{"trace_duration_p95", red.DurationP95},
			{"trace_duration_p99", red.DurationP99},
		} {
			p.db.Write(Point{
				Metric:    pct.name,
				Value:     pct.val,
				Timestamp: now,
				Tags:      tags,
			})
		}
	}
}

// tracePercentile computes the p-th percentile from unsorted samples.
func tracePercentile(samples []float64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := make([]float64, len(samples))
	copy(sorted, samples)
	// Simple insertion sort for small samples
	for i := 1; i < len(sorted); i++ {
		key := sorted[i]
		j := i - 1
		for j >= 0 && sorted[j] > key {
			sorted[j+1] = sorted[j]
			j--
		}
		sorted[j+1] = key
	}

	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// GetREDMetrics returns RED metrics for a service/operation.
func (p *TraceMetricsPipeline) GetREDMetrics(service, operation string) *REDMetrics {
	p.mu.RLock()
	defer p.mu.RUnlock()
	key := service + ":" + operation
	return p.redMetrics[key]
}

// GetAllREDMetrics returns all RED metrics.
func (p *TraceMetricsPipeline) GetAllREDMetrics() map[string]*REDMetrics {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]*REDMetrics, len(p.redMetrics))
	for k, v := range p.redMetrics {
		out[k] = v
	}
	return out
}

// GetTopology returns the discovered service topology.
func (p *TraceMetricsPipeline) GetTopology() *ServiceTopology {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.topology
}

// GenerateSLIs auto-generates SLI definitions from the trace topology.
func (p *TraceMetricsPipeline) GenerateSLIs(ctx context.Context) ([]SLODefinition, error) {
	if p.sloEngine == nil {
		return nil, fmt.Errorf("trace_metrics: SLO engine not configured")
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	var slis []SLODefinition

	for _, red := range p.redMetrics {
		if red.RequestCount < 10 {
			continue // Skip low-traffic operations
		}

		// Availability SLI: error rate < 0.1%
		availSLI := SLODefinition{
			Name:        fmt.Sprintf("sli_%s_%s_availability", red.Service, red.Operation),
			Description: fmt.Sprintf("Availability SLI for %s/%s", red.Service, red.Operation),
			SLIType:     SLIAvailability,
			Target:      99.9,
			Window:      24 * time.Hour,
			Metric:      "trace_error_count",
			Labels:      map[string]string{"service": red.Service, "operation": red.Operation},
			Enabled:     true,
		}
		slis = append(slis, availSLI)

		// Latency SLI: p99 < threshold
		latencyTarget := red.DurationP99 * 2 // 2x current p99 as target
		if latencyTarget < 100 {
			latencyTarget = 100
		}
		latSLI := SLODefinition{
			Name:               fmt.Sprintf("sli_%s_%s_latency", red.Service, red.Operation),
			Description:        fmt.Sprintf("Latency SLI for %s/%s (target: %.0fms p99)", red.Service, red.Operation, latencyTarget),
			SLIType:            SLILatency,
			Target:             99.0,
			Window:             24 * time.Hour,
			Metric:             "trace_duration_p99",
			Labels:             map[string]string{"service": red.Service, "operation": red.Operation},
			LatencyThresholdMs: latencyTarget,
			Enabled:            true,
		}
		slis = append(slis, latSLI)
	}

	// Register with SLO engine
	for _, sli := range slis {
		p.sloEngine.AddSLO(sli)
	}

	return slis, nil
}
