package chronicle

import (
	"fmt"
	"sync"
	"time"
)

// OTLPProtoConfig configures the OTLP proto ingestion engine.
type OTLPProtoConfig struct {
	Enabled                     bool `json:"enabled"`
	MaxBatchSize                int  `json:"max_batch_size"`
	EnableHistograms            bool `json:"enable_histograms"`
	EnableSummaries             bool `json:"enable_summaries"`
	EnableExponentialHistograms bool `json:"enable_exponential_histograms"`
}

// DefaultOTLPProtoConfig returns sensible defaults.
func DefaultOTLPProtoConfig() OTLPProtoConfig {
	return OTLPProtoConfig{
		Enabled:                     true,
		MaxBatchSize:                10000,
		EnableHistograms:            true,
		EnableSummaries:             true,
		EnableExponentialHistograms: false,
	}
}

// ProtoMetricPoint represents a metric data point in proto format.
type ProtoMetricPoint struct {
	Name      string            `json:"name"`
	Value     float64           `json:"value"`
	Timestamp int64             `json:"timestamp"`
	Labels    map[string]string `json:"labels"`
	Type      string            `json:"type"` // gauge, sum, histogram, summary
}

// ProtoHistogramPoint represents a histogram data point.
type ProtoHistogramPoint struct {
	Name      string            `json:"name"`
	Buckets   []ProtoBucket     `json:"buckets"`
	Sum       float64           `json:"sum"`
	Count     uint64            `json:"count"`
	Timestamp int64             `json:"timestamp"`
	Labels    map[string]string `json:"labels"`
}

// ProtoBucket represents a single histogram bucket.
type ProtoBucket struct {
	UpperBound float64 `json:"upper_bound"`
	Count      uint64  `json:"count"`
}

// ProtoIngestResult reports the outcome of an ingestion call.
type ProtoIngestResult struct {
	PointsAccepted      int           `json:"points_accepted"`
	PointsRejected      int           `json:"points_rejected"`
	HistogramsProcessed int           `json:"histograms_processed"`
	SummariesProcessed  int           `json:"summaries_processed"`
	Duration            time.Duration `json:"duration"`
}

// OTLPProtoStats holds engine statistics.
type OTLPProtoStats struct {
	TotalIngested       int64 `json:"total_ingested"`
	TotalRejected       int64 `json:"total_rejected"`
	HistogramsProcessed int64 `json:"histograms_processed"`
	SummariesProcessed  int64 `json:"summaries_processed"`
}

// OTLPProtoEngine handles OTLP proto metric ingestion.
type OTLPProtoEngine struct {
	db      *DB
	config  OTLPProtoConfig
	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}
	stats   OTLPProtoStats
}

// NewOTLPProtoEngine creates a new OTLP proto ingestion engine.
func NewOTLPProtoEngine(db *DB, cfg OTLPProtoConfig) *OTLPProtoEngine {
	return &OTLPProtoEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

func (e *OTLPProtoEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

func (e *OTLPProtoEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// IngestMetrics processes a batch of proto metric points.
func (e *OTLPProtoEngine) IngestMetrics(points []ProtoMetricPoint) (*ProtoIngestResult, error) {
	start := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()

	result := &ProtoIngestResult{}

	for _, p := range points {
		if result.PointsAccepted+result.PointsRejected >= e.config.MaxBatchSize {
			result.PointsRejected += len(points) - (result.PointsAccepted + result.PointsRejected)
			break
		}
		if p.Name == "" {
			result.PointsRejected++
			e.stats.TotalRejected++
			continue
		}
		if p.Type == "summary" && !e.config.EnableSummaries {
			result.PointsRejected++
			e.stats.TotalRejected++
			continue
		}
		if p.Type == "histogram" && !e.config.EnableHistograms {
			result.PointsRejected++
			e.stats.TotalRejected++
			continue
		}

		result.PointsAccepted++
		e.stats.TotalIngested++

		if p.Type == "histogram" {
			result.HistogramsProcessed++
			e.stats.HistogramsProcessed++
		}
		if p.Type == "summary" {
			result.SummariesProcessed++
			e.stats.SummariesProcessed++
		}
	}

	result.Duration = time.Since(start)
	return result, nil
}

// IngestHistograms processes a batch of proto histogram points.
func (e *OTLPProtoEngine) IngestHistograms(histograms []ProtoHistogramPoint) (*ProtoIngestResult, error) {
	start := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.config.EnableHistograms {
		return &ProtoIngestResult{
			PointsRejected: len(histograms),
			Duration:       time.Since(start),
		}, nil
	}

	result := &ProtoIngestResult{}

	for _, h := range histograms {
		if result.PointsAccepted+result.PointsRejected >= e.config.MaxBatchSize {
			result.PointsRejected += len(histograms) - (result.PointsAccepted + result.PointsRejected)
			break
		}
		if h.Name == "" {
			result.PointsRejected++
			e.stats.TotalRejected++
			continue
		}
		result.PointsAccepted++
		result.HistogramsProcessed++
		e.stats.TotalIngested++
		e.stats.HistogramsProcessed++
	}

	result.Duration = time.Since(start)
	return result, nil
}

// ConvertToPoints converts proto metric points to Chronicle Points.
func (e *OTLPProtoEngine) ConvertToPoints(proto []ProtoMetricPoint) []Point {
	e.mu.RLock()
	defer e.mu.RUnlock()

	points := make([]Point, 0, len(proto))
	for _, p := range proto {
		if p.Name == "" {
			continue
		}
		tags := make(map[string]string, len(p.Labels)+1)
		for k, v := range p.Labels {
			tags[k] = v
		}
		tags["__type__"] = p.Type

		points = append(points, Point{
			Metric:    p.Name,
			Value:     p.Value,
			Timestamp: p.Timestamp,
			Tags:      tags,
		})
	}
	return points
}

// GetStats returns engine statistics.
func (e *OTLPProtoEngine) GetStats() OTLPProtoStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// --- Full OTLP Metric Types ---

// OTLPMetricType identifies the OTLP metric data type.
type OTLPMetricType int

const (
	OTLPMetricGauge OTLPMetricType = iota
	OTLPMetricSum
	OTLPMetricHistogram
	OTLPMetricSummary
	OTLPMetricExponentialHistogram
)

func (t OTLPMetricType) String() string {
	switch t {
	case OTLPMetricGauge:
		return "gauge"
	case OTLPMetricSum:
		return "sum"
	case OTLPMetricHistogram:
		return "histogram"
	case OTLPMetricSummary:
		return "summary"
	case OTLPMetricExponentialHistogram:
		return "exponential_histogram"
	default:
		return "unknown"
	}
}

// OTLPAggregationTemporality defines whether metrics are delta or cumulative.
type OTLPAggregationTemporality int

const (
	OTLPTemporalityUnspecified OTLPAggregationTemporality = iota
	OTLPTemporalityDelta
	OTLPTemporalityCumulative
)

// OTLPExemplar represents a trace exemplar linked to a metric data point.
type OTLPExemplar struct {
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
	Value         float64           `json:"value"`
	Timestamp     int64             `json:"timestamp"`
	FilteredAttrs map[string]string `json:"filtered_attrs,omitempty"`
}

// OTLPExponentialHistogramPoint represents an exponential histogram data point.
type OTLPExponentialHistogramPoint struct {
	Name            string            `json:"name"`
	Timestamp       int64             `json:"timestamp"`
	Labels          map[string]string `json:"labels"`
	Count           uint64            `json:"count"`
	Sum             float64           `json:"sum"`
	Scale           int32             `json:"scale"`
	ZeroCount       uint64            `json:"zero_count"`
	PositiveOffset  int32             `json:"positive_offset"`
	PositiveBuckets []uint64          `json:"positive_buckets"`
	NegativeOffset  int32             `json:"negative_offset"`
	NegativeBuckets []uint64          `json:"negative_buckets"`
	Exemplars       []OTLPExemplar    `json:"exemplars,omitempty"`
}

// OTLPSummaryPoint represents a summary data point with quantiles.
type OTLPSummaryPoint struct {
	Name      string              `json:"name"`
	Timestamp int64               `json:"timestamp"`
	Labels    map[string]string   `json:"labels"`
	Count     uint64              `json:"count"`
	Sum       float64             `json:"sum"`
	Quantiles []OTLPQuantileValue `json:"quantiles"`
}

// OTLPMetricBatch represents a full OTLP metrics export request with resource context.
type OTLPMetricBatch struct {
	Resource OTLPResource     `json:"resource"`
	Scope    OTLPScope        `json:"scope"`
	Metrics  []OTLPMetricData `json:"metrics"`
}

// OTLPMetricData represents a single metric with its data points.
type OTLPMetricData struct {
	Name                  string                          `json:"name"`
	Description           string                          `json:"description,omitempty"`
	Unit                  string                          `json:"unit,omitempty"`
	Type                  OTLPMetricType                  `json:"type"`
	Temporality           OTLPAggregationTemporality      `json:"temporality,omitempty"`
	IsMonotonic           bool                            `json:"is_monotonic,omitempty"`
	DataPoints            []ProtoMetricPoint              `json:"data_points,omitempty"`
	Histograms            []ProtoHistogramPoint           `json:"histograms,omitempty"`
	Summaries             []OTLPSummaryPoint              `json:"summaries,omitempty"`
	ExponentialHistograms []OTLPExponentialHistogramPoint `json:"exponential_histograms,omitempty"`
	Exemplars             []OTLPExemplar                  `json:"exemplars,omitempty"`
}

// OTLPBatchIngestResult reports the outcome of a batch ingestion.
type OTLPBatchIngestResult struct {
	MetricsAccepted int           `json:"metrics_accepted"`
	MetricsRejected int           `json:"metrics_rejected"`
	PointsWritten   int           `json:"points_written"`
	WriteErrors     int           `json:"write_errors,omitempty"`
	ExemplarsLinked int           `json:"exemplars_linked"`
	Duration        time.Duration `json:"duration"`
}

// otlpKeyValuesToMap converts OTLPKeyValue slice to a string map.
func otlpKeyValuesToMap(attrs []OTLPKeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		if kv.Value.StringValue != nil {
			m[kv.Key] = *kv.Value.StringValue
		} else if kv.Value.IntValue != nil {
			m[kv.Key] = fmt.Sprintf("%d", *kv.Value.IntValue)
		} else if kv.Value.DoubleValue != nil {
			m[kv.Key] = fmt.Sprintf("%g", *kv.Value.DoubleValue)
		} else if kv.Value.BoolValue != nil {
			m[kv.Key] = fmt.Sprintf("%t", *kv.Value.BoolValue)
		}
	}
	return m
}

// IngestBatch processes a full OTLP metric batch with resource context.
func (e *OTLPProtoEngine) IngestBatch(batch *OTLPMetricBatch) (*OTLPBatchIngestResult, error) {
	start := time.Now()
	if batch == nil {
		return nil, fmt.Errorf("nil batch")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	result := &OTLPBatchIngestResult{}

	// writePoint wraps db.Write and tracks errors on the result.
	writePoint := func(p Point) {
		if e.db == nil {
			return
		}
		if err := e.db.Write(p); err != nil {
			result.WriteErrors++
		}
	}

	// Map resource attributes to tags
	baseTags := otlpKeyValuesToMap(batch.Resource.Attributes)
	for k, v := range baseTags {
		baseTags["resource."+k] = v
		delete(baseTags, k)
	}
	if batch.Scope.Name != "" {
		baseTags["otel.scope.name"] = batch.Scope.Name
	}
	if batch.Scope.Version != "" {
		baseTags["otel.scope.version"] = batch.Scope.Version
	}

	for _, metric := range batch.Metrics {
		if metric.Name == "" {
			result.MetricsRejected++
			e.stats.TotalRejected++
			continue
		}

		metricTags := mergeTagMaps(baseTags, map[string]string{
			"__otel_type__": metric.Type.String(),
		})
		if metric.Unit != "" {
			metricTags["__unit__"] = metric.Unit
		}

		switch metric.Type {
		case OTLPMetricGauge, OTLPMetricSum:
			for _, dp := range metric.DataPoints {
				tags := mergeTagMaps(metricTags, dp.Labels)
				writePoint(Point{
					Metric: metric.Name, Value: dp.Value,
					Timestamp: dp.Timestamp, Tags: tags,
				})
				result.PointsWritten++
			}

		case OTLPMetricHistogram:
			if !e.config.EnableHistograms {
				result.MetricsRejected++
				continue
			}
			for _, h := range metric.Histograms {
				tags := mergeTagMaps(metricTags, h.Labels)
				writePoint(Point{
					Metric: metric.Name + "_sum", Value: h.Sum,
					Timestamp: h.Timestamp, Tags: tags,
				})
				writePoint(Point{
					Metric: metric.Name + "_count", Value: float64(h.Count),
					Timestamp: h.Timestamp, Tags: tags,
				})
				for _, b := range h.Buckets {
					btags := mergeTagMaps(tags, map[string]string{
						"le": fmt.Sprintf("%g", b.UpperBound),
					})
					writePoint(Point{
						Metric: metric.Name + "_bucket", Value: float64(b.Count),
						Timestamp: h.Timestamp, Tags: btags,
					})
				}
				result.PointsWritten += 2 + len(h.Buckets)
				e.stats.HistogramsProcessed++
			}

		case OTLPMetricSummary:
			if !e.config.EnableSummaries {
				result.MetricsRejected++
				continue
			}
			for _, s := range metric.Summaries {
				tags := mergeTagMaps(metricTags, s.Labels)
				writePoint(Point{
					Metric: metric.Name + "_sum", Value: s.Sum,
					Timestamp: s.Timestamp, Tags: tags,
				})
				writePoint(Point{
					Metric: metric.Name + "_count", Value: float64(s.Count),
					Timestamp: s.Timestamp, Tags: tags,
				})
				for _, q := range s.Quantiles {
					qtags := mergeTagMaps(tags, map[string]string{
						"quantile": fmt.Sprintf("%g", q.Quantile),
					})
					writePoint(Point{
						Metric: metric.Name, Value: q.Value,
						Timestamp: s.Timestamp, Tags: qtags,
					})
				}
				result.PointsWritten += 2 + len(s.Quantiles)
				e.stats.SummariesProcessed++
			}

		case OTLPMetricExponentialHistogram:
			if !e.config.EnableExponentialHistograms {
				result.MetricsRejected++
				continue
			}
			for _, eh := range metric.ExponentialHistograms {
				tags := mergeTagMaps(metricTags, eh.Labels)
				writePoint(Point{
					Metric: metric.Name + "_sum", Value: eh.Sum,
					Timestamp: eh.Timestamp, Tags: tags,
				})
				writePoint(Point{
					Metric: metric.Name + "_count", Value: float64(eh.Count),
					Timestamp: eh.Timestamp, Tags: tags,
				})
				result.PointsWritten += 2
				e.stats.HistogramsProcessed++
			}
		}

		// Process exemplars for metrics-to-traces correlation
		for _, ex := range metric.Exemplars {
			if ex.TraceID != "" && ex.SpanID != "" {
				result.ExemplarsLinked++
				exTags := mergeTagMaps(metricTags, map[string]string{
					"__exemplar_trace_id__": ex.TraceID,
					"__exemplar_span_id__":  ex.SpanID,
				})
				for k, v := range ex.FilteredAttrs {
					exTags["exemplar."+k] = v
				}
				writePoint(Point{
					Metric:    metric.Name + "__exemplar",
					Value:     ex.Value,
					Timestamp: ex.Timestamp,
					Tags:      exTags,
				})
			}
		}

		result.MetricsAccepted++
		e.stats.TotalIngested++
	}

	result.Duration = time.Since(start)
	if result.WriteErrors > 0 {
		return result, fmt.Errorf("partial success: %d write errors out of %d points", result.WriteErrors, result.PointsWritten)
	}
	return result, nil
}

func mergeTagMaps(base, overlay map[string]string) map[string]string {
	result := make(map[string]string, len(base)+len(overlay))
	for k, v := range base {
		result[k] = v
	}
	for k, v := range overlay {
		result[k] = v
	}
	return result
}
