package chronicle

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/golang/snappy"
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
	HistogramsProcessed  int           `json:"histograms_processed"`
	SummariesProcessed   int           `json:"summaries_processed"`
	Duration             time.Duration `json:"duration"`
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
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	Value        float64           `json:"value"`
	Timestamp    int64             `json:"timestamp"`
	FilteredAttrs map[string]string `json:"filtered_attrs,omitempty"`
}

// OTLPExponentialHistogramPoint represents an exponential histogram data point.
type OTLPExponentialHistogramPoint struct {
	Name         string            `json:"name"`
	Timestamp    int64             `json:"timestamp"`
	Labels       map[string]string `json:"labels"`
	Count        uint64            `json:"count"`
	Sum          float64           `json:"sum"`
	Scale        int32             `json:"scale"`
	ZeroCount    uint64            `json:"zero_count"`
	PositiveOffset int32           `json:"positive_offset"`
	PositiveBuckets []uint64       `json:"positive_buckets"`
	NegativeOffset int32           `json:"negative_offset"`
	NegativeBuckets []uint64       `json:"negative_buckets"`
	Exemplars    []OTLPExemplar    `json:"exemplars,omitempty"`
}

// OTLPSummaryPoint represents a summary data point with quantiles.
type OTLPSummaryPoint struct {
	Name       string              `json:"name"`
	Timestamp  int64               `json:"timestamp"`
	Labels     map[string]string   `json:"labels"`
	Count      uint64              `json:"count"`
	Sum        float64             `json:"sum"`
	Quantiles  []OTLPQuantileValue `json:"quantiles"`
}

// OTLPMetricBatch represents a full OTLP metrics export request with resource context.
type OTLPMetricBatch struct {
	Resource OTLPResource       `json:"resource"`
	Scope    OTLPScope          `json:"scope"`
	Metrics  []OTLPMetricData   `json:"metrics"`
}

// OTLPMetricData represents a single metric with its data points.
type OTLPMetricData struct {
	Name        string             `json:"name"`
	Description string             `json:"description,omitempty"`
	Unit        string             `json:"unit,omitempty"`
	Type        OTLPMetricType     `json:"type"`
	Temporality OTLPAggregationTemporality `json:"temporality,omitempty"`
	IsMonotonic bool               `json:"is_monotonic,omitempty"`
	DataPoints  []ProtoMetricPoint `json:"data_points,omitempty"`
	Histograms  []ProtoHistogramPoint `json:"histograms,omitempty"`
	Summaries   []OTLPSummaryPoint `json:"summaries,omitempty"`
	ExponentialHistograms []OTLPExponentialHistogramPoint `json:"exponential_histograms,omitempty"`
	Exemplars   []OTLPExemplar     `json:"exemplars,omitempty"`
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

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *OTLPProtoEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/otlp-proto/ingest", func(w http.ResponseWriter, r *http.Request) {
		var points []ProtoMetricPoint
		if err := json.NewDecoder(r.Body).Decode(&points); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		result, err := e.IngestMetrics(points)
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	// Native protobuf OTLP endpoint (binary content-type)
	mux.HandleFunc("/api/v1/otlp/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		contentType := r.Header.Get("Content-Type")
		switch contentType {
		case "application/x-protobuf", "application/protobuf":
			body, err := io.ReadAll(io.LimitReader(r.Body, int64(e.config.MaxBatchSize*1024)))
			if err != nil {
				http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
				return
			}
			// Support snappy decompression (used by Prometheus remote write)
			contentEncoding := r.Header.Get("Content-Encoding")
			if contentEncoding == "snappy" {
				decoded, err := snappy.Decode(nil, body)
				if err != nil {
					http.Error(w, "snappy decode: "+err.Error(), http.StatusBadRequest)
					return
				}
				body = decoded
			}
			batch, err := DecodeOTLPMetricsBatch(body)
			if err != nil {
				http.Error(w, "decode protobuf: "+err.Error(), http.StatusBadRequest)
				return
			}
			result, err := e.IngestBatch(batch)
			if err != nil {
				internalError(w, err, "internal error")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(result)

		default:
			// JSON fallback
			var batch OTLPMetricBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			result, err := e.IngestBatch(&batch)
			if err != nil {
				internalError(w, err, "internal error")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(result)
		}
	})

	mux.HandleFunc("/api/v1/otlp-proto/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}

// DecodeOTLPMetricsBatch decodes a protobuf-encoded ExportMetricsServiceRequest.
// OTLP proto schema (simplified):
// ExportMetricsServiceRequest { repeated ResourceMetrics resource_metrics = 1; }
// ResourceMetrics { Resource resource = 1; repeated ScopeMetrics scope_metrics = 2; }
// ScopeMetrics { InstrumentationScope scope = 1; repeated Metric metrics = 2; }
// Metric { string name = 1; string description = 2; string unit = 3; oneof data { Gauge=5, Sum=7, Histogram=9, Summary=11, ExponentialHistogram=10 } }
func DecodeOTLPMetricsBatch(data []byte) (*OTLPMetricBatch, error) {
	batch := &OTLPMetricBatch{}
	dec := newProtoDecoder(data)

	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return nil, fmt.Errorf("decode export request: %w", err)
		}
		if field.Number == 1 && field.WireType == protoWireBytes {
			if err := decodeResourceMetrics(field.Bytes, batch); err != nil {
				return nil, err
			}
		}
	}
	return batch, nil
}

func decodeResourceMetrics(data []byte, batch *OTLPMetricBatch) error {
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return err
		}
		switch field.Number {
		case 1: // resource
			if field.WireType == protoWireBytes {
				decodeOTLPResource(field.Bytes, &batch.Resource)
			}
		case 2: // scope_metrics
			if field.WireType == protoWireBytes {
				if err := decodeScopeMetrics(field.Bytes, batch); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func decodeOTLPResource(data []byte, res *OTLPResource) {
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return
		}
		if field.Number == 1 && field.WireType == protoWireBytes {
			kv := decodeOTLPKeyValue(field.Bytes)
			if kv != nil {
				res.Attributes = append(res.Attributes, *kv)
			}
		}
	}
}

func decodeScopeMetrics(data []byte, batch *OTLPMetricBatch) error {
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return err
		}
		switch field.Number {
		case 1: // scope
			if field.WireType == protoWireBytes {
				decodeOTLPScope(field.Bytes, &batch.Scope)
			}
		case 2: // metrics
			if field.WireType == protoWireBytes {
				metric, err := decodeOTLPMetric(field.Bytes)
				if err != nil {
					return err
				}
				if metric != nil {
					batch.Metrics = append(batch.Metrics, *metric)
				}
			}
		}
	}
	return nil
}

func decodeOTLPScope(data []byte, scope *OTLPScope) {
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return
		}
		switch field.Number {
		case 1:
			scope.Name = string(field.Bytes)
		case 2:
			scope.Version = string(field.Bytes)
		}
	}
}

func decodeOTLPMetric(data []byte) (*OTLPMetricData, error) {
	dec := newProtoDecoder(data)
	metric := &OTLPMetricData{}

	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return nil, err
		}
		switch field.Number {
		case 1: // name
			metric.Name = string(field.Bytes)
		case 2: // description
			metric.Description = string(field.Bytes)
		case 3: // unit
			metric.Unit = string(field.Bytes)
		case 5: // gauge
			metric.Type = OTLPMetricGauge
			decodeGaugeDataPoints(field.Bytes, metric)
		case 7: // sum
			metric.Type = OTLPMetricSum
			decodeSumDataPoints(field.Bytes, metric)
		case 9: // histogram
			metric.Type = OTLPMetricHistogram
			decodeHistogramDataPoints(field.Bytes, metric)
		case 10: // exponential_histogram
			metric.Type = OTLPMetricExponentialHistogram
			decodeExpHistogramDataPoints(field.Bytes, metric)
		case 11: // summary
			metric.Type = OTLPMetricSummary
			decodeSummaryDataPoints(field.Bytes, metric)
		}
	}
	return metric, nil
}

func decodeGaugeDataPoints(data []byte, metric *OTLPMetricData) {
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return
		}
		if field.Number == 1 && field.WireType == protoWireBytes { // data_points
			dp := decodeNumberDataPoint(field.Bytes)
			dp.Type = "gauge"
			metric.DataPoints = append(metric.DataPoints, dp)
		}
	}
}

func decodeSumDataPoints(data []byte, metric *OTLPMetricData) {
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return
		}
		switch field.Number {
		case 1: // data_points
			if field.WireType == protoWireBytes {
				dp := decodeNumberDataPoint(field.Bytes)
				dp.Type = "sum"
				metric.DataPoints = append(metric.DataPoints, dp)
			}
		case 2: // aggregation_temporality
			metric.Temporality = OTLPAggregationTemporality(field.Varint)
		case 3: // is_monotonic
			metric.IsMonotonic = field.Varint != 0
		}
	}
}

func decodeNumberDataPoint(data []byte) ProtoMetricPoint {
	dec := newProtoDecoder(data)
	dp := ProtoMetricPoint{Labels: make(map[string]string)}
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return dp
		}
		switch field.Number {
		case 1: // attributes
			if field.WireType == protoWireBytes {
				kv := decodeOTLPKeyValue(field.Bytes)
				if kv != nil && kv.Value.StringValue != nil {
					dp.Labels[kv.Key] = *kv.Value.StringValue
				}
			}
		case 3: // time_unix_nano
			if field.WireType == protoWire64Bit {
				dp.Timestamp = int64(field.Fixed64)
			} else {
				dp.Timestamp = int64(field.Varint)
			}
		case 4: // as_double
			if field.WireType == protoWire64Bit {
				dp.Value = math.Float64frombits(field.Fixed64)
			}
		case 6: // as_int
			dp.Value = float64(int64(field.Varint))
		}
	}
	return dp
}

func decodeHistogramDataPoints(data []byte, metric *OTLPMetricData) {
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return
		}
		if field.Number == 1 && field.WireType == protoWireBytes {
			hp := decodeHistogramDataPoint(field.Bytes)
			metric.Histograms = append(metric.Histograms, hp)
		}
	}
}

func decodeHistogramDataPoint(data []byte) ProtoHistogramPoint {
	dec := newProtoDecoder(data)
	hp := ProtoHistogramPoint{Labels: make(map[string]string)}
	var bucketCounts []uint64
	var explicitBounds []float64

	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return hp
		}
		switch field.Number {
		case 1: // attributes
			if field.WireType == protoWireBytes {
				kv := decodeOTLPKeyValue(field.Bytes)
				if kv != nil && kv.Value.StringValue != nil {
					hp.Labels[kv.Key] = *kv.Value.StringValue
				}
			}
		case 3: // time_unix_nano
			if field.WireType == protoWire64Bit {
				hp.Timestamp = int64(field.Fixed64)
			}
		case 4: // count
			hp.Count = field.Varint
		case 5: // sum (double)
			if field.WireType == protoWire64Bit {
				hp.Sum = math.Float64frombits(field.Fixed64)
			}
		case 6: // bucket_counts (repeated fixed64)
			if field.WireType == protoWireBytes {
				inner := newProtoDecoder(field.Bytes)
				for inner.hasMore() {
					v, err := inner.readFixed64()
					if err != nil {
						break
					}
					bucketCounts = append(bucketCounts, v)
				}
			}
		case 7: // explicit_bounds (repeated double)
			if field.WireType == protoWireBytes {
				inner := newProtoDecoder(field.Bytes)
				for inner.hasMore() {
					v, err := inner.readFixed64()
					if err != nil {
						break
					}
					explicitBounds = append(explicitBounds, math.Float64frombits(v))
				}
			}
		}
	}

	// Convert bucket counts + explicit bounds to ProtoBucket format
	for i, bound := range explicitBounds {
		if i < len(bucketCounts) {
			hp.Buckets = append(hp.Buckets, ProtoBucket{
				UpperBound: bound,
				Count:      bucketCounts[i],
			})
		}
	}
	// +Inf bucket
	if len(bucketCounts) > len(explicitBounds) {
		hp.Buckets = append(hp.Buckets, ProtoBucket{
			UpperBound: math.Inf(1),
			Count:      bucketCounts[len(explicitBounds)],
		})
	}
	return hp
}

func decodeExpHistogramDataPoints(data []byte, metric *OTLPMetricData) {
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return
		}
		if field.Number == 1 && field.WireType == protoWireBytes {
			ep := decodeExpHistogramDataPoint(field.Bytes)
			metric.ExponentialHistograms = append(metric.ExponentialHistograms, ep)
		}
	}
}

func decodeExpHistogramDataPoint(data []byte) OTLPExponentialHistogramPoint {
	dec := newProtoDecoder(data)
	ep := OTLPExponentialHistogramPoint{Labels: make(map[string]string)}
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return ep
		}
		switch field.Number {
		case 1: // attributes
			if field.WireType == protoWireBytes {
				kv := decodeOTLPKeyValue(field.Bytes)
				if kv != nil && kv.Value.StringValue != nil {
					ep.Labels[kv.Key] = *kv.Value.StringValue
				}
			}
		case 3: // time_unix_nano
			if field.WireType == protoWire64Bit {
				ep.Timestamp = int64(field.Fixed64)
			}
		case 4: // count
			ep.Count = field.Varint
		case 5: // sum
			if field.WireType == protoWire64Bit {
				ep.Sum = math.Float64frombits(field.Fixed64)
			}
		case 6: // scale
			ep.Scale = int32(field.Varint)
		case 7: // zero_count
			ep.ZeroCount = field.Varint
		}
	}
	return ep
}

func decodeSummaryDataPoints(data []byte, metric *OTLPMetricData) {
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return
		}
		if field.Number == 1 && field.WireType == protoWireBytes {
			sp := decodeSummaryDataPoint(field.Bytes)
			metric.Summaries = append(metric.Summaries, sp)
		}
	}
}

func decodeSummaryDataPoint(data []byte) OTLPSummaryPoint {
	dec := newProtoDecoder(data)
	sp := OTLPSummaryPoint{Labels: make(map[string]string)}
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return sp
		}
		switch field.Number {
		case 1: // attributes
			if field.WireType == protoWireBytes {
				kv := decodeOTLPKeyValue(field.Bytes)
				if kv != nil && kv.Value.StringValue != nil {
					sp.Labels[kv.Key] = *kv.Value.StringValue
				}
			}
		case 3: // time_unix_nano
			if field.WireType == protoWire64Bit {
				sp.Timestamp = int64(field.Fixed64)
			}
		case 4: // count
			sp.Count = field.Varint
		case 5: // sum
			if field.WireType == protoWire64Bit {
				sp.Sum = math.Float64frombits(field.Fixed64)
			}
		case 6: // quantile values
			if field.WireType == protoWireBytes {
				qv := decodeQuantileValue(field.Bytes)
				sp.Quantiles = append(sp.Quantiles, qv)
			}
		}
	}
	return sp
}

func decodeQuantileValue(data []byte) OTLPQuantileValue {
	dec := newProtoDecoder(data)
	qv := OTLPQuantileValue{}
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return qv
		}
		switch field.Number {
		case 1: // quantile
			if field.WireType == protoWire64Bit {
				qv.Quantile = math.Float64frombits(field.Fixed64)
			}
		case 2: // value
			if field.WireType == protoWire64Bit {
				qv.Value = math.Float64frombits(field.Fixed64)
			}
		}
	}
	return qv
}

func decodeOTLPKeyValue(data []byte) *OTLPKeyValue {
	dec := newProtoDecoder(data)
	kv := &OTLPKeyValue{}
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return nil
		}
		switch field.Number {
		case 1: // key
			kv.Key = string(field.Bytes)
		case 2: // value (AnyValue)
			if field.WireType == protoWireBytes {
				sv := decodeAnyValue(field.Bytes)
				kv.Value = sv
			}
		}
	}
	return kv
}

func decodeAnyValue(data []byte) OTLPAnyValue {
	dec := newProtoDecoder(data)
	val := OTLPAnyValue{}
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return val
		}
		switch field.Number {
		case 1: // string_value
			s := string(field.Bytes)
			val.StringValue = &s
		case 2: // bool_value
			b := field.Varint != 0
			val.BoolValue = &b
		case 3: // int_value
			i := int64(field.Varint)
			val.IntValue = &i
		case 4: // double_value
			if field.WireType == protoWire64Bit {
				d := math.Float64frombits(field.Fixed64)
				val.DoubleValue = &d
			}
		}
	}
	return val
}
