package chronicle

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// OTLPConfig configures the OpenTelemetry receiver.
type OTLPConfig struct {
	// Enabled turns on the OTLP endpoint
	Enabled bool
	// MaxBodySize is the maximum request body size
	MaxBodySize int64
}

// DefaultOTLPConfig returns default OTLP configuration.
func DefaultOTLPConfig() OTLPConfig {
	return OTLPConfig{
		Enabled:     true,
		MaxBodySize: 10 * 1024 * 1024,
	}
}

// OTLPReceiver handles OpenTelemetry metric ingestion.
type OTLPReceiver struct {
	db     *DB
	config OTLPConfig
}

// NewOTLPReceiver creates a new OTLP receiver.
func NewOTLPReceiver(db *DB, cfg OTLPConfig) *OTLPReceiver {
	if cfg.MaxBodySize <= 0 {
		cfg.MaxBodySize = 10 * 1024 * 1024
	}
	return &OTLPReceiver{
		db:     db,
		config: cfg,
	}
}

// OTLP JSON structures (simplified - real OTLP uses protobuf)

// OTLPExportRequest is the top-level OTLP metrics request.
type OTLPExportRequest struct {
	ResourceMetrics []OTLPResourceMetrics `json:"resourceMetrics"`
}

// OTLPResourceMetrics contains metrics from a single resource.
type OTLPResourceMetrics struct {
	Resource     OTLPResource       `json:"resource"`
	ScopeMetrics []OTLPScopeMetrics `json:"scopeMetrics"`
}

// OTLPResource describes the source of metrics.
type OTLPResource struct {
	Attributes []OTLPKeyValue `json:"attributes"`
}

// OTLPScopeMetrics contains metrics from an instrumentation scope.
type OTLPScopeMetrics struct {
	Scope   OTLPScope    `json:"scope"`
	Metrics []OTLPMetric `json:"metrics"`
}

// OTLPScope identifies the instrumentation scope.
type OTLPScope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// OTLPMetric is a single metric with its data points.
type OTLPMetric struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Unit        string         `json:"unit"`
	Gauge       *OTLPGauge     `json:"gauge,omitempty"`
	Sum         *OTLPSum       `json:"sum,omitempty"`
	Histogram   *OTLPHistogram `json:"histogram,omitempty"`
	Summary     *OTLPSummary   `json:"summary,omitempty"`
}

// OTLPGauge holds gauge data points.
type OTLPGauge struct {
	DataPoints []OTLPNumberDataPoint `json:"dataPoints"`
}

// OTLPSum holds sum/counter data points.
type OTLPSum struct {
	DataPoints             []OTLPNumberDataPoint `json:"dataPoints"`
	AggregationTemporality int                   `json:"aggregationTemporality"`
	IsMonotonic            bool                  `json:"isMonotonic"`
}

// OTLPHistogram holds histogram data points.
type OTLPHistogram struct {
	DataPoints             []OTLPHistogramDataPoint `json:"dataPoints"`
	AggregationTemporality int                      `json:"aggregationTemporality"`
}

// OTLPSummary holds summary data points.
type OTLPSummary struct {
	DataPoints []OTLPSummaryDataPoint `json:"dataPoints"`
}

// OTLPNumberDataPoint is a single numeric measurement.
type OTLPNumberDataPoint struct {
	Attributes        []OTLPKeyValue `json:"attributes"`
	TimeUnixNano      string         `json:"timeUnixNano"`
	AsDouble          *float64       `json:"asDouble,omitempty"`
	AsInt             *int64         `json:"asInt,omitempty"`
	StartTimeUnixNano string         `json:"startTimeUnixNano,omitempty"`
}

// OTLPHistogramDataPoint is a histogram measurement.
type OTLPHistogramDataPoint struct {
	Attributes        []OTLPKeyValue `json:"attributes"`
	TimeUnixNano      string         `json:"timeUnixNano"`
	Count             uint64         `json:"count"`
	Sum               *float64       `json:"sum,omitempty"`
	BucketCounts      []uint64       `json:"bucketCounts"`
	ExplicitBounds    []float64      `json:"explicitBounds"`
	StartTimeUnixNano string         `json:"startTimeUnixNano,omitempty"`
}

// OTLPSummaryDataPoint is a summary measurement.
type OTLPSummaryDataPoint struct {
	Attributes        []OTLPKeyValue      `json:"attributes"`
	TimeUnixNano      string              `json:"timeUnixNano"`
	Count             uint64              `json:"count"`
	Sum               float64             `json:"sum"`
	QuantileValues    []OTLPQuantileValue `json:"quantileValues"`
	StartTimeUnixNano string              `json:"startTimeUnixNano,omitempty"`
}

// OTLPQuantileValue is a quantile in a summary.
type OTLPQuantileValue struct {
	Quantile float64 `json:"quantile"`
	Value    float64 `json:"value"`
}

// OTLPKeyValue is an attribute key-value pair.
type OTLPKeyValue struct {
	Key   string       `json:"key"`
	Value OTLPAnyValue `json:"value"`
}

// OTLPAnyValue can hold different value types.
type OTLPAnyValue struct {
	StringValue *string  `json:"stringValue,omitempty"`
	IntValue    *int64   `json:"intValue,omitempty"`
	DoubleValue *float64 `json:"doubleValue,omitempty"`
	BoolValue   *bool    `json:"boolValue,omitempty"`
}

// Handler returns an HTTP handler for the OTLP metrics endpoint.
func (r *OTLPReceiver) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		req.Body = http.MaxBytesReader(w, req.Body, r.config.MaxBodySize)

		var reader io.Reader = req.Body
		if req.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(req.Body)
			if err != nil {
				http.Error(w, "failed to decompress: "+err.Error(), http.StatusBadRequest)
				return
			}
			defer func() { _ = gz.Close() }()
			reader = gz
		}

		var export OTLPExportRequest
		if err := json.NewDecoder(reader).Decode(&export); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		points, err := r.convertToPoints(export)
		if err != nil {
			http.Error(w, "conversion error: "+err.Error(), http.StatusBadRequest)
			return
		}

		if len(points) > 0 {
			if err := r.db.WriteBatch(points); err != nil {
				http.Error(w, "write error: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}
}

func (r *OTLPReceiver) convertToPoints(export OTLPExportRequest) ([]Point, error) {
	var points []Point

	for _, rm := range export.ResourceMetrics {
		resourceTags := attributesToTags(rm.Resource.Attributes)

		for _, sm := range rm.ScopeMetrics {
			scopeTags := map[string]string{}
			if sm.Scope.Name != "" {
				scopeTags["otel_scope_name"] = sm.Scope.Name
			}
			if sm.Scope.Version != "" {
				scopeTags["otel_scope_version"] = sm.Scope.Version
			}

			for _, m := range sm.Metrics {
				baseTags := mergeTags(resourceTags, scopeTags)
				metricName := sanitizeMetricName(m.Name)

				// Handle different metric types
				if m.Gauge != nil {
					pts := r.convertGauge(metricName, baseTags, m.Gauge)
					points = append(points, pts...)
				}
				if m.Sum != nil {
					pts := r.convertSum(metricName, baseTags, m.Sum)
					points = append(points, pts...)
				}
				if m.Histogram != nil {
					pts := r.convertHistogram(metricName, baseTags, m.Histogram)
					points = append(points, pts...)
				}
				if m.Summary != nil {
					pts := r.convertSummary(metricName, baseTags, m.Summary)
					points = append(points, pts...)
				}
			}
		}
	}

	return points, nil
}

func (r *OTLPReceiver) convertGauge(name string, baseTags map[string]string, gauge *OTLPGauge) []Point {
	return r.convertNumberDataPoints(name, baseTags, gauge.DataPoints)
}

func (r *OTLPReceiver) convertSum(name string, baseTags map[string]string, sum *OTLPSum) []Point {
	return r.convertNumberDataPoints(name, baseTags, sum.DataPoints)
}

func (r *OTLPReceiver) convertNumberDataPoints(name string, baseTags map[string]string, dps []OTLPNumberDataPoint) []Point {
	points := make([]Point, 0, len(dps))
	for _, dp := range dps {
		ts := parseOTLPTimestamp(dp.TimeUnixNano)
		tags := mergeTags(baseTags, attributesToTags(dp.Attributes))

		var value float64
		if dp.AsDouble != nil {
			value = *dp.AsDouble
		} else if dp.AsInt != nil {
			value = float64(*dp.AsInt)
		}

		points = append(points, Point{
			Metric:    name,
			Tags:      tags,
			Value:     value,
			Timestamp: ts,
		})
	}
	return points
}

func (r *OTLPReceiver) convertHistogram(name string, baseTags map[string]string, hist *OTLPHistogram) []Point {
	var points []Point
	for _, dp := range hist.DataPoints {
		ts := parseOTLPTimestamp(dp.TimeUnixNano)
		tags := mergeTags(baseTags, attributesToTags(dp.Attributes))

		// Count metric
		points = append(points, Point{
			Metric:    name + "_count",
			Tags:      tags,
			Value:     float64(dp.Count),
			Timestamp: ts,
		})

		// Sum metric (if available)
		if dp.Sum != nil {
			points = append(points, Point{
				Metric:    name + "_sum",
				Tags:      tags,
				Value:     *dp.Sum,
				Timestamp: ts,
			})
		}

		// Bucket metrics
		for i, count := range dp.BucketCounts {
			bucketTags := make(map[string]string, len(tags)+1)
			for k, v := range tags {
				bucketTags[k] = v
			}
			if i < len(dp.ExplicitBounds) {
				bucketTags["le"] = strconv.FormatFloat(dp.ExplicitBounds[i], 'f', -1, 64)
			} else {
				bucketTags["le"] = "+Inf"
			}
			points = append(points, Point{
				Metric:    name + "_bucket",
				Tags:      bucketTags,
				Value:     float64(count),
				Timestamp: ts,
			})
		}
	}
	return points
}

func (r *OTLPReceiver) convertSummary(name string, baseTags map[string]string, summary *OTLPSummary) []Point {
	var points []Point
	for _, dp := range summary.DataPoints {
		ts := parseOTLPTimestamp(dp.TimeUnixNano)
		tags := mergeTags(baseTags, attributesToTags(dp.Attributes))

		// Count metric
		points = append(points, Point{
			Metric:    name + "_count",
			Tags:      tags,
			Value:     float64(dp.Count),
			Timestamp: ts,
		})

		// Sum metric
		points = append(points, Point{
			Metric:    name + "_sum",
			Tags:      tags,
			Value:     dp.Sum,
			Timestamp: ts,
		})

		// Quantile metrics
		for _, qv := range dp.QuantileValues {
			quantileTags := make(map[string]string, len(tags)+1)
			for k, v := range tags {
				quantileTags[k] = v
			}
			quantileTags["quantile"] = strconv.FormatFloat(qv.Quantile, 'f', -1, 64)
			points = append(points, Point{
				Metric:    name,
				Tags:      quantileTags,
				Value:     qv.Value,
				Timestamp: ts,
			})
		}
	}
	return points
}

func attributesToTags(attrs []OTLPKeyValue) map[string]string {
	tags := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		key := sanitizeTagKey(attr.Key)
		var value string
		switch {
		case attr.Value.StringValue != nil:
			value = *attr.Value.StringValue
		case attr.Value.IntValue != nil:
			value = strconv.FormatInt(*attr.Value.IntValue, 10)
		case attr.Value.DoubleValue != nil:
			value = strconv.FormatFloat(*attr.Value.DoubleValue, 'f', -1, 64)
		case attr.Value.BoolValue != nil:
			value = strconv.FormatBool(*attr.Value.BoolValue)
		default:
			continue
		}
		tags[key] = value
	}
	return tags
}

func mergeTags(a, b map[string]string) map[string]string {
	result := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}

func parseOTLPTimestamp(ts string) int64 {
	if ts == "" {
		return time.Now().UnixNano()
	}
	n, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Now().UnixNano()
	}
	return n
}

func sanitizeMetricName(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}

func sanitizeTagKey(key string) string {
	return strings.ReplaceAll(key, ".", "_")
}

// ValidateOTLPRequest validates an OTLP export request.
func ValidateOTLPRequest(req *OTLPExportRequest) error {
	if req == nil {
		return errors.New("request is nil")
	}
	if len(req.ResourceMetrics) == 0 {
		return errors.New("no resource metrics")
	}
	return nil
}
