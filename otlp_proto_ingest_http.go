package chronicle

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"

	"github.com/golang/snappy"
)

// HTTP handlers and endpoints for OTLP proto ingestion.

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *OTLPProtoEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/otlp-proto/ingest", func(w http.ResponseWriter, r *http.Request) {
		var points []ProtoMetricPoint
		if err := json.NewDecoder(r.Body).Decode(&points); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
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
				http.Error(w, "failed to read request body", http.StatusBadRequest)
				return
			}
			// Support snappy decompression (used by Prometheus remote write)
			contentEncoding := r.Header.Get("Content-Encoding")
			if contentEncoding == "snappy" {
				decoded, err := snappy.Decode(nil, body)
				if err != nil {
					http.Error(w, "failed to decompress request body", http.StatusBadRequest)
					return
				}
				body = decoded
			}
			batch, err := DecodeOTLPMetricsBatch(body)
			if err != nil {
				http.Error(w, "invalid protobuf payload", http.StatusBadRequest)
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
				http.Error(w, "bad request", http.StatusBadRequest)
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
