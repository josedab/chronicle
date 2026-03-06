package chronicle

import (
	"math"
	"testing"
)

// buildKeyValue builds a protobuf-encoded KeyValue message.
func buildKeyValue(key string, anyValueBytes []byte) []byte {
	kv := newProtoEncoder()
	kv.writeString(1, key) // field 1 = key
	kv.writeBytes(2, anyValueBytes) // field 2 = AnyValue
	return kv.Bytes()
}

func buildStringAnyValue(s string) []byte {
	v := newProtoEncoder()
	v.writeString(1, s) // field 1 = string_value
	return v.Bytes()
}

func buildBoolAnyValue(b bool) []byte {
	v := newProtoEncoder()
	val := uint64(0)
	if b {
		val = 1
	}
	v.writeVarint(2, val) // field 2 = bool_value
	return v.Bytes()
}

func buildIntAnyValue(i int64) []byte {
	v := newProtoEncoder()
	v.writeVarint(3, uint64(i)) // field 3 = int_value
	return v.Bytes()
}

func buildDoubleAnyValue(d float64) []byte {
	v := newProtoEncoder()
	v.writeDouble(4, d) // field 4 = double_value
	return v.Bytes()
}

func TestGRPCDecodeOTLPKeyValue_String(t *testing.T) {
	data := buildKeyValue("host", buildStringAnyValue("server1"))
	kv := grpcDecodeOTLPKeyValue(data)
	if kv.Key != "host" {
		t.Errorf("key = %q, want %q", kv.Key, "host")
	}
	if kv.Value.StringValue == nil || *kv.Value.StringValue != "server1" {
		t.Errorf("string_value = %v, want %q", kv.Value.StringValue, "server1")
	}
}

func TestGRPCDecodeOTLPKeyValue_Bool(t *testing.T) {
	data := buildKeyValue("active", buildBoolAnyValue(true))
	kv := grpcDecodeOTLPKeyValue(data)
	if kv.Key != "active" {
		t.Errorf("key = %q, want %q", kv.Key, "active")
	}
	if kv.Value.BoolValue == nil || !*kv.Value.BoolValue {
		t.Errorf("bool_value = %v, want true", kv.Value.BoolValue)
	}
}

func TestGRPCDecodeOTLPKeyValue_Int(t *testing.T) {
	data := buildKeyValue("count", buildIntAnyValue(42))
	kv := grpcDecodeOTLPKeyValue(data)
	if kv.Key != "count" {
		t.Errorf("key = %q, want %q", kv.Key, "count")
	}
	if kv.Value.IntValue == nil || *kv.Value.IntValue != 42 {
		t.Errorf("int_value = %v, want 42", kv.Value.IntValue)
	}
}

func TestGRPCDecodeOTLPKeyValue_Double(t *testing.T) {
	data := buildKeyValue("rate", buildDoubleAnyValue(3.14))
	kv := grpcDecodeOTLPKeyValue(data)
	if kv.Key != "rate" {
		t.Errorf("key = %q, want %q", kv.Key, "rate")
	}
	if kv.Value.DoubleValue == nil || math.Abs(*kv.Value.DoubleValue-3.14) > 1e-10 {
		t.Errorf("double_value = %v, want 3.14", kv.Value.DoubleValue)
	}
}

func buildNumberDataPoint(labels map[string]string, timestamp uint64, doubleVal float64) []byte {
	dp := newProtoEncoder()
	for k, v := range labels {
		attr := buildKeyValue(k, buildStringAnyValue(v))
		dp.writeBytes(1, attr) // field 1 = attributes
	}
	dp.writeVarint(3, timestamp) // field 3 = time_unix_nano
	dp.writeDouble(4, doubleVal) // field 4 = as_double
	return dp.Bytes()
}

func buildNumberDataPointInt(labels map[string]string, timestamp uint64, intVal int64) []byte {
	dp := newProtoEncoder()
	for k, v := range labels {
		attr := buildKeyValue(k, buildStringAnyValue(v))
		dp.writeBytes(1, attr)
	}
	dp.writeVarint(3, timestamp)
	dp.writeVarint(6, uint64(intVal)) // field 6 = as_int
	return dp.Bytes()
}

func TestGRPCDecodeOTLPNumberDataPoint(t *testing.T) {
	labels := map[string]string{"host": "srv1", "region": "us"}
	ts := uint64(1700000000000000000)
	data := buildNumberDataPoint(labels, ts, 99.5)

	p := grpcDecodeOTLPNumberDataPoint(data)
	if p.Labels["host"] != "srv1" {
		t.Errorf("label host = %q, want %q", p.Labels["host"], "srv1")
	}
	if p.Labels["region"] != "us" {
		t.Errorf("label region = %q, want %q", p.Labels["region"], "us")
	}
	if p.Timestamp != int64(ts) {
		t.Errorf("timestamp = %d, want %d", p.Timestamp, ts)
	}
	if math.Abs(p.Value-99.5) > 1e-10 {
		t.Errorf("value = %f, want 99.5", p.Value)
	}
}

func TestGRPCDecodeOTLPNumberDataPoint_IntValue(t *testing.T) {
	data := buildNumberDataPointInt(nil, 1000, 7)
	p := grpcDecodeOTLPNumberDataPoint(data)
	if p.Value != 7.0 {
		t.Errorf("value = %f, want 7.0", p.Value)
	}
}

func buildGauge(points ...[]byte) []byte {
	g := newProtoEncoder()
	for _, pt := range points {
		g.writeBytes(1, pt) // field 1 = repeated NumberDataPoint
	}
	return g.Bytes()
}

func buildSum(points ...[]byte) []byte {
	s := newProtoEncoder()
	for _, pt := range points {
		s.writeBytes(1, pt) // field 1 = repeated NumberDataPoint
	}
	return s.Bytes()
}

func TestGRPCDecodeOTLPDataPoints(t *testing.T) {
	dp1 := buildNumberDataPoint(map[string]string{"a": "1"}, 100, 1.0)
	dp2 := buildNumberDataPoint(map[string]string{"b": "2"}, 200, 2.0)
	data := buildGauge(dp1, dp2)

	points := grpcDecodeOTLPDataPoints(data)
	if len(points) != 2 {
		t.Fatalf("got %d points, want 2", len(points))
	}
	if math.Abs(points[0].Value-1.0) > 1e-10 {
		t.Errorf("point[0].Value = %f, want 1.0", points[0].Value)
	}
	if math.Abs(points[1].Value-2.0) > 1e-10 {
		t.Errorf("point[1].Value = %f, want 2.0", points[1].Value)
	}
}

func TestGRPCDecodeOTLPSumDataPoints(t *testing.T) {
	dp := buildNumberDataPoint(nil, 300, 42.0)
	data := buildSum(dp)

	points := grpcDecodeOTLPSumDataPoints(data)
	if len(points) != 1 {
		t.Fatalf("got %d points, want 1", len(points))
	}
	if math.Abs(points[0].Value-42.0) > 1e-10 {
		t.Errorf("point[0].Value = %f, want 42.0", points[0].Value)
	}
}

func buildMetric(name, desc, unit string, gaugeData, sumData []byte) []byte {
	m := newProtoEncoder()
	if name != "" {
		m.writeString(1, name) // field 1 = name
	}
	if desc != "" {
		m.writeString(2, desc) // field 2 = description
	}
	if unit != "" {
		m.writeString(3, unit) // field 3 = unit
	}
	if gaugeData != nil {
		m.writeBytes(5, gaugeData) // field 5 = gauge
	}
	if sumData != nil {
		m.writeBytes(7, sumData) // field 7 = sum
	}
	return m.Bytes()
}

func TestGRPCDecodeOTLPMetric_Gauge(t *testing.T) {
	dp := buildNumberDataPoint(map[string]string{"host": "a"}, 500, 10.0)
	gauge := buildGauge(dp)
	data := buildMetric("cpu.usage", "CPU usage percentage", "percent", gauge, nil)

	m := grpcDecodeOTLPMetric(data)
	if m.Name != "cpu.usage" {
		t.Errorf("name = %q, want %q", m.Name, "cpu.usage")
	}
	if m.Description != "CPU usage percentage" {
		t.Errorf("description = %q, want %q", m.Description, "CPU usage percentage")
	}
	if m.Unit != "percent" {
		t.Errorf("unit = %q, want %q", m.Unit, "percent")
	}
	if m.Type != OTLPMetricGauge {
		t.Errorf("type = %d, want OTLPMetricGauge(%d)", m.Type, OTLPMetricGauge)
	}
	if len(m.DataPoints) != 1 {
		t.Fatalf("got %d data points, want 1", len(m.DataPoints))
	}
	if math.Abs(m.DataPoints[0].Value-10.0) > 1e-10 {
		t.Errorf("data point value = %f, want 10.0", m.DataPoints[0].Value)
	}
}

func TestGRPCDecodeOTLPMetric_Sum(t *testing.T) {
	dp := buildNumberDataPoint(nil, 600, 100.0)
	sum := buildSum(dp)
	data := buildMetric("requests.total", "", "1", nil, sum)

	m := grpcDecodeOTLPMetric(data)
	if m.Name != "requests.total" {
		t.Errorf("name = %q, want %q", m.Name, "requests.total")
	}
	if m.Type != OTLPMetricSum {
		t.Errorf("type = %d, want OTLPMetricSum(%d)", m.Type, OTLPMetricSum)
	}
	if len(m.DataPoints) != 1 {
		t.Fatalf("got %d data points, want 1", len(m.DataPoints))
	}
}

func buildResourceMetrics(attrs [][]byte, scopeName, scopeVersion string, metrics [][]byte) []byte {
	rm := newProtoEncoder()

	// field 1 = resource
	if len(attrs) > 0 {
		res := newProtoEncoder()
		for _, a := range attrs {
			res.writeBytes(1, a) // field 1 = attributes
		}
		rm.writeBytes(1, res.Bytes())
	}

	// field 2 = scope_metrics
	sm := newProtoEncoder()
	if scopeName != "" || scopeVersion != "" {
		scope := newProtoEncoder()
		if scopeName != "" {
			scope.writeString(1, scopeName)
		}
		if scopeVersion != "" {
			scope.writeString(2, scopeVersion)
		}
		sm.writeBytes(1, scope.Bytes()) // field 1 = scope
	}
	for _, metric := range metrics {
		sm.writeBytes(2, metric) // field 2 = metrics
	}
	rm.writeBytes(2, sm.Bytes())

	return rm.Bytes()
}

func TestGRPCDecodeOTLPResourceMetrics(t *testing.T) {
	attr := buildKeyValue("service.name", buildStringAnyValue("myapp"))
	dp := buildNumberDataPoint(nil, 1000, 55.0)
	gauge := buildGauge(dp)
	metric := buildMetric("mem.used", "Memory used", "bytes", gauge, nil)

	data := buildResourceMetrics([][]byte{attr}, "chronicle", "1.0.0", [][]byte{metric})
	batch, err := grpcDecodeOTLPResourceMetrics(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(batch.Resource.Attributes) < 1 {
		t.Fatal("expected at least 1 resource attribute")
	}
	if batch.Resource.Attributes[0].Key != "service.name" {
		t.Errorf("attribute key = %q, want %q", batch.Resource.Attributes[0].Key, "service.name")
	}
	if batch.Scope.Name != "chronicle" {
		t.Errorf("scope name = %q, want %q", batch.Scope.Name, "chronicle")
	}
	if batch.Scope.Version != "1.0.0" {
		t.Errorf("scope version = %q, want %q", batch.Scope.Version, "1.0.0")
	}
	if len(batch.Metrics) != 1 {
		t.Fatalf("got %d metrics, want 1", len(batch.Metrics))
	}
	if batch.Metrics[0].Name != "mem.used" {
		t.Errorf("metric name = %q, want %q", batch.Metrics[0].Name, "mem.used")
	}
}

func buildExportMetricsServiceRequest(resourceMetrics ...[]byte) []byte {
	req := newProtoEncoder()
	for _, rm := range resourceMetrics {
		req.writeBytes(1, rm) // field 1 = resource_metrics
	}
	return req.Bytes()
}

func TestDecodeOTLPMetricsRequest_Protobuf(t *testing.T) {
	attr := buildKeyValue("env", buildStringAnyValue("prod"))
	dp := buildNumberDataPoint(map[string]string{"host": "h1"}, 2000, 77.7)
	gauge := buildGauge(dp)
	metric := buildMetric("cpu", "CPU", "%", gauge, nil)
	rm := buildResourceMetrics([][]byte{attr}, "test-scope", "0.1", [][]byte{metric})
	data := buildExportMetricsServiceRequest(rm)

	engine := &GRPCIngestionEngine{}
	batch, err := engine.decodeOTLPMetricsRequest(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch.Metrics) != 1 {
		t.Fatalf("got %d metrics, want 1", len(batch.Metrics))
	}
	if batch.Metrics[0].Name != "cpu" {
		t.Errorf("metric name = %q, want %q", batch.Metrics[0].Name, "cpu")
	}
	if batch.Scope.Name != "test-scope" {
		t.Errorf("scope = %q, want %q", batch.Scope.Name, "test-scope")
	}
}

func TestDecodeOTLPMetricsRequest_WithGRPCFrame(t *testing.T) {
	dp := buildNumberDataPoint(nil, 500, 1.0)
	gauge := buildGauge(dp)
	metric := buildMetric("m1", "", "", gauge, nil)
	rm := buildResourceMetrics(nil, "", "", [][]byte{metric})
	payload := buildExportMetricsServiceRequest(rm)

	// Add gRPC frame header: compressed=0, 4-byte length
	frame := make([]byte, 5+len(payload))
	frame[0] = 0x00
	frame[1] = byte(len(payload) >> 24)
	frame[2] = byte(len(payload) >> 16)
	frame[3] = byte(len(payload) >> 8)
	frame[4] = byte(len(payload))
	copy(frame[5:], payload)

	engine := &GRPCIngestionEngine{}
	batch, err := engine.decodeOTLPMetricsRequest(frame)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch.Metrics) != 1 {
		t.Fatalf("got %d metrics, want 1", len(batch.Metrics))
	}
	if batch.Metrics[0].Name != "m1" {
		t.Errorf("metric name = %q, want %q", batch.Metrics[0].Name, "m1")
	}
}
