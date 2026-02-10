package chronicle

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDefaultOTelCollectorConfig(t *testing.T) {
	config := DefaultOTelCollectorConfig()

	if !config.Enabled {
		t.Error("should be enabled by default")
	}
	if config.Endpoint == "" {
		t.Error("endpoint should not be empty")
	}
	if config.BatchSize <= 0 {
		t.Error("batch size should be positive")
	}
	if config.FlushInterval <= 0 {
		t.Error("flush interval should be positive")
	}
	if config.Timeout <= 0 {
		t.Error("timeout should be positive")
	}
}

func TestNewOTelCollectorExporter(t *testing.T) {
	config := DefaultOTelCollectorConfig()
	exporter, err := NewOTelCollectorExporter(config)
	if err != nil {
		t.Fatalf("failed to create exporter: %v", err)
	}
	defer exporter.Stop()

	if exporter == nil {
		t.Error("exporter should not be nil")
	}
}

func TestNewOTelCollectorExporter_NoEndpoint(t *testing.T) {
	config := OTelCollectorConfig{}
	_, err := NewOTelCollectorExporter(config)
	if err == nil {
		t.Error("expected error for empty endpoint")
	}
}

func TestOTelCollectorExporter_ConvertMetrics(t *testing.T) {
	config := DefaultOTelCollectorConfig()
	exporter, _ := NewOTelCollectorExporter(config)
	defer exporter.Stop()

	ts := time.Now().UnixNano()
	tsStr := "1234567890000000000"
	doubleVal := 42.5

	metrics := &OTLPExportRequest{
		ResourceMetrics: []OTLPResourceMetrics{
			{
				Resource: OTLPResource{
					Attributes: []OTLPKeyValue{
						{Key: "service.name", Value: OTLPAnyValue{StringValue: otelStrPtr("test-service")}},
					},
				},
				ScopeMetrics: []OTLPScopeMetrics{
					{
						Scope: OTLPScope{Name: "test-scope", Version: "1.0"},
						Metrics: []OTLPMetric{
							{
								Name: "cpu.usage",
								Gauge: &OTLPGauge{
									DataPoints: []OTLPNumberDataPoint{
										{
											TimeUnixNano: tsStr,
											AsDouble:     &doubleVal,
											Attributes: []OTLPKeyValue{
												{Key: "host", Value: OTLPAnyValue{StringValue: otelStrPtr("server1")}},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	points := exporter.convertMetrics(metrics)

	if len(points) == 0 {
		t.Fatal("expected at least one point")
	}

	found := false
	for _, p := range points {
		if p.Metric == "cpu.usage" {
			found = true
			if p.Value != 42.5 {
				t.Errorf("expected value 42.5, got %f", p.Value)
			}
			if p.Tags["host"] != "server1" {
				t.Error("missing host tag")
			}
			if p.Tags["service.name"] != "test-service" {
				t.Error("missing resource tag")
			}
		}
	}

	if !found {
		t.Error("cpu.usage metric not found")
	}

	_ = ts // silence unused warning
}

func TestOTelCollectorExporter_Stats(t *testing.T) {
	config := DefaultOTelCollectorConfig()
	exporter, _ := NewOTelCollectorExporter(config)
	defer exporter.Stop()

	stats := exporter.Stats()
	if stats.PointsExported != 0 {
		t.Error("initial points exported should be 0")
	}
}

func TestOTelCollectorExporter_ExportMetrics(t *testing.T) {
	// Create mock server
	received := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := DefaultOTelCollectorConfig()
	config.Endpoint = server.URL
	config.BatchSize = 1 // Flush immediately

	exporter, _ := NewOTelCollectorExporter(config)
	defer exporter.Stop()

	ctx := context.Background()
	doubleVal := 100.0
	metrics := &OTLPExportRequest{
		ResourceMetrics: []OTLPResourceMetrics{
			{
				ScopeMetrics: []OTLPScopeMetrics{
					{
						Metrics: []OTLPMetric{
							{
								Name: "test.metric",
								Gauge: &OTLPGauge{
									DataPoints: []OTLPNumberDataPoint{
										{AsDouble: &doubleVal, TimeUnixNano: "1234567890"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := exporter.ExportMetrics(ctx, metrics)
	if err != nil {
		t.Errorf("export failed: %v", err)
	}

	// Wait for the request to be received
	select {
	case <-received:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for export")
	}
}

func TestDefaultOTelCollectorReceiverConfig(t *testing.T) {
	config := DefaultOTelCollectorReceiverConfig()

	if !config.Enabled {
		t.Error("should be enabled by default")
	}
	if config.ListenAddr == "" {
		t.Error("listen address should not be empty")
	}
	if config.MaxBatchSize <= 0 {
		t.Error("max batch size should be positive")
	}
}

func TestNewOTelCollectorReceiver(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultOTelCollectorReceiverConfig()
	receiver := NewOTelCollectorReceiver(db, config)

	if receiver == nil {
		t.Error("receiver should not be nil")
	}
}

func TestOTelCollectorReceiver_Stats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultOTelCollectorReceiverConfig()
	receiver := NewOTelCollectorReceiver(db, config)

	stats := receiver.Stats()
	if stats.PointsReceived != 0 {
		t.Error("initial points received should be 0")
	}
}

func TestGenerateOTelCollectorYAML(t *testing.T) {
	config := DefaultOTelCollectorConfig()
	config.APIKey = "test-key"

	yaml := GenerateOTelCollectorYAML(config)

	if yaml == "" {
		t.Error("YAML should not be empty")
	}
	if !strings.Contains(yaml, "exporters:") {
		t.Error("should contain exporters section")
	}
	if !strings.Contains(yaml, "chronicle:") {
		t.Error("should contain chronicle exporter")
	}
	if !strings.Contains(yaml, config.Endpoint) {
		t.Error("should contain endpoint")
	}
}

func TestOTelCollectorExporter_MergeTags(t *testing.T) {
	config := DefaultOTelCollectorConfig()
	exporter, _ := NewOTelCollectorExporter(config)
	defer exporter.Stop()

	tags1 := map[string]string{"a": "1"}
	tags2 := map[string]string{"b": "2"}
	attrs := []OTLPKeyValue{{Key: "c", Value: OTLPAnyValue{StringValue: otelStrPtr("3")}}}

	merged := exporter.mergeTags(tags1, tags2, attrs)

	if merged["a"] != "1" {
		t.Error("missing tag a")
	}
	if merged["b"] != "2" {
		t.Error("missing tag b")
	}
	if merged["c"] != "3" {
		t.Error("missing attr c")
	}
}

func TestOTelAttrValueToString(t *testing.T) {
	tests := []struct {
		name     string
		value    OTLPAnyValue
		expected string
	}{
		{"string", OTLPAnyValue{StringValue: otelStrPtr("hello")}, "hello"},
		{"int", OTLPAnyValue{IntValue: otelIntPtr(42)}, "42"},
		{"double", OTLPAnyValue{DoubleValue: otelFloatPtr(3.14)}, "3.14"},
		{"bool", OTLPAnyValue{BoolValue: otelBoolPtr(true)}, "true"},
		{"empty", OTLPAnyValue{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := otelAttrValueToString(tt.value)
			if result != tt.expected {
				t.Errorf("got %s, want %s", result, tt.expected)
			}
		})
	}
}

func TestOtlpTimestampStr(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"1234567890000000000", 1234567890000000000},
		{"0", 0},
		{"", 0},        // Will return current time, just check it doesn't crash
		{"invalid", 0}, // Will return current time
	}

	for _, tt := range tests {
		result := otlpTimestampStr(tt.input)
		if tt.input != "" && tt.input != "invalid" && tt.input != "0" {
			if result != tt.expected {
				t.Errorf("otlpTimestampStr(%s) = %d, want %d", tt.input, result, tt.expected)
			}
		}
	}
}

func otelStrPtr(s string) *string {
	return &s
}

func otelIntPtr(i int64) *int64 {
	return &i
}

func otelFloatPtr(f float64) *float64 {
	return &f
}

func otelBoolPtr(b bool) *bool {
	return &b
}
