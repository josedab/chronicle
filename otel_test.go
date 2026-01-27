package chronicle

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOTLPReceiver_Gauge(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	receiver := NewOTLPReceiver(db, DefaultOTLPConfig())

	doubleVal := 42.5
	req := OTLPExportRequest{
		ResourceMetrics: []OTLPResourceMetrics{{
			Resource: OTLPResource{
				Attributes: []OTLPKeyValue{
					{Key: "service.name", Value: OTLPAnyValue{StringValue: strPtr("myservice")}},
				},
			},
			ScopeMetrics: []OTLPScopeMetrics{{
				Scope: OTLPScope{Name: "test-scope"},
				Metrics: []OTLPMetric{{
					Name: "cpu.usage",
					Gauge: &OTLPGauge{
						DataPoints: []OTLPNumberDataPoint{{
							Attributes:   []OTLPKeyValue{{Key: "host", Value: OTLPAnyValue{StringValue: strPtr("server1")}}},
							TimeUnixNano: "1700000000000000000",
							AsDouble:     &doubleVal,
						}},
					},
				}},
			}},
		}},
	}

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	// Query the data
	result, err := db.Execute(&Query{Metric: "cpu_usage"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result.Points) != 1 {
		t.Errorf("expected 1 point, got %d", len(result.Points))
	}
	if len(result.Points) > 0 {
		p := result.Points[0]
		if p.Value != 42.5 {
			t.Errorf("expected value 42.5, got %v", p.Value)
		}
		if p.Tags["host"] != "server1" {
			t.Errorf("expected host=server1, got %s", p.Tags["host"])
		}
		if p.Tags["service_name"] != "myservice" {
			t.Errorf("expected service_name=myservice, got %s", p.Tags["service_name"])
		}
	}
}

func TestOTLPReceiver_Sum(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	receiver := NewOTLPReceiver(db, DefaultOTLPConfig())

	intVal := int64(100)
	req := OTLPExportRequest{
		ResourceMetrics: []OTLPResourceMetrics{{
			ScopeMetrics: []OTLPScopeMetrics{{
				Metrics: []OTLPMetric{{
					Name: "http.requests",
					Sum: &OTLPSum{
						DataPoints: []OTLPNumberDataPoint{{
							TimeUnixNano: "1700000000000000000",
							AsInt:        &intVal,
						}},
						IsMonotonic: true,
					},
				}},
			}},
		}},
	}

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(body))

	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	result, _ := db.Execute(&Query{Metric: "http_requests"})
	if len(result.Points) != 1 {
		t.Errorf("expected 1 point, got %d", len(result.Points))
	}
	if len(result.Points) > 0 && result.Points[0].Value != 100 {
		t.Errorf("expected value 100, got %v", result.Points[0].Value)
	}
}

func TestOTLPReceiver_Histogram(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	receiver := NewOTLPReceiver(db, DefaultOTLPConfig())

	sum := 150.0
	req := OTLPExportRequest{
		ResourceMetrics: []OTLPResourceMetrics{{
			ScopeMetrics: []OTLPScopeMetrics{{
				Metrics: []OTLPMetric{{
					Name: "http.latency",
					Histogram: &OTLPHistogram{
						DataPoints: []OTLPHistogramDataPoint{{
							TimeUnixNano:   "1700000000000000000",
							Count:          10,
							Sum:            &sum,
							BucketCounts:   []uint64{2, 5, 3},
							ExplicitBounds: []float64{10, 50},
						}},
					},
				}},
			}},
		}},
	}

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(body))

	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	// Check count metric
	result, _ := db.Execute(&Query{Metric: "http_latency_count"})
	if len(result.Points) != 1 {
		t.Errorf("expected 1 count point, got %d", len(result.Points))
	}

	// Check sum metric
	result, _ = db.Execute(&Query{Metric: "http_latency_sum"})
	if len(result.Points) != 1 {
		t.Errorf("expected 1 sum point, got %d", len(result.Points))
	}

	// Check bucket metrics
	result, _ = db.Execute(&Query{Metric: "http_latency_bucket"})
	if len(result.Points) != 3 {
		t.Errorf("expected 3 bucket points, got %d", len(result.Points))
	}
}

func TestOTLPReceiver_GzipCompression(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	receiver := NewOTLPReceiver(db, DefaultOTLPConfig())

	doubleVal := 99.9
	req := OTLPExportRequest{
		ResourceMetrics: []OTLPResourceMetrics{{
			ScopeMetrics: []OTLPScopeMetrics{{
				Metrics: []OTLPMetric{{
					Name: "compressed.metric",
					Gauge: &OTLPGauge{
						DataPoints: []OTLPNumberDataPoint{{
							TimeUnixNano: "1700000000000000000",
							AsDouble:     &doubleVal,
						}},
					},
				}},
			}},
		}},
	}

	body, _ := json.Marshal(req)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(body)
	_ = gz.Close()

	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", &buf)
	httpReq.Header.Set("Content-Encoding", "gzip")

	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	result, _ := db.Execute(&Query{Metric: "compressed_metric"})
	if len(result.Points) != 1 {
		t.Errorf("expected 1 point, got %d", len(result.Points))
	}
}

func TestOTLPReceiver_InvalidJSON(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	receiver := NewOTLPReceiver(db, DefaultOTLPConfig())

	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader([]byte("not json")))
	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestOTLPReceiver_MethodNotAllowed(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	receiver := NewOTLPReceiver(db, DefaultOTLPConfig())

	httpReq := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rr.Code)
	}
}

func strPtr(s string) *string {
	return &s
}
