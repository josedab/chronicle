package chronicle

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkWriteBatch(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/bench.db"

	cfg := DefaultConfig(path)
	db, err := Open(path, cfg)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	points := make([]Point, 0, 1000)
	now := time.Now().UnixNano()
	for i := 0; i < 1000; i++ {
		points = append(points, Point{
			Metric:    "metric",
			Tags:      map[string]string{"host": "h" + strconv.Itoa(i%10)},
			Value:     float64(i),
			Timestamp: now + int64(i)*int64(time.Millisecond),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.WriteBatch(points); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

func BenchmarkQueryAggregate(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/benchq.db"

	cfg := DefaultConfig(path)
	db, err := Open(path, cfg)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	now := time.Now().UnixNano()
	points := make([]Point, 0, 10_000)
	for i := 0; i < 10_000; i++ {
		points = append(points, Point{
			Metric:    "cpu",
			Tags:      map[string]string{"host": "h" + strconv.Itoa(i%50)},
			Value:     float64(i % 100),
			Timestamp: now + int64(i)*int64(time.Millisecond),
		})
	}
	if err := db.WriteBatch(points); err != nil {
		b.Fatalf("write: %v", err)
	}

	q := &Query{
		Metric: "cpu",
		Aggregation: &Aggregation{
			Function: AggMean,
			Window:   time.Second,
		},
		GroupBy: []string{"host"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Execute(q); err != nil {
			b.Fatalf("query: %v", err)
		}
	}
}

func BenchmarkSchemaValidation(b *testing.B) {
	registry := NewSchemaRegistry(false)
	minVal := 0.0
	maxVal := 100.0
	_ = registry.Register(MetricSchema{
		Name: "validated_metric",
		Tags: []TagSchema{
			{Name: "host", Required: true},
			{Name: "env", AllowedVals: []string{"prod", "staging", "dev"}},
		},
		Fields: []FieldSchema{
			{Name: "value", MinValue: &minVal, MaxValue: &maxVal},
		},
	})

	point := Point{
		Metric: "validated_metric",
		Tags:   map[string]string{"host": "server1", "env": "prod"},
		Value:  50.0,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = registry.Validate(point)
	}
}

func BenchmarkEncryption(b *testing.B) {
	enc, _ := NewEncryptor(EncryptionConfig{
		Enabled:     true,
		KeyPassword: "benchmark-password",
	})

	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ciphertext, _ := enc.Encrypt(data)
		_, _ = enc.Decrypt(ciphertext)
	}
}

func BenchmarkPromQLParse(b *testing.B) {
	parser := &PromQLParser{}
	queries := []string{
		"http_requests_total",
		`http_requests_total{method="GET",status="200"}`,
		`sum(rate(http_requests_total[5m]))`,
		`sum by (method) (rate(http_requests_total[5m]))`,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, q := range queries {
			_, _ = parser.Parse(q)
		}
	}
}

func BenchmarkStreamingPublish(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/stream.db"
	cfg := DefaultConfig(path)
	db, _ := Open(path, cfg)
	defer db.Close()

	hub := NewStreamHub(db, DefaultStreamConfig())

	// Create 100 subscriptions
	for i := 0; i < 100; i++ {
		hub.Subscribe("metric", nil)
	}

	point := Point{Metric: "metric", Value: 42.0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hub.Publish(point)
	}
}

func BenchmarkMultiTenantWrite(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/tenant.db"
	cfg := DefaultConfig(path)
	db, _ := Open(path, cfg)
	defer db.Close()

	tm := NewTenantManager(db, TenantConfig{Enabled: true})
	tenant, _ := tm.GetTenant("benchmark")

	point := Point{
		Metric: "cpu",
		Value:  50.0,
		Tags:   map[string]string{"host": "server1"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tenant.Write(point)
	}
}
