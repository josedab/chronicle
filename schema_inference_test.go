package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNewSchemaInferenceEngine(t *testing.T) {
	cfg := DefaultSchemaInferenceConfig()
	engine := NewSchemaInferenceEngine(nil, cfg)

	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.config.SampleSize != 1000 {
		t.Errorf("expected sample size 1000, got %d", engine.config.SampleSize)
	}
	if engine.config.ConfidenceThreshold != 0.9 {
		t.Errorf("expected confidence threshold 0.9, got %f", engine.config.ConfidenceThreshold)
	}
	if !engine.config.AutoSuggestIndexes {
		t.Error("expected AutoSuggestIndexes to be true")
	}
	if !engine.config.EnableMigration {
		t.Error("expected EnableMigration to be true")
	}
	if engine.inferred == nil {
		t.Error("expected inferred map to be initialized")
	}
}

func TestSchemaInferenceInferSchema(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	now := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		db.Write(Point{
			Metric:    "cpu.usage",
			Value:     float64(50 + i),
			Timestamp: now + int64(i)*int64(time.Second),
			Tags:      map[string]string{"host": "web-1", "region": "us-east"},
		})
	}
	db.Flush()

	engine := NewSchemaInferenceEngine(db, DefaultSchemaInferenceConfig())
	schema, err := engine.InferSchema("cpu.usage")
	if err != nil {
		t.Fatalf("InferSchema failed: %v", err)
	}
	if schema.MetricName != "cpu.usage" {
		t.Errorf("expected metric cpu.usage, got %s", schema.MetricName)
	}
	if schema.SampleCount == 0 {
		t.Error("expected sample count > 0")
	}
	if len(schema.Fields) < 2 {
		t.Errorf("expected at least 2 fields, got %d", len(schema.Fields))
	}

	// Should be cached
	cached := engine.GetInferredSchema("cpu.usage")
	if cached == nil {
		t.Fatal("expected cached schema")
	}
	if cached.MetricName != "cpu.usage" {
		t.Errorf("expected cached metric cpu.usage, got %s", cached.MetricName)
	}
}

func TestSchemaInferenceInferSchemaEmpty(t *testing.T) {
	engine := NewSchemaInferenceEngine(nil, DefaultSchemaInferenceConfig())

	// Empty metric should error
	_, err := engine.InferSchema("")
	if err == nil {
		t.Error("expected error for empty metric")
	}

	// No DB, should return minimal schema
	schema, err := engine.InferSchema("test.metric")
	if err != nil {
		t.Fatalf("InferSchema failed: %v", err)
	}
	if schema.SampleCount != 0 {
		t.Errorf("expected 0 sample count, got %d", schema.SampleCount)
	}
	if schema.Confidence != 0 {
		t.Errorf("expected 0 confidence, got %f", schema.Confidence)
	}
}

func TestSchemaInferenceInferAll(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		db.Write(Point{Metric: "cpu.usage", Value: float64(i), Timestamp: now + int64(i)*int64(time.Second)})
		db.Write(Point{Metric: "mem.used", Value: float64(100 + i), Timestamp: now + int64(i)*int64(time.Second)})
	}
	db.Flush()

	engine := NewSchemaInferenceEngine(db, DefaultSchemaInferenceConfig())
	schemas, err := engine.InferAll()
	if err != nil {
		t.Fatalf("InferAll failed: %v", err)
	}
	if len(schemas) < 2 {
		t.Errorf("expected at least 2 schemas, got %d", len(schemas))
	}

	// Should all be cached
	list := engine.ListInferredSchemas()
	if len(list) < 2 {
		t.Errorf("expected at least 2 cached schemas, got %d", len(list))
	}
}

func TestSchemaInferenceInferAllNilDB(t *testing.T) {
	engine := NewSchemaInferenceEngine(nil, DefaultSchemaInferenceConfig())
	schemas, err := engine.InferAll()
	if err != nil {
		t.Fatalf("InferAll failed: %v", err)
	}
	if schemas != nil {
		t.Errorf("expected nil schemas for nil DB, got %d", len(schemas))
	}
}

func TestSchemaInferenceSuggestIndexes(t *testing.T) {
	engine := NewSchemaInferenceEngine(nil, DefaultSchemaInferenceConfig())

	// No cached schema
	indexes := engine.SuggestIndexes("unknown")
	if len(indexes) != 1 || indexes[0] != "timestamp" {
		t.Errorf("expected [timestamp] for unknown metric, got %v", indexes)
	}

	// With cached schema
	engine.mu.Lock()
	engine.inferred["test"] = &InferredSchema{
		MetricName: "test",
		Fields: []InferredField{
			{Name: "value", DataType: FieldFloat},
			{Name: "host", DataType: FieldString, Cardinality: 10},
		},
	}
	engine.mu.Unlock()

	indexes = engine.SuggestIndexes("test")
	if len(indexes) < 2 {
		t.Errorf("expected at least 2 indexes, got %v", indexes)
	}
	hasTimestamp := false
	hasHost := false
	for _, idx := range indexes {
		if idx == "timestamp" {
			hasTimestamp = true
		}
		if idx == "host" {
			hasHost = true
		}
	}
	if !hasTimestamp {
		t.Error("expected timestamp index")
	}
	if !hasHost {
		t.Error("expected host index")
	}
}

func TestSchemaInferenceSuggestCompression(t *testing.T) {
	engine := NewSchemaInferenceEngine(nil, DefaultSchemaInferenceConfig())

	// No cached schema
	comp := engine.SuggestCompression("unknown")
	if comp != "gorilla" {
		t.Errorf("expected gorilla for unknown metric, got %s", comp)
	}

	// Integer data suggests delta
	engine.mu.Lock()
	engine.inferred["counters"] = &InferredSchema{
		MetricName: "counters",
		Fields: []InferredField{
			{Name: "value", DataType: FieldInt, MinValue: 0, MaxValue: 1000},
		},
	}
	engine.mu.Unlock()

	comp = engine.SuggestCompression("counters")
	if comp != "delta" {
		t.Errorf("expected delta for integer data, got %s", comp)
	}

	// Float data suggests gorilla
	engine.mu.Lock()
	engine.inferred["gauges"] = &InferredSchema{
		MetricName: "gauges",
		Fields: []InferredField{
			{Name: "value", DataType: FieldFloat, MinValue: 0, MaxValue: 100},
		},
	}
	engine.mu.Unlock()

	comp = engine.SuggestCompression("gauges")
	if comp != "gorilla" {
		t.Errorf("expected gorilla for float data, got %s", comp)
	}
}

func TestSchemaInferenceSuggestRetention(t *testing.T) {
	engine := NewSchemaInferenceEngine(nil, DefaultSchemaInferenceConfig())

	// No cached schema
	ret := engine.SuggestRetention("unknown")
	if ret != 30*24*time.Hour {
		t.Errorf("expected 30 days for unknown metric, got %v", ret)
	}

	// Low cardinality → 90 days
	engine.mu.Lock()
	engine.inferred["low_card"] = &InferredSchema{
		MetricName: "low_card",
		Fields: []InferredField{
			{Name: "value", DataType: FieldFloat},
			{Name: "host", DataType: FieldString, Cardinality: 5},
		},
	}
	engine.mu.Unlock()

	ret = engine.SuggestRetention("low_card")
	if ret != 90*24*time.Hour {
		t.Errorf("expected 90 days for low cardinality, got %v", ret)
	}

	// High cardinality → 7 days
	engine.mu.Lock()
	engine.inferred["high_card"] = &InferredSchema{
		MetricName: "high_card",
		Fields: []InferredField{
			{Name: "value", DataType: FieldFloat},
			{Name: "uuid", DataType: FieldString, Cardinality: 1000},
		},
	}
	engine.mu.Unlock()

	ret = engine.SuggestRetention("high_card")
	if ret != 7*24*time.Hour {
		t.Errorf("expected 7 days for high cardinality, got %v", ret)
	}
}

func TestSchemaInferenceMigration(t *testing.T) {
	engine := NewSchemaInferenceEngine(nil, DefaultSchemaInferenceConfig())

	// Create migration
	mig, err := engine.CreateMigration("schema_v1", "schema_v2")
	if err != nil {
		t.Fatalf("CreateMigration failed: %v", err)
	}
	if mig.Status != MigrationPending {
		t.Errorf("expected pending status, got %s", mig.Status)
	}
	if mig.FromSchema != "schema_v1" {
		t.Errorf("expected from schema_v1, got %s", mig.FromSchema)
	}

	// List migrations
	migs := engine.ListMigrations()
	if len(migs) != 1 {
		t.Fatalf("expected 1 migration, got %d", len(migs))
	}

	// Execute migration
	if err := engine.ExecuteMigration(mig.ID); err != nil {
		t.Fatalf("ExecuteMigration failed: %v", err)
	}

	migs = engine.ListMigrations()
	if migs[0].Status != MigrationComplete {
		t.Errorf("expected complete status, got %s", migs[0].Status)
	}
	if migs[0].AffectedRows == 0 {
		t.Error("expected affected rows > 0")
	}

	// Error: empty from/to
	_, err = engine.CreateMigration("", "schema_v2")
	if err == nil {
		t.Error("expected error for empty from")
	}

	// Error: execute non-existent
	if err := engine.ExecuteMigration("nonexistent"); err == nil {
		t.Error("expected error for non-existent migration")
	}

	// Error: execute already completed
	if err := engine.ExecuteMigration(mig.ID); err == nil {
		t.Error("expected error for already completed migration")
	}

	// Disabled migrations
	cfg := DefaultSchemaInferenceConfig()
	cfg.EnableMigration = false
	engine2 := NewSchemaInferenceEngine(nil, cfg)
	_, err = engine2.CreateMigration("a", "b")
	if err == nil {
		t.Error("expected error when migrations disabled")
	}
}

func TestSchemaInferenceRollback(t *testing.T) {
	engine := NewSchemaInferenceEngine(nil, DefaultSchemaInferenceConfig())

	mig, err := engine.CreateMigration("v1", "v2")
	if err != nil {
		t.Fatalf("CreateMigration failed: %v", err)
	}

	// Cannot rollback pending migration
	if err := engine.RollbackMigration(mig.ID); err == nil {
		t.Error("expected error rolling back pending migration")
	}

	// Execute then rollback
	if err := engine.ExecuteMigration(mig.ID); err != nil {
		t.Fatalf("ExecuteMigration failed: %v", err)
	}
	if err := engine.RollbackMigration(mig.ID); err != nil {
		t.Fatalf("RollbackMigration failed: %v", err)
	}

	migs := engine.ListMigrations()
	if migs[0].Status != MigrationRolledBack {
		t.Errorf("expected rolled_back status, got %s", migs[0].Status)
	}

	// Error: rollback non-existent
	if err := engine.RollbackMigration("nonexistent"); err == nil {
		t.Error("expected error for non-existent migration")
	}
}

func TestSchemaInferenceRecommendations(t *testing.T) {
	engine := NewSchemaInferenceEngine(nil, DefaultSchemaInferenceConfig())

	// Manually inject a schema with high cardinality to trigger recommendations
	schema := &InferredSchema{
		MetricName:  "test.metric",
		SampleCount: 1000,
		Confidence:  0.95,
		Fields: []InferredField{
			{Name: "value", DataType: FieldFloat},
			{Name: "request_id", DataType: FieldString, Cardinality: 900},
		},
	}
	engine.generateRecommendations(schema)

	recs := engine.GetRecommendations()
	if len(recs) == 0 {
		t.Fatal("expected at least one recommendation")
	}

	hasHighCard := false
	hasReady := false
	for _, r := range recs {
		if r.RecommendationType == "high_cardinality" {
			hasHighCard = true
		}
		if r.RecommendationType == "schema_ready" {
			hasReady = true
		}
	}
	if !hasHighCard {
		t.Error("expected high_cardinality recommendation")
	}
	if !hasReady {
		t.Error("expected schema_ready recommendation")
	}
}

func TestSchemaInferenceStats(t *testing.T) {
	engine := NewSchemaInferenceEngine(nil, DefaultSchemaInferenceConfig())

	engine.mu.Lock()
	engine.inferred["m1"] = &InferredSchema{MetricName: "m1", InferredAt: time.Now()}
	engine.inferred["m2"] = &InferredSchema{MetricName: "m2", InferredAt: time.Now()}
	engine.mu.Unlock()

	mig, _ := engine.CreateMigration("a", "b")
	engine.ExecuteMigration(mig.ID)

	stats := engine.Stats()
	if stats.SchemasInferred != 2 {
		t.Errorf("expected 2 schemas inferred, got %d", stats.SchemasInferred)
	}
	if stats.MigrationsTotal != 1 {
		t.Errorf("expected 1 migration, got %d", stats.MigrationsTotal)
	}
	if stats.MigrationsSuccessful != 1 {
		t.Errorf("expected 1 successful migration, got %d", stats.MigrationsSuccessful)
	}
}

func TestSchemaInferenceHTTPHandlers(t *testing.T) {
	engine := NewSchemaInferenceEngine(nil, DefaultSchemaInferenceConfig())

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// GET /api/v1/schemas/inference - list inferred schemas
	req := httptest.NewRequest(http.MethodGet, "/api/v1/schemas/inference", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	// POST /api/v1/schemas/inference?metric=test
	req = httptest.NewRequest(http.MethodPost, "/api/v1/schemas/inference?metric=test", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", w.Code)
	}
	var schema InferredSchema
	json.NewDecoder(w.Body).Decode(&schema)
	if schema.MetricName != "test" {
		t.Errorf("expected metric test, got %s", schema.MetricName)
	}

	// POST without metric param
	req = httptest.NewRequest(http.MethodPost, "/api/v1/schemas/inference", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	// GET /api/v1/schemas/migrations
	req = httptest.NewRequest(http.MethodGet, "/api/v1/schemas/migrations", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	// POST /api/v1/schemas/migrations
	body := `{"from":"v1","to":"v2"}`
	req = httptest.NewRequest(http.MethodPost, "/api/v1/schemas/migrations", strings.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", w.Code)
	}

	// GET /api/v1/schemas/recommendations
	req = httptest.NewRequest(http.MethodGet, "/api/v1/schemas/recommendations", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	// GET /api/v1/schemas/inference/stats
	req = httptest.NewRequest(http.MethodGet, "/api/v1/schemas/inference/stats", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	// Method not allowed
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/schemas/inference", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}
