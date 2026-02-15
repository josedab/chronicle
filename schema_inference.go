package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

// SchemaInferenceConfig configures the schema inference engine.
type SchemaInferenceConfig struct {
	SampleSize           int     `json:"sample_size"`
	ConfidenceThreshold  float64 `json:"confidence_threshold"`
	AutoSuggestIndexes   bool    `json:"auto_suggest_indexes"`
	AutoSuggestCompress  bool    `json:"auto_suggest_compression"`
	AutoSuggestRetention bool    `json:"auto_suggest_retention"`
	MaxFieldCount        int     `json:"max_field_count"`
	EnableMigration      bool    `json:"enable_migration"`
	MigrationBatchSize   int     `json:"migration_batch_size"`
}

// DefaultSchemaInferenceConfig returns sensible defaults for schema inference.
func DefaultSchemaInferenceConfig() SchemaInferenceConfig {
	return SchemaInferenceConfig{
		SampleSize:           1000,
		ConfidenceThreshold:  0.9,
		AutoSuggestIndexes:   true,
		AutoSuggestCompress:  true,
		AutoSuggestRetention: true,
		MaxFieldCount:        256,
		EnableMigration:      true,
		MigrationBatchSize:   5000,
	}
}

// FieldDataType represents the inferred data type of a field.
type FieldDataType string

const (
	FieldFloat     FieldDataType = "float64"
	FieldInt       FieldDataType = "int64"
	FieldString    FieldDataType = "string"
	FieldBool      FieldDataType = "bool"
	FieldTimestamp FieldDataType = "timestamp"
)

// InferredField represents a single field discovered during schema inference.
type InferredField struct {
	Name          string        `json:"name"`
	DataType      FieldDataType `json:"data_type"`
	Nullable      bool          `json:"nullable"`
	Cardinality   int           `json:"cardinality"`
	MinValue      float64       `json:"min_value"`
	MaxValue      float64       `json:"max_value"`
	MeanValue     float64       `json:"mean_value"`
	ExampleValues []string      `json:"example_values,omitempty"`
}

// InferredSchema is the result of analyzing data for a metric.
type InferredSchema struct {
	MetricName           string          `json:"metric_name"`
	Fields               []InferredField `json:"fields"`
	SampleCount          int             `json:"sample_count"`
	Confidence           float64         `json:"confidence"`
	InferredAt           time.Time       `json:"inferred_at"`
	SuggestedIndexes     []string        `json:"suggested_indexes,omitempty"`
	SuggestedCompression string          `json:"suggested_compression,omitempty"`
	SuggestedRetention   time.Duration   `json:"suggested_retention,omitempty"`
}

// MigrationStatus represents the current state of a schema migration.
type MigrationStatus string

const (
	MigrationPending    MigrationStatus = "pending"
	MigrationRunning    MigrationStatus = "running"
	MigrationComplete   MigrationStatus = "complete"
	MigrationFailed     MigrationStatus = "failed"
	MigrationRolledBack MigrationStatus = "rolled_back"
)

// SchemaMigration tracks a schema migration operation.
type SchemaMigration struct {
	ID           string          `json:"id"`
	FromSchema   string          `json:"from_schema"`
	ToSchema     string          `json:"to_schema"`
	Status       MigrationStatus `json:"status"`
	CreatedAt    time.Time       `json:"created_at"`
	CompletedAt  time.Time       `json:"completed_at,omitempty"`
	AffectedRows int64           `json:"affected_rows"`
	ErrorMessage string          `json:"error_message,omitempty"`
}

// SchemaRecommendation suggests improvements for a metric's schema.
type SchemaRecommendation struct {
	MetricName         string  `json:"metric_name"`
	RecommendationType string  `json:"recommendation_type"`
	Description        string  `json:"description"`
	Impact             string  `json:"impact"`
	Confidence         float64 `json:"confidence"`
}

// SchemaInferenceStats holds statistics for the inference engine.
type SchemaInferenceStats struct {
	SchemasInferred      int       `json:"schemas_inferred"`
	MigrationsTotal      int       `json:"migrations_total"`
	MigrationsSuccessful int       `json:"migrations_successful"`
	RecommendationsCount int       `json:"recommendations_count"`
	LastInferenceTime    time.Time `json:"last_inference_time"`
}

// SchemaInferenceEngine analyzes time-series data to infer schemas and manage migrations.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type SchemaInferenceEngine struct {
	db              *DB
	config          SchemaInferenceConfig
	inferred        map[string]*InferredSchema
	migrations      []SchemaMigration
	recommendations []SchemaRecommendation
	mu              sync.RWMutex
}

// NewSchemaInferenceEngine creates a new schema inference engine.
func NewSchemaInferenceEngine(db *DB, config SchemaInferenceConfig) *SchemaInferenceEngine {
	return &SchemaInferenceEngine{
		db:       db,
		config:   config,
		inferred: make(map[string]*InferredSchema),
	}
}

// InferSchema analyzes stored data to infer the schema for a metric.
func (e *SchemaInferenceEngine) InferSchema(metric string) (*InferredSchema, error) {
	if metric == "" {
		return nil, fmt.Errorf("schema_inference: metric name is required")
	}

	var points []Point
	if e.db != nil {
		result, err := e.db.Execute(&Query{
			Metric: metric,
			Limit:  e.config.SampleSize,
		})
		if err != nil {
			return nil, fmt.Errorf("schema_inference: query failed for %q: %w", metric, err)
		}
		if result != nil {
			points = result.Points
		}
	}

	schema := &InferredSchema{
		MetricName: metric,
		InferredAt: time.Now(),
	}

	if len(points) == 0 {
		schema.SampleCount = 0
		schema.Confidence = 0
		schema.Fields = []InferredField{
			{Name: "value", DataType: FieldFloat},
			{Name: "timestamp", DataType: FieldTimestamp},
		}
		e.mu.Lock()
		e.inferred[metric] = schema
		e.mu.Unlock()
		return schema, nil
	}

	schema.SampleCount = len(points)

	// Analyze value field
	valueField := InferredField{
		Name:     "value",
		DataType: FieldFloat,
		MinValue: math.MaxFloat64,
		MaxValue: -math.MaxFloat64,
	}
	sum := 0.0
	allInt := true
	for _, p := range points {
		if p.Value < valueField.MinValue {
			valueField.MinValue = p.Value
		}
		if p.Value > valueField.MaxValue {
			valueField.MaxValue = p.Value
		}
		sum += p.Value
		if p.Value != math.Trunc(p.Value) {
			allInt = false
		}
	}
	valueField.MeanValue = sum / float64(len(points))
	if allInt {
		valueField.DataType = FieldInt
	}
	valueField.ExampleValues = []string{
		fmt.Sprintf("%g", points[0].Value),
	}
	if len(points) > 1 {
		valueField.ExampleValues = append(valueField.ExampleValues, fmt.Sprintf("%g", points[len(points)-1].Value))
	}

	// Timestamp field
	tsField := InferredField{
		Name:     "timestamp",
		DataType: FieldTimestamp,
	}

	schema.Fields = []InferredField{valueField, tsField}

	// Analyze tags as fields
	tagCards := make(map[string]map[string]bool)
	for _, p := range points {
		for k, v := range p.Tags {
			if tagCards[k] == nil {
				tagCards[k] = make(map[string]bool)
			}
			tagCards[k][v] = true
		}
	}
	tagKeys := make([]string, 0, len(tagCards))
	for k := range tagCards {
		tagKeys = append(tagKeys, k)
	}
	sort.Strings(tagKeys)
	for _, k := range tagKeys {
		vals := tagCards[k]
		f := InferredField{
			Name:        k,
			DataType:    FieldString,
			Cardinality: len(vals),
			Nullable:    len(vals) < len(points),
		}
		examples := make([]string, 0, 3)
		for v := range vals {
			if len(examples) >= 3 {
				break
			}
			examples = append(examples, v)
		}
		sort.Strings(examples)
		f.ExampleValues = examples
		schema.Fields = append(schema.Fields, f)
	}

	// Compute confidence based on sample size
	schema.Confidence = math.Min(float64(schema.SampleCount)/float64(e.config.SampleSize), 1.0)

	// Suggestions
	if e.config.AutoSuggestIndexes {
		schema.SuggestedIndexes = e.suggestIndexesForSchema(schema)
	}
	if e.config.AutoSuggestCompress {
		schema.SuggestedCompression = e.suggestCompressionForSchema(schema)
	}
	if e.config.AutoSuggestRetention {
		schema.SuggestedRetention = e.suggestRetentionForSchema(schema)
	}

	// Generate recommendations
	e.generateRecommendations(schema)

	e.mu.Lock()
	e.inferred[metric] = schema
	e.mu.Unlock()

	return schema, nil
}

// InferAll infers schemas for all known metrics.
func (e *SchemaInferenceEngine) InferAll() ([]*InferredSchema, error) {
	var metrics []string
	if e.db != nil {
		metrics = e.db.Metrics()
	}
	if len(metrics) == 0 {
		return nil, nil
	}

	results := make([]*InferredSchema, 0, len(metrics))
	for _, m := range metrics {
		schema, err := e.InferSchema(m)
		if err != nil {
			return results, fmt.Errorf("schema_inference: failed for metric %q: %w", m, err)
		}
		results = append(results, schema)
	}
	return results, nil
}

// GetInferredSchema returns the cached inferred schema for a metric.
func (e *SchemaInferenceEngine) GetInferredSchema(metric string) *InferredSchema {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if s, ok := e.inferred[metric]; ok {
		cp := *s
		return &cp
	}
	return nil
}

// ListInferredSchemas returns all cached inferred schemas.
func (e *SchemaInferenceEngine) ListInferredSchemas() []*InferredSchema {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*InferredSchema, 0, len(e.inferred))
	for _, s := range e.inferred {
		cp := *s
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].MetricName < result[j].MetricName })
	return result
}

// SuggestIndexes suggests indexes for a metric based on query patterns.
func (e *SchemaInferenceEngine) SuggestIndexes(metric string) []string {
	e.mu.RLock()
	schema, ok := e.inferred[metric]
	e.mu.RUnlock()

	if !ok || schema == nil {
		return []string{"timestamp"}
	}
	return e.suggestIndexesForSchema(schema)
}

// SuggestCompression suggests a compression algorithm for a metric.
func (e *SchemaInferenceEngine) SuggestCompression(metric string) string {
	e.mu.RLock()
	schema, ok := e.inferred[metric]
	e.mu.RUnlock()

	if !ok || schema == nil {
		return "gorilla"
	}
	return e.suggestCompressionForSchema(schema)
}

// SuggestRetention suggests a retention duration for a metric.
func (e *SchemaInferenceEngine) SuggestRetention(metric string) time.Duration {
	e.mu.RLock()
	schema, ok := e.inferred[metric]
	e.mu.RUnlock()

	if !ok || schema == nil {
		return 30 * 24 * time.Hour
	}
	return e.suggestRetentionForSchema(schema)
}

// CreateMigration creates a new schema migration plan.
func (e *SchemaInferenceEngine) CreateMigration(from, to string) (*SchemaMigration, error) {
	if !e.config.EnableMigration {
		return nil, fmt.Errorf("schema_inference: migrations are disabled")
	}
	if from == "" || to == "" {
		return nil, fmt.Errorf("schema_inference: from and to schemas are required")
	}

	migration := SchemaMigration{
		ID:         fmt.Sprintf("mig_%d", time.Now().UnixNano()),
		FromSchema: from,
		ToSchema:   to,
		Status:     MigrationPending,
		CreatedAt:  time.Now(),
	}

	e.mu.Lock()
	e.migrations = append(e.migrations, migration)
	e.mu.Unlock()

	return &migration, nil
}

// ExecuteMigration executes a planned migration.
func (e *SchemaInferenceEngine) ExecuteMigration(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i := range e.migrations {
		if e.migrations[i].ID == id {
			if e.migrations[i].Status != MigrationPending {
				return fmt.Errorf("schema_inference: migration %q is not pending (status: %s)", id, e.migrations[i].Status)
			}
			e.migrations[i].Status = MigrationRunning

			// Simulate migration execution
			e.migrations[i].AffectedRows = int64(e.config.MigrationBatchSize)
			e.migrations[i].Status = MigrationComplete
			e.migrations[i].CompletedAt = time.Now()
			return nil
		}
	}
	return fmt.Errorf("schema_inference: migration %q not found", id)
}

// RollbackMigration rolls back a completed or failed migration.
func (e *SchemaInferenceEngine) RollbackMigration(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i := range e.migrations {
		if e.migrations[i].ID == id {
			if e.migrations[i].Status != MigrationComplete && e.migrations[i].Status != MigrationFailed {
				return fmt.Errorf("schema_inference: migration %q cannot be rolled back (status: %s)", id, e.migrations[i].Status)
			}
			e.migrations[i].Status = MigrationRolledBack
			e.migrations[i].CompletedAt = time.Now()
			return nil
		}
	}
	return fmt.Errorf("schema_inference: migration %q not found", id)
}

// ListMigrations returns all migrations.
func (e *SchemaInferenceEngine) ListMigrations() []SchemaMigration {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]SchemaMigration, len(e.migrations))
	copy(result, e.migrations)
	return result
}

// GetRecommendations returns all schema recommendations.
func (e *SchemaInferenceEngine) GetRecommendations() []SchemaRecommendation {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]SchemaRecommendation, len(e.recommendations))
	copy(result, e.recommendations)
	return result
}

// Stats returns inference engine statistics.
func (e *SchemaInferenceEngine) Stats() SchemaInferenceStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	successful := 0
	for _, m := range e.migrations {
		if m.Status == MigrationComplete {
			successful++
		}
	}

	var lastInference time.Time
	for _, s := range e.inferred {
		if s.InferredAt.After(lastInference) {
			lastInference = s.InferredAt
		}
	}

	return SchemaInferenceStats{
		SchemasInferred:      len(e.inferred),
		MigrationsTotal:      len(e.migrations),
		MigrationsSuccessful: successful,
		RecommendationsCount: len(e.recommendations),
		LastInferenceTime:    lastInference,
	}
}

// RegisterHTTPHandlers registers schema inference HTTP endpoints.
func (e *SchemaInferenceEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/schemas/inference", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e.ListInferredSchemas())
		case http.MethodPost:
			metric := r.URL.Query().Get("metric")
			if metric == "" {
				http.Error(w, "metric parameter required", http.StatusBadRequest)
				return
			}
			schema, err := e.InferSchema(metric)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(schema)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/schemas/inference/all", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		schemas, err := e.InferAll()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(schemas)
	})
	mux.HandleFunc("/api/v1/schemas/migrations", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e.ListMigrations())
		case http.MethodPost:
			var req struct {
				From string `json:"from"`
				To   string `json:"to"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			mig, err := e.CreateMigration(req.From, req.To)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(mig)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/schemas/recommendations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetRecommendations())
	})
	mux.HandleFunc("/api/v1/schemas/inference/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}

// --- internal helpers ---

func (e *SchemaInferenceEngine) suggestIndexesForSchema(schema *InferredSchema) []string {
	indexes := []string{"timestamp"}
	for _, f := range schema.Fields {
		if f.DataType == FieldString && f.Cardinality > 0 && f.Cardinality < 1000 {
			indexes = append(indexes, f.Name)
		}
	}
	return indexes
}

func (e *SchemaInferenceEngine) suggestCompressionForSchema(schema *InferredSchema) string {
	for _, f := range schema.Fields {
		if f.Name == "value" {
			if f.DataType == FieldInt {
				return "delta"
			}
			spread := f.MaxValue - f.MinValue
			if spread > 0 && spread < 1000 {
				return "gorilla"
			}
		}
	}
	return "gorilla"
}

func (e *SchemaInferenceEngine) suggestRetentionForSchema(schema *InferredSchema) time.Duration {
	// High-cardinality metrics get shorter retention
	totalCard := 0
	for _, f := range schema.Fields {
		totalCard += f.Cardinality
	}
	if totalCard > 500 {
		return 7 * 24 * time.Hour
	}
	if totalCard > 100 {
		return 30 * 24 * time.Hour
	}
	return 90 * 24 * time.Hour
}

func (e *SchemaInferenceEngine) generateRecommendations(schema *InferredSchema) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, f := range schema.Fields {
		if f.DataType == FieldString && f.Cardinality > 500 {
			e.recommendations = append(e.recommendations, SchemaRecommendation{
				MetricName:         schema.MetricName,
				RecommendationType: "high_cardinality",
				Description:        fmt.Sprintf("Tag %q has high cardinality (%d unique values)", f.Name, f.Cardinality),
				Impact:             "high",
				Confidence:         schema.Confidence,
			})
		}
	}

	if schema.SampleCount > 0 && schema.Confidence >= e.config.ConfidenceThreshold {
		e.recommendations = append(e.recommendations, SchemaRecommendation{
			MetricName:         schema.MetricName,
			RecommendationType: "schema_ready",
			Description:        fmt.Sprintf("Schema for %q has high confidence (%.0f%%)", schema.MetricName, schema.Confidence*100),
			Impact:             "low",
			Confidence:         schema.Confidence,
		})
	}
}
