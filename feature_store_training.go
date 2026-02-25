package chronicle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Training data retrieval and helper functions for the feature store.

// GetTrainingData retrieves historical feature data for ML training.
func (fs *FeatureStore) GetTrainingData(ctx context.Context, req *TrainingDataRequest) (*TrainingDataResponse, error) {
	if !fs.config.BatchServingEnabled {
		return nil, errors.New("batch serving not enabled")
	}

	response := &TrainingDataResponse{
		Features: req.FeatureNames,
		Rows:     make([]TrainingRow, 0),
	}

	// For each entity-timestamp pair
	for _, entity := range req.Entities {
		for _, ts := range req.Timestamps {
			row := TrainingRow{
				EntityID:  entity,
				Timestamp: ts,
				Values:    make(map[string]any),
			}

			// Get point-in-time correct values
			for _, fname := range req.FeatureNames {
				value, err := fs.GetFeatureValueAsOf(ctx, fname, entity, ts)
				if err == nil {
					row.Values[fname] = value.Value
				}
			}

			if len(row.Values) > 0 {
				response.Rows = append(response.Rows, row)
			}
		}
	}

	return response, nil
}

// TrainingDataRequest specifies training data retrieval parameters.
type TrainingDataRequest struct {
	FeatureNames []string `json:"feature_names"`
	Entities     []string `json:"entities"`
	Timestamps   []int64  `json:"timestamps"`
	StartTime    int64    `json:"start_time"`
	EndTime      int64    `json:"end_time"`
}

// TrainingDataResponse contains training data.
type TrainingDataResponse struct {
	Features []string      `json:"features"`
	Rows     []TrainingRow `json:"rows"`
}

// TrainingRow represents a single training example.
type TrainingRow struct {
	EntityID  string         `json:"entity_id"`
	Timestamp int64          `json:"timestamp"`
	Values    map[string]any `json:"values"`
}

// Stats returns feature store statistics.
func (fs *FeatureStore) Stats() FeatureStoreStats {
	fs.statsMu.RLock()
	defer fs.statsMu.RUnlock()

	fs.featuresMu.RLock()
	featureCount := len(fs.features)
	fs.featuresMu.RUnlock()

	fs.groupsMu.RLock()
	groupCount := len(fs.groups)
	fs.groupsMu.RUnlock()

	stats := fs.stats
	stats.FeatureCount = featureCount
	stats.GroupCount = groupCount

	return stats
}

// --- Helper Functions ---

func (fs *FeatureStore) validateValue(value any, def *FeatureDefinition) error {
	if def.Schema == nil {
		return nil
	}

	// Check nullable
	if value == nil {
		if !def.Schema.Nullable {
			return errors.New("value cannot be null")
		}
		return nil
	}

	// Type-specific validation
	switch v := value.(type) {
	case float64:
		if def.Schema.MinValue != nil && v < *def.Schema.MinValue {
			return fmt.Errorf("value %f below minimum %f", v, *def.Schema.MinValue)
		}
		if def.Schema.MaxValue != nil && v > *def.Schema.MaxValue {
			return fmt.Errorf("value %f above maximum %f", v, *def.Schema.MaxValue)
		}
	case string:
		if len(def.Schema.AllowedValues) > 0 {
			found := false
			for _, allowed := range def.Schema.AllowedValues {
				if v == allowed {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("value %s not in allowed values", v)
			}
		}
	}

	return nil
}

func (fs *FeatureStore) valueToFloat(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case bool:
		if v {
			return 1.0
		}
		return 0.0
	default:
		return 0.0
	}
}

func (fs *FeatureStore) cacheKey(featureName, entityID string) string {
	data := featureName + ":" + entityID
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:8])
}

// --- Feature Cache ---

type featureCache struct {
	data map[string]*cachedFeature
	ttl  time.Duration
	mu   sync.RWMutex
}

type cachedFeature struct {
	value     *FeatureValue
	expiresAt time.Time
}

func newFeatureCache(ttl time.Duration) *featureCache {
	return &featureCache{
		data: make(map[string]*cachedFeature),
		ttl:  ttl,
	}
}

func (c *featureCache) get(key string) (*FeatureValue, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.data[key]
	if !ok {
		return nil, false
	}

	if time.Now().After(entry.expiresAt) {
		return nil, false
	}

	return entry.value, true
}

func (c *featureCache) set(key string, value *FeatureValue) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[key] = &cachedFeature{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// --- Online Serving API ---

// OnlineServingRequest is a request for online feature serving.
type OnlineServingRequest struct {
	EntityIDs    []string `json:"entity_ids"`
	FeatureNames []string `json:"feature_names"`
	AsOfTime     *int64   `json:"as_of_time,omitempty"`
}

// OnlineServingResponse is the response for online serving.
type OnlineServingResponse struct {
	Vectors []*FeatureVector `json:"vectors"`
	Latency float64          `json:"latency_ms"`
}

// ServeOnline handles online feature serving requests.
func (fs *FeatureStore) ServeOnline(ctx context.Context, req *OnlineServingRequest) (*OnlineServingResponse, error) {
	if !fs.config.OnlineServingEnabled {
		return nil, errors.New("online serving not enabled")
	}

	start := time.Now()

	response := &OnlineServingResponse{
		Vectors: make([]*FeatureVector, 0, len(req.EntityIDs)),
	}

	for _, entityID := range req.EntityIDs {
		var fv *FeatureVector
		var err error

		if req.AsOfTime != nil {
			fv, err = fs.GetFeatureVectorAsOf(ctx, entityID, req.FeatureNames, *req.AsOfTime)
		} else {
			fv, err = fs.GetFeatureVector(ctx, entityID, req.FeatureNames)
		}

		if err != nil {
			continue
		}

		response.Vectors = append(response.Vectors, fv)
	}

	response.Latency = float64(time.Since(start).Microseconds()) / 1000.0

	return response, nil
}

// --- Feature Materialization ---

// MaterializationConfig configures feature materialization.
type MaterializationConfig struct {
	FeatureName string        `json:"feature_name"`
	SourceQuery string        `json:"source_query"`
	Schedule    string        `json:"schedule"` // Cron expression
	TTL         time.Duration `json:"ttl"`
}

// MaterializeFeature computes and stores a derived feature.
func (fs *FeatureStore) MaterializeFeature(ctx context.Context, config *MaterializationConfig) error {
	// Get feature definition
	def, err := fs.GetFeature(config.FeatureName)
	if err != nil {
		return err
	}

	if def.Transform == nil {
		return errors.New("feature has no transform defined")
	}

	// Execute source query based on transform type
	switch def.Transform.Type {
	case TransformAggregate:
		return fs.materializeAggregate(ctx, def, config)
	case TransformDerived:
		return fs.materializeDerived(ctx, def, config)
	default:
		return errors.New("unsupported transform type for materialization")
	}
}

func (fs *FeatureStore) materializeAggregate(ctx context.Context, def *FeatureDefinition, config *MaterializationConfig) error {
	// Get source features
	sourceFeatures := def.Transform.SourceFeatures
	if len(sourceFeatures) == 0 {
		return errors.New("no source features defined")
	}

	// Get aggregation parameters
	window, _ := def.Transform.Parameters["window"].(string)
	aggFunc, _ := def.Transform.Parameters["function"].(string)

	// Query source data
	for _, source := range sourceFeatures {
		result, err := fs.db.Execute(&Query{
			Metric: fmt.Sprintf("feature:%s", source),
			Start:  time.Now().Add(-24 * time.Hour).UnixNano(),
			End:    time.Now().UnixNano(),
		})

		if err != nil {
			continue
		}

		// Group by entity and compute aggregation
		byEntity := make(map[string][]float64)
		for _, p := range result.Points {
			entityID := p.Tags["entity_id"]
			byEntity[entityID] = append(byEntity[entityID], p.Value)
		}

		// Write aggregated values
		for entityID, values := range byEntity {
			aggValue := computeFeatureAgg(values, aggFunc)

			fv := &FeatureValue{
				FeatureName: def.Name,
				EntityID:    entityID,
				Value:       aggValue,
				Metadata: map[string]string{
					"source":   source,
					"window":   window,
					"function": aggFunc,
				},
			}

			fs.WriteFeature(ctx, fv)
		}
	}

	return nil
}

func (fs *FeatureStore) materializeDerived(ctx context.Context, def *FeatureDefinition, config *MaterializationConfig) error {
	// Derived features combine multiple source features
	sourceFeatures := def.Transform.SourceFeatures
	if len(sourceFeatures) < 2 {
		return errors.New("derived features require at least 2 source features")
	}

	// Get entities that have all source features
	entities := make(map[string]bool)
	featureValues := make(map[string]map[string]float64) // entity -> feature -> value

	for _, source := range sourceFeatures {
		result, err := fs.db.Execute(&Query{
			Metric: fmt.Sprintf("feature:%s", source),
			Start:  time.Now().Add(-24 * time.Hour).UnixNano(),
			End:    time.Now().UnixNano(),
		})

		if err != nil {
			continue
		}

		for _, p := range result.Points {
			entityID := p.Tags["entity_id"]
			entities[entityID] = true

			if featureValues[entityID] == nil {
				featureValues[entityID] = make(map[string]float64)
			}
			featureValues[entityID][source] = p.Value
		}
	}

	// Compute derived values for entities with all sources
	operation, _ := def.Transform.Parameters["operation"].(string)

	for entityID := range entities {
		values := featureValues[entityID]
		if len(values) < len(sourceFeatures) {
			continue // Missing some source features
		}

		derivedValue := computeDerived(values, sourceFeatures, operation)

		fv := &FeatureValue{
			FeatureName: def.Name,
			EntityID:    entityID,
			Value:       derivedValue,
			Metadata: map[string]string{
				"operation": operation,
				"derived":   "true",
			},
		}

		fs.WriteFeature(ctx, fv)
	}

	return nil
}

func computeFeatureAgg(values []float64, function string) float64 {
	if len(values) == 0 {
		return 0
	}

	switch function {
	case "sum":
		var sum float64
		for _, v := range values {
			sum += v
		}
		return sum
	case "avg", "mean":
		var sum float64
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))
	case "min":
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		return min
	case "max":
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		return max
	case "count":
		return float64(len(values))
	default:
		return values[len(values)-1]
	}
}

func computeDerived(values map[string]float64, sources []string, operation string) float64 {
	if len(sources) < 2 {
		return 0
	}

	a := values[sources[0]]
	b := values[sources[1]]

	switch operation {
	case "add":
		return a + b
	case "subtract":
		return a - b
	case "multiply":
		return a * b
	case "divide":
		if b == 0 {
			return 0
		}
		return a / b
	case "ratio":
		if b == 0 {
			return 0
		}
		return a / b
	default:
		return a
	}
}

// --- Export for ML Frameworks ---

// ExportToJSON exports feature data in JSON format.
func (fs *FeatureStore) ExportToJSON(ctx context.Context, req *TrainingDataRequest) ([]byte, error) {
	data, err := fs.GetTrainingData(ctx, req)
	if err != nil {
		return nil, err
	}

	return json.Marshal(data)
}

// FSExportFormat specifies feature store export format.
type FSExportFormat int

const (
	FSExportFormatJSON FSExportFormat = iota
	FSExportFormatCSV
	FSExportFormatParquet
)
