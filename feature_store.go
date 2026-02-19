package chronicle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// FeatureStoreConfig configures the ML feature store.
type FeatureStoreConfig struct {
	// MaxFeatures limits the number of registered features.
	MaxFeatures int `json:"max_features"`

	// DefaultTTL is the default feature value TTL.
	DefaultTTL time.Duration `json:"default_ttl"`

	// EnableVersioning enables feature versioning.
	EnableVersioning bool `json:"enable_versioning"`

	// MaxVersions is the maximum versions per feature.
	MaxVersions int `json:"max_versions"`

	// CacheEnabled enables feature value caching.
	CacheEnabled bool `json:"cache_enabled"`

	// CacheTTL is the cache TTL for feature values.
	CacheTTL time.Duration `json:"cache_ttl"`

	// OnlineServingEnabled enables low-latency serving.
	OnlineServingEnabled bool `json:"online_serving_enabled"`

	// BatchServingEnabled enables batch feature retrieval.
	BatchServingEnabled bool `json:"batch_serving_enabled"`

	// PointInTimeEnabled enables point-in-time correct lookups.
	PointInTimeEnabled bool `json:"point_in_time_enabled"`
}

// DefaultFeatureStoreConfig returns default configuration.
func DefaultFeatureStoreConfig() FeatureStoreConfig {
	return FeatureStoreConfig{
		MaxFeatures:          10000,
		DefaultTTL:           24 * time.Hour,
		EnableVersioning:     true,
		MaxVersions:          100,
		CacheEnabled:         true,
		CacheTTL:             5 * time.Minute,
		OnlineServingEnabled: true,
		BatchServingEnabled:  true,
		PointInTimeEnabled:   true,
	}
}

// FeatureStore provides ML feature storage and serving.
type FeatureStore struct {
	db     *DB
	config FeatureStoreConfig

	// Feature registry
	features   map[string]*FeatureDefinition
	featuresMu sync.RWMutex

	// Feature groups
	groups   map[string]*FeatureGroup
	groupsMu sync.RWMutex

	// Online cache
	cache   *featureCache
	cacheMu sync.RWMutex

	// Statistics
	stats   FeatureStoreStats
	statsMu sync.RWMutex
}

// FeatureDefinition describes a feature.
type FeatureDefinition struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	ValueType   FeatureValueType  `json:"value_type"`
	EntityType  string            `json:"entity_type"`
	Tags        map[string]string `json:"tags"`
	TTL         time.Duration     `json:"ttl"`
	Version     int               `json:"version"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	Schema      *FeatureSchema    `json:"schema,omitempty"`
	Transform   *FeatureTransform `json:"transform,omitempty"`
}

// FeatureValueType specifies the feature value type.
type FeatureValueType int

const (
	FeatureTypeFloat FeatureValueType = iota
	FeatureTypeInt
	FeatureTypeString
	FeatureTypeBool
	FeatureTypeVector
	FeatureTypeMap
	FeatureTypeList
)

// FeatureSchema defines the feature schema.
type FeatureSchema struct {
	Fields        []FeatureField `json:"fields"`
	Required      []string       `json:"required"`
	Nullable      bool           `json:"nullable"`
	MinValue      *float64       `json:"min_value,omitempty"`
	MaxValue      *float64       `json:"max_value,omitempty"`
	AllowedValues []string       `json:"allowed_values,omitempty"`
}

// FeatureField describes a field in a complex feature.
type FeatureField struct {
	Name      string           `json:"name"`
	Type      FeatureValueType `json:"type"`
	Dimension int              `json:"dimension,omitempty"` // For vector types
}

// FeatureTransform describes feature transformations.
type FeatureTransform struct {
	Type           TransformType  `json:"type"`
	Parameters     map[string]any `json:"parameters"`
	SourceFeatures []string       `json:"source_features,omitempty"`
}

// TransformType specifies the transformation type.
type TransformType int

const (
	TransformNone TransformType = iota
	TransformNormalize
	TransformStandardize
	TransformLog
	TransformBucketize
	TransformOneHot
	TransformAggregate
	TransformDerived
)

// FeatureGroup is a collection of related features.
type FeatureGroup struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	EntityType  string        `json:"entity_type"`
	Features    []string      `json:"features"`
	TTL         time.Duration `json:"ttl"`
	Version     int           `json:"version"`
	CreatedAt   time.Time     `json:"created_at"`
}

// FeatureValue represents a stored feature value.
type FeatureValue struct {
	FeatureName string            `json:"feature_name"`
	EntityID    string            `json:"entity_id"`
	Value       any               `json:"value"`
	Timestamp   int64             `json:"timestamp"`
	EventTime   int64             `json:"event_time"`
	Version     int               `json:"version"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// FeatureVector represents a set of feature values.
type FeatureVector struct {
	EntityID   string           `json:"entity_id"`
	Features   map[string]any   `json:"features"`
	Timestamps map[string]int64 `json:"timestamps"`
	AsOfTime   int64            `json:"as_of_time"`
}

// FeatureStoreStats contains feature store statistics.
type FeatureStoreStats struct {
	FeatureCount       int     `json:"feature_count"`
	GroupCount         int     `json:"group_count"`
	TotalWrites        int64   `json:"total_writes"`
	TotalReads         int64   `json:"total_reads"`
	CacheHits          int64   `json:"cache_hits"`
	CacheMisses        int64   `json:"cache_misses"`
	AverageLatencyMs   float64 `json:"average_latency_ms"`
	PointInTimeQueries int64   `json:"point_in_time_queries"`
}

// NewFeatureStore creates a new feature store.
func NewFeatureStore(db *DB, config FeatureStoreConfig) (*FeatureStore, error) {
	fs := &FeatureStore{
		db:       db,
		config:   config,
		features: make(map[string]*FeatureDefinition),
		groups:   make(map[string]*FeatureGroup),
	}

	if config.CacheEnabled {
		fs.cache = newFeatureCache(config.CacheTTL)
	}

	return fs, nil
}

// RegisterFeature registers a new feature definition.
func (fs *FeatureStore) RegisterFeature(def *FeatureDefinition) error {
	if def.Name == "" {
		return errors.New("feature name is required")
	}
	if def.EntityType == "" {
		return errors.New("entity type is required")
	}

	fs.featuresMu.Lock()
	defer fs.featuresMu.Unlock()

	// Check limit
	if len(fs.features) >= fs.config.MaxFeatures {
		return errors.New("feature limit reached")
	}

	// Check if feature exists
	existing, exists := fs.features[def.Name]
	if exists {
		if fs.config.EnableVersioning {
			// Create new version
			def.Version = existing.Version + 1
			if def.Version > fs.config.MaxVersions {
				return errors.New("max versions reached for feature")
			}
		} else {
			return fmt.Errorf("feature %s already exists", def.Name)
		}
	} else {
		def.Version = 1
	}

	def.CreatedAt = time.Now()
	def.UpdatedAt = time.Now()

	if def.TTL == 0 {
		def.TTL = fs.config.DefaultTTL
	}

	fs.features[def.Name] = def
	return nil
}

// GetFeature retrieves a feature definition.
func (fs *FeatureStore) GetFeature(name string) (*FeatureDefinition, error) {
	fs.featuresMu.RLock()
	defer fs.featuresMu.RUnlock()

	feat, ok := fs.features[name]
	if !ok {
		return nil, fmt.Errorf("feature %s not found", name)
	}

	return feat, nil
}

// ListFeatures returns all registered features.
func (fs *FeatureStore) ListFeatures() []*FeatureDefinition {
	fs.featuresMu.RLock()
	defer fs.featuresMu.RUnlock()

	result := make([]*FeatureDefinition, 0, len(fs.features))
	for _, f := range fs.features {
		result = append(result, f)
	}

	// Sort by name
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// CreateGroup creates a feature group.
func (fs *FeatureStore) CreateGroup(group *FeatureGroup) error {
	if group.Name == "" {
		return errors.New("group name is required")
	}

	fs.groupsMu.Lock()
	defer fs.groupsMu.Unlock()

	if _, exists := fs.groups[group.Name]; exists {
		return fmt.Errorf("group %s already exists", group.Name)
	}

	group.Version = 1
	group.CreatedAt = time.Now()
	fs.groups[group.Name] = group

	return nil
}

// GetGroup retrieves a feature group.
func (fs *FeatureStore) GetGroup(name string) (*FeatureGroup, error) {
	fs.groupsMu.RLock()
	defer fs.groupsMu.RUnlock()

	group, ok := fs.groups[name]
	if !ok {
		return nil, fmt.Errorf("group %s not found", name)
	}

	return group, nil
}

// WriteFeature writes a feature value.
func (fs *FeatureStore) WriteFeature(ctx context.Context, fv *FeatureValue) error {
	start := time.Now()
	defer func() {
		fs.statsMu.Lock()
		fs.stats.TotalWrites++
		elapsed := time.Since(start).Milliseconds()
		fs.stats.AverageLatencyMs = fs.stats.AverageLatencyMs*0.9 + float64(elapsed)*0.1
		fs.statsMu.Unlock()
	}()

	// Validate feature exists
	fs.featuresMu.RLock()
	def, exists := fs.features[fv.FeatureName]
	fs.featuresMu.RUnlock()

	if !exists {
		return fmt.Errorf("feature %s not found", fv.FeatureName)
	}

	// Validate value against schema
	if def.Schema != nil {
		if err := fs.validateValue(fv.Value, def); err != nil {
			return fmt.Errorf("validation failed: %w", err)
		}
	}

	// Set timestamps
	if fv.Timestamp == 0 {
		fv.Timestamp = time.Now().UnixNano()
	}
	if fv.EventTime == 0 {
		fv.EventTime = fv.Timestamp
	}
	fv.Version = def.Version

	// Write to database as a point
	p := Point{
		Metric: fmt.Sprintf("feature:%s", fv.FeatureName),
		Tags: map[string]string{
			"entity_id":   fv.EntityID,
			"entity_type": def.EntityType,
			"version":     fmt.Sprintf("%d", fv.Version),
		},
		Value:     fs.valueToFloat(fv.Value),
		Timestamp: fv.EventTime,
	}

	// Add metadata as tags
	for k, v := range fv.Metadata {
		p.Tags["meta_"+k] = v
	}

	if err := fs.db.Write(p); err != nil {
		return err
	}

	// Update cache
	if fs.cache != nil {
		cacheKey := fs.cacheKey(fv.FeatureName, fv.EntityID)
		fs.cache.set(cacheKey, fv)
	}

	return nil
}

// WriteFeatureBatch writes multiple feature values.
func (fs *FeatureStore) WriteFeatureBatch(ctx context.Context, values []*FeatureValue) error {
	for _, fv := range values {
		if err := fs.WriteFeature(ctx, fv); err != nil {
			return err
		}
	}
	return nil
}

// GetFeatureValue retrieves the latest feature value.
func (fs *FeatureStore) GetFeatureValue(ctx context.Context, featureName, entityID string) (*FeatureValue, error) {
	start := time.Now()
	defer func() {
		fs.statsMu.Lock()
		fs.stats.TotalReads++
		elapsed := time.Since(start).Milliseconds()
		fs.stats.AverageLatencyMs = fs.stats.AverageLatencyMs*0.9 + float64(elapsed)*0.1
		fs.statsMu.Unlock()
	}()

	// Check cache first
	if fs.cache != nil {
		cacheKey := fs.cacheKey(featureName, entityID)
		if fv, ok := fs.cache.get(cacheKey); ok {
			fs.statsMu.Lock()
			fs.stats.CacheHits++
			fs.statsMu.Unlock()
			return fv, nil
		}
		fs.statsMu.Lock()
		fs.stats.CacheMisses++
		fs.statsMu.Unlock()
	}

	// Query from database
	result, err := fs.db.Execute(&Query{
		Metric: fmt.Sprintf("feature:%s", featureName),
		Tags:   map[string]string{"entity_id": entityID},
		Start:  0,
		End:    time.Now().UnixNano(),
		Limit:  1,
	})

	if err != nil {
		return nil, err
	}

	if len(result.Points) == 0 {
		return nil, fmt.Errorf("no value found for feature %s entity %s", featureName, entityID)
	}

	// Convert point to feature value
	p := result.Points[len(result.Points)-1] // Get latest
	fv := &FeatureValue{
		FeatureName: featureName,
		EntityID:    entityID,
		Value:       p.Value,
		Timestamp:   p.Timestamp,
		EventTime:   p.Timestamp,
	}

	// Cache the result
	if fs.cache != nil {
		cacheKey := fs.cacheKey(featureName, entityID)
		fs.cache.set(cacheKey, fv)
	}

	return fv, nil
}

// GetFeatureValueAsOf retrieves feature value as of a specific time (point-in-time correct).
func (fs *FeatureStore) GetFeatureValueAsOf(ctx context.Context, featureName, entityID string, asOfTime int64) (*FeatureValue, error) {
	if !fs.config.PointInTimeEnabled {
		return nil, errors.New("point-in-time queries not enabled")
	}

	fs.statsMu.Lock()
	fs.stats.PointInTimeQueries++
	fs.statsMu.Unlock()

	// Query for value at or before asOfTime
	result, err := fs.db.Execute(&Query{
		Metric: fmt.Sprintf("feature:%s", featureName),
		Tags:   map[string]string{"entity_id": entityID},
		Start:  0,
		End:    asOfTime,
	})

	if err != nil {
		return nil, err
	}

	if len(result.Points) == 0 {
		return nil, fmt.Errorf("no value found for feature %s entity %s as of %d", featureName, entityID, asOfTime)
	}

	// Get the latest value before asOfTime
	p := result.Points[len(result.Points)-1]
	return &FeatureValue{
		FeatureName: featureName,
		EntityID:    entityID,
		Value:       p.Value,
		Timestamp:   p.Timestamp,
		EventTime:   p.Timestamp,
	}, nil
}

// GetFeatureVector retrieves multiple features for an entity.
func (fs *FeatureStore) GetFeatureVector(ctx context.Context, entityID string, featureNames []string) (*FeatureVector, error) {
	fv := &FeatureVector{
		EntityID:   entityID,
		Features:   make(map[string]any),
		Timestamps: make(map[string]int64),
		AsOfTime:   time.Now().UnixNano(),
	}

	for _, name := range featureNames {
		value, err := fs.GetFeatureValue(ctx, name, entityID)
		if err != nil {
			continue // Skip missing features
		}
		fv.Features[name] = value.Value
		fv.Timestamps[name] = value.Timestamp
	}

	return fv, nil
}

// GetFeatureVectorAsOf retrieves feature vector at a specific time.
func (fs *FeatureStore) GetFeatureVectorAsOf(ctx context.Context, entityID string, featureNames []string, asOfTime int64) (*FeatureVector, error) {
	fv := &FeatureVector{
		EntityID:   entityID,
		Features:   make(map[string]any),
		Timestamps: make(map[string]int64),
		AsOfTime:   asOfTime,
	}

	for _, name := range featureNames {
		value, err := fs.GetFeatureValueAsOf(ctx, name, entityID, asOfTime)
		if err != nil {
			continue
		}
		fv.Features[name] = value.Value
		fv.Timestamps[name] = value.Timestamp
	}

	return fv, nil
}

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
