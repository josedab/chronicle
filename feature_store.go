package chronicle

import (
	"context"
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
	p.ensureTags()
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
