package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestNewFeatureStore(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultFeatureStoreConfig()
	fs, err := NewFeatureStore(db, config)
	if err != nil {
		t.Fatalf("NewFeatureStore() error = %v", err)
	}

	if fs.features == nil {
		t.Error("features map should be initialized")
	}
	if fs.groups == nil {
		t.Error("groups map should be initialized")
	}
	if fs.cache == nil {
		t.Error("cache should be initialized when enabled")
	}
}

func TestFeatureRegistration(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultFeatureStoreConfig()
	fs, _ := NewFeatureStore(db, config)

	def := &FeatureDefinition{
		Name:        "user_age",
		Description: "User age in years",
		ValueType:   FeatureTypeInt,
		EntityType:  "user",
		Tags:        map[string]string{"domain": "demographics"},
	}

	// Register feature
	err = fs.RegisterFeature(def)
	if err != nil {
		t.Fatalf("RegisterFeature() error = %v", err)
	}

	// Retrieve feature
	retrieved, err := fs.GetFeature("user_age")
	if err != nil {
		t.Fatalf("GetFeature() error = %v", err)
	}

	if retrieved.Name != "user_age" {
		t.Errorf("Name = %s, want user_age", retrieved.Name)
	}
	if retrieved.Version != 1 {
		t.Errorf("Version = %d, want 1", retrieved.Version)
	}
	if retrieved.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
}

func TestFeatureVersioning(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultFeatureStoreConfig()
	config.EnableVersioning = true
	fs, _ := NewFeatureStore(db, config)

	// Register initial version
	def := &FeatureDefinition{
		Name:       "purchase_count",
		EntityType: "user",
	}
	fs.RegisterFeature(def)

	// Register updated version
	def2 := &FeatureDefinition{
		Name:        "purchase_count",
		Description: "Updated description",
		EntityType:  "user",
	}
	err = fs.RegisterFeature(def2)
	if err != nil {
		t.Fatalf("RegisterFeature v2 error = %v", err)
	}

	// Check version incremented
	retrieved, _ := fs.GetFeature("purchase_count")
	if retrieved.Version != 2 {
		t.Errorf("Version = %d, want 2", retrieved.Version)
	}
}

func TestFeatureValidation(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	fs, _ := NewFeatureStore(db, DefaultFeatureStoreConfig())

	// Missing name
	err = fs.RegisterFeature(&FeatureDefinition{EntityType: "user"})
	if err == nil {
		t.Error("Expected error for missing name")
	}

	// Missing entity type
	err = fs.RegisterFeature(&FeatureDefinition{Name: "test"})
	if err == nil {
		t.Error("Expected error for missing entity type")
	}
}

func TestFeatureGroup(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	fs, _ := NewFeatureStore(db, DefaultFeatureStoreConfig())

	// Register some features first
	fs.RegisterFeature(&FeatureDefinition{Name: "feature1", EntityType: "user"})
	fs.RegisterFeature(&FeatureDefinition{Name: "feature2", EntityType: "user"})

	// Create group
	group := &FeatureGroup{
		Name:        "user_features",
		Description: "User profile features",
		EntityType:  "user",
		Features:    []string{"feature1", "feature2"},
	}

	err = fs.CreateGroup(group)
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}

	// Retrieve group
	retrieved, err := fs.GetGroup("user_features")
	if err != nil {
		t.Fatalf("GetGroup() error = %v", err)
	}

	if retrieved.Name != "user_features" {
		t.Errorf("Name = %s, want user_features", retrieved.Name)
	}
	if len(retrieved.Features) != 2 {
		t.Errorf("Features count = %d, want 2", len(retrieved.Features))
	}
}

func TestFeatureWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	fs, _ := NewFeatureStore(db, DefaultFeatureStoreConfig())

	// Register feature
	fs.RegisterFeature(&FeatureDefinition{
		Name:       "user_score",
		EntityType: "user",
		ValueType:  FeatureTypeFloat,
	})

	ctx := context.Background()

	// Write feature value
	fv := &FeatureValue{
		FeatureName: "user_score",
		EntityID:    "user123",
		Value:       85.5,
	}

	err = fs.WriteFeature(ctx, fv)
	if err != nil {
		t.Fatalf("WriteFeature() error = %v", err)
	}

	// Verify stats updated
	stats := fs.Stats()
	if stats.TotalWrites != 1 {
		t.Errorf("TotalWrites = %d, want 1", stats.TotalWrites)
	}
}

func TestFeatureRead(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	fs, _ := NewFeatureStore(db, DefaultFeatureStoreConfig())

	// Register and write
	fs.RegisterFeature(&FeatureDefinition{
		Name:       "user_balance",
		EntityType: "user",
		ValueType:  FeatureTypeFloat,
	})

	ctx := context.Background()
	fs.WriteFeature(ctx, &FeatureValue{
		FeatureName: "user_balance",
		EntityID:    "user456",
		Value:       1000.0,
	})

	// Read back
	fv, err := fs.GetFeatureValue(ctx, "user_balance", "user456")
	if err != nil {
		t.Fatalf("GetFeatureValue() error = %v", err)
	}

	if fv.Value.(float64) != 1000.0 {
		t.Errorf("Value = %v, want 1000.0", fv.Value)
	}
}

func TestFeatureVector(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	fs, _ := NewFeatureStore(db, DefaultFeatureStoreConfig())

	ctx := context.Background()

	// Register multiple features
	fs.RegisterFeature(&FeatureDefinition{Name: "f1", EntityType: "user"})
	fs.RegisterFeature(&FeatureDefinition{Name: "f2", EntityType: "user"})
	fs.RegisterFeature(&FeatureDefinition{Name: "f3", EntityType: "user"})

	// Write values
	fs.WriteFeature(ctx, &FeatureValue{FeatureName: "f1", EntityID: "e1", Value: 1.0})
	fs.WriteFeature(ctx, &FeatureValue{FeatureName: "f2", EntityID: "e1", Value: 2.0})
	fs.WriteFeature(ctx, &FeatureValue{FeatureName: "f3", EntityID: "e1", Value: 3.0})

	// Get vector
	vector, err := fs.GetFeatureVector(ctx, "e1", []string{"f1", "f2", "f3"})
	if err != nil {
		t.Fatalf("GetFeatureVector() error = %v", err)
	}

	if len(vector.Features) != 3 {
		t.Errorf("Features count = %d, want 3", len(vector.Features))
	}
	if vector.EntityID != "e1" {
		t.Errorf("EntityID = %s, want e1", vector.EntityID)
	}
}

func TestPointInTimeQuery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultFeatureStoreConfig()
	config.PointInTimeEnabled = true
	fs, _ := NewFeatureStore(db, config)

	ctx := context.Background()

	// Register feature
	fs.RegisterFeature(&FeatureDefinition{Name: "price", EntityType: "product"})

	// Write values at different times
	baseTime := time.Now().Add(-time.Hour).UnixNano()
	fs.WriteFeature(ctx, &FeatureValue{
		FeatureName: "price",
		EntityID:    "product1",
		Value:       100.0,
		EventTime:   baseTime,
	})

	fs.WriteFeature(ctx, &FeatureValue{
		FeatureName: "price",
		EntityID:    "product1",
		Value:       120.0,
		EventTime:   baseTime + int64(30*time.Minute),
	})

	// Query at specific time
	asOfTime := baseTime + int64(15*time.Minute)
	fv, err := fs.GetFeatureValueAsOf(ctx, "price", "product1", asOfTime)
	if err != nil {
		t.Fatalf("GetFeatureValueAsOf() error = %v", err)
	}

	// Should get the value at baseTime (100.0), not the later value
	if fv.Value.(float64) != 100.0 {
		t.Errorf("Value = %v, want 100.0 (point-in-time correct)", fv.Value)
	}
}

func TestFeatureSchema(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	fs, _ := NewFeatureStore(db, DefaultFeatureStoreConfig())

	minVal := 0.0
	maxVal := 100.0

	// Register feature with schema
	fs.RegisterFeature(&FeatureDefinition{
		Name:       "percentage",
		EntityType: "metric",
		Schema: &FeatureSchema{
			MinValue: &minVal,
			MaxValue: &maxVal,
			Nullable: false,
		},
	})

	ctx := context.Background()

	// Valid write
	err = fs.WriteFeature(ctx, &FeatureValue{
		FeatureName: "percentage",
		EntityID:    "m1",
		Value:       50.0,
	})
	if err != nil {
		t.Errorf("Valid write failed: %v", err)
	}

	// Invalid write (below min)
	err = fs.WriteFeature(ctx, &FeatureValue{
		FeatureName: "percentage",
		EntityID:    "m2",
		Value:       -10.0,
	})
	if err == nil {
		t.Error("Expected error for value below minimum")
	}

	// Invalid write (above max)
	err = fs.WriteFeature(ctx, &FeatureValue{
		FeatureName: "percentage",
		EntityID:    "m3",
		Value:       150.0,
	})
	if err == nil {
		t.Error("Expected error for value above maximum")
	}
}

func TestOnlineServing(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultFeatureStoreConfig()
	config.OnlineServingEnabled = true
	fs, _ := NewFeatureStore(db, config)

	ctx := context.Background()

	// Setup features
	fs.RegisterFeature(&FeatureDefinition{Name: "f1", EntityType: "user"})
	fs.RegisterFeature(&FeatureDefinition{Name: "f2", EntityType: "user"})

	fs.WriteFeature(ctx, &FeatureValue{FeatureName: "f1", EntityID: "u1", Value: 10.0})
	fs.WriteFeature(ctx, &FeatureValue{FeatureName: "f2", EntityID: "u1", Value: 20.0})
	fs.WriteFeature(ctx, &FeatureValue{FeatureName: "f1", EntityID: "u2", Value: 30.0})
	fs.WriteFeature(ctx, &FeatureValue{FeatureName: "f2", EntityID: "u2", Value: 40.0})

	// Online serving request
	req := &OnlineServingRequest{
		EntityIDs:    []string{"u1", "u2"},
		FeatureNames: []string{"f1", "f2"},
	}

	resp, err := fs.ServeOnline(ctx, req)
	if err != nil {
		t.Fatalf("ServeOnline() error = %v", err)
	}

	if len(resp.Vectors) != 2 {
		t.Errorf("Vectors count = %d, want 2", len(resp.Vectors))
	}
	if resp.Latency <= 0 {
		t.Error("Latency should be positive")
	}
}

func TestFeatureCache(t *testing.T) {
	cache := newFeatureCache(5 * time.Minute)

	fv := &FeatureValue{
		FeatureName: "test",
		EntityID:    "e1",
		Value:       42.0,
	}

	// Set
	cache.set("key1", fv)

	// Get
	retrieved, ok := cache.get("key1")
	if !ok {
		t.Error("Cache should hit")
	}
	if retrieved.Value != 42.0 {
		t.Errorf("Value = %v, want 42.0", retrieved.Value)
	}

	// Miss
	_, ok = cache.get("nonexistent")
	if ok {
		t.Error("Cache should miss for nonexistent key")
	}
}

func TestListFeatures(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	fs, _ := NewFeatureStore(db, DefaultFeatureStoreConfig())

	// Register features
	fs.RegisterFeature(&FeatureDefinition{Name: "zebra", EntityType: "animal"})
	fs.RegisterFeature(&FeatureDefinition{Name: "apple", EntityType: "fruit"})
	fs.RegisterFeature(&FeatureDefinition{Name: "banana", EntityType: "fruit"})

	features := fs.ListFeatures()

	if len(features) != 3 {
		t.Errorf("ListFeatures() count = %d, want 3", len(features))
	}

	// Should be sorted by name
	if features[0].Name != "apple" {
		t.Errorf("First feature = %s, want apple", features[0].Name)
	}
	if features[2].Name != "zebra" {
		t.Errorf("Last feature = %s, want zebra", features[2].Name)
	}
}

func TestStats(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	fs, _ := NewFeatureStore(db, DefaultFeatureStoreConfig())

	ctx := context.Background()

	// Register and write
	fs.RegisterFeature(&FeatureDefinition{Name: "stat_test", EntityType: "entity"})
	fs.WriteFeature(ctx, &FeatureValue{FeatureName: "stat_test", EntityID: "e1", Value: 1.0})
	fs.WriteFeature(ctx, &FeatureValue{FeatureName: "stat_test", EntityID: "e2", Value: 2.0})

	// Read
	fs.GetFeatureValue(ctx, "stat_test", "e1")
	fs.GetFeatureValue(ctx, "stat_test", "e1") // Cache hit

	stats := fs.Stats()

	if stats.FeatureCount != 1 {
		t.Errorf("FeatureCount = %d, want 1", stats.FeatureCount)
	}
	if stats.TotalWrites != 2 {
		t.Errorf("TotalWrites = %d, want 2", stats.TotalWrites)
	}
	if stats.TotalReads < 2 {
		t.Errorf("TotalReads = %d, want >= 2", stats.TotalReads)
	}
}

func TestComputeFeatureAgg(t *testing.T) {
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}

	tests := []struct {
		function string
		expected float64
	}{
		{"sum", 15.0},
		{"avg", 3.0},
		{"mean", 3.0},
		{"min", 1.0},
		{"max", 5.0},
		{"count", 5.0},
	}

	for _, tt := range tests {
		t.Run(tt.function, func(t *testing.T) {
			result := computeFeatureAgg(values, tt.function)
			if result != tt.expected {
				t.Errorf("computeFeatureAgg(%s) = %f, want %f", tt.function, result, tt.expected)
			}
		})
	}
}

func TestComputeDerived(t *testing.T) {
	values := map[string]float64{
		"a": 10.0,
		"b": 5.0,
	}
	sources := []string{"a", "b"}

	tests := []struct {
		operation string
		expected  float64
	}{
		{"add", 15.0},
		{"subtract", 5.0},
		{"multiply", 50.0},
		{"divide", 2.0},
		{"ratio", 2.0},
	}

	for _, tt := range tests {
		t.Run(tt.operation, func(t *testing.T) {
			result := computeDerived(values, sources, tt.operation)
			if result != tt.expected {
				t.Errorf("computeDerived(%s) = %f, want %f", tt.operation, result, tt.expected)
			}
		})
	}
}

func TestValueToFloat(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	fs, _ := NewFeatureStore(db, DefaultFeatureStoreConfig())

	tests := []struct {
		input    interface{}
		expected float64
	}{
		{1.5, 1.5},
		{float32(2.5), 2.5},
		{10, 10.0},
		{int64(20), 20.0},
		{true, 1.0},
		{false, 0.0},
		{"string", 0.0}, // Unsupported type
	}

	for _, tt := range tests {
		result := fs.valueToFloat(tt.input)
		if result != tt.expected {
			t.Errorf("valueToFloat(%v) = %f, want %f", tt.input, result, tt.expected)
		}
	}
}
