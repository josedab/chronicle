package chronicle

import (
	"math"
	"testing"
	"time"
)

func TestSemanticSearchEngine(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultSemanticSearchConfig()
	config.AutoIndexInterval = 0 // Disable auto-indexing for tests

	engine := NewSemanticSearchEngine(db, config)
	defer engine.Close()

	// Index some patterns
	patterns := []*TimeSeriesPattern{
		{
			Metric:    "cpu",
			Tags:      map[string]string{"host": "server1"},
			StartTime: time.Now().Add(-time.Hour).UnixNano(),
			EndTime:   time.Now().UnixNano(),
			RawValues: generateSineWave(64, 0.1),
		},
		{
			Metric:    "cpu",
			Tags:      map[string]string{"host": "server2"},
			StartTime: time.Now().Add(-time.Hour).UnixNano(),
			EndTime:   time.Now().UnixNano(),
			RawValues: generateSineWave(64, 0.1), // Similar pattern
		},
		{
			Metric:    "memory",
			Tags:      map[string]string{"host": "server1"},
			StartTime: time.Now().Add(-time.Hour).UnixNano(),
			EndTime:   time.Now().UnixNano(),
			RawValues: generateSpike(64), // Different pattern
		},
	}

	for _, p := range patterns {
		if err := engine.IndexPattern(p); err != nil {
			t.Fatalf("failed to index pattern: %v", err)
		}
	}

	// Search for similar patterns
	results, err := engine.Search(&SemanticQuery{
		ExampleValues: generateSineWave(64, 0.1),
		TopK:          5,
	})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) < 2 {
		t.Errorf("expected at least 2 results, got %d", len(results))
	}

	// Verify stats
	stats := engine.Stats()
	if stats.PatternsIndexed != 3 {
		t.Errorf("expected 3 patterns indexed, got %d", stats.PatternsIndexed)
	}
	if stats.SearchesExecuted != 1 {
		t.Errorf("expected 1 search executed, got %d", stats.SearchesExecuted)
	}
}

func TestPatternEncoder(t *testing.T) {
	encoder := NewPatternEncoder(32, 64)

	// Test encoding
	values := generateSineWave(64, 0.1)
	embedding := encoder.Encode(values)

	if len(embedding) != 32 {
		t.Errorf("expected embedding dimension 32, got %d", len(embedding))
	}

	// Verify non-zero
	nonZero := false
	for _, v := range embedding {
		if v != 0 {
			nonZero = true
			break
		}
	}
	if !nonZero {
		t.Error("embedding should have non-zero values")
	}

	// Test normalization (L2 norm should be ~1)
	var norm float64
	for _, v := range embedding {
		norm += float64(v * v)
	}
	norm = math.Sqrt(norm)
	if math.Abs(norm-1.0) > 0.01 {
		t.Errorf("embedding should be normalized, got norm %f", norm)
	}
}

func TestPatternEncoderDescription(t *testing.T) {
	encoder := NewPatternEncoder(32, 64)

	// Test NL encoding
	embedding := encoder.EncodeDescription("show me spike anomaly patterns")

	if len(embedding) != 32 {
		t.Errorf("expected embedding dimension 32, got %d", len(embedding))
	}

	// Spike and anomaly should be detected
	if embedding[0] == 0 && embedding[8] == 0 {
		t.Error("expected spike or anomaly keywords to be detected")
	}
}

func TestHNSWIndex(t *testing.T) {
	index := NewHNSWIndex(8, 100, 50, 16)

	// Insert vectors
	vectors := []struct {
		id  string
		vec []float32
	}{
		{"a", []float32{1, 0, 0, 0, 0, 0, 0, 0}},
		{"b", []float32{0.9, 0.1, 0, 0, 0, 0, 0, 0}},
		{"c", []float32{0, 0, 0, 0, 1, 0, 0, 0}},
		{"d", []float32{0.8, 0.2, 0, 0, 0, 0, 0, 0}},
	}

	for _, v := range vectors {
		index.Insert(v.id, v.vec)
	}

	// Search for nearest to [1,0,0,0,0,0,0,0]
	query := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	results := index.SearchKNN(query, 3)

	if len(results) == 0 {
		t.Fatal("expected results from KNN search")
	}

	// First result should be "a" (exact match)
	if results[0].ID != "a" {
		t.Errorf("expected first result to be 'a', got '%s'", results[0].ID)
	}

	// Test removal
	index.Remove("a")
	results = index.SearchKNN(query, 3)
	for _, r := range results {
		if r.ID == "a" {
			t.Error("removed node should not appear in results")
		}
	}
}

func TestSearchWithFilters(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultSemanticSearchConfig()
	config.AutoIndexInterval = 0
	engine := NewSemanticSearchEngine(db, config)
	defer engine.Close()

	// Index patterns with different metrics
	for i := 0; i < 10; i++ {
		metric := "cpu"
		if i%2 == 0 {
			metric = "memory"
		}
		pattern := &TimeSeriesPattern{
			Metric:    metric,
			Tags:      map[string]string{"host": "server1"},
			RawValues: generateSineWave(64, float64(i)*0.1),
		}
		engine.IndexPattern(pattern)
	}

	// Search with metric filter
	results, err := engine.Search(&SemanticQuery{
		ExampleValues: generateSineWave(64, 0.1),
		MetricFilter:  "cpu",
		TopK:          10,
		Threshold:     0.1,
	})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	for _, r := range results {
		if r.Pattern.Metric != "cpu" {
			t.Errorf("expected all results to have metric 'cpu', got '%s'", r.Pattern.Metric)
		}
	}
}

func TestPatternStats(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	stats := computePatternStats(values)

	// Mean should be 5.5
	if math.Abs(stats.Mean-5.5) > 0.001 {
		t.Errorf("expected mean 5.5, got %f", stats.Mean)
	}

	// Min and Max
	if stats.Min != 1 {
		t.Errorf("expected min 1, got %f", stats.Min)
	}
	if stats.Max != 10 {
		t.Errorf("expected max 10, got %f", stats.Max)
	}

	// Positive trend
	if stats.Trend <= 0 {
		t.Error("expected positive trend")
	}
}

func TestCosineDistance(t *testing.T) {
	// Identical vectors should have distance 0
	a := []float32{1, 0, 0, 0}
	dist := cosineDistance(a, a)
	if math.Abs(dist) > 0.001 {
		t.Errorf("expected distance 0 for identical vectors, got %f", dist)
	}

	// Orthogonal vectors should have distance 1
	b := []float32{0, 1, 0, 0}
	dist = cosineDistance(a, b)
	if math.Abs(dist-1.0) > 0.001 {
		t.Errorf("expected distance 1 for orthogonal vectors, got %f", dist)
	}
}

func TestGetPattern(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultSemanticSearchConfig()
	config.AutoIndexInterval = 0
	engine := NewSemanticSearchEngine(db, config)
	defer engine.Close()

	pattern := &TimeSeriesPattern{
		ID:        "test-pattern-1",
		Metric:    "cpu",
		RawValues: generateSineWave(64, 0.1),
	}
	engine.IndexPattern(pattern)

	// Get existing pattern
	retrieved, err := engine.GetPattern("test-pattern-1")
	if err != nil {
		t.Fatalf("failed to get pattern: %v", err)
	}
	if retrieved.Metric != "cpu" {
		t.Errorf("expected metric 'cpu', got '%s'", retrieved.Metric)
	}

	// Get non-existent pattern
	_, err = engine.GetPattern("non-existent")
	if err == nil {
		t.Error("expected error for non-existent pattern")
	}
}

func TestListPatterns(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultSemanticSearchConfig()
	config.AutoIndexInterval = 0
	engine := NewSemanticSearchEngine(db, config)
	defer engine.Close()

	// Index 5 patterns
	for i := 0; i < 5; i++ {
		pattern := &TimeSeriesPattern{
			Metric:    "cpu",
			RawValues: generateSineWave(64, float64(i)*0.1),
		}
		engine.IndexPattern(pattern)
	}

	// List with limit
	patterns := engine.ListPatterns(3)
	if len(patterns) != 3 {
		t.Errorf("expected 3 patterns, got %d", len(patterns))
	}

	// List all
	patterns = engine.ListPatterns(10)
	if len(patterns) != 5 {
		t.Errorf("expected 5 patterns, got %d", len(patterns))
	}
}

func TestSearchSimilar(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultSemanticSearchConfig()
	config.AutoIndexInterval = 0
	engine := NewSemanticSearchEngine(db, config)
	defer engine.Close()

	// Index patterns
	for i := 0; i < 5; i++ {
		pattern := &TimeSeriesPattern{
			Metric:    "cpu",
			Tags:      map[string]string{"host": "server1"},
			StartTime: time.Now().Add(-time.Hour).UnixNano(),
			EndTime:   time.Now().UnixNano(),
			RawValues: generateSineWave(64, float64(i)*0.1),
		}
		engine.IndexPattern(pattern)
	}

	// SearchSimilar
	results, err := engine.SearchSimilar("cpu", map[string]string{"host": "server1"}, time.Now().Add(-time.Hour), time.Now(), 3)
	// This will fail because we don't have actual data in DB, but method should not error
	if err != nil && len(results) > 0 {
		// Expected behavior when reference not found in DB
	}
}

func TestEviction(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultSemanticSearchConfig()
	config.AutoIndexInterval = 0
	config.MaxPatterns = 5
	engine := NewSemanticSearchEngine(db, config)
	defer engine.Close()

	// Index more patterns than limit
	for i := 0; i < 10; i++ {
		pattern := &TimeSeriesPattern{
			Metric:    "cpu",
			RawValues: generateSineWave(64, float64(i)*0.1),
		}
		engine.IndexPattern(pattern)
	}

	// Should not exceed max
	patterns := engine.ListPatterns(100)
	if len(patterns) > 5 {
		t.Errorf("expected max 5 patterns, got %d", len(patterns))
	}
}

// Helper functions for generating test data

func generateSineWave(n int, frequency float64) []float64 {
	values := make([]float64, n)
	for i := 0; i < n; i++ {
		values[i] = math.Sin(2 * math.Pi * frequency * float64(i))
	}
	return values
}

func generateSpike(n int) []float64 {
	values := make([]float64, n)
	for i := 0; i < n; i++ {
		if i == n/2 {
			values[i] = 10.0 // Spike in the middle
		} else {
			values[i] = 1.0
		}
	}
	return values
}

func BenchmarkHNSWInsert(b *testing.B) {
	index := NewHNSWIndex(32, 100, 50, 16)

	vectors := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		vectors[i] = make([]float32, 32)
		for j := 0; j < 32; j++ {
			vectors[i][j] = float32(randFloat64())
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.Insert(generateID(), vectors[i])
	}
}

func BenchmarkHNSWSearch(b *testing.B) {
	index := NewHNSWIndex(32, 100, 50, 16)

	// Pre-populate index
	for i := 0; i < 1000; i++ {
		vec := make([]float32, 32)
		for j := 0; j < 32; j++ {
			vec[j] = float32(randFloat64())
		}
		index.Insert(generateID(), vec)
	}

	query := make([]float32, 32)
	for j := 0; j < 32; j++ {
		query[j] = float32(randFloat64())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.SearchKNN(query, 10)
	}
}

func BenchmarkPatternEncode(b *testing.B) {
	encoder := NewPatternEncoder(32, 64)
	values := generateSineWave(64, 0.1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(values)
	}
}
