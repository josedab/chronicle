package chronicle

import (
	"fmt"
	"testing"
	"time"
)

func TestHNSWGraph_InsertSearch(t *testing.T) {
	cfg := DefaultHybridIndexConfig()
	cfg.HNSWMaxConnections = 4
	cfg.HNSWEfConstruction = 16
	cfg.HNSWEfSearch = 16

	g := newHNSWGraph(&cfg)

	vectors := [][]float64{
		{1, 0, 0, 0},
		{0, 1, 0, 0},
		{0, 0, 1, 0},
		{0.9, 0.1, 0, 0},
	}
	for i, v := range vectors {
		g.insert(fmt.Sprintf("v%d", i), v, int64(i))
	}

	results := g.search([]float64{1, 0, 0, 0}, 2, 16)
	if len(results) == 0 {
		t.Fatal("expected search results")
	}
	// The closest to [1,0,0,0] should be v0 or v3.
	if results[0].ID != "v0" && results[0].ID != "v3" {
		t.Errorf("expected v0 or v3 as closest, got %s", results[0].ID)
	}
}

func TestTemporalPartitionedIndex_InsertSearch(t *testing.T) {
	cfg := DefaultHybridIndexConfig()
	cfg.TemporalBucketDuration = time.Hour

	idx := NewTemporalPartitionedIndex(cfg)

	now := time.Now().UnixNano()
	points := []*HybridPoint{
		{ID: "p1", Vector: []float64{1, 0, 0, 0}, Timestamp: now, Metric: "cpu", Value: 42},
		{ID: "p2", Vector: []float64{0, 1, 0, 0}, Timestamp: now, Metric: "cpu", Value: 55},
		{ID: "p3", Vector: []float64{0.9, 0.1, 0, 0}, Timestamp: now, Metric: "mem", Value: 80},
	}

	for _, p := range points {
		if err := idx.Insert(p); err != nil {
			t.Fatalf("Insert %s: %v", p.ID, err)
		}
	}

	stats := idx.Stats()
	if stats.TotalPoints != 3 {
		t.Errorf("expected 3 total points, got %d", stats.TotalPoints)
	}

	results, err := idx.Search(&HybridSearchQuery{
		Vector: []float64{1, 0, 0, 0},
		K:      2,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected search results")
	}
	if len(results) > 2 {
		t.Errorf("expected at most 2 results, got %d", len(results))
	}

	t.Run("MetricFilter", func(t *testing.T) {
		results, err := idx.Search(&HybridSearchQuery{
			Vector:       []float64{1, 0, 0, 0},
			K:            10,
			MetricFilter: "cpu",
		})
		if err != nil {
			t.Fatalf("Search with filter: %v", err)
		}
		for _, r := range results {
			if r.Metric != "cpu" {
				t.Errorf("expected metric 'cpu', got %q", r.Metric)
			}
		}
	})
}

func TestHybridQueryPlanner_Strategies(t *testing.T) {
	cfg := DefaultHybridIndexConfig()
	cfg.TemporalBucketDuration = time.Hour

	idx := NewTemporalPartitionedIndex(cfg)

	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		_ = idx.Insert(&HybridPoint{
			ID:        fmt.Sprintf("p%d", i),
			Vector:    []float64{float64(i), 0, 0, 0},
			Timestamp: now + int64(i)*time.Hour.Nanoseconds(),
			Metric:    "cpu",
		})
	}

	planner := NewHybridQueryPlanner(idx)

	t.Run("VectorOnly", func(t *testing.T) {
		plan := planner.Plan(&HybridSearchQuery{
			Vector: []float64{1, 0, 0, 0},
			K:      5,
		})
		if plan.Strategy != StrategyVectorOnly {
			t.Errorf("expected StrategyVectorOnly, got %d", plan.Strategy)
		}
	})

	t.Run("TemporalOnly", func(t *testing.T) {
		plan := planner.Plan(&HybridSearchQuery{
			K:         5,
			StartTime: now,
			EndTime:   now + time.Hour.Nanoseconds(),
		})
		if plan.Strategy != StrategyTemporalOnly {
			t.Errorf("expected StrategyTemporalOnly, got %d", plan.Strategy)
		}
	})
}

func TestPatternLibrary_BuiltinPatterns(t *testing.T) {
	lib := NewPatternLibrary()

	patterns := builtinPatterns()
	if len(patterns) == 0 {
		t.Fatal("expected builtin patterns")
	}

	// Check that all builtin patterns are loaded.
	for _, p := range patterns {
		if p.Name == "" {
			t.Errorf("expected non-empty pattern name")
		}
		if len(p.Vector) == 0 {
			t.Errorf("expected non-empty pattern vector for %s", p.Name)
		}
	}

	// Register a custom pattern.
	lib.RegisterPattern(&HybridTimeSeriesPattern{
		Name:     "CustomSpike",
		Vector:   []float64{0, 0, 0, 1, 0, 0, 0, 0},
		Category: "custom",
	})
}

func TestPatternLibrary_FindSimilar(t *testing.T) {
	lib := NewPatternLibrary()

	// Spike pattern: [0, 0, 0, 0, 1, 0, 0, 0]
	spikeData := []float64{0, 0, 0, 0, 1, 0, 0, 0}
	matches := lib.FindSimilarPatterns(spikeData, 3)
	if len(matches) == 0 {
		t.Fatal("expected at least one match")
	}
	if matches[0].Pattern.Name != "Spike" {
		t.Errorf("expected 'Spike' as top match, got %q", matches[0].Pattern.Name)
	}
	if matches[0].Similarity <= 0 {
		t.Errorf("expected positive similarity, got %f", matches[0].Similarity)
	}

	t.Run("EmptyData", func(t *testing.T) {
		matches := lib.FindSimilarPatterns(nil, 3)
		if len(matches) != 0 {
			t.Errorf("expected no matches for empty data")
		}
	})
}

func TestHybridDistanceFunctions(t *testing.T) {
	a := []float64{1, 0, 0, 0}
	b := []float64{0, 1, 0, 0}
	same := []float64{1, 0, 0, 0}

	t.Run("Cosine", func(t *testing.T) {
		d := hybridCosineDistance(a, b)
		if d <= 0 {
			t.Errorf("expected positive cosine distance for orthogonal vectors, got %f", d)
		}
		dSame := hybridCosineDistance(a, same)
		if dSame > 1e-9 {
			t.Errorf("expected ~0 cosine distance for identical vectors, got %f", dSame)
		}
	})

	t.Run("Euclidean", func(t *testing.T) {
		d := hybridEuclideanDistance(a, b)
		if d <= 0 {
			t.Errorf("expected positive euclidean distance, got %f", d)
		}
		dSame := hybridEuclideanDistance(a, same)
		if dSame > 1e-9 {
			t.Errorf("expected ~0 euclidean distance for identical vectors, got %f", dSame)
		}
	})

	t.Run("DotProduct", func(t *testing.T) {
		d := hybridDotProductDistance(a, b)
		// Orthogonal vectors: dot product = 0, negated = 0.
		if d != 0 {
			t.Errorf("expected 0 dot product distance for orthogonal vectors, got %f", d)
		}
		dSame := hybridDotProductDistance(a, same)
		// Same vector: dot = 1, negated = -1 (lower = more similar).
		if dSame >= 0 {
			t.Errorf("expected negative dot product distance for identical vectors, got %f", dSame)
		}
	})
}
