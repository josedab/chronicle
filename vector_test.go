package chronicle

import (
	"math"
	"testing"
	"time"
)

func TestVectorStore_Write(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	vs := NewVectorStore(db, DefaultVectorConfig())

	err := vs.Write(VectorPoint{
		Metric:    "embeddings",
		Tags:      map[string]string{"model": "bert"},
		Vector:    []float32{0.1, 0.2, 0.3, 0.4},
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	stats := vs.Stats()
	if stats.SeriesCount != 1 {
		t.Errorf("expected 1 series, got %d", stats.SeriesCount)
	}
	if stats.VectorCount != 1 {
		t.Errorf("expected 1 vector, got %d", stats.VectorCount)
	}
}

func TestVectorStore_WriteValidation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	vs := NewVectorStore(db, DefaultVectorConfig())

	// Empty metric
	err := vs.Write(VectorPoint{Vector: []float32{1, 2, 3}})
	if err == nil {
		t.Error("expected error for empty metric")
	}

	// Empty vector
	err = vs.Write(VectorPoint{Metric: "test"})
	if err == nil {
		t.Error("expected error for empty vector")
	}

	// Dimension too large
	cfg := DefaultVectorConfig()
	cfg.MaxDimensions = 10
	vs = NewVectorStore(db, cfg)
	err = vs.Write(VectorPoint{
		Metric: "test",
		Vector: make([]float32, 100),
	})
	if err == nil {
		t.Error("expected error for dimension exceeding max")
	}
}

func TestVectorStore_DimensionConsistency(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	vs := NewVectorStore(db, DefaultVectorConfig())

	// First write establishes dimension
	vs.Write(VectorPoint{
		Metric: "test",
		Tags:   map[string]string{"k": "v"},
		Vector: []float32{1, 2, 3},
	})

	// Second write with different dimension should fail
	err := vs.Write(VectorPoint{
		Metric: "test",
		Tags:   map[string]string{"k": "v"},
		Vector: []float32{1, 2, 3, 4, 5},
	})
	if err == nil {
		t.Error("expected error for dimension mismatch")
	}
}

func TestVectorStore_Search(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	vs := NewVectorStore(db, DefaultVectorConfig())

	// Insert test vectors
	vectors := [][]float32{
		{1, 0, 0, 0},
		{0, 1, 0, 0},
		{0, 0, 1, 0},
		{0.9, 0.1, 0, 0}, // Similar to first
	}

	for i, v := range vectors {
		vs.Write(VectorPoint{
			ID:        string(rune('A' + i)),
			Metric:    "embeddings",
			Vector:    v,
			Timestamp: time.Now().UnixNano() + int64(i),
		})
	}

	// Search for vectors similar to [1, 0, 0, 0]
	results, err := vs.Search([]float32{1, 0, 0, 0}, 2, "", nil)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// First result should be exact match or very similar
	if results[0].Score < 0.9 {
		t.Errorf("expected high score for first result, got %f", results[0].Score)
	}
}

func TestVectorStore_SearchWithFilter(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	vs := NewVectorStore(db, DefaultVectorConfig())

	// Insert vectors with different metrics
	vs.Write(VectorPoint{
		Metric: "bert",
		Tags:   map[string]string{"type": "text"},
		Vector: []float32{1, 0, 0},
	})
	vs.Write(VectorPoint{
		Metric: "clip",
		Tags:   map[string]string{"type": "image"},
		Vector: []float32{0, 1, 0},
	})
	vs.Write(VectorPoint{
		Metric: "bert",
		Tags:   map[string]string{"type": "code"},
		Vector: []float32{0.5, 0.5, 0},
	})

	// Search only bert embeddings
	results, _ := vs.Search([]float32{1, 0, 0}, 10, "bert", nil)
	if len(results) != 2 {
		t.Errorf("expected 2 bert results, got %d", len(results))
	}

	// Search with tag filter
	results, _ = vs.Search([]float32{1, 0, 0}, 10, "", map[string]string{"type": "text"})
	if len(results) != 1 {
		t.Errorf("expected 1 text result, got %d", len(results))
	}
}

func TestVectorStore_Query(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	vs := NewVectorStore(db, DefaultVectorConfig())

	baseTime := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		vs.Write(VectorPoint{
			Metric:    "test",
			Vector:    []float32{float32(i), 0, 0},
			Timestamp: baseTime + int64(i)*int64(time.Hour),
		})
	}

	// Query time range
	results, err := vs.Query("test", nil, baseTime+int64(time.Hour), baseTime+3*int64(time.Hour))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 results in range, got %d", len(results))
	}
}

func TestVectorStore_DistanceMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Test cosine
	cfg := DefaultVectorConfig()
	cfg.DistanceMetric = DistanceCosine
	vs := NewVectorStore(db, cfg)

	vs.Write(VectorPoint{Metric: "test", Vector: []float32{1, 0}})
	vs.Write(VectorPoint{Metric: "test", Vector: []float32{0, 1}})

	results, _ := vs.Search([]float32{1, 0}, 2, "", nil)
	if results[0].Distance > results[1].Distance {
		t.Error("cosine: identical vector should have smaller distance")
	}

	// Test euclidean
	cfg.DistanceMetric = DistanceEuclidean
	cfg.NormalizeVectors = false
	vs = NewVectorStore(db, cfg)

	vs.Write(VectorPoint{Metric: "test", Vector: []float32{0, 0}})
	vs.Write(VectorPoint{Metric: "test", Vector: []float32{3, 4}})

	results, _ = vs.Search([]float32{0, 0}, 2, "", nil)
	// Distance to [3,4] should be 5
	if math.Abs(results[1].Distance-5.0) > 0.01 {
		t.Errorf("expected euclidean distance 5, got %f", results[1].Distance)
	}
}

func TestVectorStore_Normalization(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultVectorConfig()
	cfg.NormalizeVectors = true
	vs := NewVectorStore(db, cfg)

	vs.Write(VectorPoint{
		Metric: "test",
		Vector: []float32{3, 4}, // Magnitude 5
	})

	results, _ := vs.Query("test", nil, 0, time.Now().UnixNano()+int64(time.Hour))
	if len(results) != 1 {
		t.Fatal("expected 1 result")
	}

	// Check normalization
	vec := results[0].Vector
	mag := float64(vec[0])*float64(vec[0]) + float64(vec[1])*float64(vec[1])
	if math.Abs(mag-1.0) > 0.001 {
		t.Errorf("expected normalized vector (magnitude 1), got magnitude %f", math.Sqrt(mag))
	}
}

func TestEncodeDecodeVector(t *testing.T) {
	original := []float32{1.5, -2.5, 3.14159, 0, -0.001}

	encoded := EncodeVector(original)
	decoded, err := DecodeVector(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(decoded) != len(original) {
		t.Fatalf("length mismatch: %d vs %d", len(decoded), len(original))
	}

	for i := range original {
		if decoded[i] != original[i] {
			t.Errorf("value mismatch at %d: %f vs %f", i, decoded[i], original[i])
		}
	}
}

func TestVectorMathFunctions(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}
	c := []float32{1, 0, 0}

	// Cosine similarity
	simAB := cosineSimilarity(a, b)
	if simAB != 0 {
		t.Errorf("orthogonal vectors should have 0 cosine similarity, got %f", simAB)
	}

	simAC := cosineSimilarity(a, c)
	if simAC != 1 {
		t.Errorf("identical vectors should have 1 cosine similarity, got %f", simAC)
	}

	// Euclidean distance
	distAB := euclideanDistance(a, b)
	expected := math.Sqrt(2)
	if math.Abs(distAB-expected) > 0.001 {
		t.Errorf("expected euclidean distance %f, got %f", expected, distAB)
	}

	distAC := euclideanDistance(a, c)
	if distAC != 0 {
		t.Errorf("identical vectors should have 0 distance, got %f", distAC)
	}

	// Dot product
	dotAB := dotProduct(a, b)
	if dotAB != 0 {
		t.Errorf("orthogonal vectors should have 0 dot product, got %f", dotAB)
	}
}

func TestVectorStore_MaxVectorsLimit(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultVectorConfig()
	cfg.MaxVectorsPerSeries = 5
	vs := NewVectorStore(db, cfg)

	// Write more than max
	for i := 0; i < 10; i++ {
		vs.Write(VectorPoint{
			Metric:    "test",
			Vector:    []float32{float32(i)},
			Timestamp: int64(i),
		})
	}

	stats := vs.Stats()
	if stats.VectorCount > 5 {
		t.Errorf("expected max 5 vectors, got %d", stats.VectorCount)
	}
}
