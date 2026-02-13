package chronicle

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestTSRAGEmbedPattern(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rag := NewTSRAGEngine(db, DefaultTSRAGConfig())

	points := generateTestPoints("cpu", 100, time.Now().Add(-time.Hour), time.Minute)
	emb, err := rag.EmbedPattern("cpu", points)
	if err != nil {
		t.Fatalf("embed failed: %v", err)
	}

	if emb.Metric != "cpu" {
		t.Errorf("expected metric cpu, got %s", emb.Metric)
	}
	if len(emb.Vector) != 64 {
		t.Errorf("expected 64-dim vector, got %d", len(emb.Vector))
	}
	if rag.EmbeddingCount() != 1 {
		t.Errorf("expected 1 embedding, got %d", rag.EmbeddingCount())
	}
}

func TestTSRAGRetrieve(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultTSRAGConfig()
	config.SimilarityThreshold = 0.0 // Accept all for testing
	rag := NewTSRAGEngine(db, config)

	now := time.Now()
	points1 := generateTestPoints("cpu", 50, now.Add(-2*time.Hour), time.Minute)
	points2 := generateTestPoints("mem", 50, now.Add(-2*time.Hour), time.Minute)

	rag.EmbedPattern("cpu", points1)
	rag.EmbedPattern("mem", points2)

	queryVector := make([]float64, 64)
	queryVector[0] = 1.0

	evidence := rag.Retrieve(queryVector, 5)
	if len(evidence) == 0 {
		t.Fatal("expected at least one evidence result")
	}
}

func TestTSRAGAsk(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rag := NewTSRAGEngine(db, DefaultTSRAGConfig())

	resp, err := rag.Ask(context.Background(), RAGQuery{
		Question: "Why did latency spike?",
	})
	if err != nil {
		t.Fatalf("ask failed: %v", err)
	}

	if resp.Answer == "" {
		t.Error("expected non-empty answer")
	}
	if resp.ConversationID == "" {
		t.Error("expected conversation ID")
	}
}

func TestTSRAGConversation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rag := NewTSRAGEngine(db, DefaultTSRAGConfig())

	resp1, _ := rag.Ask(context.Background(), RAGQuery{
		Question: "What happened with CPU?",
	})

	resp2, _ := rag.Ask(context.Background(), RAGQuery{
		Question:       "Tell me more",
		ConversationID: resp1.ConversationID,
	})

	if resp2.ConversationID != resp1.ConversationID {
		t.Error("expected same conversation ID")
	}
}

func TestPatternFeatureExtraction(t *testing.T) {
	points := []Point{
		{Value: 1.0}, {Value: 2.0}, {Value: 3.0}, {Value: 4.0}, {Value: 5.0},
	}
	features := extractPatternFeatures(points)

	if features.Mean != 3.0 {
		t.Errorf("expected mean 3.0, got %f", features.Mean)
	}
	if features.Min != 1.0 {
		t.Errorf("expected min 1.0, got %f", features.Min)
	}
	if features.Max != 5.0 {
		t.Errorf("expected max 5.0, got %f", features.Max)
	}
	if features.Trend <= 0 {
		t.Errorf("expected positive trend, got %f", features.Trend)
	}
	if features.PointCount != 5 {
		t.Errorf("expected 5 points, got %d", features.PointCount)
	}
}

func TestCosineSimilarity(t *testing.T) {
	a := []float64{1, 0, 0}
	b := []float64{1, 0, 0}
	sim := ragCosineSimilarity(a, b)
	if math.Abs(sim-1.0) > 0.001 {
		t.Errorf("expected similarity 1.0, got %f", sim)
	}

	c := []float64{0, 1, 0}
	sim2 := ragCosineSimilarity(a, c)
	if math.Abs(sim2) > 0.001 {
		t.Errorf("expected similarity 0.0, got %f", sim2)
	}
}

func TestQuestionIntentAnalysis(t *testing.T) {
	tests := []struct {
		question string
		expected string
	}{
		{"Why did latency spike at 3pm?", "anomaly_explanation"},
		{"What is the trend over time?", "trend_analysis"},
		{"Compare CPU and memory", "comparison"},
		{"What will happen next?", "general"},
		{"Predict tomorrow's load", "forecasting"},
	}

	for _, tt := range tests {
		intent := analyzeQuestionIntent(tt.question)
		if intent.Type != tt.expected {
			t.Errorf("question %q: expected %s, got %s", tt.question, tt.expected, intent.Type)
		}
	}
}

func TestCalculateEntropy(t *testing.T) {
	// All same values = 0 entropy
	uniform := []float64{5, 5, 5, 5}
	e := calculateEntropy(uniform, 4)
	if e != 0 {
		t.Errorf("expected 0 entropy for constant, got %f", e)
	}

	// Spread values = positive entropy
	spread := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	e2 := calculateEntropy(spread, 4)
	if e2 <= 0 {
		t.Errorf("expected positive entropy for spread, got %f", e2)
	}
}

func TestTSRAGStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rag := NewTSRAGEngine(db, DefaultTSRAGConfig())
	stats := rag.Stats()
	if stats.QueriesProcessed != 0 {
		t.Errorf("expected 0 queries, got %d", stats.QueriesProcessed)
	}

	rag.Ask(context.Background(), RAGQuery{Question: "test"})

	stats = rag.Stats()
	if stats.QueriesProcessed != 1 {
		t.Errorf("expected 1 query, got %d", stats.QueriesProcessed)
	}
}

func generateTestPoints(metric string, count int, start time.Time, interval time.Duration) []Point {
	points := make([]Point, count)
	for i := 0; i < count; i++ {
		points[i] = Point{
			Metric:    metric,
			Value:     float64(i) + 10.0,
			Timestamp: start.Add(time.Duration(i) * interval).UnixNano(),
		}
	}
	return points
}
