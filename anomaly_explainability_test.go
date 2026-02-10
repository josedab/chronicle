package chronicle

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNewAnomalyExplainabilityEngine(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyExplainabilityConfig()
	engine := NewAnomalyExplainabilityEngine(db, config)

	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.explanations == nil {
		t.Fatal("expected non-nil explanations map")
	}
	if engine.cache == nil {
		t.Fatal("expected non-nil cache map")
	}
	if engine.promptTemplates == nil {
		t.Fatal("expected non-nil prompt templates")
	}
	if config.LLMProvider != ProviderLocal {
		t.Errorf("expected ProviderLocal, got %s", config.LLMProvider)
	}
	if config.ExplanationDepth != ExplainDetailed {
		t.Errorf("expected ExplainDetailed, got %d", config.ExplanationDepth)
	}
	if !config.CacheExplanations {
		t.Error("expected CacheExplanations to be true")
	}
	if config.MaxRecommendations != 5 {
		t.Errorf("expected MaxRecommendations 5, got %d", config.MaxRecommendations)
	}
	if config.CacheTTL != 10*time.Minute {
		t.Errorf("expected CacheTTL 10m, got %v", config.CacheTTL)
	}
}

func TestAnomalyExplainabilityExplain(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyExplainabilityConfig()
	engine := NewAnomalyExplainabilityEngine(db, config)

	now := time.Now()
	expl, err := engine.ExplainAnomaly("cpu_usage", 95.0, now)
	if err != nil {
		t.Fatalf("ExplainAnomaly failed: %v", err)
	}
	if expl == nil {
		t.Fatal("expected non-nil explanation")
	}
	if expl.Metric != "cpu_usage" {
		t.Errorf("expected metric cpu_usage, got %s", expl.Metric)
	}
	if expl.AnomalyID == "" {
		t.Error("expected non-empty anomaly ID")
	}
	if expl.Explanation == "" {
		t.Error("expected non-empty explanation text")
	}
	if expl.ModelUsed != "rule-based" {
		t.Errorf("expected model rule-based, got %s", expl.ModelUsed)
	}
	if expl.GeneratedAt.IsZero() {
		t.Error("expected non-zero GeneratedAt")
	}
	if expl.Severity < 1 || expl.Severity > 10 {
		t.Errorf("expected severity 1-10, got %d", expl.Severity)
	}
	if expl.Confidence <= 0 {
		t.Error("expected positive confidence")
	}

	// Error case: empty metric
	_, err = engine.ExplainAnomaly("", 0, now)
	if err == nil {
		t.Error("expected error for empty metric")
	}
}

func TestAnomalyExplainabilityWithContext(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyExplainabilityConfig()
	engine := NewAnomalyExplainabilityEngine(db, config)

	ctx := ExplainabilityContext{
		AnomalyMetric:  "request_latency",
		AnomalyValue:   250.0,
		AnomalyTime:    time.Now(),
		BaselineMean:   50.0,
		BaselineStddev: 10.0,
		RelatedMetricValues: map[string]float64{
			"cpu_usage":    85.0,
			"memory_usage": 72.0,
		},
		RecentTrend: "increasing",
	}

	expl, err := engine.ExplainWithContext(ctx)
	if err != nil {
		t.Fatalf("ExplainWithContext failed: %v", err)
	}
	if expl == nil {
		t.Fatal("expected non-nil explanation")
	}
	if expl.Metric != "request_latency" {
		t.Errorf("expected metric request_latency, got %s", expl.Metric)
	}
	if expl.Severity < 1 || expl.Severity > 10 {
		t.Errorf("expected severity 1-10, got %d", expl.Severity)
	}
	// 250 is 20 sigma above mean 50 with stddev 10, so severity should be 10
	if expl.Severity != 10 {
		t.Errorf("expected severity 10 for extreme anomaly, got %d", expl.Severity)
	}
	if expl.Confidence <= 0.5 {
		t.Errorf("expected confidence > 0.5 with full context, got %f", expl.Confidence)
	}
	if len(expl.RecommendedActions) == 0 {
		t.Error("expected at least one recommendation")
	}
}

func TestAnomalyExplainabilityContributingFactors(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyExplainabilityConfig()
	engine := NewAnomalyExplainabilityEngine(db, config)

	factors := engine.GetContributingFactors("cpu_usage", time.Now())
	if len(factors) == 0 {
		t.Fatal("expected at least one contributing factor")
	}

	// Should be sorted by impact score descending
	for i := 1; i < len(factors); i++ {
		if factors[i].ImpactScore > factors[i-1].ImpactScore {
			t.Error("expected factors sorted by impact score descending")
			break
		}
	}

	first := factors[0]
	if first.Metric == "" {
		t.Error("expected non-empty metric")
	}
	if first.Direction != "up" && first.Direction != "down" {
		t.Errorf("expected direction up or down, got %s", first.Direction)
	}
	if first.Description == "" {
		t.Error("expected non-empty description")
	}
}

func TestAnomalyExplainabilityHistoricalComparisons(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyExplainabilityConfig()
	engine := NewAnomalyExplainabilityEngine(db, config)

	comparisons := engine.GetHistoricalComparisons("cpu_usage", 95.0)
	if len(comparisons) == 0 {
		t.Fatal("expected at least one historical comparison")
	}

	// Should be sorted by similarity score descending
	for i := 1; i < len(comparisons); i++ {
		if comparisons[i].SimilarityScore > comparisons[i-1].SimilarityScore {
			t.Error("expected comparisons sorted by similarity score descending")
			break
		}
	}

	first := comparisons[0]
	if first.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}
	if first.Context == "" {
		t.Error("expected non-empty context")
	}
	if first.SimilarityScore <= 0 || first.SimilarityScore > 1 {
		t.Errorf("expected similarity score in (0,1], got %f", first.SimilarityScore)
	}
}

func TestAnomalyExplainabilityRecommendations(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyExplainabilityConfig()
	engine := NewAnomalyExplainabilityEngine(db, config)

	// Generate an explanation first
	ctx := ExplainabilityContext{
		AnomalyMetric:  "cpu_usage",
		AnomalyValue:   99.0,
		AnomalyTime:    time.Now(),
		BaselineMean:   40.0,
		BaselineStddev: 5.0,
	}
	expl, err := engine.ExplainWithContext(ctx)
	if err != nil {
		t.Fatalf("ExplainWithContext failed: %v", err)
	}

	recs := engine.GetRecommendations(expl.AnomalyID)
	if len(recs) == 0 {
		t.Fatal("expected at least one recommendation")
	}

	// Check priority ordering
	for i := 1; i < len(recs); i++ {
		if recs[i].Priority < recs[i-1].Priority {
			t.Error("expected recommendations sorted by priority ascending")
			break
		}
	}

	first := recs[0]
	if first.Action == "" {
		t.Error("expected non-empty action")
	}
	if first.Rationale == "" {
		t.Error("expected non-empty rationale")
	}
	if first.Priority < 1 || first.Priority > 5 {
		t.Errorf("expected priority 1-5, got %d", first.Priority)
	}

	// Nonexistent anomaly
	nilRecs := engine.GetRecommendations("nonexistent")
	if nilRecs != nil {
		t.Error("expected nil for nonexistent anomaly")
	}
}

func TestAnomalyExplainabilityBuildPrompt(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ctx := ExplainabilityContext{
		AnomalyMetric:  "cpu_usage",
		AnomalyValue:   95.0,
		AnomalyTime:    time.Now(),
		BaselineMean:   50.0,
		BaselineStddev: 10.0,
		RecentTrend:    "increasing",
		RelatedMetricValues: map[string]float64{
			"memory": 80.0,
		},
	}

	// Test brief prompt
	briefConfig := DefaultAnomalyExplainabilityConfig()
	briefConfig.ExplanationDepth = ExplainBrief
	briefEngine := NewAnomalyExplainabilityEngine(db, briefConfig)
	briefPrompt := briefEngine.BuildPrompt(ctx)
	if briefPrompt == "" {
		t.Error("expected non-empty brief prompt")
	}
	if len(briefPrompt) == 0 {
		t.Error("expected brief prompt to contain content")
	}

	// Test detailed prompt
	detailedConfig := DefaultAnomalyExplainabilityConfig()
	detailedConfig.ExplanationDepth = ExplainDetailed
	detailedEngine := NewAnomalyExplainabilityEngine(db, detailedConfig)
	detailedPrompt := detailedEngine.BuildPrompt(ctx)
	if detailedPrompt == "" {
		t.Error("expected non-empty detailed prompt")
	}
	if len(detailedPrompt) <= len(briefPrompt) {
		t.Error("expected detailed prompt to be longer than brief")
	}

	// Test expert prompt
	expertConfig := DefaultAnomalyExplainabilityConfig()
	expertConfig.ExplanationDepth = ExplainExpert
	expertEngine := NewAnomalyExplainabilityEngine(db, expertConfig)
	expertPrompt := expertEngine.BuildPrompt(ctx)
	if expertPrompt == "" {
		t.Error("expected non-empty expert prompt")
	}
	if len(expertPrompt) <= len(detailedPrompt) {
		t.Error("expected expert prompt to be longer than detailed")
	}
}

func TestAnomalyExplainabilityLocalExplanation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyExplainabilityConfig()
	config.ExplanationDepth = ExplainExpert
	engine := NewAnomalyExplainabilityEngine(db, config)

	ctx := ExplainabilityContext{
		AnomalyMetric:  "cpu_usage",
		AnomalyValue:   95.0,
		AnomalyTime:    time.Now(),
		BaselineMean:   50.0,
		BaselineStddev: 10.0,
		RecentTrend:    "increasing",
		RelatedMetricValues: map[string]float64{
			"memory": 80.0,
		},
	}

	explanation := engine.GenerateLocalExplanation(ctx)
	if explanation == "" {
		t.Fatal("expected non-empty explanation")
	}

	// Should contain metric name
	if !strings.Contains(explanation, "cpu_usage") {
		t.Error("expected explanation to mention metric name")
	}

	// Should mention deviation
	if !strings.Contains(explanation, "standard deviations") {
		t.Error("expected explanation to mention standard deviations")
	}

	// Expert depth should include additional analysis
	if !strings.Contains(explanation, "Expert analysis") {
		t.Error("expected expert analysis section")
	}

	// Test with zero stddev (no deviation info)
	ctxZero := ExplainabilityContext{
		AnomalyMetric: "test_metric",
		AnomalyValue:  10.0,
	}
	explZero := engine.GenerateLocalExplanation(ctxZero)
	if explZero == "" {
		t.Error("expected non-empty explanation even with zero stddev")
	}
}

func TestAnomalyExplainabilityCache(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyExplainabilityConfig()
	config.CacheExplanations = true
	config.CacheTTL = 5 * time.Minute
	engine := NewAnomalyExplainabilityEngine(db, config)

	now := time.Now()

	// First call should miss cache
	expl1, err := engine.ExplainAnomaly("cpu_usage", 95.0, now)
	if err != nil {
		t.Fatalf("ExplainAnomaly failed: %v", err)
	}

	stats1 := engine.Stats()
	if stats1.CacheMisses != 1 {
		t.Errorf("expected 1 cache miss, got %d", stats1.CacheMisses)
	}

	// Second call with same params should hit cache
	expl2, err := engine.ExplainAnomaly("cpu_usage", 95.0, now)
	if err != nil {
		t.Fatalf("ExplainAnomaly failed: %v", err)
	}

	stats2 := engine.Stats()
	if stats2.CacheHits != 1 {
		t.Errorf("expected 1 cache hit, got %d", stats2.CacheHits)
	}

	// Cached result should be the same
	if expl1.AnomalyID != expl2.AnomalyID {
		t.Error("expected same explanation from cache")
	}

	// Different params should miss cache
	_, err = engine.ExplainAnomaly("cpu_usage", 50.0, now)
	if err != nil {
		t.Fatalf("ExplainAnomaly failed: %v", err)
	}

	stats3 := engine.Stats()
	if stats3.CacheMisses != 2 {
		t.Errorf("expected 2 cache misses, got %d", stats3.CacheMisses)
	}
}

func TestAnomalyExplainabilityStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyExplainabilityConfig()
	engine := NewAnomalyExplainabilityEngine(db, config)

	// Initial stats should be zero
	stats := engine.Stats()
	if stats.TotalExplanations != 0 {
		t.Errorf("expected 0 total explanations, got %d", stats.TotalExplanations)
	}

	// Generate some explanations
	now := time.Now()
	for i := 0; i < 3; i++ {
		_, err := engine.ExplainAnomaly("metric_"+string(rune('a'+i)), float64(i*10+50), now.Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatalf("ExplainAnomaly failed: %v", err)
		}
	}

	stats = engine.Stats()
	if stats.TotalExplanations != 3 {
		t.Errorf("expected 3 total explanations, got %d", stats.TotalExplanations)
	}
	if stats.TotalTokensUsed <= 0 {
		t.Error("expected positive total tokens used")
	}
	if stats.RecommendationsGenerated <= 0 {
		t.Error("expected positive recommendations generated")
	}
	if stats.AvgGenerationTime < 0 {
		t.Error("expected non-negative avg generation time")
	}

	// Verify list and get work
	list := engine.ListExplanations("", 0)
	if len(list) != 3 {
		t.Errorf("expected 3 explanations in list, got %d", len(list))
	}

	filtered := engine.ListExplanations("metric_a", 0)
	if len(filtered) != 1 {
		t.Errorf("expected 1 filtered explanation, got %d", len(filtered))
	}

	limited := engine.ListExplanations("", 2)
	if len(limited) != 2 {
		t.Errorf("expected 2 limited explanations, got %d", len(limited))
	}

	expl := engine.GetExplanation(list[0].AnomalyID)
	if expl == nil {
		t.Error("expected to find explanation by ID")
	}

	missing := engine.GetExplanation("nonexistent")
	if missing != nil {
		t.Error("expected nil for nonexistent explanation")
	}
}

func TestAnomalyExplainabilityHTTPHandlers(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyExplainabilityConfig()
	engine := NewAnomalyExplainabilityEngine(db, config)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// Test POST /api/v1/explain/anomaly
	reqBody, _ := json.Marshal(map[string]interface{}{
		"metric": "cpu_usage",
		"value":  95.0,
		"time":   time.Now().Format(time.RFC3339Nano),
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/explain/anomaly", bytes.NewReader(reqBody))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var expl AnomalyExplanation
	if err := json.NewDecoder(w.Body).Decode(&expl); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if expl.Metric != "cpu_usage" {
		t.Errorf("expected metric cpu_usage, got %s", expl.Metric)
	}

	// Test GET not allowed on POST endpoint
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/explain/anomaly", nil)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)
	if w2.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w2.Code)
	}

	// Test GET /api/v1/explain/list
	req3 := httptest.NewRequest(http.MethodGet, "/api/v1/explain/list", nil)
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, req3)
	if w3.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w3.Code)
	}

	// Test GET /api/v1/explain/stats
	req4 := httptest.NewRequest(http.MethodGet, "/api/v1/explain/stats", nil)
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, req4)
	if w4.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w4.Code)
	}

	// Test POST /api/v1/explain/context
	ctxBody, _ := json.Marshal(ExplainabilityContext{
		AnomalyMetric:  "latency",
		AnomalyValue:   200.0,
		AnomalyTime:    time.Now(),
		BaselineMean:   50.0,
		BaselineStddev: 10.0,
	})
	req5 := httptest.NewRequest(http.MethodPost, "/api/v1/explain/context", bytes.NewReader(ctxBody))
	w5 := httptest.NewRecorder()
	mux.ServeHTTP(w5, req5)
	if w5.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w5.Code, w5.Body.String())
	}

	// Test GET /api/v1/explain/recommendations/ with nonexistent ID
	req6 := httptest.NewRequest(http.MethodGet, "/api/v1/explain/recommendations/nonexistent", nil)
	w6 := httptest.NewRecorder()
	mux.ServeHTTP(w6, req6)
	if w6.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w6.Code)
	}

	// Test recommendations with valid ID
	req7 := httptest.NewRequest(http.MethodGet, "/api/v1/explain/recommendations/"+expl.AnomalyID, nil)
	w7 := httptest.NewRecorder()
	mux.ServeHTTP(w7, req7)
	if w7.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w7.Code, w7.Body.String())
	}
}
