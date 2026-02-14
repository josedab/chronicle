package chronicle

import (
	"math"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDefaultRootCauseAnalysisConfig(t *testing.T) {
	cfg := DefaultRootCauseAnalysisConfig()

	if cfg.Enabled {
		t.Error("expected Enabled to be false by default")
	}
	if cfg.CausalWindowSize != 5*time.Minute {
		t.Errorf("expected CausalWindowSize 5m, got %v", cfg.CausalWindowSize)
	}
	if cfg.MinConfidence != 0.7 {
		t.Errorf("expected MinConfidence 0.7, got %f", cfg.MinConfidence)
	}
	if cfg.MaxCausalDepth != 5 {
		t.Errorf("expected MaxCausalDepth 5, got %d", cfg.MaxCausalDepth)
	}
	if cfg.GrangerLagSteps != 10 {
		t.Errorf("expected GrangerLagSteps 10, got %d", cfg.GrangerLagSteps)
	}
	if cfg.CorrelationThreshold != 0.7 {
		t.Errorf("expected CorrelationThreshold 0.7, got %f", cfg.CorrelationThreshold)
	}
	if cfg.TopKCauses != 5 {
		t.Errorf("expected TopKCauses 5, got %d", cfg.TopKCauses)
	}
	if !cfg.ExplanationEnabled {
		t.Error("expected ExplanationEnabled to be true by default")
	}
}

func TestRCANewEngine(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultRootCauseAnalysisConfig()
	engine := NewRootCauseAnalysisEngine(db, cfg)

	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.graph == nil {
		t.Fatal("expected non-nil graph")
	}
	if engine.incidents == nil {
		t.Fatal("expected non-nil incidents map")
	}
}

func TestRCAIngestMetricValue(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewRootCauseAnalysisEngine(db, DefaultRootCauseAnalysisConfig())
	now := time.Now()

	engine.IngestMetricValue("cpu_usage", 85.0, now)
	engine.IngestMetricValue("cpu_usage", 90.0, now.Add(time.Second))
	engine.IngestMetricValue("memory_used", 70.0, now)

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if len(engine.metricHistory["cpu_usage"]) != 2 {
		t.Errorf("expected 2 cpu_usage values, got %d", len(engine.metricHistory["cpu_usage"]))
	}
	if len(engine.metricHistory["memory_used"]) != 1 {
		t.Errorf("expected 1 memory_used value, got %d", len(engine.metricHistory["memory_used"]))
	}

	node, ok := engine.graph.Nodes["cpu_usage"]
	if !ok {
		t.Fatal("expected cpu_usage node in graph")
	}
	if node.LastValue != 90.0 {
		t.Errorf("expected last value 90.0, got %f", node.LastValue)
	}
}

func TestRCAPearsonCorrelation(t *testing.T) {
	// Perfect positive correlation
	x := []float64{1, 2, 3, 4, 5}
	y := []float64{2, 4, 6, 8, 10}
	r := pearsonCorrelation(x, y)
	if math.Abs(r-1.0) > 1e-9 {
		t.Errorf("expected correlation ~1.0, got %f", r)
	}

	// Perfect negative correlation
	y2 := []float64{10, 8, 6, 4, 2}
	r2 := pearsonCorrelation(x, y2)
	if math.Abs(r2-(-1.0)) > 1e-9 {
		t.Errorf("expected correlation ~-1.0, got %f", r2)
	}

	// No correlation (constant)
	y3 := []float64{5, 5, 5, 5, 5}
	r3 := pearsonCorrelation(x, y3)
	if r3 != 0 {
		t.Errorf("expected correlation 0, got %f", r3)
	}

	// Empty slices
	r4 := pearsonCorrelation(nil, nil)
	if r4 != 0 {
		t.Errorf("expected 0 for empty slices, got %f", r4)
	}

	// Mismatched lengths
	r5 := pearsonCorrelation([]float64{1, 2}, []float64{1})
	if r5 != 0 {
		t.Errorf("expected 0 for mismatched lengths, got %f", r5)
	}
}

func TestRCAGrangerCausalityTest(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewRootCauseAnalysisEngine(db, DefaultRootCauseAnalysisConfig())

	// Create a causal relationship: source leads target by lag
	n := 100
	source := make([]float64, n)
	target := make([]float64, n)
	for i := 0; i < n; i++ {
		source[i] = float64(i) + math.Sin(float64(i)*0.1)*10
	}
	for i := 3; i < n; i++ {
		target[i] = source[i-3]*0.8 + float64(i)*0.1
	}

	result := engine.grangerCausalityTest(source, target, 5)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.LagSteps != 5 {
		t.Errorf("expected 5 lag steps, got %d", result.LagSteps)
	}
	if result.FStatistic < 0 {
		t.Error("F-statistic should be non-negative")
	}

	// Test with too-short data
	shortResult := engine.grangerCausalityTest([]float64{1, 2}, []float64{3, 4}, 5)
	if shortResult.IsCausal {
		t.Error("should not be causal with insufficient data")
	}
}

func TestRCABuildCausalGraph(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultRootCauseAnalysisConfig()
	cfg.MinConfidence = 0.1
	cfg.CorrelationThreshold = 0.5
	engine := NewRootCauseAnalysisEngine(db, cfg)

	now := time.Now()
	// Create correlated metrics
	for i := 0; i < 50; i++ {
		v := float64(i) + math.Sin(float64(i)*0.2)*5
		engine.IngestMetricValue("metric_a", v, now.Add(time.Duration(i)*time.Second))
		engine.IngestMetricValue("metric_b", v*1.5+2, now.Add(time.Duration(i)*time.Second))
		engine.IngestMetricValue("metric_c", float64(i%10), now.Add(time.Duration(i)*time.Second))
	}

	err := engine.BuildCausalGraph()
	if err != nil {
		t.Fatalf("BuildCausalGraph failed: %v", err)
	}

	graph := engine.GetGraph()
	if graph.MetricCount < 2 {
		t.Errorf("expected at least 2 nodes, got %d", graph.MetricCount)
	}
	if graph.LastBuilt.IsZero() {
		t.Error("expected non-zero LastBuilt")
	}
}

func TestRCAAnalyzeIncident(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultRootCauseAnalysisConfig()
	cfg.MinConfidence = 0.1
	cfg.CorrelationThreshold = 0.5
	engine := NewRootCauseAnalysisEngine(db, cfg)

	now := time.Now()
	for i := 0; i < 50; i++ {
		v := float64(i) + math.Sin(float64(i)*0.2)*5
		engine.IngestMetricValue("cpu_usage", v, now.Add(time.Duration(i)*time.Second))
		engine.IngestMetricValue("memory_used", v*0.8+10, now.Add(time.Duration(i)*time.Second))
		engine.IngestMetricValue("latency", v*1.2, now.Add(time.Duration(i)*time.Second))
	}

	_ = engine.BuildCausalGraph()

	incident, err := engine.AnalyzeIncident(
		[]string{"cpu_usage", "latency"},
		now, now.Add(50*time.Second),
	)
	if err != nil {
		t.Fatalf("AnalyzeIncident failed: %v", err)
	}
	if incident == nil {
		t.Fatal("expected non-nil incident")
	}
	if incident.State != "completed" {
		t.Errorf("expected state completed, got %s", incident.State)
	}
	if incident.ID == "" {
		t.Error("expected non-empty incident ID")
	}
	if len(incident.AnomalousMetrics) != 2 {
		t.Errorf("expected 2 anomalous metrics, got %d", len(incident.AnomalousMetrics))
	}
	if incident.AnalysisDuration <= 0 {
		t.Error("expected positive analysis duration")
	}
}

func TestRCAAnalyzeIncidentNoMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewRootCauseAnalysisEngine(db, DefaultRootCauseAnalysisConfig())

	_, err := engine.AnalyzeIncident(nil, time.Now(), time.Now())
	if err == nil {
		t.Error("expected error for empty metrics")
	}
}

func TestRCAPropagationPath(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewRootCauseAnalysisEngine(db, DefaultRootCauseAnalysisConfig())

	// Manually set up a simple graph: A -> B -> C
	engine.mu.Lock()
	engine.graph.Nodes["A"] = &CausalGraphNode{Metric: "A", OutDegree: 1}
	engine.graph.Nodes["B"] = &CausalGraphNode{Metric: "B", InDegree: 1, OutDegree: 1}
	engine.graph.Nodes["C"] = &CausalGraphNode{Metric: "C", InDegree: 1}
	engine.graph.Edges = []MetricRelationship{
		{Source: "A", Target: "B", Type: RelationGranger, Confidence: 0.9},
		{Source: "B", Target: "C", Type: RelationGranger, Confidence: 0.8},
	}
	engine.mu.Unlock()

	path := engine.buildPropagationPath("A", "C")
	if len(path) != 3 {
		t.Fatalf("expected path of length 3, got %d: %v", len(path), path)
	}
	if path[0] != "A" || path[1] != "B" || path[2] != "C" {
		t.Errorf("expected path [A B C], got %v", path)
	}

	// No path case
	path2 := engine.buildPropagationPath("C", "A")
	if len(path2) != 1 || path2[0] != "C" {
		t.Errorf("expected fallback path [C], got %v", path2)
	}

	// Same node
	path3 := engine.buildPropagationPath("A", "A")
	if len(path3) != 1 || path3[0] != "A" {
		t.Errorf("expected path [A], got %v", path3)
	}
}

func TestRCAExplanation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewRootCauseAnalysisEngine(db, DefaultRootCauseAnalysisConfig())

	// With root causes
	incident := &RCAIncident{
		AnomalousMetrics: []string{"cpu_usage", "latency"},
		RootCauses: []RankedCause{
			{
				Metric:          "cpu_usage",
				Score:           0.9,
				Confidence:      0.85,
				Evidence:        []string{"High upstream influence"},
				PropagationPath: []string{"cpu_usage", "latency"},
				Type:            "infrastructure",
			},
		},
		Confidence: 0.85,
	}
	explanation := engine.generateExplanation(incident)
	if explanation == "" {
		t.Error("expected non-empty explanation")
	}
	if !strings.Contains(explanation, "cpu_usage") {
		t.Error("expected explanation to mention cpu_usage")
	}
	if !strings.Contains(explanation, "infrastructure") {
		t.Error("expected explanation to mention cause type")
	}

	// Without root causes
	incident2 := &RCAIncident{
		AnomalousMetrics: []string{"metric_a"},
	}
	explanation2 := engine.generateExplanation(incident2)
	if explanation2 == "" {
		t.Error("expected non-empty explanation even with no root causes")
	}
}

func TestRCAStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultRootCauseAnalysisConfig()
	cfg.MinConfidence = 0.1
	cfg.CorrelationThreshold = 0.5
	engine := NewRootCauseAnalysisEngine(db, cfg)

	now := time.Now()
	for i := 0; i < 50; i++ {
		v := float64(i)
		engine.IngestMetricValue("m1", v, now.Add(time.Duration(i)*time.Second))
		engine.IngestMetricValue("m2", v*2, now.Add(time.Duration(i)*time.Second))
	}
	_ = engine.BuildCausalGraph()

	_, _ = engine.AnalyzeIncident([]string{"m1"}, now, now.Add(50*time.Second))

	stats := engine.Stats()
	if stats.IncidentsAnalyzed != 1 {
		t.Errorf("expected 1 incident analyzed, got %d", stats.IncidentsAnalyzed)
	}
	if stats.GrangerTestsRun == 0 {
		t.Error("expected granger tests to have been run")
	}
	if stats.GraphNodes == 0 {
		t.Error("expected non-zero graph nodes")
	}
}

func TestRCAListAndGetIncidents(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewRootCauseAnalysisEngine(db, DefaultRootCauseAnalysisConfig())

	now := time.Now()
	for i := 0; i < 20; i++ {
		engine.IngestMetricValue("m1", float64(i), now.Add(time.Duration(i)*time.Second))
	}

	inc, _ := engine.AnalyzeIncident([]string{"m1"}, now, now.Add(20*time.Second))

	list := engine.ListIncidents()
	if len(list) != 1 {
		t.Errorf("expected 1 incident, got %d", len(list))
	}

	got := engine.GetIncident(inc.ID)
	if got == nil {
		t.Error("expected to find incident by ID")
	}

	missing := engine.GetIncident("nonexistent")
	if missing != nil {
		t.Error("expected nil for nonexistent incident")
	}
}

func TestRCAFeedback(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewRootCauseAnalysisEngine(db, DefaultRootCauseAnalysisConfig())

	now := time.Now()
	for i := 0; i < 20; i++ {
		engine.IngestMetricValue("m1", float64(i), now.Add(time.Duration(i)*time.Second))
	}

	inc, _ := engine.AnalyzeIncident([]string{"m1"}, now, now.Add(20*time.Second))

	err := engine.ProvideFeedback(inc.ID, "m1")
	if err != nil {
		t.Fatalf("ProvideFeedback failed: %v", err)
	}

	err = engine.ProvideFeedback("nonexistent", "m1")
	if err == nil {
		t.Error("expected error for nonexistent incident")
	}
}

func TestRCARegisterHTTPHandlers(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewRootCauseAnalysisEngine(db, DefaultRootCauseAnalysisConfig())
	mux := http.NewServeMux()

	// Should not panic
	engine.RegisterHTTPHandlers(mux)
}
