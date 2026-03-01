package chronicle

import (
	"math"
	"testing"
	"time"
)

func TestCausalAnomalyExplainer(t *testing.T) {
	db := setupTestDB(t)
	explainEngine := NewAnomalyExplainabilityEngine(db, DefaultAnomalyExplainabilityConfig())
	correlationEngine := NewMetricCorrelationEngine(db, MetricCorrelationConfig{})
	config := DefaultCausalExplainerConfig()

	explainer := NewCausalAnomalyExplainer(db, explainEngine, correlationEngine, config)

	t.Run("ingest and build graph", func(t *testing.T) {
		// Create two related metrics: cause leads effect by a lag
		for i := 0; i < 100; i++ {
			causeVal := float64(i) + math.Sin(float64(i)/10)*5
			explainer.IngestValue("cpu_usage", causeVal)
			// Effect follows cause with lag
			if i > 2 {
				effectVal := float64(i-2) + math.Sin(float64(i-2)/10)*5 + float64(i%3)
				explainer.IngestValue("response_time", effectVal)
			}
			explainer.IngestValue("memory", float64(i%20))
		}

		graph := explainer.BuildCausalExplainerGraph()
		if graph == nil {
			t.Fatal("expected non-nil graph")
		}
		if len(graph.Nodes) < 2 {
			t.Errorf("expected at least 2 nodes, got %d", len(graph.Nodes))
		}
	})

	t.Run("explain anomaly", func(t *testing.T) {
		result, err := explainer.ExplainAnomaly(
			"cpu_usage",
			150.0,
			time.Now(),
			map[string]string{"host": "server-1", "region": "us-east"},
		)
		if err != nil {
			t.Fatalf("explain failed: %v", err)
		}
		if result.Metric != "cpu_usage" {
			t.Errorf("expected cpu_usage, got %s", result.Metric)
		}
		if result.NLExplanation == "" {
			t.Error("expected non-empty explanation")
		}
		if result.AnomalyID == "" {
			t.Error("expected non-empty anomaly ID")
		}
		if len(result.DimensionAttrs) != 2 {
			t.Errorf("expected 2 dimension attrs, got %d", len(result.DimensionAttrs))
		}
	})

	t.Run("explain empty metric", func(t *testing.T) {
		_, err := explainer.ExplainAnomaly("", 100, time.Now(), nil)
		if err == nil {
			t.Error("expected error for empty metric")
		}
	})

	t.Run("explain unknown metric", func(t *testing.T) {
		result, err := explainer.ExplainAnomaly("unknown_metric", 100, time.Now(), nil)
		if err != nil {
			t.Fatalf("should not error for unknown metric: %v", err)
		}
		if result.Deviation != 0 {
			t.Error("expected 0 deviation for unknown metric")
		}
	})

	t.Run("shapley values", func(t *testing.T) {
		dims := map[string]string{"host": "a", "dc": "us", "env": "prod"}
		attrs := explainer.ComputeShapleyValues("test", dims, 150, 100)
		if len(attrs) != 3 {
			t.Errorf("expected 3 attrs, got %d", len(attrs))
		}
		// Total contributions should sum to ~100%
		var totalPct float64
		for _, a := range attrs {
			totalPct += a.Contribution
		}
		if totalPct < 90 || totalPct > 110 {
			t.Errorf("expected ~100%% total contribution, got %.1f%%", totalPct)
		}
	})

	t.Run("shapley empty dimensions", func(t *testing.T) {
		attrs := explainer.ComputeShapleyValues("test", nil, 150, 100)
		if len(attrs) != 0 {
			t.Error("expected empty attrs for nil dimensions")
		}
	})

	t.Run("get causal graph", func(t *testing.T) {
		graph := explainer.GetCausalExplainerGraph()
		if graph == nil {
			t.Error("expected non-nil graph")
		}
	})
}

func TestGrangerCausalResult(t *testing.T) {
	result := GrangerCausalResult{
		Cause:      "cpu",
		Effect:     "latency",
		FStatistic: 5.0,
		PValue:     0.01,
		IsCausal:   true,
		Lag:        3,
	}
	if !result.IsCausal {
		t.Error("expected causal")
	}
}

func TestCausalExplainerNode(t *testing.T) {
	node := CausalExplainerNode{
		Metric:     "cpu",
		Centrality: 0.8,
		InDegree:   2,
		OutDegree:  3,
	}
	if node.Centrality != 0.8 {
		t.Errorf("expected 0.8, got %f", node.Centrality)
	}
}
