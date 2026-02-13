package chronicle

import (
	"testing"
	"time"
)

func TestPrometheusDropInEngine(t *testing.T) {
	db, err := Open(t.TempDir()+"/test.db", DefaultConfig(t.TempDir()+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now()
	for i := 0; i < 10; i++ {
		db.Write(Point{Metric: "http_requests_total", Tags: map[string]string{"method": "GET"}, Value: float64(i), Timestamp: now.Add(time.Duration(i) * time.Second).UnixNano()})
	}
	db.Flush()

	t.Run("instant query", func(t *testing.T) {
		engine := NewPrometheusDropInEngine(db, DefaultPrometheusDropInConfig())

		result, err := engine.QueryInstant("http_requests_total", now.Add(20*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Error("expected result")
		}
		resp := result.(map[string]interface{})
		if resp["resultType"] != "vector" {
			t.Errorf("expected vector, got %v", resp["resultType"])
		}
	})

	t.Run("range query", func(t *testing.T) {
		engine := NewPrometheusDropInEngine(db, DefaultPrometheusDropInConfig())

		result, err := engine.QueryRange("http_requests_total", now.Add(-time.Hour), now.Add(time.Hour), 15*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		resp := result.(map[string]interface{})
		if resp["resultType"] != "matrix" {
			t.Errorf("expected matrix, got %v", resp["resultType"])
		}
	})

	t.Run("empty query", func(t *testing.T) {
		engine := NewPrometheusDropInEngine(db, DefaultPrometheusDropInConfig())

		if _, err := engine.QueryInstant("", time.Now()); err == nil {
			t.Error("expected error for empty query")
		}
		if _, err := engine.QueryRange("", time.Now(), time.Now(), time.Second); err == nil {
			t.Error("expected error for empty query")
		}
	})

	t.Run("targets", func(t *testing.T) {
		engine := NewPrometheusDropInEngine(db, DefaultPrometheusDropInConfig())

		engine.AddTarget(PromTarget{
			ScrapeURL: "http://localhost:9090/metrics",
			Labels:    map[string]string{"job": "prometheus"},
			Health:    "up",
		})
		engine.AddTarget(PromTarget{
			ScrapeURL: "http://localhost:8080/metrics",
			Labels:    map[string]string{"job": "app"},
			Health:    "up",
		})

		targets := engine.Targets()
		if len(targets) != 2 {
			t.Errorf("expected 2 targets, got %d", len(targets))
		}

		engine.RemoveTarget("http://localhost:9090/metrics")
		targets = engine.Targets()
		if len(targets) != 1 {
			t.Errorf("expected 1 target, got %d", len(targets))
		}

		// Remove nonexistent (no error)
		engine.RemoveTarget("nonexistent")
	})

	t.Run("target default health", func(t *testing.T) {
		engine := NewPrometheusDropInEngine(db, DefaultPrometheusDropInConfig())
		engine.AddTarget(PromTarget{ScrapeURL: "http://x"})
		targets := engine.Targets()
		if targets[0].Health != "unknown" {
			t.Errorf("expected unknown health, got %s", targets[0].Health)
		}
	})

	t.Run("rule groups", func(t *testing.T) {
		engine := NewPrometheusDropInEngine(db, DefaultPrometheusDropInConfig())

		group := PromRuleGroup{
			Name:     "test_rules",
			Interval: 15 * time.Second,
			Rules: []PromAlertingRule{
				{
					Name:     "HighErrorRate",
					Query:    "rate(errors_total[5m]) > 0.1",
					Duration: 5 * time.Minute,
					Labels:   map[string]string{"severity": "critical"},
					State:    "inactive",
				},
			},
		}

		engine.AddRuleGroup(group)
		groups := engine.RuleGroups()
		if len(groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(groups))
		}
		if len(groups[0].Rules) != 1 {
			t.Errorf("expected 1 rule, got %d", len(groups[0].Rules))
		}
	})

	t.Run("metadata", func(t *testing.T) {
		engine := NewPrometheusDropInEngine(db, DefaultPrometheusDropInConfig())

		engine.SetMetadata("http_requests_total", PromMetadata{
			Type: "counter",
			Help: "Total HTTP requests",
			Unit: "requests",
		})

		md, ok := engine.GetMetadata("http_requests_total")
		if !ok {
			t.Fatal("expected metadata")
		}
		if md.Type != "counter" {
			t.Errorf("expected counter, got %s", md.Type)
		}

		_, ok = engine.GetMetadata("nonexistent")
		if ok {
			t.Error("expected no metadata")
		}

		all := engine.AllMetadata()
		if len(all) != 1 {
			t.Errorf("expected 1 metadata, got %d", len(all))
		}
	})

	t.Run("label names and values", func(t *testing.T) {
		engine := NewPrometheusDropInEngine(db, DefaultPrometheusDropInConfig())
		engine.AddTarget(PromTarget{
			ScrapeURL: "http://x",
			Labels:    map[string]string{"job": "api", "instance": "localhost:8080"},
		})

		names := engine.LabelNames()
		if len(names) < 2 {
			t.Errorf("expected at least 2 label names, got %d", len(names))
		}

		values := engine.LabelValues("job")
		if len(values) != 1 || values[0] != "api" {
			t.Errorf("unexpected label values: %v", values)
		}
	})

	t.Run("stats", func(t *testing.T) {
		engine := NewPrometheusDropInEngine(db, DefaultPrometheusDropInConfig())
		engine.QueryInstant("test", time.Now())

		stats := engine.GetStats()
		if stats.QueriesTotal != 1 {
			t.Errorf("expected 1 query, got %d", stats.QueriesTotal)
		}
		if stats.CompatibilityScore != 85 {
			t.Errorf("expected 85 compat score, got %d", stats.CompatibilityScore)
		}
	})

	t.Run("extract metric from promql", func(t *testing.T) {
		cases := []struct {
			query  string
			metric string
		}{
			{"cpu_usage", "cpu_usage"},
			{"rate(cpu_usage[5m])", "cpu_usage"},
			{"cpu_usage{host=\"a\"}", "cpu_usage"},
			{"sum(rate(requests_total[5m]))", "rate(requests_total"},
		}
		for _, tc := range cases {
			got := extractMetricFromPromQL(tc.query)
			if got != tc.metric {
				t.Errorf("extractMetricFromPromQL(%q) = %q, want %q", tc.query, got, tc.metric)
			}
		}
	})

	t.Run("start stop", func(t *testing.T) {
		engine := NewPrometheusDropInEngine(db, DefaultPrometheusDropInConfig())
		engine.Start()
		engine.Start() // idempotent
		engine.Stop()
		engine.Stop() // idempotent
	})

	t.Run("config defaults", func(t *testing.T) {
		cfg := DefaultPrometheusDropInConfig()
		if !cfg.Enabled {
			t.Error("should be enabled by default")
		}
		if cfg.MaxSamples != 50000000 {
			t.Errorf("unexpected max samples: %d", cfg.MaxSamples)
		}
		if cfg.LookbackDelta != 5*time.Minute {
			t.Error("unexpected lookback delta")
		}
	})
}
