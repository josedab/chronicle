package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestDetectQueryLanguage(t *testing.T) {
	tests := []struct {
		query    string
		expected string
	}{
		{"SELECT avg(value) FROM cpu", "sql"},
		{`cpu_usage{host="web1"}`, "promql"},
		{`sum(cpu_usage{host="web1"})`, "promql"},
		{"SELECT avg(value) FROM cpu WINDOW 5m GAP_FILL", "cql"},
		{"SHOW METRICS", "sql"},
	}

	for _, tt := range tests {
		got := detectQueryLanguage(tt.query)
		if got != tt.expected {
			t.Errorf("detectQueryLanguage(%q) = %q, want %q", tt.query, got, tt.expected)
		}
	}
}

func TestQueryCompilerSQL(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	compiler := NewQueryCompiler(db, DefaultQueryCompilerConfig())

	plan, err := compiler.Compile("SELECT avg(value) FROM cpu_usage")
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	if plan.SourceLanguage != "sql" {
		t.Errorf("expected sql, got %s", plan.SourceLanguage)
	}
	if plan.Root == nil {
		t.Fatal("expected non-nil IR root")
	}
	if plan.Root.Type != IRAggregation {
		t.Errorf("expected Aggregation root, got %s", plan.Root.Type)
	}
}

func TestQueryCompilerPromQL(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	compiler := NewQueryCompiler(db, DefaultQueryCompilerConfig())

	plan, err := compiler.Compile(`sum(http_requests{status="200"})`)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	if plan.SourceLanguage != "promql" {
		t.Errorf("expected promql, got %s", plan.SourceLanguage)
	}
	if plan.Root.Type != IRAggregation {
		t.Errorf("expected Aggregation root, got %s", plan.Root.Type)
	}
}

func TestQueryCompilerCQL(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	compiler := NewQueryCompiler(db, DefaultQueryCompilerConfig())

	plan, err := compiler.Compile("SELECT avg(value) FROM cpu WINDOW 5m GAP_FILL")
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	if plan.SourceLanguage != "cql" {
		t.Errorf("expected cql, got %s", plan.SourceLanguage)
	}
}

func TestQueryCompilerCache(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	compiler := NewQueryCompiler(db, DefaultQueryCompilerConfig())

	_, _ = compiler.Compile("SELECT * FROM cpu_usage")
	_, _ = compiler.Compile("SELECT * FROM cpu_usage") // cache hit

	stats := compiler.Stats()
	if stats.CacheHits != 1 {
		t.Errorf("expected 1 cache hit, got %d", stats.CacheHits)
	}
	if stats.CacheMisses != 1 {
		t.Errorf("expected 1 cache miss, got %d", stats.CacheMisses)
	}

	compiler.ClearCache()
	if compiler.Stats().CacheSize != 0 {
		t.Error("expected empty cache after clear")
	}
}

func TestPromQLSelectorParsing(t *testing.T) {
	metric, tags, err := parsePromQLSelector(`http_requests{method="GET", status="200"}`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if metric != "http_requests" {
		t.Errorf("expected http_requests, got %s", metric)
	}
	if tags["method"] != "GET" {
		t.Errorf("expected method=GET, got %s", tags["method"])
	}
	if tags["status"] != "200" {
		t.Errorf("expected status=200, got %s", tags["status"])
	}
}

func TestIRNodeTypes(t *testing.T) {
	if IRScan.String() != "Scan" {
		t.Errorf("expected Scan, got %s", IRScan.String())
	}
	if IRAggregation.String() != "Aggregation" {
		t.Errorf("expected Aggregation, got %s", IRAggregation.String())
	}
}

func TestSQLClauseParsing(t *testing.T) {
	clauses := sqlSplitClauses("SELECT avg(value) FROM cpu WHERE host = 'web1' ORDER BY time LIMIT 100")
	if clauses["FROM"] == "" {
		t.Error("expected FROM clause")
	}
	metric := extractFromClause(clauses["FROM"])
	if metric != "cpu" {
		t.Errorf("expected cpu, got %s", metric)
	}
}

func TestPredicateEvaluation(t *testing.T) {
	pred := &IRPredicate{
		Field: "value",
		Op:    IROpGt,
		Value: float64(50),
	}

	p1 := Point{Value: 60}
	p2 := Point{Value: 40}

	if !evaluatePredicate(pred, p1) {
		t.Error("expected 60 > 50 to pass")
	}
	if evaluatePredicate(pred, p2) {
		t.Error("expected 40 > 50 to fail")
	}
}

func TestQueryCompilerUDFAggregation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write test data
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		db.Write(Point{
			Metric:    "udf_metric",
			Value:     float64(i + 1),
			Timestamp: baseTime.Add(time.Duration(i) * time.Second).UnixNano(),
		})
	}
	db.Flush()

	compiler := NewQueryCompiler(db, DefaultQueryCompilerConfig())
	ctx := context.Background()

	t.Run("StandardAggregation", func(t *testing.T) {
		// Compile a standard SQL aggregation query
		plan, err := compiler.Compile("SELECT sum(value) FROM udf_metric")
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}
		if plan == nil {
			t.Fatal("plan should not be nil")
		}

		result, err := compiler.Execute(ctx, "SELECT sum(value) FROM udf_metric")
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if len(result.Points) == 0 {
			t.Fatal("expected non-empty result")
		}
	})

	t.Run("UDFFieldInIR", func(t *testing.T) {
		// Verify that UDFName can be set in IR nodes
		node := &IRNode{
			Type: IRAggregation,
			Properties: IRProps{
				AggFunc: AggSum,
				UDFName: "custom_sum",
			},
			Children: []*IRNode{
				{Type: IRScan, Properties: IRProps{Metric: "udf_metric", Start: 0, End: baseTime.Add(10 * time.Second).UnixNano()}},
			},
		}
		if node.Properties.UDFName != "custom_sum" {
			t.Errorf("expected UDFName custom_sum, got %s", node.Properties.UDFName)
		}
	})

	t.Run("ExecuteUDFAggReturnsError", func(t *testing.T) {
		// Calling executeUDFAggregation with no UDF engine should return an error
		result := &Result{
			Points: []Point{{Metric: "test", Value: 42, Timestamp: 1000}},
		}
		compiler2 := NewQueryCompiler(db, DefaultQueryCompilerConfig())
		_, err := compiler2.executeUDFAggregation(context.Background(), result, "nonexistent_udf")
		if err == nil {
			t.Error("expected error when UDF engine not available")
		}
	})
}

func TestQueryCompilerCacheStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	compiler := NewQueryCompiler(db, DefaultQueryCompilerConfig())

	// Compile same query twice
	compiler.Compile("SELECT * FROM test_metric")
	compiler.Compile("SELECT * FROM test_metric")

	stats := compiler.Stats()
	if stats.Compilations < 1 {
		t.Errorf("expected at least 1 compilation, got %d", stats.Compilations)
	}
}
