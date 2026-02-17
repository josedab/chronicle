package chronicle

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestQueryFederation(t *testing.T) {
	// Create temp DB
	path := "test_federation.db"
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	db, err := Open(path, Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write test data
	now := time.Now()
	for i := 0; i < 100; i++ {
		p := Point{
			Metric:    "cpu_usage",
			Value:     float64(50 + i%20),
			Timestamp: now.Add(time.Duration(i) * time.Minute).UnixNano(),
			Tags:      map[string]string{"host": "server1"},
		}
		db.Write(p)
	}
	db.Flush()

	config := DefaultQueryFederationConfig()
	federation := NewQueryFederation(db, config)

	t.Run("QueryLocal", func(t *testing.T) {
		ctx := context.Background()
		query := &QueryFederatedQuery{
			Metric: "cpu_usage",
			Start:  now.Add(-time.Hour).UnixNano(),
			End:    now.Add(time.Hour).UnixNano(),
		}

		result, err := federation.Query(ctx, query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Points) == 0 {
			t.Error("Expected points in result")
		}
	})

	t.Run("RegisterSource", func(t *testing.T) {
		source, err := NewClickHouseSource(ClickHouseFederationConfig{
			Name:     "clickhouse1",
			Host:     "localhost",
			Port:     9000,
			Database: "default",
		})
		if err != nil {
			t.Fatalf("Failed to create source: %v", err)
		}

		err = federation.RegisterSource(source)
		if err != nil {
			t.Fatalf("Failed to register source: %v", err)
		}

		sources := federation.ListSources()
		if len(sources) != 1 {
			t.Errorf("Expected 1 source, got %d", len(sources))
		}
	})

	t.Run("DuplicateSource", func(t *testing.T) {
		source, _ := NewClickHouseSource(ClickHouseFederationConfig{
			Name: "clickhouse1",
		})

		err := federation.RegisterSource(source)
		if err == nil {
			t.Error("Expected error for duplicate source")
		}
	})

	t.Run("UnregisterSource", func(t *testing.T) {
		err := federation.UnregisterSource("clickhouse1")
		if err != nil {
			t.Fatalf("Failed to unregister source: %v", err)
		}

		sources := federation.ListSources()
		if len(sources) != 0 {
			t.Errorf("Expected 0 sources, got %d", len(sources))
		}
	})

	t.Run("GetNonexistentSource", func(t *testing.T) {
		_, ok := federation.GetSource("nonexistent")
		if ok {
			t.Error("Expected source not to be found")
		}
	})
}

func TestQueryFederation_ParseSQL(t *testing.T) {
	config := DefaultQueryFederationConfig()
	federation := NewQueryFederation(nil, config)

	testCases := []struct {
		sql         string
		numSources  int
		firstSource string
	}{
		{"SELECT * FROM local.metrics", 1, "local"},
		{"SELECT * FROM clickhouse.events", 1, "clickhouse"},
		{"SELECT * FROM ch.events JOIN pg.users ON ...", 2, "ch"},
		{"SELECT * FROM metrics", 0, ""},
	}

	for _, tc := range testCases {
		plan := federation.parseSQL(tc.sql)

		if len(plan.Sources) != tc.numSources {
			t.Errorf("SQL '%s': expected %d sources, got %d", tc.sql, tc.numSources, len(plan.Sources))
		}

		if tc.numSources > 0 && plan.Sources[0] != tc.firstSource {
			t.Errorf("SQL '%s': expected first source '%s', got '%s'", tc.sql, tc.firstSource, plan.Sources[0])
		}
	}
}

func TestQueryFederation_Cache(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow federation cache test in short mode")
	}
	path := "test_federation_cache.db"
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	db, err := Open(path, Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	config := DefaultQueryFederationConfig()
	config.CacheEnabled = true
	config.CacheTTL = time.Second
	federation := NewQueryFederation(db, config)

	ctx := context.Background()
	query := &QueryFederatedQuery{
		Metric: "test",
		Start:  0,
		End:    time.Now().UnixNano(),
	}

	// First query - should not be cached
	result1, err := federation.Query(ctx, query)
	if err != nil {
		t.Fatalf("First query failed: %v", err)
	}

	// Second query - should hit cache
	result2, err := federation.Query(ctx, query)
	if err != nil {
		t.Fatalf("Second query failed: %v", err)
	}

	// Results should be identical (same pointer since cached)
	if result1 == nil || result2 == nil {
		t.Error("Results should not be nil")
	}

	// Wait for cache to expire
	time.Sleep(2 * time.Second)

	// Third query - cache should be expired
	_, err = federation.Query(ctx, query)
	if err != nil {
		t.Fatalf("Third query failed: %v", err)
	}
}

func TestHTTPSource(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
		case "/query":
			result := QueryFederatedResult{
				Columns: []QueryFederatedColumn{
					{Name: "timestamp", Type: "int64"},
					{Name: "value", Type: "float64"},
				},
				Rows: [][]interface{}{
					{int64(1000000), 42.0},
					{int64(2000000), 43.0},
				},
			}
			json.NewEncoder(w).Encode(result)
		case "/schema":
			schema := QueryFederatedSchema{
				Tables: []QueryFederatedTable{
					{Name: "metrics", Columns: []QueryFederatedColumn{{Name: "value", Type: "float64"}}},
				},
			}
			json.NewEncoder(w).Encode(schema)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	source, err := NewHTTPSource(HTTPSourceConfig{
		Name:    "test-http",
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("Failed to create HTTP source: %v", err)
	}

	ctx := context.Background()

	t.Run("HealthCheck", func(t *testing.T) {
		err := source.HealthCheck(ctx)
		if err != nil {
			t.Errorf("Health check failed: %v", err)
		}
	})

	t.Run("Query", func(t *testing.T) {
		result, err := source.Query(ctx, &QueryFederatedQuery{
			SQL: "SELECT * FROM metrics",
		})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(result.Rows))
		}
	})

	t.Run("GetSchema", func(t *testing.T) {
		schema, err := source.GetSchema(ctx)
		if err != nil {
			t.Fatalf("GetSchema failed: %v", err)
		}

		if len(schema.Tables) != 1 {
			t.Errorf("Expected 1 table, got %d", len(schema.Tables))
		}
	})
}

func TestChronicleSource(t *testing.T) {
	// Create mock Chronicle server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
		case "/query":
			result := struct {
				Points []Point `json:"points"`
			}{
				Points: []Point{
					{Metric: "cpu", Value: 42.0, Timestamp: 1000},
					{Metric: "cpu", Value: 43.0, Timestamp: 2000},
				},
			}
			json.NewEncoder(w).Encode(result)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	source, err := NewChronicleSource(HTTPSourceConfig{
		Name:    "remote-chronicle",
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("Failed to create Chronicle source: %v", err)
	}

	ctx := context.Background()

	t.Run("Query", func(t *testing.T) {
		result, err := source.Query(ctx, &QueryFederatedQuery{
			Metric: "cpu",
			Start:  0,
			End:    10000,
		})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Points) != 2 {
			t.Errorf("Expected 2 points, got %d", len(result.Points))
		}
	})

	t.Run("Type", func(t *testing.T) {
		if source.Type() != FederatedSourceTypeChronicle {
			t.Errorf("Expected Chronicle type, got %s", source.Type())
		}
	})
}

func TestClickHouseSource(t *testing.T) {
	source, err := NewClickHouseSource(ClickHouseFederationConfig{
		Name:     "clickhouse-test",
		Host:     "localhost",
		Port:     9000,
		Database: "default",
	})
	if err != nil {
		t.Fatalf("Failed to create ClickHouse source: %v", err)
	}

	if source.Name() != "clickhouse-test" {
		t.Errorf("Expected name 'clickhouse-test', got '%s'", source.Name())
	}

	if source.Type() != FederatedSourceTypeClickHouse {
		t.Errorf("Expected ClickHouse type, got %s", source.Type())
	}

	// Test query (simulated)
	ctx := context.Background()
	result, err := source.Query(ctx, &QueryFederatedQuery{SQL: "SELECT 1"})
	if err != nil {
		t.Errorf("Query failed: %v", err)
	}

	if result.Source != "clickhouse-test" {
		t.Errorf("Expected source 'clickhouse-test', got '%s'", result.Source)
	}
}

func TestDuckDBSource(t *testing.T) {
	source, err := NewDuckDBSource(DuckDBConfig{
		Name: "duckdb-test",
		Path: ":memory:",
	})
	if err != nil {
		t.Fatalf("Failed to create DuckDB source: %v", err)
	}

	if source.Name() != "duckdb-test" {
		t.Errorf("Expected name 'duckdb-test', got '%s'", source.Name())
	}

	if source.Type() != FederatedSourceTypeDuckDB {
		t.Errorf("Expected DuckDB type, got %s", source.Type())
	}
}

func TestPostgresSource(t *testing.T) {
	source, err := NewPostgresSource(PostgresConfig{
		Name:     "postgres-test",
		Host:     "localhost",
		Port:     5432,
		Database: "test",
		Username: "user",
		Password: "pass",
	})
	if err != nil {
		t.Fatalf("Failed to create Postgres source: %v", err)
	}

	if source.Name() != "postgres-test" {
		t.Errorf("Expected name 'postgres-test', got '%s'", source.Name())
	}

	if source.Type() != FederatedSourceTypePostgres {
		t.Errorf("Expected Postgres type, got %s", source.Type())
	}
}

func TestQueryFederation_MergeResults(t *testing.T) {
	config := DefaultQueryFederationConfig()
	federation := NewQueryFederation(nil, config)

	results := []*QueryFederatedResult{
		{
			Source:  "source1",
			Columns: []QueryFederatedColumn{{Name: "value", Type: "float64"}},
			Rows:    [][]interface{}{{1.0}, {2.0}},
			Points:  []Point{{Value: 1.0}, {Value: 2.0}},
		},
		{
			Source:  "source2",
			Columns: []QueryFederatedColumn{{Name: "value", Type: "float64"}},
			Rows:    [][]interface{}{{3.0}, {4.0}},
			Points:  []Point{{Value: 3.0}, {Value: 4.0}},
		},
	}

	merged, err := federation.mergeResults(results)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	if len(merged.Rows) != 4 {
		t.Errorf("Expected 4 rows, got %d", len(merged.Rows))
	}

	if len(merged.Points) != 4 {
		t.Errorf("Expected 4 points, got %d", len(merged.Points))
	}

	if merged.Source != "federated" {
		t.Errorf("Expected source 'federated', got '%s'", merged.Source)
	}
}

func TestFederatedQuery_Timeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow federation timeout test in short mode")
	}
	config := DefaultQueryFederationConfig()
	config.QueryTimeout = 100 * time.Millisecond
	federation := NewQueryFederation(nil, config)

	// Create a slow mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	source, _ := NewHTTPSource(HTTPSourceConfig{
		Name:    "slow-source",
		BaseURL: server.URL,
		Timeout: 50 * time.Millisecond, // Even shorter timeout
	})
	federation.RegisterSource(source)

	ctx := context.Background()
	_, err := federation.Query(ctx, &QueryFederatedQuery{
		Source: "slow-source",
		SQL:    "SELECT 1",
	})

	if err == nil {
		t.Error("Expected timeout error")
	}
}
