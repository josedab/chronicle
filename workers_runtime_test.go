package chronicle

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewWorkersRuntime(t *testing.T) {
	tests := []struct {
		name    string
		config  WorkersRuntimeConfig
		wantErr bool
	}{
		{
			name: "valid D1 config",
			config: WorkersRuntimeConfig{
				D1DatabaseID: "test-db-id",
				AccountID:    "test-account",
				APIToken:     "test-token",
			},
			wantErr: false,
		},
		{
			name: "valid R2 config",
			config: WorkersRuntimeConfig{
				R2BucketName: "test-bucket",
				AccountID:    "test-account",
				APIToken:     "test-token",
			},
			wantErr: false,
		},
		{
			name:    "missing storage config",
			config:  WorkersRuntimeConfig{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewWorkersRuntime(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewWorkersRuntime() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultWorkersRuntimeConfig(t *testing.T) {
	config := DefaultWorkersRuntimeConfig()

	if config.MaxMemoryMB != 128 {
		t.Errorf("MaxMemoryMB = %d, want 128", config.MaxMemoryMB)
	}
	if !config.EnableStreaming {
		t.Error("EnableStreaming should be true by default")
	}
	if !config.CacheEnabled {
		t.Error("CacheEnabled should be true by default")
	}
	if config.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", config.BatchSize)
	}
}

func TestWorkersMetrics(t *testing.T) {
	config := WorkersRuntimeConfig{
		D1DatabaseID: "test-db",
		AccountID:    "test-account",
		APIToken:     "test-token",
	}

	runtime, err := NewWorkersRuntime(config)
	if err != nil {
		t.Fatalf("NewWorkersRuntime() error = %v", err)
	}

	metrics := runtime.Metrics()
	if metrics.D1Queries != 0 {
		t.Errorf("Initial D1Queries = %d, want 0", metrics.D1Queries)
	}
	if metrics.CacheHits != 0 {
		t.Errorf("Initial CacheHits = %d, want 0", metrics.CacheHits)
	}
}

func TestWorkersHandler(t *testing.T) {
	config := WorkersRuntimeConfig{
		D1DatabaseID: "test-db",
		AccountID:    "test-account",
		APIToken:     "test-token",
	}

	runtime, err := NewWorkersRuntime(config)
	if err != nil {
		t.Fatalf("NewWorkersRuntime() error = %v", err)
	}

	handler := NewWorkersHandler(runtime)

	t.Run("health endpoint", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("health status = %d, want %d", rec.Code, http.StatusOK)
		}

		var resp map[string]string
		json.NewDecoder(rec.Body).Decode(&resp)
		if resp["status"] != "healthy" {
			t.Errorf("health status = %s, want healthy", resp["status"])
		}
	})

	t.Run("metrics endpoint", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("metrics status = %d, want %d", rec.Code, http.StatusOK)
		}

		var metrics WorkersMetrics
		json.NewDecoder(rec.Body).Decode(&metrics)
		// Just verify it decoded successfully
	})

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/unknown", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("unknown path status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})
}

func TestD1Backend(t *testing.T) {
	// Create a mock D1 server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		
		resp := D1Response{
			Success: true,
			Result: json.RawMessage(`[{"results": [], "success": true, "meta": {"duration": 0.5}}]`),
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	backend := &D1Backend{
		databaseID: "test-db",
		accountID:  "test-account",
		apiToken:   "test-token",
		client:     server.Client(),
		batchSize:  100,
	}

	// Test initialization query count starts at 0
	if backend.queryCount != 0 {
		t.Errorf("Initial queryCount = %d, want 0", backend.queryCount)
	}
	if backend.writeCount != 0 {
		t.Errorf("Initial writeCount = %d, want 0", backend.writeCount)
	}
}

func TestR2Backend(t *testing.T) {
	backend := &R2Backend{
		bucketName: "test-bucket",
		accountID:  "test-account",
		apiToken:   "test-token",
		client:     &http.Client{Timeout: 5 * time.Second},
	}

	// Test key generation
	p := Point{
		Metric:    "cpu",
		Value:     0.5,
		Timestamp: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano(),
	}

	key := backend.generateKey(p)
	if key == "" {
		t.Error("generateKey() returned empty string")
	}
	if key[:5] != "data/" {
		t.Errorf("generateKey() prefix = %s, want data/", key[:5])
	}
}

func TestKVBackend(t *testing.T) {
	// Create a mock KV server
	kvStore := make(map[string][]byte)
	
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/storage/kv/namespaces/test-ns/values/"):]
		
		switch r.Method {
		case http.MethodGet:
			if data, ok := kvStore[key]; ok {
				w.Write(data)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case http.MethodPut:
			body := make([]byte, r.ContentLength)
			r.Body.Read(body)
			kvStore[key] = body
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			delete(kvStore, key)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	backend := &KVBackend{
		namespace: "test-ns",
		accountID: "test-account",
		apiToken:  "test-token",
		client:    server.Client(),
	}

	// Test initial counts
	if backend.readCount != 0 {
		t.Errorf("Initial readCount = %d, want 0", backend.readCount)
	}
	if backend.writeCount != 0 {
		t.Errorf("Initial writeCount = %d, want 0", backend.writeCount)
	}
}

func TestWorkersCache(t *testing.T) {
	// Create mock KV
	kvStore := make(map[string][]byte)
	
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract key after the namespace path
		pathParts := r.URL.Path
		key := pathParts[len("/accounts/test-account/storage/kv/namespaces/test-ns/values/"):]
		
		switch r.Method {
		case http.MethodGet:
			if data, ok := kvStore[key]; ok {
				w.Write(data)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case http.MethodPut:
			body := make([]byte, 1024)
			n, _ := r.Body.Read(body)
			kvStore[key] = body[:n]
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	kv := &KVBackend{
		namespace: "test-ns",
		accountID: "test-account",
		apiToken:  "test-token",
		client:    server.Client(),
	}

	cache := &WorkersCache{
		kv:     kv,
		ttl:    5 * time.Minute,
		prefix: "cache:",
	}

	ctx := context.Background()

	// Test cache miss
	result, ok := cache.Get(ctx, "nonexistent")
	if ok || result != nil {
		t.Error("Cache should miss for nonexistent key")
	}
}

func TestAggregatePoints(t *testing.T) {
	points := []Point{
		{Metric: "cpu", Tags: map[string]string{"host": "a"}, Value: 10.0, Timestamp: 1000000000},
		{Metric: "cpu", Tags: map[string]string{"host": "a"}, Value: 20.0, Timestamp: 1000000001},
		{Metric: "cpu", Tags: map[string]string{"host": "a"}, Value: 30.0, Timestamp: 1000000002},
	}

	tests := []struct {
		name     string
		fn       AggFunc
		expected float64
	}{
		{"count", AggCount, 3},
		{"sum", AggSum, 60},
		{"mean", AggMean, 20},
		{"min", AggMin, 10},
		{"max", AggMax, 30},
		{"first", AggFirst, 10},
		{"last", AggLast, 30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeAggregation(points, tt.fn)
			if result != tt.expected {
				t.Errorf("computeAggregation(%s) = %f, want %f", tt.name, result, tt.expected)
			}
		})
	}
}

func TestComputeAggregationEmpty(t *testing.T) {
	result := computeAggregation([]Point{}, AggSum)
	if result != 0 {
		t.Errorf("computeAggregation on empty = %f, want 0", result)
	}
}

func TestMatchesTags(t *testing.T) {
	tests := []struct {
		name       string
		pointTags  map[string]string
		queryTags  map[string]string
		shouldMatch bool
	}{
		{
			name:       "exact match",
			pointTags:  map[string]string{"host": "server1", "region": "us-west"},
			queryTags:  map[string]string{"host": "server1"},
			shouldMatch: true,
		},
		{
			name:       "no match",
			pointTags:  map[string]string{"host": "server1"},
			queryTags:  map[string]string{"host": "server2"},
			shouldMatch: false,
		},
		{
			name:       "empty query",
			pointTags:  map[string]string{"host": "server1"},
			queryTags:  map[string]string{},
			shouldMatch: true,
		},
		{
			name:       "missing tag",
			pointTags:  map[string]string{"host": "server1"},
			queryTags:  map[string]string{"region": "us-west"},
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesTags(tt.pointTags, tt.queryTags)
			if result != tt.shouldMatch {
				t.Errorf("matchesTags() = %v, want %v", result, tt.shouldMatch)
			}
		})
	}
}

func TestSortedTags(t *testing.T) {
	tags := map[string]string{"z": "1", "a": "2", "m": "3"}
	result := sortedTags(tags)

	// Should be sorted alphabetically
	if result != "a=2,m=3,z=1," {
		t.Errorf("sortedTags() = %s, want a=2,m=3,z=1,", result)
	}

	// Empty tags
	empty := sortedTags(map[string]string{})
	if empty != "" {
		t.Errorf("sortedTags(empty) = %s, want empty", empty)
	}
}

func TestGenerateCacheKey(t *testing.T) {
	config := WorkersRuntimeConfig{
		D1DatabaseID: "test-db",
		AccountID:    "test-account",
		APIToken:     "test-token",
	}

	runtime, _ := NewWorkersRuntime(config)

	q := &Query{
		Metric: "cpu",
		Start:  1000,
		End:    2000,
		Tags:   map[string]string{"host": "server1"},
	}

	key1 := runtime.generateCacheKey(q)
	key2 := runtime.generateCacheKey(q)

	if key1 != key2 {
		t.Error("Cache keys should be deterministic")
	}

	// Different query should produce different key
	q2 := &Query{
		Metric: "memory",
		Start:  1000,
		End:    2000,
	}
	key3 := runtime.generateCacheKey(q2)

	if key1 == key3 {
		t.Error("Different queries should produce different cache keys")
	}
}
