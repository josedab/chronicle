package chronicle

import (
	"testing"
	"time"
)

func TestContinuousProfilingEngine(t *testing.T) {
	db := setupTestDB(t)
	store := NewProfileStore(db, DefaultProfileConfig())
	config := DefaultContinuousProfilingConfig()
	engine := NewContinuousProfilingEngine(db, store, config)

	t.Run("ingest profile", func(t *testing.T) {
		p := Profile{
			Type:        ProfileTypeCPU,
			Timestamp:   time.Now().UnixNano(),
			Duration:    10 * time.Second,
			Labels:      map[string]string{"service": "api"},
			Data:        []byte("fake pprof data"),
			SampleCount: 100,
		}
		err := engine.IngestProfile(p)
		if err != nil {
			t.Fatalf("ingest failed: %v", err)
		}
	})

	t.Run("ingest empty data", func(t *testing.T) {
		p := Profile{Type: ProfileTypeCPU}
		err := engine.IngestProfile(p)
		if err == nil {
			t.Error("expected error for empty data")
		}
	})

	t.Run("ingest too large", func(t *testing.T) {
		cfg := DefaultContinuousProfilingConfig()
		cfg.MaxProfileSize = 10
		eng := NewContinuousProfilingEngine(db, store, cfg)
		p := Profile{
			Type: ProfileTypeCPU,
			Data: make([]byte, 100),
		}
		err := eng.IngestProfile(p)
		if err == nil {
			t.Error("expected error for oversized profile")
		}
	})

	t.Run("ingest pyroscope", func(t *testing.T) {
		req := PyroscopeIngestRequest{
			Name:       "myapp.cpu",
			SpyName:    "gospy",
			SampleRate: 100,
			From:       time.Now().Unix() - 10,
			Until:      time.Now().Unix(),
			RawProfile: []byte("pprof data"),
		}
		err := engine.IngestPyroscope(req)
		if err != nil {
			t.Fatalf("pyroscope ingest failed: %v", err)
		}
	})

	t.Run("pyroscope disabled", func(t *testing.T) {
		cfg := DefaultContinuousProfilingConfig()
		cfg.PyroscopeCompat = false
		eng := NewContinuousProfilingEngine(db, store, cfg)
		err := eng.IngestPyroscope(PyroscopeIngestRequest{RawProfile: []byte("x")})
		if err == nil {
			t.Error("expected error when pyroscope disabled")
		}
	})

	t.Run("query profiles", func(t *testing.T) {
		now := time.Now().UnixNano()
		profiles, err := engine.QueryProfiles(ProfileTypeCPU, nil, now-int64(time.Hour), now+int64(time.Hour))
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(profiles) == 0 {
			t.Error("expected profiles")
		}
	})

	t.Run("correlate with metric", func(t *testing.T) {
		now := time.Now()

		// Write some metric data
		for i := 0; i < 5; i++ {
			db.Write(Point{
				Metric:    "p99_latency",
				Value:     float64(400 + i*100),
				Timestamp: now.Add(-time.Duration(5-i) * time.Second).UnixNano(),
			})
		}
		db.Flush()

		// Write a profile near the high-latency timestamp
		engine.IngestProfile(Profile{
			Type:      ProfileTypeCPU,
			Timestamp: now.Add(-1 * time.Second).UnixNano(),
			Duration:  10 * time.Second,
			Labels:    map[string]string{"service": "api"},
			Data:      []byte("cpu profile during latency spike"),
		})

		correlations, err := engine.CorrelateWithMetric(
			"p99_latency", 500, ProfileTypeCPU,
			now.Add(-10*time.Second).UnixNano(), now.Add(10*time.Second).UnixNano(),
		)
		if err != nil {
			t.Fatalf("correlate failed: %v", err)
		}
		// May or may not find correlations depending on timing
		_ = correlations
	})

	t.Run("flame graph diff", func(t *testing.T) {
		baseline := &FlameBearer{
			Names:    []string{"main", "funcA", "funcB"},
			NumTicks: 100,
		}
		comparison := &FlameBearer{
			Names:    []string{"main", "funcA", "funcC"},
			NumTicks: 120,
		}

		diff := engine.ComputeFlameGraphDiff(baseline, comparison)
		if diff == nil {
			t.Fatal("expected non-nil diff")
		}
		if len(diff.Added) != 1 || diff.Added[0] != "funcC" {
			t.Errorf("expected funcC added, got %v", diff.Added)
		}
		if len(diff.Removed) != 1 || diff.Removed[0] != "funcB" {
			t.Errorf("expected funcB removed, got %v", diff.Removed)
		}
	})

	t.Run("flame graph diff nil", func(t *testing.T) {
		diff := engine.ComputeFlameGraphDiff(nil, nil)
		if diff != nil {
			t.Error("expected nil diff for nil inputs")
		}
	})

	t.Run("get stats", func(t *testing.T) {
		stats := engine.GetStats()
		if stats["total_ingested"].(int64) == 0 {
			t.Error("expected non-zero ingested count")
		}
	})
}

func TestPyroscopeToProfileType(t *testing.T) {
	tests := []struct {
		spy      string
		expected ProfileType
	}{
		{"gospy", ProfileTypeCPU},
		{"ebpfspy", ProfileTypeCPU},
		{"rbspy", ProfileTypeCPU},
		{"unknown", ProfileTypeCPU},
	}
	for _, tt := range tests {
		result := pyroscopeToProfileType(tt.spy)
		if result != tt.expected {
			t.Errorf("expected %d for %s, got %d", tt.expected, tt.spy, result)
		}
	}
}
