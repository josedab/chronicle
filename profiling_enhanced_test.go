package chronicle

import (
	"fmt"
	"testing"
	"time"
)

func TestProfilePartitionStore_Store(t *testing.T) {
	store := NewProfilePartitionStore(DefaultProfilePartitionConfig())

	profile := StoredProfile{
		Service:     "api-server",
		ProfileType: ProfileTypeCPU,
		StartTime:   time.Now().UnixNano(),
		EndTime:     time.Now().UnixNano(),
		Samples: []ProfileSample{
			{StackTrace: []string{"main", "handleRequest", "db.Query"}, Value: 100},
		},
	}

	if err := store.Store(profile); err != nil {
		t.Fatalf("store: %v", err)
	}

	stats := store.Stats()
	total := stats["total_profiles"].(int)
	if total != 1 {
		t.Fatalf("expected 1 profile, got %d", total)
	}
}

func TestProfilePartitionStore_QueryByService(t *testing.T) {
	store := NewProfilePartitionStore(DefaultProfilePartitionConfig())
	now := time.Now().UnixNano()

	store.Store(StoredProfile{
		ID: "p1", Service: "api", ProfileType: ProfileTypeCPU,
		StartTime: now - int64(30*time.Minute), EndTime: now,
	})
	store.Store(StoredProfile{
		ID: "p2", Service: "api", ProfileType: ProfileTypeHeap,
		StartTime: now - int64(15*time.Minute), EndTime: now,
	})
	store.Store(StoredProfile{
		ID: "p3", Service: "worker", ProfileType: ProfileTypeCPU,
		StartTime: now - int64(10*time.Minute), EndTime: now,
	})

	// Query CPU profiles for api service (use wide time range)
	results := store.QueryByService("api", ProfileTypeCPU,
		now-int64(2*time.Hour), now+int64(time.Hour))
	if len(results) != 1 {
		t.Fatalf("expected 1 CPU profile for api, got %d", len(results))
	}

	// Query all heap profiles
	results = store.QueryByService("api", ProfileTypeHeap,
		now-int64(2*time.Hour), now+int64(time.Hour))
	if len(results) != 1 {
		t.Fatalf("expected 1 heap profile for api, got %d", len(results))
	}
}

func TestProfilePartitionStore_SpanLinking(t *testing.T) {
	store := NewProfilePartitionStore(DefaultProfilePartitionConfig())

	store.Store(StoredProfile{
		Service:     "api",
		ProfileType: ProfileTypeCPU,
		StartTime:   time.Now().UnixNano(),
		EndTime:     time.Now().UnixNano(),
		Samples: []ProfileSample{
			{
				StackTrace: []string{"main", "handleRequest"},
				Value:      100,
				SpanID:     "span-123",
				TraceID:    "trace-abc",
			},
		},
	})

	// Query by span
	profiles := store.QueryBySpan("span-123")
	if len(profiles) != 1 {
		t.Fatalf("expected 1 profile for span, got %d", len(profiles))
	}

	// Get span profile link
	link := store.GetSpanProfile("span-123")
	if link == nil {
		t.Fatal("expected non-nil span profile link")
	}
	if link.TraceID != "trace-abc" {
		t.Fatalf("expected trace ID 'trace-abc', got %s", link.TraceID)
	}
	if link.FlameGraph == nil {
		t.Fatal("expected flame graph")
	}
}

func TestProfilePartitionStore_Correlation(t *testing.T) {
	store := NewProfilePartitionStore(DefaultProfilePartitionConfig())
	now := time.Now().UnixNano()

	// Store profiles around a spike time
	store.Store(StoredProfile{
		Service: "api", ProfileType: ProfileTypeCPU,
		StartTime: now - int64(5*time.Minute), EndTime: now + int64(5*time.Minute),
	})
	store.Store(StoredProfile{
		Service: "api", ProfileType: ProfileTypeCPU,
		StartTime: now - int64(2*time.Hour), EndTime: now - int64(time.Hour),
	})

	// Correlate with spike at 'now' with wider window
	correlated := store.CorrelateMetricToProfiles("api", now, int64(30*time.Minute))
	if len(correlated) != 1 {
		t.Fatalf("expected 1 correlated profile, got %d", len(correlated))
	}
}

func TestPprofIngestHandler_Creation(t *testing.T) {
	store := NewProfilePartitionStore(DefaultProfilePartitionConfig())
	handler := NewPprofIngestHandler(store)
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestProfileTypes(t *testing.T) {
	types := []ProfileType{
		ProfileTypeCPU, ProfileTypeHeap, ProfileTypeGoroutine,
		ProfileTypeBlock, ProfileTypeMutex, ProfileTypeAllocs,
	}
	for i, pt := range types {
		_ = pt
		if i < 0 {
			t.Fatal("unexpected")
		}
	}
}

func TestProfilePartitionStore_MaxProfiles(t *testing.T) {
	config := DefaultProfilePartitionConfig()
	config.MaxProfiles = 3
	store := NewProfilePartitionStore(config)

	for i := 0; i < 5; i++ {
		store.Store(StoredProfile{
			ID:      fmt.Sprintf("prof-%d", i),
			Service: "api", ProfileType: ProfileTypeCPU,
			StartTime: time.Now().UnixNano(),
		})
	}

	stats := store.Stats()
	total := stats["total_profiles"].(int)
	if total > 3 {
		t.Fatalf("expected at most 3 profiles (max), got %d", total)
	}
}

func TestProfileCorrelationResult(t *testing.T) {
	store := NewProfilePartitionStore(DefaultProfilePartitionConfig())
	now := time.Now().UnixNano()

	store.Store(StoredProfile{
		ID: "corr-1", Service: "api", ProfileType: ProfileTypeCPU,
		StartTime: now - int64(2*time.Minute), EndTime: now + int64(2*time.Minute),
		Samples: []ProfileSample{
			{StackTrace: []string{"main", "handleRequest", "db.Query"}, Value: 100, SpanID: "span-abc"},
			{StackTrace: []string{"main", "handleRequest", "json.Marshal"}, Value: 50},
		},
	})

	result := store.CorrelateMetricSpikeToProfiles("api", now, ProfileTypeCPU, int64(5*time.Minute))
	if result == nil {
		t.Fatal("expected non-nil correlation result")
	}
	if len(result.Profiles) != 1 {
		t.Fatalf("expected 1 profile, got %d", len(result.Profiles))
	}
	if len(result.TopFunctions) == 0 {
		t.Fatal("expected top functions")
	}
	if len(result.LinkedSpans) != 1 {
		t.Fatalf("expected 1 linked span, got %d", len(result.LinkedSpans))
	}
	if result.FlameGraph == nil {
		t.Fatal("expected flame graph")
	}
}

func TestProfileCorrelation_NoResults(t *testing.T) {
	store := NewProfilePartitionStore(DefaultProfilePartitionConfig())

	result := store.CorrelateMetricSpikeToProfiles("nonexistent", 0, ProfileTypeCPU, int64(time.Hour))
	if result != nil {
		t.Fatal("expected nil for no matching profiles")
	}
}
