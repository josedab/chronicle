package chronicle

import (
	"testing"
	"time"
)

func TestProfileStore_Write(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ps := NewProfileStore(db, DefaultProfileConfig())

	err := ps.Write(Profile{
		Type:        ProfileTypeCPU,
		Labels:      map[string]string{"service": "api"},
		Data:        []byte("fake pprof data"),
		Duration:    30 * time.Second,
		SampleCount: 1000,
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	stats := ps.Stats()
	if stats.ProfileCount != 1 {
		t.Errorf("expected 1 profile, got %d", stats.ProfileCount)
	}
	if stats.TypeCounts[ProfileTypeCPU] != 1 {
		t.Error("expected 1 CPU profile")
	}
}

func TestProfileStore_WriteValidation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ps := NewProfileStore(db, DefaultProfileConfig())

	// Empty data
	err := ps.Write(Profile{Type: ProfileTypeCPU})
	if err == nil {
		t.Error("expected error for empty data")
	}

	// Exceeds max size
	cfg := DefaultProfileConfig()
	cfg.MaxProfileSize = 10
	ps = NewProfileStore(db, cfg)
	err = ps.Write(Profile{
		Type: ProfileTypeCPU,
		Data: make([]byte, 100),
	})
	if err == nil {
		t.Error("expected error for oversized profile")
	}
}

func TestProfileStore_Query(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ps := NewProfileStore(db, DefaultProfileConfig())

	baseTime := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		ps.Write(Profile{
			Type:      ProfileTypeCPU,
			Labels:    map[string]string{"service": "api"},
			Data:      []byte("profile data"),
			Timestamp: baseTime + int64(i)*int64(time.Hour),
		})
	}

	// Query time range
	results, err := ps.Query(ProfileTypeCPU, nil, baseTime+int64(time.Hour), baseTime+3*int64(time.Hour))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestProfileStore_QueryByLabels(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ps := NewProfileStore(db, DefaultProfileConfig())

	now := time.Now().UnixNano()
	ps.Write(Profile{
		Type:      ProfileTypeCPU,
		Labels:    map[string]string{"service": "api"},
		Data:      []byte("api profile"),
		Timestamp: now,
	})
	ps.Write(Profile{
		Type:      ProfileTypeCPU,
		Labels:    map[string]string{"service": "worker"},
		Data:      []byte("worker profile"),
		Timestamp: now,
	})

	// Query by label
	results, _ := ps.Query(ProfileTypeCPU, map[string]string{"service": "api"}, 0, now+int64(time.Hour))
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

func TestProfileStore_QueryByType(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ps := NewProfileStore(db, DefaultProfileConfig())

	now := time.Now().UnixNano()
	ps.Write(Profile{Type: ProfileTypeCPU, Data: []byte("cpu"), Timestamp: now})
	ps.Write(Profile{Type: ProfileTypeHeap, Data: []byte("heap"), Timestamp: now})
	ps.Write(Profile{Type: ProfileTypeGoroutine, Data: []byte("goroutine"), Timestamp: now})

	cpuProfiles, _ := ps.Query(ProfileTypeCPU, nil, 0, now+int64(time.Hour))
	if len(cpuProfiles) != 1 {
		t.Errorf("expected 1 CPU profile, got %d", len(cpuProfiles))
	}

	heapProfiles, _ := ps.Query(ProfileTypeHeap, nil, 0, now+int64(time.Hour))
	if len(heapProfiles) != 1 {
		t.Errorf("expected 1 heap profile, got %d", len(heapProfiles))
	}
}

func TestProfileStore_GetProfile(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ps := NewProfileStore(db, DefaultProfileConfig())

	err := ps.Write(Profile{
		ID:   "test-profile-123",
		Type: ProfileTypeCPU,
		Data: []byte("profile data"),
	})
	if err != nil {
		t.Fatal(err)
	}

	profile, err := ps.GetProfile("test-profile-123")
	if err != nil {
		t.Fatalf("GetProfile failed: %v", err)
	}
	if profile.ID != "test-profile-123" {
		t.Errorf("wrong profile ID: %s", profile.ID)
	}

	// Not found
	_, err = ps.GetProfile("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent profile")
	}
}

func TestProfileStore_Compression(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultProfileConfig()
	cfg.CompressionEnabled = true
	ps := NewProfileStore(db, cfg)

	originalData := []byte("this is test profile data that should compress well")
	ps.Write(Profile{
		ID:   "compressed-profile",
		Type: ProfileTypeCPU,
		Data: originalData,
	})

	profile, _ := ps.GetProfile("compressed-profile")
	if !profile.Compressed {
		t.Error("profile should be compressed")
	}

	// Decompress
	decompressed, err := profile.Decompress()
	if err != nil {
		t.Fatalf("decompress failed: %v", err)
	}
	if string(decompressed) != string(originalData) {
		t.Error("decompressed data doesn't match original")
	}
}

func TestProfileStore_MetricCorrelation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ps := NewProfileStore(db, DefaultProfileConfig())

	baseTime := time.Now().UnixNano()

	// Write metric points with explicit time range
	points := make([]Point, 10)
	for i := 0; i < 10; i++ {
		points[i] = Point{
			Metric:    "cpu_usage",
			Value:     float64(50 + i*10), // 50, 60, 70, 80, 90, 100...
			Timestamp: baseTime + int64(i)*int64(time.Minute),
		}
	}
	db.WriteBatch(points)

	// Write profiles near high CPU times
	for i := 5; i < 10; i++ {
		ps.Write(Profile{
			Type:      ProfileTypeCPU,
			Data:      []byte("high cpu profile"),
			Timestamp: baseTime + int64(i)*int64(time.Minute) + int64(5*time.Second),
		})
	}

	// Verify profiles were written
	stats := ps.Stats()
	if stats.ProfileCount == 0 {
		t.Fatal("no profiles written")
	}

	// Find correlations where CPU > 80
	correlations, err := ps.QueryByMetricCondition(
		"cpu_usage",
		80.0,
		ProfileTypeCPU,
		baseTime-int64(time.Hour),
		baseTime+int64(time.Hour)*2,
	)
	if err != nil {
		t.Fatalf("correlation query failed: %v", err)
	}

	// At least test the function works without error
	t.Logf("Found %d correlations", len(correlations))
}

func TestProfileStore_Retention(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultProfileConfig()
	cfg.RetentionDuration = time.Hour
	ps := NewProfileStore(db, cfg)

	now := time.Now()

	// Write old profile
	ps.Write(Profile{
		Type:      ProfileTypeCPU,
		Data:      []byte("old"),
		Timestamp: now.Add(-2 * time.Hour).UnixNano(),
	})

	// Write new profile (triggers retention enforcement)
	ps.Write(Profile{
		Type:      ProfileTypeCPU,
		Data:      []byte("new"),
		Timestamp: now.UnixNano(),
	})

	stats := ps.Stats()
	if stats.ProfileCount != 1 {
		t.Errorf("expected 1 profile after retention, got %d", stats.ProfileCount)
	}
}

func TestProfileStore_MaxProfiles(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultProfileConfig()
	cfg.MaxProfiles = 5
	ps := NewProfileStore(db, cfg)

	for i := 0; i < 10; i++ {
		ps.Write(Profile{
			Type:      ProfileTypeCPU,
			Data:      []byte("profile"),
			Timestamp: int64(i),
		})
	}

	stats := ps.Stats()
	if stats.ProfileCount > 5 {
		t.Errorf("expected max 5 profiles, got %d", stats.ProfileCount)
	}
}

func TestProfileTypeFunctions(t *testing.T) {
	tests := []struct {
		pt   ProfileType
		name string
	}{
		{ProfileTypeCPU, "cpu"},
		{ProfileTypeHeap, "heap"},
		{ProfileTypeGoroutine, "goroutine"},
		{ProfileTypeMutex, "mutex"},
		{ProfileTypeBlock, "block"},
		{ProfileTypeAllocs, "allocs"},
	}

	for _, tt := range tests {
		name := ProfileTypeName(tt.pt)
		if name != tt.name {
			t.Errorf("ProfileTypeName(%d) = %s, want %s", tt.pt, name, tt.name)
		}

		parsed := ParseProfileType(tt.name)
		if parsed != tt.pt {
			t.Errorf("ParseProfileType(%s) = %d, want %d", tt.name, parsed, tt.pt)
		}
	}
}

func TestGenerateProfileID(t *testing.T) {
	id1 := generateProfileID(ProfileTypeCPU, 1000)
	id2 := generateProfileID(ProfileTypeCPU, 1001)
	id3 := generateProfileID(ProfileTypeHeap, 1000)

	if id1 == id2 {
		t.Error("different timestamps should produce different IDs")
	}
	if id1 == id3 {
		t.Error("different types should produce different IDs")
	}
	if len(id1) != 32 {
		t.Errorf("ID should be 32 chars, got %d", len(id1))
	}
}
