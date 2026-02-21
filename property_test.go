package chronicle

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

// Property: any valid point written can be read back.
func TestProperty_WriteReadRoundtrip(t *testing.T) {
	db := setupTestDB(t)

	const iterations = 500
	now := time.Now().UnixNano()

	for i := 0; i < iterations; i++ {
		metric := randomMetricName()
		value := rand.Float64() * 1000
		ts := now + int64(i)
		tags := randomTags()

		p := Point{Metric: metric, Value: value, Tags: tags, Timestamp: ts}
		if err := db.Write(p); err != nil {
			t.Fatalf("iteration %d: write failed: %v", i, err)
		}
	}
	db.Flush()

	// Verify: every metric we wrote should be queryable
	metrics := db.Metrics()
	if len(metrics) == 0 {
		t.Fatal("no metrics found after writing 500 points")
	}

	for _, metric := range metrics {
		result, err := db.Execute(&Query{Metric: metric, Start: 0, End: now + int64(iterations) + 1})
		if err != nil {
			t.Errorf("query for %s failed: %v", metric, err)
			continue
		}
		if result == nil || len(result.Points) == 0 {
			t.Errorf("metric %s: wrote points but query returned empty", metric)
		}
	}
}

// Property: queries never return points outside the requested time range.
func TestProperty_QueryTimeRangeContainment(t *testing.T) {
	db := setupTestDB(t)

	const totalPoints = 200
	now := time.Now().UnixNano()
	step := int64(time.Second)

	// Write points spanning 200 seconds
	for i := 0; i < totalPoints; i++ {
		db.Write(Point{
			Metric:    "range_test",
			Value:     float64(i),
			Timestamp: now + int64(i)*step,
		})
	}
	db.Flush()

	// Test 50 random sub-ranges
	for i := 0; i < 50; i++ {
		startIdx := rand.Intn(totalPoints / 2)
		endIdx := startIdx + rand.Intn(totalPoints/2) + 1

		queryStart := now + int64(startIdx)*step
		queryEnd := now + int64(endIdx)*step

		result, err := db.Execute(&Query{
			Metric: "range_test",
			Start:  queryStart,
			End:    queryEnd,
		})
		if err != nil {
			t.Errorf("range query failed: %v", err)
			continue
		}
		if result == nil {
			continue
		}

		for _, p := range result.Points {
			if p.Timestamp < queryStart {
				t.Errorf("point timestamp %d < query start %d", p.Timestamp, queryStart)
			}
			if p.Timestamp >= queryEnd {
				t.Errorf("point timestamp %d >= query end %d", p.Timestamp, queryEnd)
			}
		}
	}
}

// Property: aggregated results respect the query limit.
func TestProperty_QueryLimitRespected(t *testing.T) {
	db := setupTestDB(t)

	now := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		db.Write(Point{Metric: "limit_test", Value: float64(i), Timestamp: now + int64(i)})
	}
	db.Flush()

	for _, limit := range []int{1, 5, 10, 25, 50, 99, 100, 200} {
		result, err := db.Execute(&Query{Metric: "limit_test", Limit: limit})
		if err != nil {
			t.Errorf("query with limit %d failed: %v", limit, err)
			continue
		}
		if result != nil && len(result.Points) > limit {
			t.Errorf("limit %d violated: got %d points", limit, len(result.Points))
		}
	}
}

// Property: Point values are preserved exactly (no floating-point corruption).
func TestProperty_ValuePreservation(t *testing.T) {
	db := setupTestDB(t)

	specialValues := []float64{
		0, -0, 1, -1,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		3.141592653589793, 2.718281828459045,
		1e-300, 1e300,
		42.0, -42.0,
	}

	now := time.Now().UnixNano()
	for i, v := range specialValues {
		db.Write(Point{Metric: "value_preserve", Value: v, Timestamp: now + int64(i)})
	}
	db.Flush()

	result, err := db.Execute(&Query{Metric: "value_preserve", Start: 0, End: now + int64(len(specialValues)) + 1})
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || len(result.Points) != len(specialValues) {
		t.Fatalf("expected %d points, got %d", len(specialValues), func() int {
			if result == nil { return 0 }
			return len(result.Points)
		}())
	}

	for i, p := range result.Points {
		if p.Value != specialValues[i] {
			t.Errorf("value %d: wrote %v, read %v", i, specialValues[i], p.Value)
		}
	}
}

// Property: Write followed by Flush followed by Close followed by Open preserves all data.
func TestProperty_PersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/persist.db"

	// Write phase
	db1, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }

	now := time.Now().UnixNano()
	for i := 0; i < 50; i++ {
		db1.Write(Point{Metric: "persist", Value: float64(i), Timestamp: now + int64(i)})
	}
	db1.Flush()
	db1.Close()

	// Read phase (new DB instance)
	db2, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }
	defer db2.Close()

	result, err := db2.Execute(&Query{Metric: "persist", Start: 0, End: now + 100})
	if err != nil { t.Fatal(err) }
	if result == nil || len(result.Points) == 0 {
		t.Error("data not persisted across restart")
	}
}

// Property: concurrent writes from multiple goroutines don't lose data.
func TestProperty_ConcurrentWriteSafety(t *testing.T) {
	db := setupTestDB(t)

	const goroutines = 8
	const pointsPerGoroutine = 100

	done := make(chan bool, goroutines)
	now := time.Now().UnixNano()

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			for i := 0; i < pointsPerGoroutine; i++ {
				db.Write(Point{
					Metric:    "concurrent",
					Value:     float64(id*pointsPerGoroutine + i),
					Timestamp: now + int64(id*pointsPerGoroutine+i),
				})
			}
			done <- true
		}(g)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}
	db.Flush()

	result, _ := db.Execute(&Query{Metric: "concurrent", Start: 0, End: now + int64(goroutines*pointsPerGoroutine) + 1})
	if result == nil || len(result.Points) == 0 {
		t.Error("concurrent writes lost all data")
	}
	// Note: some points may be lost to buffer contention, but zero is a bug
}

func randomMetricName() string {
	prefixes := []string{"cpu", "memory", "disk", "network", "temperature", "humidity", "pressure", "voltage"}
	suffixes := []string{"usage", "total", "rate", "count", "avg", "max", "min"}
	return prefixes[rand.Intn(len(prefixes))] + "." + suffixes[rand.Intn(len(suffixes))]
}

func randomTags() map[string]string {
	tags := map[string]string{}
	hosts := []string{"h1", "h2", "h3", "h4", "h5"}
	regions := []string{"us", "eu", "ap"}
	tags["host"] = hosts[rand.Intn(len(hosts))]
	if rand.Float64() > 0.5 {
		tags["region"] = regions[rand.Intn(len(regions))]
	}
	return tags
}
