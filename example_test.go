package chronicle_test

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/chronicle-db/chronicle"
)

func Example() {
	// Create temp directory for example
	dir, _ := os.MkdirTemp("", "chronicle-example-*")
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "example.db")

	// Open or create a database
	db, err := chronicle.Open(dbPath, chronicle.DefaultConfig(dbPath))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Write a data point
	err = db.Write(chronicle.Point{
		Metric:    "temperature",
		Tags:      map[string]string{"room": "kitchen"},
		Value:     21.5,
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		panic(err)
	}

	// Flush to storage
	_ = db.Flush()

	// Query data
	result, err := db.Execute(&chronicle.Query{
		Metric: "temperature",
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Found %d points\n", len(result.Points))
	// Output: Found 1 points
}

func ExampleDB_WriteBatch() {
	dir, _ := os.MkdirTemp("", "chronicle-batch-*")
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "batch.db")

	db, _ := chronicle.Open(dbPath, chronicle.DefaultConfig(dbPath))
	defer db.Close()

	// Write multiple points at once
	points := []chronicle.Point{
		{Metric: "cpu", Tags: map[string]string{"host": "server1"}, Value: 45.2, Timestamp: time.Now().UnixNano()},
		{Metric: "cpu", Tags: map[string]string{"host": "server2"}, Value: 32.1, Timestamp: time.Now().UnixNano()},
		{Metric: "mem", Tags: map[string]string{"host": "server1"}, Value: 78.5, Timestamp: time.Now().UnixNano()},
	}

	err := db.WriteBatch(points)
	if err != nil {
		panic(err)
	}

	fmt.Println("Batch written successfully")
	// Output: Batch written successfully
}

func ExampleDB_Execute_aggregation() {
	dir, _ := os.MkdirTemp("", "chronicle-agg-*")
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "agg.db")

	db, _ := chronicle.Open(dbPath, chronicle.DefaultConfig(dbPath))
	defer db.Close()

	// Use a fixed base time aligned to hour boundary for deterministic results
	baseTime := int64(1704067200000000000) // 2024-01-01 00:00:00 UTC in nanoseconds
	for i := 0; i < 10; i++ {
		_ = db.Write(chronicle.Point{
			Metric:    "sensor",
			Value:     float64(i * 10),
			Timestamp: baseTime + int64(i)*int64(time.Minute),
		})
	}
	_ = db.Flush()

	// Query with aggregation - all points within one hour bucket
	result, err := db.Execute(&chronicle.Query{
		Metric: "sensor",
		Start:  baseTime,
		End:    baseTime + int64(time.Hour),
		Aggregation: &chronicle.Aggregation{
			Function: chronicle.AggMean,
			Window:   time.Hour,
		},
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Aggregated to %d buckets\n", len(result.Points))
	// Output: Aggregated to 1 buckets
}

func ExampleQueryParser() {
	parser := &chronicle.QueryParser{}

	query, err := parser.Parse("SELECT mean(value) FROM cpu WHERE host = 'server1' GROUP BY time(5m)")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Metric: %s\n", query.Metric)
	fmt.Printf("Has aggregation: %v\n", query.Aggregation != nil)
	// Output:
	// Metric: cpu
	// Has aggregation: true
}

func ExampleOpen() {
	dir, _ := os.MkdirTemp("", "chronicle-open-*")
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "data.db")

	db, err := chronicle.Open(dbPath, chronicle.DefaultConfig(dbPath))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fmt.Println("Database opened")
	// Output: Database opened
}

func ExampleDefaultConfig() {
	cfg := chronicle.DefaultConfig("/tmp/data.db")

	fmt.Printf("Path: %s\n", cfg.Path)
	fmt.Printf("Has storage config: %v\n", cfg.Storage.MaxMemory > 0)
	fmt.Printf("Has partition duration: %v\n", cfg.Storage.PartitionDuration > 0)
	// Output:
	// Path: /tmp/data.db
	// Has storage config: true
	// Has partition duration: true
}

func ExampleNewConfigBuilder() {
	cfg, err := chronicle.NewConfigBuilder("/tmp/metrics.db").
		WithMaxMemory(128 * 1024 * 1024).
		WithRetention(30 * 24 * time.Hour).
		WithPartitionDuration(time.Hour).
		WithQueryTimeout(10 * time.Second).
		Build()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Path: %s\n", cfg.Path)
	fmt.Printf("Memory: %d MB\n", cfg.Storage.MaxMemory/(1024*1024))
	// Output:
	// Path: /tmp/metrics.db
	// Memory: 128 MB
}

func ExampleDB_Execute_tagFilter() {
	dir, _ := os.MkdirTemp("", "chronicle-tags-*")
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "tags.db")

	db, _ := chronicle.Open(dbPath, chronicle.DefaultConfig(dbPath))
	defer db.Close()

	now := time.Now().UnixNano()
	_ = db.Write(chronicle.Point{Metric: "temp", Tags: map[string]string{"room": "kitchen"}, Value: 22.0, Timestamp: now})
	_ = db.Write(chronicle.Point{Metric: "temp", Tags: map[string]string{"room": "bedroom"}, Value: 19.5, Timestamp: now})
	_ = db.Flush()

	result, err := db.Execute(&chronicle.Query{
		Metric: "temp",
		Tags:   map[string]string{"room": "kitchen"},
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Kitchen points: %d\n", len(result.Points))
	// Output: Kitchen points: 1
}
