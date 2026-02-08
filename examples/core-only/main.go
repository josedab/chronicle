// Core-only example — the simplest possible Chronicle program.
//
// Demonstrates: Open, Write, Flush, Execute, Close.
// No optional features, no HTTP, no advanced config.
//
// Run:
//
//	cd examples/core-only && go run .
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/chronicle-db/chronicle"
)

func main() {
	// Open a database with sensible defaults
	db, err := chronicle.Open("demo.db", chronicle.DefaultConfig("demo.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Write some data points
	now := time.Now().UnixNano()
	for i := range 5 {
		err := db.Write(chronicle.Point{
			Metric:    "cpu",
			Value:     40.0 + float64(i)*5.0,
			Timestamp: now + int64(i)*int64(time.Second),
			Tags:      map[string]string{"host": "server-01"},
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	// Flush writes to storage so queries can find them
	db.Flush()

	// Query all points
	result, err := db.Execute(&chronicle.Query{Metric: "cpu"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Total points: %d\n", len(result.Points))
	for _, p := range result.Points {
		fmt.Printf("  %s host=%s value=%.1f\n",
			time.Unix(0, p.Timestamp).Format(time.RFC3339), p.Tags["host"], p.Value)
	}

	// Query with aggregation
	result, err = db.Execute(&chronicle.Query{
		Metric: "cpu",
		Aggregation: &chronicle.Aggregation{
			Function: chronicle.AggMean,
			Window:   time.Minute,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if len(result.Points) > 0 {
		fmt.Printf("Average CPU: %.1f\n", result.Points[0].Value)
	}

	// List known metrics
	fmt.Printf("Metrics: %v\n", db.Metrics())
}
