package main

import (
	"fmt"
	"log"
	"time"

	"github.com/chronicle-db/chronicle"
)

func main() {
	// Database file is created in the current directory.
	// Run from this example's directory: cd examples/simple && go run .
	db, err := chronicle.Open("sensors.db", chronicle.DefaultConfig("sensors.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Write(chronicle.Point{
		Metric:    "temperature",
		Tags:      map[string]string{"room": "lab"},
		Value:     21.7,
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		log.Fatal(err)
	}

	// Flush ensures buffered writes are persisted before querying.
	if err := db.Flush(); err != nil {
		log.Fatal(err)
	}

	result, err := db.Execute(&chronicle.Query{
		Metric: "temperature",
		Aggregation: &chronicle.Aggregation{
			Function: chronicle.AggMean,
			Window:   5 * time.Minute,
		},
		GroupBy: []string{"room"},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Query returned %d point(s)\n", len(result.Points))
	for _, p := range result.Points {
		fmt.Printf("  %s [%v] = %.2f at %s\n",
			p.Metric, p.Tags, p.Value,
			time.Unix(0, p.Timestamp).Format(time.RFC3339))
	}
}
