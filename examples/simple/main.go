package main

import (
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

	_ = db.Write(chronicle.Point{
		Metric:    "temperature",
		Tags:      map[string]string{"room": "lab"},
		Value:     21.7,
		Timestamp: time.Now().UnixNano(),
	})

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

	_ = result
}
