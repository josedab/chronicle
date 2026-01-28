// Package main demonstrates an IoT data collector using Chronicle.
//
// This example simulates multiple sensors sending data and shows how to:
// - Collect data from multiple sources
// - Use schema validation
// - Apply retention policies
// - Query aggregated data
//
// Run: go run main.go
package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/chronicle-db/chronicle"
)

// Sensor represents an IoT sensor
type Sensor struct {
	ID       string
	Type     string
	Location string
	MinValue float64
	MaxValue float64
}

func main() {
	// Configure database for IoT workload
	db, err := chronicle.Open("iot_data.db", chronicle.Config{
		Path:              "iot_data.db",
		MaxMemory:         32 * 1024 * 1024,   // 32MB - constrained environment
		MaxStorageBytes:   1024 * 1024 * 1024, // 1GB max
		PartitionDuration: 30 * time.Minute,
		RetentionDuration: 24 * time.Hour, // Keep 24 hours of raw data
		BufferSize:        1000,
		SyncInterval:      5 * time.Second,
		// Downsample old data
		DownsampleRules: []chronicle.DownsampleRule{
			{
				SourceResolution: time.Minute,
				TargetResolution: time.Hour,
				Aggregations:     []chronicle.AggFunc{chronicle.AggMean, chronicle.AggMin, chronicle.AggMax},
				Retention:        7 * 24 * time.Hour, // Keep hourly for 7 days
			},
		},
		// Schema validation using Tags
		StrictSchema: false, // Warn but don't reject
		Schemas: []chronicle.MetricSchema{
			{
				Name:        "temperature",
				Description: "Temperature reading in Celsius",
				Tags: []chronicle.TagSchema{
					{Name: "sensor_id", Required: true},
					{Name: "location", Required: true},
				},
			},
			{
				Name:        "humidity",
				Description: "Relative humidity percentage",
				Tags: []chronicle.TagSchema{
					{Name: "sensor_id", Required: true},
					{Name: "location", Required: true},
				},
			},
			{
				Name:        "pressure",
				Description: "Atmospheric pressure in hPa",
				Tags: []chronicle.TagSchema{
					{Name: "sensor_id", Required: true},
					{Name: "location", Required: true},
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Define our sensors
	sensors := []Sensor{
		{ID: "temp-001", Type: "temperature", Location: "warehouse-a", MinValue: 15, MaxValue: 30},
		{ID: "temp-002", Type: "temperature", Location: "warehouse-b", MinValue: 10, MaxValue: 25},
		{ID: "temp-003", Type: "temperature", Location: "office", MinValue: 20, MaxValue: 26},
		{ID: "hum-001", Type: "humidity", Location: "warehouse-a", MinValue: 30, MaxValue: 70},
		{ID: "hum-002", Type: "humidity", Location: "warehouse-b", MinValue: 40, MaxValue: 80},
		{ID: "pres-001", Type: "pressure", Location: "roof", MinValue: 990, MaxValue: 1030},
	}

	log.Println("IoT Data Collector started")
	log.Printf("Collecting from %d sensors\n", len(sensors))
	log.Println()

	// Simulate collecting data for 1 minute
	collectDuration := 1 * time.Minute
	collectInterval := 1 * time.Second
	endTime := time.Now().Add(collectDuration)

	pointCount := 0
	for time.Now().Before(endTime) {
		batch := collectFromSensors(sensors)
		if err := db.WriteBatch(batch); err != nil {
			log.Printf("Write error: %v", err)
		}
		pointCount += len(batch)
		fmt.Printf("\rCollected %d points...", pointCount)
		time.Sleep(collectInterval)
	}
	fmt.Println()

	// Flush and query
	db.Flush()

	log.Println()
	log.Println("=== Query Results ===")
	log.Println()

	// Query: Average temperature per location
	log.Println("Average temperature by location (last hour):")
	result, err := db.Execute(&chronicle.Query{
		Metric:  "temperature",
		Start:   time.Now().Add(-time.Hour).UnixNano(),
		End:     time.Now().UnixNano(),
		GroupBy: []string{"location"},
		Aggregation: &chronicle.Aggregation{
			Function: chronicle.AggMean,
			Window:   time.Hour,
		},
	})
	if err != nil {
		log.Printf("Query error: %v", err)
	} else {
		for _, p := range result.Points {
			log.Printf("  %s: %.2f°C", p.Tags["location"], p.Value)
		}
	}

	// Query: Min/Max humidity
	log.Println("\nHumidity range by location:")
	for _, loc := range []string{"warehouse-a", "warehouse-b"} {
		minResult, _ := db.Execute(&chronicle.Query{
			Metric: "humidity",
			Tags:   map[string]string{"location": loc},
			Start:  time.Now().Add(-time.Hour).UnixNano(),
			End:    time.Now().UnixNano(),
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggMin,
				Window:   time.Hour,
			},
		})
		maxResult, _ := db.Execute(&chronicle.Query{
			Metric: "humidity",
			Tags:   map[string]string{"location": loc},
			Start:  time.Now().Add(-time.Hour).UnixNano(),
			End:    time.Now().UnixNano(),
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggMax,
				Window:   time.Hour,
			},
		})
		if len(minResult.Points) > 0 && len(maxResult.Points) > 0 {
			log.Printf("  %s: %.1f%% - %.1f%%", loc, minResult.Points[0].Value, maxResult.Points[0].Value)
		}
	}

	// Show cardinality stats
	stats := db.CardinalityStats()
	log.Printf("\nDatabase stats:")
	log.Printf("  Total series: %d", stats.TotalSeries)
	log.Printf("  Total points collected: %d", pointCount)

	log.Println("\nDone!")
}

func collectFromSensors(sensors []Sensor) []chronicle.Point {
	now := time.Now().UnixNano()
	points := make([]chronicle.Point, 0, len(sensors))

	for _, s := range sensors {
		// Simulate sensor reading with some noise
		baseValue := (s.MinValue + s.MaxValue) / 2
		noise := (rand.Float64() - 0.5) * (s.MaxValue - s.MinValue) * 0.3
		// Add time-based variation (simulating day/night cycle)
		hourOfDay := float64(time.Now().Hour())
		timeVariation := math.Sin(hourOfDay/24*2*math.Pi) * (s.MaxValue - s.MinValue) * 0.1

		value := baseValue + noise + timeVariation

		points = append(points, chronicle.Point{
			Metric: s.Type,
			Tags: map[string]string{
				"sensor_id": s.ID,
				"location":  s.Location,
			},
			Value:     value,
			Timestamp: now,
		})
	}

	return points
}
