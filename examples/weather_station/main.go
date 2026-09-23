package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/chronicle-db/chronicle"
)

func main() {
	// 1. Initialize Chronicle database
	cfg := chronicle.DefaultConfig()
	db, err := chronicle.NewDB(cfg)
	if err != nil {
		log.Fatalf("failed to initialize Chronicle DB: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	stations := []string{"station-hanoi", "station-danang", "station-hcmc"}
	baseTemp := map[string]float64{
		"station-hanoi": 24.5,
		"station-danang": 28.0,
		"station-hcmc": 31.2,
	}

	fmt.Println("=== 1. Ingesting Weather Station Sensor Data ===")
	now := time.Now().UnixNano()
	var points []chronicle.Point

	for _, st := range stations {
		for i := 0; i < 60; i++ {
			t := now - int64(i)*int64(time.Minute)
			temp := baseTemp[st] + (rand.Float64()*4.0 - 2.0)
			humidity := 65.0 + (rand.Float64()*20.0 - 10.0)
			pressure := 1013.25 + (rand.Float64()*10.0 - 5.0)

			points = append(points,
				chronicle.Point{
					Metric:    "weather.temperature",
					Tags:      map[string]string{"station": st, "region": "VN"},
					Value:     temp,
					Timestamp: t,
				},
				chronicle.Point{
					Metric:    "weather.humidity",
					Tags:      map[string]string{"station": st, "region": "VN"},
					Value:     humidity,
					Timestamp: t,
				},
				chronicle.Point{
					Metric:    "weather.pressure",
					Tags:      map[string]string{"station": st, "region": "VN"},
					Value:     pressure,
					Timestamp: t,
				},
			)
		}
	}

	if err := db.WriteBatchContext(ctx, points); err != nil {
		log.Fatalf("failed to write weather batch: %v", err)
	}
	fmt.Printf("Successfully ingested %d weather data points\n", len(points))

	fmt.Println("\n=== 2. Querying Average Temperature Per Station ===")
	for _, st := range stations {
		res, err := db.ExecuteContext(ctx, &chronicle.Query{
			Metric: "weather.temperature",
			Tags:   map[string]string{"station": st},
		})
		if err != nil {
			log.Printf("error querying %s: %v", st, err)
			continue
		}
		var sum float64
		for _, pt := range res.Points {
			sum += pt.Value
		}
		avg := sum / float64(len(res.Points))
		fmt.Printf("Station %s: %d readings, Avg Temp: %.2f °C\n", st, len(res.Points), avg)
	}

	fmt.Println("\n=== 3. Querying Humidity Outliers (> 75%) ===")
	hRes, err := db.ExecuteContext(ctx, &chronicle.Query{
		Metric: "weather.humidity",
	})
	if err == nil {
		highCount := 0
		for _, pt := range hRes.Points {
			if pt.Value > 75.0 {
				highCount++
			}
		}
		fmt.Printf("Total high-humidity sensor events (>75%%): %d\n", highCount)
	}

	fmt.Println("\n=== Weather station demonstration completed successfully ===")
}
