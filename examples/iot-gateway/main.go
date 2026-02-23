// IoT Edge Gateway Example
//
// This example demonstrates a complete IoT edge gateway using Chronicle:
// - Simulates 10 sensors reporting temperature/humidity/pressure
// - Stores all data locally in Chronicle
// - Exposes HTTP API for querying
// - Health checks for K8s readiness/liveness
// - Automatic retention and downsampling
//
// Run:
//
//	go run main.go
//
// Then:
//
//	curl http://localhost:8080/health
//	curl http://localhost:8080/api/v1/query -d '{"metric":"temperature"}'
//	curl http://localhost:8080/metrics
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chronicle-db/chronicle"
)

const (
	numSensors     = 10
	reportInterval = 5 * time.Second
	httpPort       = 8080
	dbPath         = "iot-gateway.db"
)

func main() {
	log.Println("🏭 IoT Edge Gateway starting...")

	// Configure Chronicle for edge deployment
	cfg := chronicle.DefaultConfig(dbPath)
	cfg.Storage.MaxMemory = 64 * 1024 * 1024         // 64MB memory budget
	cfg.Storage.BufferSize = 1000                      // Buffer 1000 points
	cfg.Storage.PartitionDuration = 30 * time.Minute   // 30-minute partitions
	cfg.Retention.RetentionDuration = 24 * time.Hour   // Keep 24 hours of data
	cfg.HTTP.HTTPEnabled = true
	cfg.HTTP.HTTPPort = httpPort

	// Open database
	db, err := chronicle.Open(dbPath, cfg)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	log.Printf("📦 Database opened at %s", dbPath)

	// Start sensor simulation
	stopSensors := make(chan struct{})
	go simulateSensors(db, stopSensors)
	log.Printf("🌡️  Simulating %d sensors every %s", numSensors, reportInterval)

	// Start custom query endpoint
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/gateway/sensors", func(w http.ResponseWriter, r *http.Request) {
		metrics := db.Metrics()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"sensors":      numSensors,
			"metrics":      metrics,
			"metric_count": len(metrics),
			"uptime":       time.Since(time.Now()).String(),
		})
	})
	mux.HandleFunc("/api/v1/gateway/latest", func(w http.ResponseWriter, r *http.Request) {
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			metric = "temperature"
		}
		result, err := db.Execute(&chronicle.Query{
			Metric: metric,
			Limit:  10,
		})
		if err != nil {
			log.Printf("[ERROR] query failed: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	go func() {
		addr := fmt.Sprintf(":%d", httpPort+1)
		log.Printf("🌐 Gateway API listening on %s", addr)
		http.ListenAndServe(addr, mux)
	}()

	log.Printf("✅ Gateway ready. Chronicle HTTP API on :%d, Gateway API on :%d", httpPort, httpPort+1)

	// Wait for shutdown signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("🛑 Shutting down...")
	close(stopSensors)
	db.Close()
	log.Println("👋 Goodbye!")
}

func simulateSensors(db *chronicle.DB, stop chan struct{}) {
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	locations := []string{"warehouse-A", "warehouse-B", "outdoor", "cold-storage", "server-room"}

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			now := time.Now().UnixNano()
			var batch []chronicle.Point

			for i := 0; i < numSensors; i++ {
				sensorID := fmt.Sprintf("sensor-%03d", i)
				location := locations[i%len(locations)]

				// Temperature: 15-30°C with daily seasonality
				hour := float64(time.Now().Hour())
				tempBase := 20.0 + 5.0*math.Sin(hour/24.0*2*math.Pi)
				temp := tempBase + rand.Float64()*3 - 1.5

				// Humidity: 40-80%
				humidity := 60.0 + rand.Float64()*20 - 10

				// Pressure: 1010-1020 hPa
				pressure := 1015.0 + rand.Float64()*5 - 2.5

				// Battery: slowly draining
				battery := 3.3 - float64(i)*0.01 - rand.Float64()*0.1

				tags := map[string]string{
					"sensor":   sensorID,
					"location": location,
				}

				batch = append(batch,
					chronicle.Point{Metric: "temperature", Value: temp, Tags: tags, Timestamp: now},
					chronicle.Point{Metric: "humidity", Value: humidity, Tags: tags, Timestamp: now},
					chronicle.Point{Metric: "pressure", Value: pressure, Tags: tags, Timestamp: now},
					chronicle.Point{Metric: "battery_voltage", Value: battery, Tags: tags, Timestamp: now},
				)
			}

			if err := db.WriteBatch(batch); err != nil {
				log.Printf("⚠️  Write error: %v", err)
			}
		}
	}
}
