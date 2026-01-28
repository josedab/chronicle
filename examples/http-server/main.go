// Package main demonstrates a Chronicle HTTP server with full API.
//
// Run: go run main.go
// Then: curl http://localhost:8086/health
package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chronicle-db/chronicle"
)

func main() {
	// Create database with HTTP API enabled
	db, err := chronicle.Open("metrics.db", chronicle.Config{
		Path:                         "metrics.db",
		MaxMemory:                    128 * 1024 * 1024, // 128MB
		PartitionDuration:            time.Hour,
		RetentionDuration:            7 * 24 * time.Hour, // 7 days
		HTTPEnabled:                  true,
		HTTPPort:                     8086,
		PrometheusRemoteWriteEnabled: true,
		BufferSize:                   10_000,
		QueryTimeout:                 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	log.Println("Chronicle HTTP server started on http://localhost:8086")
	log.Println("")
	log.Println("Available endpoints:")
	log.Println("  POST /write              - Write data (line protocol or JSON)")
	log.Println("  POST /query              - Query data")
	log.Println("  GET  /metrics            - List all metrics")
	log.Println("  GET  /health             - Health check")
	log.Println("  GET  /api/v1/query       - PromQL instant query")
	log.Println("  GET  /api/v1/query_range - PromQL range query")
	log.Println("  POST /prometheus/write   - Prometheus remote write")
	log.Println("  POST /graphql            - GraphQL endpoint")
	log.Println("  GET  /graphql/playground - GraphQL playground")
	log.Println("  GET  /admin              - Admin dashboard")
	log.Println("")

	// Generate sample data in background
	go generateSampleData(db)

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
}

func generateSampleData(db *chronicle.DB) {
	hosts := []string{"web-1", "web-2", "web-3", "db-1", "cache-1"}
	metrics := []string{"cpu_usage", "memory_used", "disk_io", "network_rx", "network_tx"}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Println("Generating sample metrics every 5 seconds...")

	for range ticker.C {
		var points []chronicle.Point
		now := time.Now().UnixNano()

		for _, host := range hosts {
			for _, metric := range metrics {
				points = append(points, chronicle.Point{
					Metric:    metric,
					Tags:      map[string]string{"host": host, "env": "demo"},
					Value:     rand.Float64() * 100,
					Timestamp: now,
				})
			}
		}

		if err := db.WriteBatch(points); err != nil {
			log.Printf("Write error: %v", err)
		} else {
			fmt.Printf(".")
		}
	}
}
