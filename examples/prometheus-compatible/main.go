// Package main demonstrates Chronicle as a Prometheus-compatible backend.
//
// This example shows how to:
// - Enable Prometheus remote write ingestion
// - Query data using PromQL-style queries
// - Export metrics in Prometheus format
//
// Run: go run main.go
// Then configure Prometheus to remote_write to http://localhost:8086/prometheus/write
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
	// Create database with Prometheus compatibility
	db, err := chronicle.Open("prometheus_data.db", chronicle.Config{
		Path:                         "prometheus_data.db",
		MaxMemory:                    256 * 1024 * 1024,
		PartitionDuration:            time.Hour,
		RetentionDuration:            30 * 24 * time.Hour, // 30 days
		HTTPEnabled:                  true,
		HTTPPort:                     8086,
		PrometheusRemoteWriteEnabled: true,
		BufferSize:                   50_000,
		QueryTimeout:                 60 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	log.Println("Chronicle Prometheus Backend started on http://localhost:8086")
	log.Println()
	log.Println("Prometheus Configuration:")
	log.Println("  Add to prometheus.yml:")
	log.Println("  remote_write:")
	log.Println("    - url: http://localhost:8086/prometheus/write")
	log.Println()
	log.Println("PromQL Endpoints:")
	log.Println("  GET /api/v1/query       - Instant query")
	log.Println("  GET /api/v1/query_range - Range query")
	log.Println()

	// Generate sample Prometheus-style metrics
	go generatePrometheusMetrics(db)

	// Demonstrate PromQL-style queries
	go func() {
		time.Sleep(10 * time.Second) // Wait for some data

		log.Println()
		log.Println("=== Query Examples ===")
		log.Println()

		// Example 1: Simple query
		log.Println("1. Simple query: http_requests_total with method=GET")
		result, err := db.Execute(&chronicle.Query{
			Metric: "http_requests_total",
			Tags:   map[string]string{"method": "GET"},
			Start:  time.Now().Add(-5 * time.Minute).UnixNano(),
			End:    time.Now().UnixNano(),
		})
		if err != nil {
			log.Printf("   Error: %v", err)
		} else {
			log.Printf("   Found %d points", len(result.Points))
		}

		// Example 2: Rate calculation (using AggRate)
		log.Println("\n2. Rate: http_requests_total over 5 minutes")
		result, err = db.Execute(&chronicle.Query{
			Metric:  "http_requests_total",
			Start:   time.Now().Add(-5 * time.Minute).UnixNano(),
			End:     time.Now().UnixNano(),
			GroupBy: []string{"handler"},
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggRate,
				Window:   time.Minute,
			},
		})
		if err != nil {
			log.Printf("   Error: %v", err)
		} else {
			for _, p := range result.Points {
				if p.Tags["handler"] != "" {
					log.Printf("   %s: %.4f req/s", p.Tags["handler"], p.Value)
				}
			}
		}

		// Example 3: Aggregation
		log.Println("\n3. Sum by method: http_requests_total")
		result, err = db.Execute(&chronicle.Query{
			Metric:  "http_requests_total",
			Start:   time.Now().Add(-5 * time.Minute).UnixNano(),
			End:     time.Now().UnixNano(),
			GroupBy: []string{"method"},
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggSum,
				Window:   5 * time.Minute,
			},
		})
		if err != nil {
			log.Printf("   Error: %v", err)
		} else {
			for _, p := range result.Points {
				if p.Tags["method"] != "" {
					log.Printf("   %s: %.0f", p.Tags["method"], p.Value)
				}
			}
		}

		log.Println("\nPromQL queries available via HTTP:")
		log.Println("  curl 'http://localhost:8086/api/v1/query?query=up'")
		log.Println("  curl 'http://localhost:8086/api/v1/query?query=http_requests_total{method=\"GET\"}'")
	}()

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("\nShutting down...")
}

func generatePrometheusMetrics(db *chronicle.DB) {
	// Simulate Prometheus-style metrics
	handlers := []string{"/api/users", "/api/orders", "/api/products", "/health"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	statuses := []string{"200", "201", "400", "404", "500"}
	instances := []string{"web-1:8080", "web-2:8080", "web-3:8080"}

	// Counters for request totals
	requestCounters := make(map[string]float64)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	log.Println("Generating Prometheus-style metrics...")

	for range ticker.C {
		now := time.Now().UnixNano()
		var points []chronicle.Point

		// Generate http_requests_total (counter)
		for _, instance := range instances {
			for _, handler := range handlers {
				for _, method := range methods {
					// Only some combinations make sense
					if method == "DELETE" && handler == "/health" {
						continue
					}

					status := statuses[rand.Intn(len(statuses))]
					key := fmt.Sprintf("%s_%s_%s_%s", instance, handler, method, status)

					// Increment counter
					increment := float64(rand.Intn(10))
					requestCounters[key] += increment

					points = append(points, chronicle.Point{
						Metric: "http_requests_total",
						Tags: map[string]string{
							"instance": instance,
							"handler":  handler,
							"method":   method,
							"status":   status,
							"job":      "api-server",
						},
						Value:     requestCounters[key],
						Timestamp: now,
					})
				}
			}
		}

		// Generate http_request_duration_seconds (histogram buckets)
		buckets := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
		for _, instance := range instances {
			for _, handler := range handlers {
				// Simulate histogram buckets
				var cumulative float64
				for _, le := range buckets {
					cumulative += float64(rand.Intn(100))
					points = append(points, chronicle.Point{
						Metric: "http_request_duration_seconds_bucket",
						Tags: map[string]string{
							"instance": instance,
							"handler":  handler,
							"le":       fmt.Sprintf("%g", le),
						},
						Value:     cumulative,
						Timestamp: now,
					})
				}
				// +Inf bucket
				cumulative += float64(rand.Intn(10))
				points = append(points, chronicle.Point{
					Metric: "http_request_duration_seconds_bucket",
					Tags: map[string]string{
						"instance": instance,
						"handler":  handler,
						"le":       "+Inf",
					},
					Value:     cumulative,
					Timestamp: now,
				})
			}
		}

		// Generate up metric (gauge)
		for _, instance := range instances {
			points = append(points, chronicle.Point{
				Metric: "up",
				Tags: map[string]string{
					"instance": instance,
					"job":      "api-server",
				},
				Value:     1,
				Timestamp: now,
			})
		}

		// Generate process metrics (gauges)
		for _, instance := range instances {
			points = append(points, chronicle.Point{
				Metric:    "process_cpu_seconds_total",
				Tags:      map[string]string{"instance": instance},
				Value:     float64(time.Now().Unix()) * 0.1 * rand.Float64(),
				Timestamp: now,
			})
			points = append(points, chronicle.Point{
				Metric:    "process_resident_memory_bytes",
				Tags:      map[string]string{"instance": instance},
				Value:     100*1024*1024 + rand.Float64()*50*1024*1024,
				Timestamp: now,
			})
		}

		if err := db.WriteBatch(points); err != nil {
			log.Printf("Write error: %v", err)
		}
	}
}
