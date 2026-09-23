// Package main demonstrates Chronicle running as a Kubernetes-style sidecar
// that scrapes Prometheus metrics from a co-located application on localhost.
//
// Locally this process:
//  1. Serves a demo app /metrics endpoint on :9090
//  2. Starts Chronicle K8sSidecar scraping that endpoint every few seconds
//  3. Tags scraped points with Downward-API-style POD_* environment variables
//  4. Exposes sidecar health/stats on :8080
//
// Run from this directory:
//
//	POD_NAME=demo-0 POD_NAMESPACE=default NODE_NAME=local go run .
//
// Then:
//
//	curl -s localhost:9090/metrics | head
//	curl -s localhost:8080/health
//	curl -s localhost:8080/stats
//
// See manifests/ for a real multi-container Deployment that mirrors this layout.
package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chronicle-db/chronicle"
)

func main() {
	setDefaultEnv("POD_NAME", "demo-sidecar-0")
	setDefaultEnv("POD_NAMESPACE", "default")
	setDefaultEnv("NODE_NAME", "local-node")
	setDefaultEnv("POD_IP", "127.0.0.1")
	setDefaultEnv("HOST_IP", "127.0.0.1")

	go serveDemoAppMetrics(":9090")

	dbPath := "sidecar_demo.db"
	_ = os.Remove(dbPath)
	_ = os.Remove(dbPath + "-wal")
	_ = os.Remove(dbPath + "-shm")

	db, err := chronicle.Open(dbPath, chronicle.Config{
		Path:              dbPath,
		MaxMemory:         32 * 1024 * 1024,
		PartitionDuration: time.Hour,
		RetentionDuration: 24 * time.Hour,
		BufferSize:        10_000,
	})
	if err != nil {
		log.Fatalf("open chronicle: %v", err)
	}
	defer db.Close()

	cfg := chronicle.DefaultK8sSidecarConfig()
	cfg.Enabled = true
	cfg.DiscoveryEnabled = false // local demo has no Kubernetes API
	cfg.ScrapeInterval = 3 * time.Second
	cfg.ScrapeTimeout = 2 * time.Second
	cfg.MetricsPort = 9090
	cfg.MetricsPath = "/metrics"
	cfg.HealthPort = 8080
	cfg.AddPodName = true
	cfg.AddNamespace = true
	cfg.AddNodeName = true

	sidecar, err := chronicle.NewK8sSidecar(db, cfg)
	if err != nil {
		log.Fatalf("new sidecar: %v", err)
	}
	if err := sidecar.Start(); err != nil {
		log.Fatalf("start sidecar: %v", err)
	}
	defer sidecar.Stop()

	log.Printf("Chronicle K8s sidecar demo started")
	log.Printf("  app metrics : http://localhost:9090/metrics")
	log.Printf("  sidecar health: http://localhost:8080/health")
	log.Printf("  sidecar stats : http://localhost:8080/stats")
	log.Printf("  pod tags      : name=%s ns=%s node=%s",
		os.Getenv("POD_NAME"), os.Getenv("POD_NAMESPACE"), os.Getenv("NODE_NAME"))

	// Periodically show that scraped points landed in Chronicle.
	go func() {
		for {
			time.Sleep(6 * time.Second)
			stats := sidecar.GetStats()
			result, qerr := db.Execute(&chronicle.Query{
				Metric: "demo_http_requests_total",
				Start:  time.Now().Add(-2 * time.Minute).UnixNano(),
				End:    time.Now().UnixNano(),
			})
			n := 0
			if qerr == nil && result != nil {
				n = len(result.Points)
			}
			log.Printf("scrape_count=%d scrape_errors=%d stored_points(demo_http_requests_total)=%d",
				stats.ScrapeCount, stats.ScrapeErrors, n)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")
}

func setDefaultEnv(key, value string) {
	if os.Getenv(key) == "" {
		_ = os.Setenv(key, value)
	}
}

func serveDemoAppMetrics(addr string) {
	var requests atomic.Uint64
	var inFlight atomic.Int64

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		inFlight.Add(1)
		defer inFlight.Add(-1)

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP demo_http_requests_total Total demo HTTP metrics scrapes served.\n")
		fmt.Fprintf(w, "# TYPE demo_http_requests_total counter\n")
		fmt.Fprintf(w, "demo_http_requests_total{handler=\"metrics\"} %d\n", requests.Load())
		fmt.Fprintf(w, "# HELP demo_http_in_flight Current in-flight scrapes.\n")
		fmt.Fprintf(w, "# TYPE demo_http_in_flight gauge\n")
		fmt.Fprintf(w, "demo_http_in_flight %d\n", inFlight.Load())
		fmt.Fprintf(w, "# HELP demo_cpu_usage_ratio Synthetic CPU usage ratio.\n")
		fmt.Fprintf(w, "# TYPE demo_cpu_usage_ratio gauge\n")
		fmt.Fprintf(w, "demo_cpu_usage_ratio %.3f\n", 0.15+rand.Float64()*0.4)
		fmt.Fprintf(w, "# HELP demo_mem_bytes Synthetic memory usage in bytes.\n")
		fmt.Fprintf(w, "# TYPE demo_mem_bytes gauge\n")
		fmt.Fprintf(w, "demo_mem_bytes %d\n", 32*1024*1024+rand.Intn(8*1024*1024))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	log.Printf("demo app metrics listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("demo app metrics server: %v", err)
	}
}
