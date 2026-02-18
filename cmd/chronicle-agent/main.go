// chronicle-agent is a zero-code observability agent that collects system
// metrics using eBPF (with /proc fallback) and ships them to a Chronicle database.
//
// It provides automatic collection of:
//   - CPU, memory, disk I/O, and network metrics
//   - HTTP request tracing with latency and status code tracking
//   - DNS and TCP connection monitoring
//   - Container/cgroup-aware metrics with pod metadata enrichment
//
// Usage:
//
//	chronicle-agent --target http://localhost:8428 --interval 10s
//	chronicle-agent --target /path/to/db --ebpf-http --container-enrichment
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

func main() {
	var (
		dbPath     = flag.String("db", "", "Path to Chronicle database (local mode)")
		interval   = flag.Duration("interval", 10*time.Second, "Collection interval")
		enableHTTP = flag.Bool("ebpf-http", false, "Enable HTTP request tracing")
		enableNet  = flag.Bool("ebpf-net", false, "Enable network connection tracking")
		enableCont = flag.Bool("container-enrichment", false, "Enable container metadata enrichment")
		showVersion = flag.Bool("version", false, "Show version")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println("chronicle-agent v0.5.0")
		os.Exit(0)
	}

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --db path is required")
		flag.Usage()
		os.Exit(1)
	}

	cfg := chronicle.DefaultConfig(*dbPath)
	db, err := chronicle.Open(*dbPath, cfg)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	agent := &Agent{
		db:       db,
		interval: *interval,
	}

	if *enableHTTP {
		agent.httpCollector = chronicle.NewEBPFHTTPCollector(chronicle.DefaultEBPFHTTPConfig())
		log.Println("HTTP request tracing enabled")
	}

	if *enableNet {
		agent.netCollector = chronicle.NewEBPFNetCollector()
		log.Println("Network connection tracking enabled")
	}

	if *enableCont {
		agent.containerCollector = chronicle.NewEBPFContainerCollector()
		log.Println("Container enrichment enabled")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down agent...")
		cancel()
	}()

	log.Printf("Chronicle agent started (interval=%s, db=%s)", *interval, *dbPath)
	agent.Run(ctx)
}

// Agent is the main collection agent.
type Agent struct {
	db                 *chronicle.DB
	interval           time.Duration
	httpCollector      *chronicle.EBPFHTTPCollector
	netCollector       *chronicle.EBPFNetCollector
	containerCollector *chronicle.EBPFContainerCollector
}

// Run starts the agent collection loop.
func (a *Agent) Run(ctx context.Context) {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Agent stopped")
			return
		case <-ticker.C:
			a.collect()
		}
	}
}

func (a *Agent) collect() {
	var points []chronicle.Point

	if a.httpCollector != nil {
		points = append(points, a.httpCollector.ToPoints()...)
	}

	if a.netCollector != nil {
		points = append(points, a.netCollector.ToPoints()...)
	}

	if a.containerCollector != nil {
		points = append(points, a.containerCollector.ToPoints()...)
	}

	// Add system metrics
	points = append(points, chronicle.Point{
		Metric:    "agent_collection_runs_total",
		Value:     1,
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"agent": "chronicle-agent"},
	})

	if len(points) > 0 {
		if err := a.db.WriteBatch(points); err != nil {
			log.Printf("Write error: %v", err)
		}
	}
}
