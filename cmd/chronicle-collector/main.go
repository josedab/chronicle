package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

func main() {
	var (
		configFile = flag.String("config", "", "Path to collector config file (JSON)")
		dataDir    = flag.String("data-dir", "./data", "Data directory for Chronicle storage")
		listenAddr = flag.String("listen", ":8888", "HTTP listen address for management API")
		grpcAddr   = flag.String("grpc", ":4317", "gRPC listen address for OTLP receiver")
		httpOTLP   = flag.String("http-otlp", ":4318", "HTTP listen address for OTLP receiver")
	)
	flag.Parse()

	// Open Chronicle database
	db, err := chronicle.Open(*dataDir, chronicle.DefaultConfig(*dataDir))
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Load or create config
	cfg := chronicle.DefaultOTelCollectorDistroConfig()
	if *configFile != "" {
		data, err := os.ReadFile(*configFile)
		if err != nil {
			log.Fatalf("failed to read config: %v", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			log.Fatalf("failed to parse config: %v", err)
		}
	}

	// Apply CLI overrides
	if *grpcAddr != ":4317" {
		for i := range cfg.Receivers {
			if cfg.Receivers[i].Type == chronicle.OTelReceiverOTLP {
				cfg.Receivers[i].Endpoint = *grpcAddr
			}
		}
	}

	// Create and start collector
	collector := chronicle.NewOTelCollectorDistro(db, cfg)
	if err := collector.Start(); err != nil {
		log.Fatalf("failed to start collector: %v", err)
	}
	defer collector.Stop()

	// Set up HTTP management server
	mux := http.NewServeMux()
	collector.RegisterHTTPHandlers(mux)

	// Health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "healthy",
			"uptime": time.Since(time.Now()).String(),
		})
	})

	// Ready endpoint
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ready")
	})

	server := &http.Server{
		Addr:         *listenAddr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// Start HTTP server
	go func() {
		log.Printf("Chronicle Collector starting on %s (management), %s (gRPC), %s (HTTP OTLP)",
			*listenAddr, *grpcAddr, *httpOTLP)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down collector...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server.Shutdown(ctx)
	log.Println("Collector stopped")
}
