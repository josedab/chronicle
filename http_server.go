package chronicle

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

func startHTTPServer(db *DB, port int) (*httpServer, error) {
	if port <= 0 || port > 65535 {
		port = 8086
	}

	// Rate limiter from config or default
	rateLimit := db.config.RateLimitPerSecond
	if rateLimit <= 0 {
		rateLimit = 1000
	}
	var rl *rateLimiter
	if rateLimit > 0 {
		rl = newRateLimiter(rateLimit, time.Second)
	}

	// Authentication from config
	auth := newAuthenticator(db.config.Auth)

	// Helper to wrap handlers with middleware
	wrap := func(h http.HandlerFunc) http.HandlerFunc {
		h = authMiddleware(auth, h)
		if rl != nil {
			h = rateLimitMiddleware(rl, h)
		}
		return h
	}

	mux := http.NewServeMux()

	// Setup route groups
	setupWriteRoutes(mux, db, wrap)
	setupQueryRoutes(mux, db, wrap)
	setupPrometheusRoutes(mux, db, wrap)
	setupAdminRoutes(mux, db, wrap)
	setupAlertingRoutes(mux, db, wrap)
	setupFeatureRoutes(mux, db, wrap)
	setupNextGenRoutes(mux, db, wrap)

	// Setup ClickHouse-compatible routes if enabled
	if db.config.ClickHouse != nil && db.config.ClickHouse.Enabled {
		setupClickHouseRoutes(mux, db, *db.config.ClickHouse, wrap)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	srv := &http.Server{
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		_ = srv.Serve(listener)
	}()

	return &httpServer{srv: srv}, nil
}

func (s *httpServer) Close() error {
	if s == nil || s.srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}
