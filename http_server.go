package chronicle

import (
	"context"
	"fmt"
	"log/slog"
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
		h = securityHeadersMiddleware(h)
		h = csrfProtectionMiddleware(h)
		h = bodySizeLimitMiddleware(h)
		h = requestIDMiddleware(h)
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
	setupAdminRoutes(mux, db, wrap, auth)
	setupAlertingRoutes(mux, db, wrap, auth)
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

	// Wrap the mux with a global body size limit to protect all handlers,
	// including feature handlers registered via RegisterHTTPHandlers.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && r.ContentLength != 0 {
			r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		}
		mux.ServeHTTP(w, r)
	})

	srv := &http.Server{
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	s := &httpServer{srv: srv, rl: rl}

	s.wg.Add(1)
	go func(srv *http.Server) {
		defer s.wg.Done()
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "err", err)
		}
	}(srv)

	return s, nil
}

func (s *httpServer) Close() error {
	if s == nil || s.srv == nil {
		return nil
	}
	if s.rl != nil {
		s.rl.Stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.srv.Shutdown(ctx)
	s.wg.Wait()
	return err
}
