package oteldistro

import (
	"context"
	"encoding/json"
	"net/http"
)

func (e *HealthCheckExtension) Start(ctx context.Context, host Host) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", e.handleHealth)

	e.server = &http.Server{
		Addr:    e.config.Endpoint,
		Handler: mux,
	}

	go e.server.ListenAndServe()
	return nil
}

func (e *HealthCheckExtension) Shutdown(ctx context.Context) error {
	if e.server != nil {
		return e.server.Shutdown(ctx)
	}
	return nil
}

func (e *HealthCheckExtension) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"uptime": e.distro.GetMetrics().Uptime.String(),
	})
}
