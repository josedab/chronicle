package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// WebhookConfig configures the webhook notification system.
type WebhookConfig struct {
	Enabled       bool          `json:"enabled"`
	MaxWebhooks   int           `json:"max_webhooks"`
	RetryAttempts int           `json:"retry_attempts"`
	RetryDelay    time.Duration `json:"retry_delay"`
}

// DefaultWebhookConfig returns sensible defaults for WebhookConfig.
func DefaultWebhookConfig() WebhookConfig {
	return WebhookConfig{
		Enabled:       true,
		MaxWebhooks:   50,
		RetryAttempts: 3,
		RetryDelay:    5 * time.Second,
	}
}

// WebhookEndpoint represents a registered webhook endpoint.
type WebhookEndpoint struct {
	ID      string            `json:"id"`
	URL     string            `json:"url"`
	Events  []string          `json:"events"`
	Headers map[string]string `json:"headers"`
	Active  bool              `json:"active"`
	Secret  string            `json:"secret"`
}

// WebhookDelivery represents a delivery attempt for a webhook event.
type WebhookDelivery struct {
	ID          string    `json:"id"`
	EndpointID  string    `json:"endpoint_id"`
	Event       string    `json:"event"`
	Payload     string    `json:"payload"`
	Status      string    `json:"status"` // pending, delivered, failed
	Attempts    int       `json:"attempts"`
	LastAttempt time.Time `json:"last_attempt"`
}

// WebhookStats holds aggregate webhook statistics.
type WebhookStats struct {
	TotalEndpoints  int   `json:"total_endpoints"`
	TotalDeliveries int64 `json:"total_deliveries"`
	TotalPending    int64 `json:"total_pending"`
	TotalDelivered  int64 `json:"total_delivered"`
	TotalFailed     int64 `json:"total_failed"`
}

// WebhookEngine manages webhook endpoints and event delivery.
type WebhookEngine struct {
	db         *DB
	config     WebhookConfig
	mu         sync.RWMutex
	endpoints  map[string]*WebhookEndpoint
	deliveries []WebhookDelivery
	stats      WebhookStats
	nextID     int64
	stopCh     chan struct{}
	running    bool
}

// NewWebhookEngine creates a new WebhookEngine.
func NewWebhookEngine(db *DB, cfg WebhookConfig) *WebhookEngine {
	return &WebhookEngine{
		db:        db,
		config:    cfg,
		endpoints: make(map[string]*WebhookEndpoint),
		stopCh:    make(chan struct{}),
	}
}

// Start begins the webhook engine.
func (e *WebhookEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop halts the webhook engine.
func (e *WebhookEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// Register adds a new webhook endpoint.
func (e *WebhookEngine) Register(ep WebhookEndpoint) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.endpoints) >= e.config.MaxWebhooks {
		return "", fmt.Errorf("webhook: max endpoints reached (%d)", e.config.MaxWebhooks)
	}
	if ep.ID == "" {
		e.nextID++
		ep.ID = fmt.Sprintf("wh_%d", e.nextID)
	}
	cp := ep
	e.endpoints[ep.ID] = &cp
	e.stats.TotalEndpoints = len(e.endpoints)
	return ep.ID, nil
}

// Unregister removes a webhook endpoint by ID.
func (e *WebhookEngine) Unregister(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.endpoints[id]; !ok {
		return false
	}
	delete(e.endpoints, id)
	e.stats.TotalEndpoints = len(e.endpoints)
	return true
}

// Emit broadcasts an event to all matching endpoints and records delivery records.
func (e *WebhookEngine) Emit(event string, payload string) []WebhookDelivery {
	e.mu.Lock()
	defer e.mu.Unlock()
	var deliveries []WebhookDelivery
	for _, ep := range e.endpoints {
		if !ep.Active {
			continue
		}
		if !webhookEndpointMatchesEvent(ep, event) {
			continue
		}
		e.nextID++
		d := WebhookDelivery{
			ID:          fmt.Sprintf("del_%d", e.nextID),
			EndpointID:  ep.ID,
			Event:       event,
			Payload:     payload,
			Status:      "pending",
			Attempts:    1,
			LastAttempt: time.Now(),
		}
		deliveries = append(deliveries, d)
		e.deliveries = append(e.deliveries, d)
		e.stats.TotalDeliveries++
		e.stats.TotalPending++
	}
	return deliveries
}

// ListEndpoints returns all registered endpoints.
func (e *WebhookEngine) ListEndpoints() []WebhookEndpoint {
	e.mu.RLock()
	defer e.mu.RUnlock()
	eps := make([]WebhookEndpoint, 0, len(e.endpoints))
	for _, ep := range e.endpoints {
		eps = append(eps, *ep)
	}
	return eps
}

// ListDeliveries returns all delivery records, optionally filtered by event.
func (e *WebhookEngine) ListDeliveries(event string) []WebhookDelivery {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if event == "" {
		result := make([]WebhookDelivery, len(e.deliveries))
		copy(result, e.deliveries)
		return result
	}
	var result []WebhookDelivery
	for _, d := range e.deliveries {
		if d.Event == event {
			result = append(result, d)
		}
	}
	return result
}

// GetStats returns webhook statistics.
func (e *WebhookEngine) GetStats() WebhookStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

func webhookEndpointMatchesEvent(ep *WebhookEndpoint, event string) bool {
	if len(ep.Events) == 0 {
		return true
	}
	for _, ev := range ep.Events {
		if ev == event {
			return true
		}
	}
	return false
}

// RegisterHTTPHandlers registers webhook HTTP endpoints.
func (e *WebhookEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/webhooks/endpoints", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListEndpoints())
	})
	mux.HandleFunc("/api/v1/webhooks/deliveries", func(w http.ResponseWriter, r *http.Request) {
		event := r.URL.Query().Get("event")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListDeliveries(event))
	})
	mux.HandleFunc("/api/v1/webhooks/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
	mux.HandleFunc("/api/v1/webhooks/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var ep WebhookEndpoint
		if err := json.NewDecoder(r.Body).Decode(&ep); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		id, err := e.Register(ep)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"id": id})
	})
	mux.HandleFunc("/api/v1/webhooks/emit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Event   string `json:"event"`
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		deliveries := e.Emit(req.Event, req.Payload)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(deliveries)
	})
}
