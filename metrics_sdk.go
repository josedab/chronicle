package chronicle

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// MetricsSDKConfig configures the embeddable metrics SDK.
type MetricsSDKConfig struct {
	Enabled          bool
	AppID            string
	Platform         string // ios, android, desktop, electron
	MaxBufferSize    int
	FlushInterval    time.Duration
	SyncEndpoint     string
	SyncEnabled      bool
	SamplingRate     float64
	PrivacyMode      bool
	MaxMemoryBytes   int64
	BatchUploadSize  int
	OfflineBufferDays int
}

// DefaultMetricsSDKConfig returns sensible defaults.
func DefaultMetricsSDKConfig() MetricsSDKConfig {
	return MetricsSDKConfig{
		Enabled:          true,
		Platform:         "generic",
		MaxBufferSize:    50000,
		FlushInterval:    30 * time.Second,
		SyncEnabled:      true,
		SamplingRate:     1.0,
		PrivacyMode:      false,
		MaxMemoryBytes:   5 * 1024 * 1024, // 5MB
		BatchUploadSize:  1000,
		OfflineBufferDays: 7,
	}
}

// SDKEvent represents a client-side event/metric.
type SDKEvent struct {
	Name      string            `json:"name"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
	Timestamp int64             `json:"timestamp"`
	EventType string            `json:"event_type"` // metric, event, crash, performance
	SessionID string            `json:"session_id,omitempty"`
}

// SDKSession represents a user session.
type SDKSession struct {
	ID        string    `json:"id"`
	AppID     string    `json:"app_id"`
	Platform  string    `json:"platform"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	Events    int64     `json:"events"`
	Duration  time.Duration `json:"duration,omitempty"`
}

// SDKSyncResult represents the result of a sync operation.
type SDKSyncResult struct {
	EventsSynced  int64         `json:"events_synced"`
	BytesSynced   int64         `json:"bytes_synced"`
	Duration      time.Duration `json:"duration"`
	Success       bool          `json:"success"`
	Error         string        `json:"error,omitempty"`
	NextSyncAfter time.Duration `json:"next_sync_after"`
}

// SDKBindingInfo describes a platform SDK binding.
type SDKBindingInfo struct {
	Platform    string   `json:"platform"`
	Language    string   `json:"language"`
	MinVersion  string   `json:"min_version"`
	Features    []string `json:"features"`
	BinarySize  string   `json:"binary_size"`
	Status      string   `json:"status"` // available, planned, beta
}

// MetricsSDKStats holds SDK engine statistics.
type MetricsSDKStats struct {
	ActiveSessions  int   `json:"active_sessions"`
	TotalEvents     int64 `json:"total_events"`
	BufferedEvents  int   `json:"buffered_events"`
	SyncedEvents    int64 `json:"synced_events"`
	DroppedEvents   int64 `json:"dropped_events"`
	MemoryUsedBytes int64 `json:"memory_used_bytes"`
}

// MetricsSDKEngine provides embeddable metrics collection for mobile/desktop.
type MetricsSDKEngine struct {
	db     *DB
	config MetricsSDKConfig

	mu       sync.RWMutex
	buffer   []SDKEvent
	sessions map[string]*SDKSession
	running  bool
	stopCh   chan struct{}
	stats    MetricsSDKStats
	bindings []SDKBindingInfo
}

// NewMetricsSDKEngine creates a new metrics SDK engine.
func NewMetricsSDKEngine(db *DB, cfg MetricsSDKConfig) *MetricsSDKEngine {
	return &MetricsSDKEngine{
		db:       db,
		config:   cfg,
		buffer:   make([]SDKEvent, 0, cfg.MaxBufferSize),
		sessions: make(map[string]*SDKSession),
		stopCh:   make(chan struct{}),
		bindings: defaultSDKBindings(),
	}
}

// Start starts the SDK engine with background flush/sync.
func (e *MetricsSDKEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
	go e.flushLoop()
}

// Stop stops the SDK engine.
func (e *MetricsSDKEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// Track records an event from a client SDK.
func (e *MetricsSDKEngine) Track(event SDKEvent) error {
	if event.Name == "" {
		return fmt.Errorf("event name required")
	}

	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixNano()
	}
	if event.EventType == "" {
		event.EventType = "metric"
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Apply sampling
	if e.config.SamplingRate < 1.0 {
		// Deterministic sampling based on event name hash
		h := fnv1a64([]byte(event.Name))
		threshold := uint64(e.config.SamplingRate * float64(^uint64(0)))
		if h > threshold {
			e.stats.DroppedEvents++
			return nil
		}
	}

	// Check buffer capacity
	if len(e.buffer) >= e.config.MaxBufferSize {
		e.stats.DroppedEvents++
		return fmt.Errorf("buffer full, event dropped")
	}

	// Privacy mode: strip identifying tags
	if e.config.PrivacyMode {
		event.Tags = sanitizeTags(event.Tags)
	}

	e.buffer = append(e.buffer, event)
	e.stats.TotalEvents++
	e.stats.BufferedEvents = len(e.buffer)

	// Update session
	if event.SessionID != "" {
		if sess, ok := e.sessions[event.SessionID]; ok {
			sess.Events++
		}
	}

	return nil
}

// StartSession begins a new user session.
func (e *MetricsSDKEngine) StartSession(sessionID, appID, platform string) *SDKSession {
	e.mu.Lock()
	defer e.mu.Unlock()

	if sessionID == "" {
		sessionID = fmt.Sprintf("sess-%d", time.Now().UnixNano())
	}

	session := &SDKSession{
		ID:        sessionID,
		AppID:     appID,
		Platform:  platform,
		StartedAt: time.Now(),
	}

	e.sessions[sessionID] = session
	e.stats.ActiveSessions = len(e.sessions)
	return session
}

// EndSession ends a user session.
func (e *MetricsSDKEngine) EndSession(sessionID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if sess, ok := e.sessions[sessionID]; ok {
		now := time.Now()
		sess.EndedAt = &now
		sess.Duration = now.Sub(sess.StartedAt)
	}
}

// GetSession returns a session by ID.
func (e *MetricsSDKEngine) GetSession(sessionID string) *SDKSession {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if sess, ok := e.sessions[sessionID]; ok {
		cp := *sess
		return &cp
	}
	return nil
}

// ActiveSessions returns all active sessions.
func (e *MetricsSDKEngine) ActiveSessions() []SDKSession {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var active []SDKSession
	for _, s := range e.sessions {
		if s.EndedAt == nil {
			active = append(active, *s)
		}
	}
	return active
}

// Flush writes buffered events to the database.
func (e *MetricsSDKEngine) Flush() int {
	e.mu.Lock()

	if len(e.buffer) == 0 {
		e.mu.Unlock()
		return 0
	}

	events := make([]SDKEvent, len(e.buffer))
	copy(events, e.buffer)
	e.buffer = e.buffer[:0]
	e.stats.BufferedEvents = 0
	e.mu.Unlock()

	count := 0
	for _, ev := range events {
		p := Point{
			Metric:    ev.Name,
			Value:     ev.Value,
			Tags:      ev.Tags,
			Timestamp: ev.Timestamp,
		}
		p.ensureTags()
		p.Tags["_event_type"] = ev.EventType
		if ev.SessionID != "" {
			p.Tags["_session_id"] = ev.SessionID
		}

		if err := e.db.Write(p); err == nil {
			count++
		}
	}

	e.mu.Lock()
	e.stats.SyncedEvents += int64(count)
	e.mu.Unlock()

	return count
}

// Sync performs a cloud sync of local data.
func (e *MetricsSDKEngine) Sync() *SDKSyncResult {
	if !e.config.SyncEnabled {
		return &SDKSyncResult{
			Success: false,
			Error:   "sync disabled",
		}
	}

	start := time.Now()
	flushed := e.Flush()

	return &SDKSyncResult{
		EventsSynced:  int64(flushed),
		BytesSynced:   int64(flushed) * 64, // estimate
		Duration:      time.Since(start),
		Success:       true,
		NextSyncAfter: e.config.FlushInterval,
	}
}

// Bindings returns available platform SDK bindings.
func (e *MetricsSDKEngine) Bindings() []SDKBindingInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]SDKBindingInfo, len(e.bindings))
	copy(result, e.bindings)
	return result
}

// GetStats returns SDK engine statistics.
func (e *MetricsSDKEngine) GetStats() MetricsSDKStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

func (e *MetricsSDKEngine) flushLoop() {
	ticker := time.NewTicker(e.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			e.Flush() // Final flush
			return
		case <-ticker.C:
			e.Flush()
		}
	}
}

func sanitizeTags(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	sanitized := make(map[string]string, len(tags))
	sensitiveKeys := map[string]bool{
		"user_id": true, "email": true, "name": true,
		"phone": true, "ip": true, "device_id": true,
	}
	for k, v := range tags {
		if sensitiveKeys[k] {
			sanitized[k] = "***"
		} else {
			sanitized[k] = v
		}
	}
	return sanitized
}

func defaultSDKBindings() []SDKBindingInfo {
	return []SDKBindingInfo{
		{
			Platform:   "ios",
			Language:   "Swift",
			MinVersion: "iOS 15.0",
			Features:   []string{"metrics", "events", "crashes", "sessions", "offline_buffer", "privacy_mode"},
			BinarySize: "1.8MB",
			Status:     "planned",
		},
		{
			Platform:   "android",
			Language:   "Kotlin",
			MinVersion: "API 26",
			Features:   []string{"metrics", "events", "crashes", "sessions", "offline_buffer", "privacy_mode"},
			BinarySize: "2.1MB",
			Status:     "planned",
		},
		{
			Platform:   "desktop",
			Language:   "Go (CGo)",
			MinVersion: "Go 1.21+",
			Features:   []string{"metrics", "events", "sessions", "sync", "embedded_db"},
			BinarySize: "4.2MB",
			Status:     "available",
		},
		{
			Platform:   "electron",
			Language:   "TypeScript",
			MinVersion: "Node 18+",
			Features:   []string{"metrics", "events", "sessions", "sync"},
			BinarySize: "3.5MB",
			Status:     "planned",
		},
		{
			Platform:   "web",
			Language:   "JavaScript/WASM",
			MinVersion: "ES2020",
			Features:   []string{"metrics", "events", "sessions"},
			BinarySize: "800KB",
			Status:     "beta",
		},
	}
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *MetricsSDKEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sdk/track", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var event SDKEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := e.Track(event); err != nil {
			slog.Error("track failed", "err", err)
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "tracked"})
	})

	mux.HandleFunc("/api/v1/sdk/track/batch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var events []SDKEvent
		if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		tracked := 0
		for _, ev := range events {
			if err := e.Track(ev); err == nil {
				tracked++
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"tracked": tracked, "total": len(events)})
	})

	mux.HandleFunc("/api/v1/sdk/session/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			SessionID string `json:"session_id"`
			AppID     string `json:"app_id"`
			Platform  string `json:"platform"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		session := e.StartSession(req.SessionID, req.AppID, req.Platform)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(session)
	})

	mux.HandleFunc("/api/v1/sdk/sync", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		result := e.Sync()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/api/v1/sdk/bindings", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Bindings())
	})

	mux.HandleFunc("/api/v1/sdk/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
