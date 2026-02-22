package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// StreamReplayConfig configures the stream replay engine.
type StreamReplayConfig struct {
	Enabled      bool
	MaxReplays   int
	DefaultSpeed float64 // 1.0 = real-time, 2.0 = 2x speed, 0 = as-fast-as-possible
	BufferSize   int
}

// DefaultStreamReplayConfig returns sensible defaults.
func DefaultStreamReplayConfig() StreamReplayConfig {
	return StreamReplayConfig{
		Enabled:    true,
		MaxReplays: 10,
		DefaultSpeed: 1.0,
		BufferSize: 10000,
	}
}

// StreamReplaySession represents a running replay of historical data.
type StreamReplaySession struct {
	ID          string    `json:"id"`
	Metric      string    `json:"metric"`
	StartTime   int64     `json:"start_time"`
	EndTime     int64     `json:"end_time"`
	Speed       float64   `json:"speed"`
	Status      string    `json:"status"` // pending, running, paused, completed, cancelled
	PointsTotal int64     `json:"points_total"`
	PointsSent  int64     `json:"points_sent"`
	Progress    float64   `json:"progress"` // 0-100
	CreatedAt   time.Time `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// ReplayCallback is invoked for each replayed point.
type ReplayCallback func(p Point) error

// StreamReplayStats holds engine statistics.
type StreamReplayStats struct {
	ActiveReplays   int   `json:"active_replays"`
	CompletedReplays int64 `json:"completed_replays"`
	TotalPointsSent int64 `json:"total_points_sent"`
	CancelledReplays int64 `json:"cancelled_replays"`
}

// StreamReplayEngine replays historical data streams.
type StreamReplayEngine struct {
	db     *DB
	config StreamReplayConfig
	mu     sync.RWMutex
	sessions map[string]*replayState
	running  bool
	stopCh   chan struct{}
	stats    StreamReplayStats
	sessionSeq int64
}

type replayState struct {
	session  StreamReplaySession
	cancel   context.CancelFunc
	callback ReplayCallback
}

// NewStreamReplayEngine creates a new stream replay engine.
func NewStreamReplayEngine(db *DB, cfg StreamReplayConfig) *StreamReplayEngine {
	return &StreamReplayEngine{
		db:       db,
		config:   cfg,
		sessions: make(map[string]*replayState),
		stopCh:   make(chan struct{}),
	}
}

func (e *StreamReplayEngine) Start() {
	e.mu.Lock(); if e.running { e.mu.Unlock(); return }; e.running = true; e.mu.Unlock()
}

func (e *StreamReplayEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running { return }
	e.running = false
	close(e.stopCh)
	// Cancel all active replays
	for _, rs := range e.sessions {
		if rs.cancel != nil { rs.cancel() }
	}
}

// CreateReplay creates a new replay session.
func (e *StreamReplayEngine) CreateReplay(metric string, startTime, endTime int64, speed float64, cb ReplayCallback) (*StreamReplaySession, error) {
	if metric == "" { return nil, fmt.Errorf("metric required") }
	if endTime <= startTime { return nil, fmt.Errorf("end time must be after start time") }

	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.sessions) >= e.config.MaxReplays {
		return nil, fmt.Errorf("max concurrent replays reached (%d)", e.config.MaxReplays)
	}

	if speed <= 0 { speed = e.config.DefaultSpeed }

	e.sessionSeq++
	session := StreamReplaySession{
		ID:        fmt.Sprintf("replay-%d", e.sessionSeq),
		Metric:    metric,
		StartTime: startTime,
		EndTime:   endTime,
		Speed:     speed,
		Status:    "pending",
		CreatedAt: time.Now(),
	}

	// Fetch data to get point count
	q := &Query{Metric: metric, Start: startTime, End: endTime}
	result, err := e.db.Execute(q)
	if err != nil { return nil, fmt.Errorf("fetch data: %w", err) }
	if result != nil {
		session.PointsTotal = int64(len(result.Points))
	}

	rs := &replayState{session: session, callback: cb}
	e.sessions[session.ID] = rs
	e.stats.ActiveReplays = len(e.sessions)

	return &session, nil
}

// StartReplay begins replaying data for a session.
func (e *StreamReplayEngine) StartReplay(sessionID string) error {
	e.mu.Lock()
	rs, exists := e.sessions[sessionID]
	if !exists { e.mu.Unlock(); return fmt.Errorf("session %q not found", sessionID) }
	if rs.session.Status == "running" { e.mu.Unlock(); return nil }

	ctx, cancel := context.WithCancel(context.Background())
	rs.cancel = cancel
	rs.session.Status = "running"
	now := time.Now()
	rs.session.StartedAt = &now
	e.mu.Unlock()

	go e.runReplay(ctx, sessionID)
	return nil
}

// CancelReplay cancels an active replay.
func (e *StreamReplayEngine) CancelReplay(sessionID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	rs, exists := e.sessions[sessionID]
	if !exists { return fmt.Errorf("session %q not found", sessionID) }
	if rs.cancel != nil { rs.cancel() }
	rs.session.Status = "cancelled"
	e.stats.CancelledReplays++
	return nil
}

// GetSession returns a replay session.
func (e *StreamReplayEngine) GetSession(sessionID string) *StreamReplaySession {
	e.mu.RLock(); defer e.mu.RUnlock()
	if rs, ok := e.sessions[sessionID]; ok { cp := rs.session; return &cp }
	return nil
}

// ListSessions returns all sessions.
func (e *StreamReplayEngine) ListSessions() []StreamReplaySession {
	e.mu.RLock(); defer e.mu.RUnlock()
	result := make([]StreamReplaySession, 0, len(e.sessions))
	for _, rs := range e.sessions { result = append(result, rs.session) }
	return result
}

// GetStats returns engine stats.
func (e *StreamReplayEngine) GetStats() StreamReplayStats {
	e.mu.RLock(); defer e.mu.RUnlock(); return e.stats
}

func (e *StreamReplayEngine) runReplay(ctx context.Context, sessionID string) {
	e.mu.RLock()
	rs, exists := e.sessions[sessionID]
	if !exists { e.mu.RUnlock(); return }
	session := rs.session
	cb := rs.callback
	e.mu.RUnlock()

	// Fetch data
	q := &Query{Metric: session.Metric, Start: session.StartTime, End: session.EndTime}
	result, err := e.db.Execute(q)
	if err != nil || result == nil {
		e.mu.Lock()
		rs.session.Status = "completed"
		now := time.Now(); rs.session.CompletedAt = &now
		e.mu.Unlock()
		return
	}

	// Replay each point
	var lastTS int64
	for i, p := range result.Points {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Apply speed delay
		if session.Speed > 0 && lastTS > 0 && p.Timestamp > lastTS {
			delay := time.Duration(float64(p.Timestamp-lastTS) / session.Speed)
			if delay > 0 && delay < time.Minute {
				time.Sleep(delay)
			}
		}
		lastTS = p.Timestamp

		if cb != nil { cb(p) }

		e.mu.Lock()
		rs.session.PointsSent++
		if session.PointsTotal > 0 {
			rs.session.Progress = float64(i+1) / float64(session.PointsTotal) * 100
		}
		e.stats.TotalPointsSent++
		e.mu.Unlock()
	}

	e.mu.Lock()
	rs.session.Status = "completed"
	rs.session.Progress = 100
	now := time.Now(); rs.session.CompletedAt = &now
	e.stats.CompletedReplays++
	e.mu.Unlock()
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *StreamReplayEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/replay/create", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		var req struct {
			Metric    string  `json:"metric"`
			StartTime int64   `json:"start_time"`
			EndTime   int64   `json:"end_time"`
			Speed     float64 `json:"speed"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { http.Error(w, err.Error(), http.StatusBadRequest); return }
		session, err := e.CreateReplay(req.Metric, req.StartTime, req.EndTime, req.Speed, nil)
		if err != nil { http.Error(w, err.Error(), http.StatusBadRequest); return }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(session)
	})
	mux.HandleFunc("/api/v1/replay/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		var req struct { SessionID string `json:"session_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { http.Error(w, err.Error(), http.StatusBadRequest); return }
		if err := e.StartReplay(req.SessionID); err != nil { http.Error(w, err.Error(), http.StatusNotFound); return }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "started"})
	})
	mux.HandleFunc("/api/v1/replay/cancel", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		var req struct { SessionID string `json:"session_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { http.Error(w, err.Error(), http.StatusBadRequest); return }
		if err := e.CancelReplay(req.SessionID); err != nil { http.Error(w, err.Error(), http.StatusNotFound); return }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "cancelled"})
	})
	mux.HandleFunc("/api/v1/replay/sessions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListSessions())
	})
	mux.HandleFunc("/api/v1/replay/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
