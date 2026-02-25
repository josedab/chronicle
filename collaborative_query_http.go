package chronicle

// collaborative_query_http.go contains HTTP handlers and utility functions
// for the collaborative query hub.
// See collaborative_query.go for types and CRUD operations.

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync/atomic"
)

func (hub *CollaborativeQueryHub) WebSocketHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionID := r.URL.Query().Get("session")
		participantID := r.URL.Query().Get("participant")
		participantName := r.URL.Query().Get("name")

		if sessionID == "" || participantID == "" {
			http.Error(w, "session and participant parameters required", http.StatusBadRequest)
			return
		}

		conn, err := newUpgrader(hub.config.AllowedOrigins).Upgrade(w, r, nil)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		participant := &Participant{
			ID:          participantID,
			Name:        participantName,
			Permissions: []string{"read", "write", "execute"},
			Metadata:    make(map[string]string),
		}

		if err := hub.JoinSession(sessionID, participant, conn); err != nil {
			closeQuietly(conn)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
	}
}

// Stats returns hub statistics.
func (hub *CollaborativeQueryHub) Stats() CollaborativeQueryStats {
	hub.mu.RLock()
	defer hub.mu.RUnlock()

	totalParticipants := 0
	for _, s := range hub.sessions {
		s.mu.RLock()
		totalParticipants += len(s.Participants)
		s.mu.RUnlock()
	}

	return CollaborativeQueryStats{
		TotalSessions:     atomic.LoadInt64(&hub.totalSessions),
		ActiveSessions:    atomic.LoadInt64(&hub.activeSessions),
		TotalOperations:   atomic.LoadInt64(&hub.totalOperations),
		TotalParticipants: int64(totalParticipants),
	}
}

// CollaborativeQueryStats contains hub statistics.
type CollaborativeQueryStats struct {
	TotalSessions     int64 `json:"total_sessions"`
	ActiveSessions    int64 `json:"active_sessions"`
	TotalOperations   int64 `json:"total_operations"`
	TotalParticipants int64 `json:"total_participants"`
}

// Close shuts down the collaborative query hub.
func (hub *CollaborativeQueryHub) Close() error {
	hub.cancel()

	hub.mu.Lock()
	for _, session := range hub.sessions {
		close(session.done)
	}
	hub.sessions = make(map[string]*CollaborativeSession)
	hub.mu.Unlock()

	hub.wg.Wait()
	return nil
}

// RegisterHTTPHandlers registers all collaborative query HTTP handlers.
func (hub *CollaborativeQueryHub) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/collab/sessions", hub.HandleSessions)
	mux.HandleFunc("/api/collab/sessions/create", hub.HandleCreateSession)
	mux.HandleFunc("/api/collab/sessions/join", hub.HandleJoinSession)
	mux.HandleFunc("/api/collab/sessions/leave", hub.HandleLeaveSession)
	mux.HandleFunc("/api/collab/ws", hub.WebSocketHandler())
	mux.HandleFunc("/api/collab/stats", hub.HandleStats)
}

// HandleSessions returns list of active sessions.
func (hub *CollaborativeQueryHub) HandleSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hub.mu.RLock()
	sessions := make([]map[string]any, 0, len(hub.sessions))
	for id, s := range hub.sessions {
		s.mu.RLock()
		sessions = append(sessions, map[string]any{
			"id":           id,
			"name":         s.Name,
			"participants": len(s.Participants),
			"created_at":   s.CreatedAt,
			"owner_id":     s.CreatedBy,
		})
		s.mu.RUnlock()
	}
	hub.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"sessions": sessions,
	})
}

// HandleCreateSession creates a new collaborative session.
func (hub *CollaborativeQueryHub) HandleCreateSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Name      string `json:"name"`
		OwnerID   string `json:"owner_id"`
		OwnerName string `json:"owner_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	session, err := hub.CreateSession(req.Name, req.OwnerID)
	if err != nil {
		internalError(w, err, "internal error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"session_id": session.ID,
		"name":       session.Name,
		"owner_id":   session.CreatedBy,
		"created_at": session.CreatedAt,
	})
}

// HandleJoinSession handles a participant joining a session.
func (hub *CollaborativeQueryHub) HandleJoinSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SessionID     string `json:"session_id"`
		ParticipantID string `json:"participant_id"`
		Name          string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	participant := &Participant{
		ID:          req.ParticipantID,
		Name:        req.Name,
		Permissions: []string{"read", "write", "execute"},
		Metadata:    make(map[string]string),
	}

	// This returns the WebSocket URL to connect to
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"websocket_url": fmt.Sprintf("/api/collab/ws?session=%s&participant=%s&name=%s",
			req.SessionID, participant.ID, participant.Name),
		"participant": participant,
	})
}

// HandleLeaveSession handles a participant leaving a session.
func (hub *CollaborativeQueryHub) HandleLeaveSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SessionID     string `json:"session_id"`
		ParticipantID string `json:"participant_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if err := hub.LeaveSession(req.SessionID, req.ParticipantID); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
	})
}

// HandleStats returns hub statistics.
func (hub *CollaborativeQueryHub) HandleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := hub.Stats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// Helper functions

func generateCollabSessionID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func generateID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func generateParticipantColor(index int) string {
	colors := []string{
		"#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4",
		"#FFEAA7", "#DDA0DD", "#98D8C8", "#F7DC6F",
		"#BB8FCE", "#85C1E9",
	}
	return colors[index%len(colors)]
}

func copyVectorClock(vc map[string]int64) map[string]int64 {
	copy := make(map[string]int64)
	for k, v := range vc {
		copy[k] = v
	}
	return copy
}

func copyParams(params map[string]any) map[string]any {
	copy := make(map[string]any)
	for k, v := range params {
		copy[k] = v
	}
	return copy
}

func parseQueryText(queryText string, timeRange *CollabQueryTimeRange) (*Query, error) {
	parser := &QueryParser{}
	q, err := parser.Parse(queryText)
	if err != nil {
		// Return basic query if parsing fails
		q = &Query{}
	}

	if timeRange != nil {
		if !timeRange.Start.IsZero() {
			q.Start = timeRange.Start.UnixNano()
		}
		if !timeRange.End.IsZero() {
			q.End = timeRange.End.UnixNano()
		}
	}

	return q, nil
}

// TransformOperations applies operational transformation to concurrent edits.
func TransformOperations(op1, op2 CollabCRDTOperation) (CollabCRDTOperation, CollabCRDTOperation) {
	// If op1 and op2 are concurrent (neither causally precedes the other)
	if !causallyPrecedes(op1.VectorClock, op2.VectorClock) &&
		!causallyPrecedes(op2.VectorClock, op1.VectorClock) {
		// Transform based on position
		if op1.Position <= op2.Position {
			// op1 affects op2's position
			if op1.Type == "insert" {
				op2.Position += len(op1.Character)
			} else if op1.Type == "delete" {
				op2.Position -= op1.Length
				if op2.Position < op1.Position {
					op2.Position = op1.Position
				}
			}
		} else {
			// op2 affects op1's position
			if op2.Type == "insert" {
				op1.Position += len(op2.Character)
			} else if op2.Type == "delete" {
				op1.Position -= op2.Length
				if op1.Position < op2.Position {
					op1.Position = op2.Position
				}
			}
		}
	}
	return op1, op2
}

func causallyPrecedes(vc1, vc2 map[string]int64) bool {
	atLeastOne := false
	for k, v := range vc1 {
		if v2, ok := vc2[k]; ok {
			if v > v2 {
				return false
			}
			if v < v2 {
				atLeastOne = true
			}
		}
	}
	return atLeastOne
}

// ResolveConflicts resolves conflicts in concurrent operations using CRDT semantics.
func (state *CollaborativeQueryState) ResolveConflicts() {
	if len(state.operations) < 2 {
		return
	}

	// Sort operations by vector clock (partial order) then by participant ID for tie-breaking
	sort.Slice(state.operations, func(i, j int) bool {
		if causallyPrecedes(state.operations[i].VectorClock, state.operations[j].VectorClock) {
			return true
		}
		if causallyPrecedes(state.operations[j].VectorClock, state.operations[i].VectorClock) {
			return false
		}
		// Concurrent - use participant ID as tie-breaker
		return state.operations[i].ParticipantID < state.operations[j].ParticipantID
	})

	// Apply transformation to concurrent operations
	for i := 0; i < len(state.operations); i++ {
		for j := i + 1; j < len(state.operations); j++ {
			state.operations[i], state.operations[j] = TransformOperations(
				state.operations[i], state.operations[j],
			)
		}
	}
}
