package chronicle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// CollaborativeQueryConfig configures the collaborative query system.
type CollaborativeQueryConfig struct {
	// Enabled turns on collaborative querying
	Enabled bool

	// MaxSessionDuration is the maximum lifetime of a session
	MaxSessionDuration time.Duration

	// MaxParticipants per session
	MaxParticipants int

	// SyncInterval for cursor/state updates
	SyncInterval time.Duration

	// BufferSize for operation channels
	BufferSize int

	// EnableConflictResolution uses CRDT for concurrent edits
	EnableConflictResolution bool

	// HistoryRetention for undo/redo operations
	HistoryRetention int
}

// DefaultCollaborativeQueryConfig returns default configuration.
func DefaultCollaborativeQueryConfig() CollaborativeQueryConfig {
	return CollaborativeQueryConfig{
		Enabled:                  true,
		MaxSessionDuration:       4 * time.Hour,
		MaxParticipants:          10,
		SyncInterval:             50 * time.Millisecond,
		BufferSize:               1000,
		EnableConflictResolution: true,
		HistoryRetention:         100,
	}
}

// CollaborativeSession represents a shared query editing session.
type CollaborativeSession struct {
	ID           string                 `json:"id"`
	Name         string                 `json:"name"`
	CreatedAt    time.Time              `json:"created_at"`
	CreatedBy    string                 `json:"created_by"`
	Participants map[string]*Participant `json:"participants"`
	QueryState   *CollaborativeQueryState `json:"query_state"`
	Annotations  []*QueryAnnotation     `json:"annotations"`
	Bookmarks    []*QueryBookmark       `json:"bookmarks"`
	LastActivity time.Time              `json:"last_activity"`
	
	mu           sync.RWMutex
	operations   chan CollaborativeOp
	broadcast    chan []byte
	done         chan struct{}
}

// Participant represents a user in a collaborative session.
type Participant struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Color       string            `json:"color"`
	Cursor      *CursorPosition   `json:"cursor"`
	Selection   *SelectionRange   `json:"selection"`
	JoinedAt    time.Time         `json:"joined_at"`
	LastSeen    time.Time         `json:"last_seen"`
	Permissions []string          `json:"permissions"`
	Metadata    map[string]string `json:"metadata"`
	
	conn        *websocket.Conn
	send        chan []byte
}

// CursorPosition tracks a participant's cursor location.
type CursorPosition struct {
	Line   int `json:"line"`
	Column int `json:"column"`
	Offset int `json:"offset"`
}

// SelectionRange represents text selection.
type SelectionRange struct {
	Start CursorPosition `json:"start"`
	End   CursorPosition `json:"end"`
}

// CollaborativeQueryState holds the shared query state.
type CollaborativeQueryState struct {
	QueryText    string                 `json:"query_text"`
	QueryType    string                 `json:"query_type"` // sql, promql, graphql
	Parameters   map[string]interface{} `json:"parameters"`
	TimeRange    *CollabQueryTimeRange        `json:"time_range"`
	Version      int64                  `json:"version"`
	LastModified time.Time              `json:"last_modified"`
	ModifiedBy   string                 `json:"modified_by"`
	
	// CRDT state for conflict resolution
	operations   []CollabCRDTOperation
	vectorClock  map[string]int64
}

// CollabQueryTimeRange specifies the time bounds for a query.
type CollabQueryTimeRange struct {
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	Relative string    `json:"relative"` // e.g., "last 1h", "last 24h"
}

// QueryAnnotation allows participants to add notes to query results.
type QueryAnnotation struct {
	ID          string    `json:"id"`
	ParticipantID string  `json:"participant_id"`
	Text        string    `json:"text"`
	Position    *CursorPosition `json:"position,omitempty"`
	Timestamp   int64     `json:"timestamp,omitempty"` // For result annotations
	CreatedAt   time.Time `json:"created_at"`
}

// QueryBookmark saves interesting query states.
type QueryBookmark struct {
	ID          string                  `json:"id"`
	Name        string                  `json:"name"`
	QueryState  *CollaborativeQueryState `json:"query_state"`
	CreatedBy   string                  `json:"created_by"`
	CreatedAt   time.Time               `json:"created_at"`
}

// CollaborativeOp represents an operation in the collaborative session.
type CollaborativeOp struct {
	Type          CollabOpType      `json:"type"`
	ParticipantID string            `json:"participant_id"`
	Timestamp     time.Time         `json:"timestamp"`
	Data          json.RawMessage   `json:"data"`
	Version       int64             `json:"version"`
}

// CollabOpType defines operation types.
type CollabOpType string

const (
	CollabOpJoin           CollabOpType = "join"
	CollabOpLeave          CollabOpType = "leave"
	CollabOpCursorMove     CollabOpType = "cursor_move"
	CollabOpSelectionChange CollabOpType = "selection_change"
	CollabOpQueryEdit      CollabOpType = "query_edit"
	CollabOpExecuteQuery   CollabOpType = "execute_query"
	CollabOpAnnotationAdd  CollabOpType = "annotation_add"
	CollabOpAnnotationRemove CollabOpType = "annotation_remove"
	CollabOpBookmarkAdd    CollabOpType = "bookmark_add"
	CollabOpResultHighlight CollabOpType = "result_highlight"
	CollabOpSync           CollabOpType = "sync"
)

// CollabCRDTOperation represents a conflict-free operation.
type CollabCRDTOperation struct {
	ID            string    `json:"id"`
	Type          string    `json:"type"` // insert, delete
	Position      int       `json:"position"`
	Character     string    `json:"character,omitempty"`
	Length        int       `json:"length,omitempty"`
	ParticipantID string    `json:"participant_id"`
	Timestamp     time.Time `json:"timestamp"`
	VectorClock   map[string]int64 `json:"vector_clock"`
}

// CollaborativeQueryHub manages all collaborative sessions.
type CollaborativeQueryHub struct {
	db       *DB
	config   CollaborativeQueryConfig
	sessions map[string]*CollaborativeSession
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	// Stats
	totalSessions    int64
	activeSessions   int64
	totalOperations  int64
}

// NewCollaborativeQueryHub creates a new collaborative query hub.
func NewCollaborativeQueryHub(db *DB, config CollaborativeQueryConfig) *CollaborativeQueryHub {
	ctx, cancel := context.WithCancel(context.Background())
	
	hub := &CollaborativeQueryHub{
		db:       db,
		config:   config,
		sessions: make(map[string]*CollaborativeSession),
		ctx:      ctx,
		cancel:   cancel,
	}

	// Start cleanup goroutine
	hub.wg.Add(1)
	go hub.cleanupWorker()

	return hub
}

// CreateSession creates a new collaborative session.
func (hub *CollaborativeQueryHub) CreateSession(name, creatorID string) (*CollaborativeSession, error) {
	hub.mu.Lock()
	defer hub.mu.Unlock()

	sessionID := generateCollabSessionID()
	
	session := &CollaborativeSession{
		ID:           sessionID,
		Name:         name,
		CreatedAt:    time.Now(),
		CreatedBy:    creatorID,
		Participants: make(map[string]*Participant),
		QueryState: &CollaborativeQueryState{
			QueryText:   "",
			QueryType:   "sql",
			Parameters:  make(map[string]interface{}),
			Version:     0,
			vectorClock: make(map[string]int64),
			operations:  make([]CollabCRDTOperation, 0),
		},
		Annotations:  make([]*QueryAnnotation, 0),
		Bookmarks:    make([]*QueryBookmark, 0),
		LastActivity: time.Now(),
		operations:   make(chan CollaborativeOp, hub.config.BufferSize),
		broadcast:    make(chan []byte, hub.config.BufferSize),
		done:         make(chan struct{}),
	}

	hub.sessions[sessionID] = session
	atomic.AddInt64(&hub.totalSessions, 1)
	atomic.AddInt64(&hub.activeSessions, 1)

	// Start session workers
	hub.wg.Add(2)
	go hub.runSession(session)
	go hub.broadcastWorker(session)

	return session, nil
}

// GetSession returns a session by ID.
func (hub *CollaborativeQueryHub) GetSession(sessionID string) (*CollaborativeSession, error) {
	hub.mu.RLock()
	defer hub.mu.RUnlock()

	session, exists := hub.sessions[sessionID]
	if !exists {
		return nil, fmt.Errorf("session not found: %s", sessionID)
	}
	return session, nil
}

// ListSessions returns all active sessions.
func (hub *CollaborativeQueryHub) ListSessions() []*CollaborativeSession {
	hub.mu.RLock()
	defer hub.mu.RUnlock()

	sessions := make([]*CollaborativeSession, 0, len(hub.sessions))
	for _, s := range hub.sessions {
		sessions = append(sessions, s)
	}
	return sessions
}

// CloseSession closes a collaborative session.
func (hub *CollaborativeQueryHub) CloseSession(sessionID string) error {
	hub.mu.Lock()
	session, exists := hub.sessions[sessionID]
	if exists {
		delete(hub.sessions, sessionID)
	}
	hub.mu.Unlock()

	if !exists {
		return fmt.Errorf("session not found: %s", sessionID)
	}

	close(session.done)
	atomic.AddInt64(&hub.activeSessions, -1)
	return nil
}

// JoinSession adds a participant to a session.
func (hub *CollaborativeQueryHub) JoinSession(sessionID string, participant *Participant, conn *websocket.Conn) error {
	session, err := hub.GetSession(sessionID)
	if err != nil {
		return err
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	if len(session.Participants) >= hub.config.MaxParticipants {
		return errors.New("session is full")
	}

	participant.JoinedAt = time.Now()
	participant.LastSeen = time.Now()
	participant.conn = conn
	participant.send = make(chan []byte, 256)
	participant.Color = generateParticipantColor(len(session.Participants))

	session.Participants[participant.ID] = participant
	session.LastActivity = time.Now()

	// Notify others
	joinMsg, _ := json.Marshal(CollaborativeMessage{
		Type: "participant_joined",
		Data: participant,
	})
	session.broadcast <- joinMsg

	// Send current state to new participant
	stateMsg, _ := json.Marshal(CollaborativeMessage{
		Type: "session_state",
		Data: map[string]interface{}{
			"query_state":   session.QueryState,
			"participants":  session.Participants,
			"annotations":   session.Annotations,
			"bookmarks":     session.Bookmarks,
		},
	})
	participant.send <- stateMsg

	// Start participant handlers
	hub.wg.Add(2)
	go hub.readPump(session, participant)
	go hub.writePump(session, participant)

	return nil
}

// LeaveSession removes a participant from a session.
func (hub *CollaborativeQueryHub) LeaveSession(sessionID, participantID string) error {
	session, err := hub.GetSession(sessionID)
	if err != nil {
		return err
	}

	session.mu.Lock()
	participant, exists := session.Participants[participantID]
	if exists {
		delete(session.Participants, participantID)
		if participant.conn != nil {
			_ = participant.conn.Close()
		}
		close(participant.send)
	}
	session.mu.Unlock()

	if !exists {
		return fmt.Errorf("participant not found: %s", participantID)
	}

	// Notify others
	leaveMsg, _ := json.Marshal(CollaborativeMessage{
		Type: "participant_left",
		Data: map[string]string{"participant_id": participantID},
	})
	session.broadcast <- leaveMsg

	return nil
}

// CollaborativeMessage is the WebSocket message format.
type CollaborativeMessage struct {
	Type    string      `json:"type"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
	Version int64       `json:"version,omitempty"`
}

// QueryEditOperation represents a text edit.
type QueryEditOperation struct {
	Type      string `json:"type"` // insert, delete, replace
	Position  int    `json:"position"`
	Text      string `json:"text,omitempty"`
	Length    int    `json:"length,omitempty"`
	OldText   string `json:"old_text,omitempty"`
}

func (hub *CollaborativeQueryHub) runSession(session *CollaborativeSession) {
	defer hub.wg.Done()

	ticker := time.NewTicker(hub.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-session.done:
			return
		case <-hub.ctx.Done():
			return
		case op := <-session.operations:
			hub.processOperation(session, op)
		case <-ticker.C:
			hub.syncSessionState(session)
		}
	}
}

func (hub *CollaborativeQueryHub) broadcastWorker(session *CollaborativeSession) {
	defer hub.wg.Done()

	for {
		select {
		case <-session.done:
			return
		case <-hub.ctx.Done():
			return
		case msg := <-session.broadcast:
			session.mu.RLock()
			for _, p := range session.Participants {
				select {
				case p.send <- msg:
				default:
					// Buffer full, skip
				}
			}
			session.mu.RUnlock()
		}
	}
}

func (hub *CollaborativeQueryHub) readPump(session *CollaborativeSession, participant *Participant) {
	defer hub.wg.Done()
	defer func() { _ = hub.LeaveSession(session.ID, participant.ID) }()

	for {
		_, message, err := participant.conn.ReadMessage()
		if err != nil {
			return
		}

		var msg CollaborativeMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		atomic.AddInt64(&hub.totalOperations, 1)
		participant.LastSeen = time.Now()

		switch msg.Type {
		case "cursor_move":
			var pos CursorPosition
			if data, err := json.Marshal(msg.Data); err == nil {
				if json.Unmarshal(data, &pos) == nil {
					participant.Cursor = &pos
					hub.broadcastCursorUpdate(session, participant)
				}
			}

		case "selection_change":
			var sel SelectionRange
			if data, err := json.Marshal(msg.Data); err == nil {
				if json.Unmarshal(data, &sel) == nil {
					participant.Selection = &sel
					hub.broadcastSelectionUpdate(session, participant)
				}
			}

		case "query_edit":
			var edit QueryEditOperation
			if data, err := json.Marshal(msg.Data); err == nil {
				if json.Unmarshal(data, &edit) == nil {
					hub.applyQueryEdit(session, participant.ID, edit)
				}
			}

		case "execute_query":
			hub.executeCollaborativeQuery(session, participant)

		case "add_annotation":
			var annotation QueryAnnotation
			if data, err := json.Marshal(msg.Data); err == nil {
				if json.Unmarshal(data, &annotation) == nil {
					annotation.ParticipantID = participant.ID
					annotation.CreatedAt = time.Now()
					annotation.ID = generateID()
					hub.addAnnotation(session, &annotation)
				}
			}

		case "add_bookmark":
			var bookmark QueryBookmark
			if data, err := json.Marshal(msg.Data); err == nil {
				if json.Unmarshal(data, &bookmark) == nil {
					bookmark.CreatedBy = participant.ID
					bookmark.CreatedAt = time.Now()
					bookmark.ID = generateID()
					hub.addBookmark(session, &bookmark)
				}
			}

		case "result_highlight":
			hub.broadcastResultHighlight(session, participant.ID, msg.Data)
		}
	}
}

func (hub *CollaborativeQueryHub) writePump(session *CollaborativeSession, participant *Participant) {
	defer hub.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-session.done:
			return
		case <-hub.ctx.Done():
			return
		case msg, ok := <-participant.send:
			if !ok {
				return
			}
			if err := participant.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			if err := participant.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (hub *CollaborativeQueryHub) processOperation(session *CollaborativeSession, op CollaborativeOp) {
	session.mu.Lock()
	defer session.mu.Unlock()

	// Apply operation based on type
	switch op.Type {
	case CollabOpQueryEdit:
		// Already handled in readPump
	case CollabOpAnnotationAdd:
		// Already handled in readPump
	}

	session.LastActivity = time.Now()
}

func (hub *CollaborativeQueryHub) syncSessionState(session *CollaborativeSession) {
	session.mu.RLock()
	defer session.mu.RUnlock()

	// Broadcast participant cursors and presence
	cursors := make(map[string]*CursorPosition)
	for id, p := range session.Participants {
		if p.Cursor != nil {
			cursors[id] = p.Cursor
		}
	}

	msg, _ := json.Marshal(CollaborativeMessage{
		Type: "cursor_sync",
		Data: cursors,
	})

	for _, p := range session.Participants {
		select {
		case p.send <- msg:
		default:
		}
	}
}

func (hub *CollaborativeQueryHub) applyQueryEdit(session *CollaborativeSession, participantID string, edit QueryEditOperation) {
	session.mu.Lock()
	
	state := session.QueryState
	text := state.QueryText

	// Apply CRDT operation for conflict resolution
	if hub.config.EnableConflictResolution {
		op := CollabCRDTOperation{
			ID:            generateID(),
			Type:          edit.Type,
			Position:      edit.Position,
			Character:     edit.Text,
			Length:        edit.Length,
			ParticipantID: participantID,
			Timestamp:     time.Now(),
			VectorClock:   copyVectorClock(state.vectorClock),
		}
		
		// Increment vector clock
		state.vectorClock[participantID]++
		op.VectorClock[participantID] = state.vectorClock[participantID]
		
		state.operations = append(state.operations, op)
		
		// Prune old operations
		if len(state.operations) > hub.config.HistoryRetention {
			state.operations = state.operations[len(state.operations)-hub.config.HistoryRetention:]
		}
	}

	// Apply the edit
	switch edit.Type {
	case "insert":
		if edit.Position >= 0 && edit.Position <= len(text) {
			text = text[:edit.Position] + edit.Text + text[edit.Position:]
		}
	case "delete":
		if edit.Position >= 0 && edit.Position+edit.Length <= len(text) {
			text = text[:edit.Position] + text[edit.Position+edit.Length:]
		}
	case "replace":
		if edit.Position >= 0 && edit.Position+edit.Length <= len(text) {
			text = text[:edit.Position] + edit.Text + text[edit.Position+edit.Length:]
		}
	}

	state.QueryText = text
	state.Version++
	state.LastModified = time.Now()
	state.ModifiedBy = participantID
	session.mu.Unlock()

	// Broadcast the change
	msg, _ := json.Marshal(CollaborativeMessage{
		Type:    "query_updated",
		Version: state.Version,
		Data: map[string]interface{}{
			"edit":        edit,
			"participant": participantID,
			"query_text":  text,
		},
	})
	session.broadcast <- msg
}

func (hub *CollaborativeQueryHub) executeCollaborativeQuery(session *CollaborativeSession, participant *Participant) {
	session.mu.RLock()
	queryText := session.QueryState.QueryText
	queryType := session.QueryState.QueryType
	timeRange := session.QueryState.TimeRange
	session.mu.RUnlock()

	// Broadcast execution start
	startMsg, _ := json.Marshal(CollaborativeMessage{
		Type: "query_executing",
		Data: map[string]string{"participant": participant.ID},
	})
	session.broadcast <- startMsg

	// Execute the query
	start := time.Now()
	var result interface{}
	var execErr error

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch queryType {
	case "sql":
		// Parse and execute SQL-like query
		var q *Query
		q, execErr = parseQueryText(queryText, timeRange)
		if execErr == nil {
			result, execErr = hub.db.ExecuteContext(ctx, q)
		}
	case "promql":
		// Execute PromQL query using parser
		parser := &QueryParser{}
		var q *Query
		q, execErr = parser.Parse(queryText)
		if execErr == nil {
			result, execErr = hub.db.Execute(q)
		}
	default:
		execErr = fmt.Errorf("unsupported query type: %s", queryType)
	}

	duration := time.Since(start)

	// Broadcast result
	var responseMsg CollaborativeMessage
	if execErr != nil {
		responseMsg = CollaborativeMessage{
			Type:  "query_error",
			Error: execErr.Error(),
			Data: map[string]interface{}{
				"participant": participant.ID,
				"duration_ms": duration.Milliseconds(),
			},
		}
	} else {
		responseMsg = CollaborativeMessage{
			Type: "query_result",
			Data: map[string]interface{}{
				"participant": participant.ID,
				"result":      result,
				"duration_ms": duration.Milliseconds(),
			},
		}
	}

	msg, _ := json.Marshal(responseMsg)
	session.broadcast <- msg
}

func (hub *CollaborativeQueryHub) broadcastCursorUpdate(session *CollaborativeSession, participant *Participant) {
	msg, _ := json.Marshal(CollaborativeMessage{
		Type: "cursor_update",
		Data: map[string]interface{}{
			"participant_id": participant.ID,
			"cursor":         participant.Cursor,
			"color":          participant.Color,
		},
	})

	session.mu.RLock()
	for id, p := range session.Participants {
		if id != participant.ID {
			select {
			case p.send <- msg:
			default:
			}
		}
	}
	session.mu.RUnlock()
}

func (hub *CollaborativeQueryHub) broadcastSelectionUpdate(session *CollaborativeSession, participant *Participant) {
	msg, _ := json.Marshal(CollaborativeMessage{
		Type: "selection_update",
		Data: map[string]interface{}{
			"participant_id": participant.ID,
			"selection":      participant.Selection,
			"color":          participant.Color,
		},
	})

	session.mu.RLock()
	for id, p := range session.Participants {
		if id != participant.ID {
			select {
			case p.send <- msg:
			default:
			}
		}
	}
	session.mu.RUnlock()
}

func (hub *CollaborativeQueryHub) addAnnotation(session *CollaborativeSession, annotation *QueryAnnotation) {
	session.mu.Lock()
	session.Annotations = append(session.Annotations, annotation)
	session.mu.Unlock()

	msg, _ := json.Marshal(CollaborativeMessage{
		Type: "annotation_added",
		Data: annotation,
	})
	session.broadcast <- msg
}

func (hub *CollaborativeQueryHub) addBookmark(session *CollaborativeSession, bookmark *QueryBookmark) {
	session.mu.Lock()
	// Clone current state
	bookmark.QueryState = &CollaborativeQueryState{
		QueryText:    session.QueryState.QueryText,
		QueryType:    session.QueryState.QueryType,
		Parameters:   copyParams(session.QueryState.Parameters),
		TimeRange:    session.QueryState.TimeRange,
		Version:      session.QueryState.Version,
		LastModified: session.QueryState.LastModified,
	}
	session.Bookmarks = append(session.Bookmarks, bookmark)
	session.mu.Unlock()

	msg, _ := json.Marshal(CollaborativeMessage{
		Type: "bookmark_added",
		Data: bookmark,
	})
	session.broadcast <- msg
}

func (hub *CollaborativeQueryHub) broadcastResultHighlight(session *CollaborativeSession, participantID string, data interface{}) {
	msg, _ := json.Marshal(CollaborativeMessage{
		Type: "result_highlight",
		Data: map[string]interface{}{
			"participant_id": participantID,
			"highlight":      data,
		},
	})
	session.broadcast <- msg
}

func (hub *CollaborativeQueryHub) cleanupWorker() {
	defer hub.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-hub.ctx.Done():
			return
		case <-ticker.C:
			hub.cleanupExpiredSessions()
		}
	}
}

func (hub *CollaborativeQueryHub) cleanupExpiredSessions() {
	hub.mu.Lock()
	defer hub.mu.Unlock()

	now := time.Now()
	expired := make([]string, 0)

	for id, session := range hub.sessions {
		session.mu.RLock()
		age := now.Sub(session.CreatedAt)
		inactive := now.Sub(session.LastActivity)
		isEmpty := len(session.Participants) == 0
		session.mu.RUnlock()

		// Remove if exceeded max duration or inactive with no participants
		if age > hub.config.MaxSessionDuration || (inactive > 30*time.Minute && isEmpty) {
			expired = append(expired, id)
		}
	}

	for _, id := range expired {
		if session, ok := hub.sessions[id]; ok {
			close(session.done)
			delete(hub.sessions, id)
			atomic.AddInt64(&hub.activeSessions, -1)
		}
	}
}

// WebSocketHandler returns an HTTP handler for collaborative sessions.
func (hub *CollaborativeQueryHub) WebSocketHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionID := r.URL.Query().Get("session")
		participantID := r.URL.Query().Get("participant")
		participantName := r.URL.Query().Get("name")

		if sessionID == "" || participantID == "" {
			http.Error(w, "session and participant parameters required", http.StatusBadRequest)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		participant := &Participant{
			ID:          participantID,
			Name:        participantName,
			Permissions: []string{"read", "write", "execute"},
			Metadata:    make(map[string]string),
		}

		if err := hub.JoinSession(sessionID, participant, conn); err != nil {
			_ = conn.Close()
			http.Error(w, err.Error(), http.StatusBadRequest)
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
	sessions := make([]map[string]interface{}, 0, len(hub.sessions))
	for id, s := range hub.sessions {
		s.mu.RLock()
		sessions = append(sessions, map[string]interface{}{
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
	json.NewEncoder(w).Encode(map[string]interface{}{
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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	session, err := hub.CreateSession(req.Name, req.OwnerID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
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
		http.Error(w, err.Error(), http.StatusBadRequest)
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
	json.NewEncoder(w).Encode(map[string]interface{}{
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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := hub.LeaveSession(req.SessionID, req.ParticipantID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
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

func generateCollabSessionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func generateID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
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

func copyParams(params map[string]interface{}) map[string]interface{} {
	copy := make(map[string]interface{})
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
