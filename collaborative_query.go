package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	// AllowedOrigins restricts WebSocket CORS to these origins.
	// An empty list defaults to same-origin only (no wildcard).
	AllowedOrigins []string
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
	ID           string                   `json:"id"`
	Name         string                   `json:"name"`
	CreatedAt    time.Time                `json:"created_at"`
	CreatedBy    string                   `json:"created_by"`
	Participants map[string]*Participant  `json:"participants"`
	QueryState   *CollaborativeQueryState `json:"query_state"`
	Annotations  []*QueryAnnotation       `json:"annotations"`
	Bookmarks    []*QueryBookmark         `json:"bookmarks"`
	LastActivity time.Time                `json:"last_activity"`

	mu         sync.RWMutex
	operations chan CollaborativeOp
	broadcast  chan []byte
	done       chan struct{}
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

	conn *websocket.Conn
	send chan []byte
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
	QueryText    string                `json:"query_text"`
	QueryType    string                `json:"query_type"` // sql, promql, graphql
	Parameters   map[string]any        `json:"parameters"`
	TimeRange    *CollabQueryTimeRange `json:"time_range"`
	Version      int64                 `json:"version"`
	LastModified time.Time             `json:"last_modified"`
	ModifiedBy   string                `json:"modified_by"`

	// CRDT state for conflict resolution
	operations  []CollabCRDTOperation
	vectorClock map[string]int64
}

// CollabQueryTimeRange specifies the time bounds for a query.
type CollabQueryTimeRange struct {
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	Relative string    `json:"relative"` // e.g., "last 1h", "last 24h"
}

// QueryAnnotation allows participants to add notes to query results.
type QueryAnnotation struct {
	ID            string          `json:"id"`
	ParticipantID string          `json:"participant_id"`
	Text          string          `json:"text"`
	Position      *CursorPosition `json:"position,omitempty"`
	Timestamp     int64           `json:"timestamp,omitempty"` // For result annotations
	CreatedAt     time.Time       `json:"created_at"`
}

// QueryBookmark saves interesting query states.
type QueryBookmark struct {
	ID         string                   `json:"id"`
	Name       string                   `json:"name"`
	QueryState *CollaborativeQueryState `json:"query_state"`
	CreatedBy  string                   `json:"created_by"`
	CreatedAt  time.Time                `json:"created_at"`
}

// CollaborativeOp represents an operation in the collaborative session.
type CollaborativeOp struct {
	Type          CollabOpType    `json:"type"`
	ParticipantID string          `json:"participant_id"`
	Timestamp     time.Time       `json:"timestamp"`
	Data          json.RawMessage `json:"data"`
	Version       int64           `json:"version"`
}

// CollabOpType defines operation types.
type CollabOpType string

const (
	CollabOpJoin             CollabOpType = "join"
	CollabOpLeave            CollabOpType = "leave"
	CollabOpCursorMove       CollabOpType = "cursor_move"
	CollabOpSelectionChange  CollabOpType = "selection_change"
	CollabOpQueryEdit        CollabOpType = "query_edit"
	CollabOpExecuteQuery     CollabOpType = "execute_query"
	CollabOpAnnotationAdd    CollabOpType = "annotation_add"
	CollabOpAnnotationRemove CollabOpType = "annotation_remove"
	CollabOpBookmarkAdd      CollabOpType = "bookmark_add"
	CollabOpResultHighlight  CollabOpType = "result_highlight"
	CollabOpSync             CollabOpType = "sync"
)

// CollabCRDTOperation represents a conflict-free operation.
type CollabCRDTOperation struct {
	ID            string           `json:"id"`
	Type          string           `json:"type"` // insert, delete
	Position      int              `json:"position"`
	Character     string           `json:"character,omitempty"`
	Length        int              `json:"length,omitempty"`
	ParticipantID string           `json:"participant_id"`
	Timestamp     time.Time        `json:"timestamp"`
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
	totalSessions   int64
	activeSessions  int64
	totalOperations int64
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

	sessionID, err := generateCollabSessionID()
	if err != nil {
		return nil, err
	}

	session := &CollaborativeSession{
		ID:           sessionID,
		Name:         name,
		CreatedAt:    time.Now(),
		CreatedBy:    creatorID,
		Participants: make(map[string]*Participant),
		QueryState: &CollaborativeQueryState{
			QueryText:   "",
			QueryType:   "sql",
			Parameters:  make(map[string]any),
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
		Data: map[string]any{
			"query_state":  session.QueryState,
			"participants": session.Participants,
			"annotations":  session.Annotations,
			"bookmarks":    session.Bookmarks,
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
			closeQuietly(participant.conn)
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
	Type    string `json:"type"`
	Data    any    `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
	Version int64  `json:"version,omitempty"`
}

// QueryEditOperation represents a text edit.
type QueryEditOperation struct {
	Type     string `json:"type"` // insert, delete, replace
	Position int    `json:"position"`
	Text     string `json:"text,omitempty"`
	Length   int    `json:"length,omitempty"`
	OldText  string `json:"old_text,omitempty"`
}
