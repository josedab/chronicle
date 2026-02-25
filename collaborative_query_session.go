package chronicle

// collaborative_query_session.go contains the WebSocket session management,
// read/write pumps, and CRDT operations for collaborative queries.
// See collaborative_query.go for types and CRUD operations.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

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
	defer func() { _ = hub.LeaveSession(session.ID, participant.ID) }() //nolint:errcheck // best-effort WebSocket send

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
					if id, err := generateID(); err == nil {
						annotation.ID = id
					}
					hub.addAnnotation(session, &annotation)
				}
			}

		case "add_bookmark":
			var bookmark QueryBookmark
			if data, err := json.Marshal(msg.Data); err == nil {
				if json.Unmarshal(data, &bookmark) == nil {
					bookmark.CreatedBy = participant.ID
					bookmark.CreatedAt = time.Now()
					if id, err := generateID(); err == nil {
						bookmark.ID = id
					}
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
		opID, _ := generateID()
		op := CollabCRDTOperation{
			ID:            opID,
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
		Data: map[string]any{
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
	var result any
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
			Data: map[string]any{
				"participant": participant.ID,
				"duration_ms": duration.Milliseconds(),
			},
		}
	} else {
		responseMsg = CollaborativeMessage{
			Type: "query_result",
			Data: map[string]any{
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
		Data: map[string]any{
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
		Data: map[string]any{
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

func (hub *CollaborativeQueryHub) broadcastResultHighlight(session *CollaborativeSession, participantID string, data any) {
	msg, _ := json.Marshal(CollaborativeMessage{
		Type: "result_highlight",
		Data: map[string]any{
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
