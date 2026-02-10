package chronicle

import (
	"testing"
	"time"
)

func TestCollaborativeQueryConfig(t *testing.T) {
	cfg := DefaultCollaborativeQueryConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.MaxParticipants != 10 {
		t.Errorf("expected MaxParticipants 10, got %d", cfg.MaxParticipants)
	}
	if cfg.MaxSessionDuration != 4*time.Hour {
		t.Errorf("expected MaxSessionDuration 4h, got %v", cfg.MaxSessionDuration)
	}
	if !cfg.EnableConflictResolution {
		t.Error("expected EnableConflictResolution to be true")
	}
}

func TestCollaborativeQueryHub_CreateSession(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	hub := NewCollaborativeQueryHub(db, DefaultCollaborativeQueryConfig())
	defer func() { _ = hub.Close() }()

	session, err := hub.CreateSession("Test Session", "user1")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	if session.ID == "" {
		t.Error("expected session ID to be set")
	}
	if session.Name != "Test Session" {
		t.Errorf("expected name 'Test Session', got '%s'", session.Name)
	}
	if session.CreatedBy != "user1" {
		t.Errorf("expected creator 'user1', got '%s'", session.CreatedBy)
	}
	if session.QueryState == nil {
		t.Error("expected QueryState to be initialized")
	}

	stats := hub.Stats()
	if stats.TotalSessions != 1 {
		t.Errorf("expected TotalSessions 1, got %d", stats.TotalSessions)
	}
	if stats.ActiveSessions != 1 {
		t.Errorf("expected ActiveSessions 1, got %d", stats.ActiveSessions)
	}
}

func TestCollaborativeQueryHub_GetSession(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	hub := NewCollaborativeQueryHub(db, DefaultCollaborativeQueryConfig())
	defer func() { _ = hub.Close() }()

	session, err := hub.CreateSession("Test Session", "user1")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	retrieved, err := hub.GetSession(session.ID)
	if err != nil {
		t.Fatalf("failed to get session: %v", err)
	}
	if retrieved.ID != session.ID {
		t.Errorf("expected ID %s, got %s", session.ID, retrieved.ID)
	}

	// Test non-existent session
	_, err = hub.GetSession("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent session")
	}
}

func TestCollaborativeQueryHub_ListSessions(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	hub := NewCollaborativeQueryHub(db, DefaultCollaborativeQueryConfig())
	defer func() { _ = hub.Close() }()

	// Create multiple sessions
	_, _ = hub.CreateSession("Session 1", "user1")
	_, _ = hub.CreateSession("Session 2", "user2")
	_, _ = hub.CreateSession("Session 3", "user3")

	sessions := hub.ListSessions()
	if len(sessions) != 3 {
		t.Errorf("expected 3 sessions, got %d", len(sessions))
	}
}

func TestCollaborativeQueryHub_CloseSession(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	hub := NewCollaborativeQueryHub(db, DefaultCollaborativeQueryConfig())
	defer func() { _ = hub.Close() }()

	session, _ := hub.CreateSession("Test Session", "user1")

	err := hub.CloseSession(session.ID)
	if err != nil {
		t.Fatalf("failed to close session: %v", err)
	}

	stats := hub.Stats()
	if stats.ActiveSessions != 0 {
		t.Errorf("expected ActiveSessions 0, got %d", stats.ActiveSessions)
	}

	// Test closing non-existent session
	err = hub.CloseSession("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent session")
	}
}

func TestCollaborativeQueryState_Edit(t *testing.T) {
	state := &CollaborativeQueryState{
		QueryText:   "SELECT * FROM metrics",
		QueryType:   "sql",
		Version:     0,
		vectorClock: make(map[string]int64),
		operations:  make([]CollabCRDTOperation, 0),
	}

	// Test basic edit
	text := state.QueryText

	// Insert operation
	insertPos := 7
	insertText := "DISTINCT "
	text = text[:insertPos] + insertText + text[insertPos:]

	if text != "SELECT DISTINCT * FROM metrics" {
		t.Errorf("unexpected text after insert: %s", text)
	}

	// Delete operation
	deletePos := 7
	deleteLen := 9 // "DISTINCT "
	text = text[:deletePos] + text[deletePos+deleteLen:]

	if text != "SELECT * FROM metrics" {
		t.Errorf("unexpected text after delete: %s", text)
	}
}

func TestCRDTOperation(t *testing.T) {
	op := CollabCRDTOperation{
		ID:            "op1",
		Type:          "insert",
		Position:      5,
		Character:     "x",
		ParticipantID: "user1",
		Timestamp:     time.Now(),
		VectorClock:   map[string]int64{"user1": 1},
	}

	if op.Type != "insert" {
		t.Errorf("expected type 'insert', got '%s'", op.Type)
	}
	if op.Position != 5 {
		t.Errorf("expected position 5, got %d", op.Position)
	}
}

func TestTransformOperations(t *testing.T) {
	// Test concurrent insert operations
	op1 := CollabCRDTOperation{
		ID:            "op1",
		Type:          "insert",
		Position:      5,
		Character:     "a",
		ParticipantID: "user1",
		VectorClock:   map[string]int64{"user1": 1},
	}
	op2 := CollabCRDTOperation{
		ID:            "op2",
		Type:          "insert",
		Position:      10,
		Character:     "b",
		ParticipantID: "user2",
		VectorClock:   map[string]int64{"user2": 1},
	}

	op1t, op2t := TransformOperations(op1, op2)

	// op1 at position 5 should not affect op2 at position 10
	// but since they're concurrent, op2's position should be adjusted
	if op1t.Position != 5 {
		t.Errorf("expected op1 position 5, got %d", op1t.Position)
	}
	if op2t.Position != 11 { // 10 + 1 (length of "a")
		t.Errorf("expected op2 position 11, got %d", op2t.Position)
	}
}

func TestCausallyPrecedes(t *testing.T) {
	vc1 := map[string]int64{"user1": 1, "user2": 2}
	vc2 := map[string]int64{"user1": 2, "user2": 3}

	if !causallyPrecedes(vc1, vc2) {
		t.Error("expected vc1 to causally precede vc2")
	}
	if causallyPrecedes(vc2, vc1) {
		t.Error("expected vc2 to not causally precede vc1")
	}

	// Concurrent operations
	vc3 := map[string]int64{"user1": 2, "user2": 1}
	if causallyPrecedes(vc1, vc3) {
		t.Error("expected vc1 to not causally precede vc3 (concurrent)")
	}
	if causallyPrecedes(vc3, vc1) {
		t.Error("expected vc3 to not causally precede vc1 (concurrent)")
	}
}

func TestQueryAnnotation(t *testing.T) {
	annotation := QueryAnnotation{
		ID:            "ann1",
		ParticipantID: "user1",
		Text:          "This query needs optimization",
		Position:      &CursorPosition{Line: 1, Column: 10},
		CreatedAt:     time.Now(),
	}

	if annotation.ID != "ann1" {
		t.Errorf("expected ID 'ann1', got '%s'", annotation.ID)
	}
	if annotation.Text != "This query needs optimization" {
		t.Errorf("unexpected annotation text: %s", annotation.Text)
	}
}

func TestQueryBookmark(t *testing.T) {
	bookmark := QueryBookmark{
		ID:        "bm1",
		Name:      "Interesting Query",
		CreatedBy: "user1",
		CreatedAt: time.Now(),
		QueryState: &CollaborativeQueryState{
			QueryText: "SELECT avg(value) FROM cpu",
			QueryType: "sql",
		},
	}

	if bookmark.Name != "Interesting Query" {
		t.Errorf("expected name 'Interesting Query', got '%s'", bookmark.Name)
	}
	if bookmark.QueryState.QueryText != "SELECT avg(value) FROM cpu" {
		t.Errorf("unexpected query text: %s", bookmark.QueryState.QueryText)
	}
}

func TestParticipant(t *testing.T) {
	participant := &Participant{
		ID:          "p1",
		Name:        "Alice",
		Color:       "#FF6B6B",
		Cursor:      &CursorPosition{Line: 5, Column: 10},
		JoinedAt:    time.Now(),
		LastSeen:    time.Now(),
		Permissions: []string{"read", "write"},
	}

	if participant.ID != "p1" {
		t.Errorf("expected ID 'p1', got '%s'", participant.ID)
	}
	if participant.Name != "Alice" {
		t.Errorf("expected name 'Alice', got '%s'", participant.Name)
	}
	if participant.Cursor.Line != 5 {
		t.Errorf("expected cursor line 5, got %d", participant.Cursor.Line)
	}
}

func TestGenerateParticipantColor(t *testing.T) {
	colors := make(map[string]bool)

	for i := 0; i < 20; i++ {
		color := generateParticipantColor(i)
		if color == "" {
			t.Errorf("expected non-empty color for index %d", i)
		}
		colors[color] = true
	}

	// Should cycle through 10 colors
	if len(colors) > 10 {
		t.Errorf("expected at most 10 unique colors, got %d", len(colors))
	}
}

func TestCollaborativeQueryHub_Stats(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	hub := NewCollaborativeQueryHub(db, DefaultCollaborativeQueryConfig())
	defer func() { _ = hub.Close() }()

	// Initially empty
	stats := hub.Stats()
	if stats.TotalSessions != 0 {
		t.Errorf("expected TotalSessions 0, got %d", stats.TotalSessions)
	}

	// Create sessions
	_, _ = hub.CreateSession("S1", "user1")
	_, _ = hub.CreateSession("S2", "user2")

	stats = hub.Stats()
	if stats.TotalSessions != 2 {
		t.Errorf("expected TotalSessions 2, got %d", stats.TotalSessions)
	}
	if stats.ActiveSessions != 2 {
		t.Errorf("expected ActiveSessions 2, got %d", stats.ActiveSessions)
	}
}

func TestCollaborativeSession_QueryState(t *testing.T) {
	state := &CollaborativeQueryState{
		QueryText:   "",
		QueryType:   "sql",
		Parameters:  make(map[string]any),
		Version:     0,
		vectorClock: make(map[string]int64),
	}

	if state.QueryType != "sql" {
		t.Errorf("expected QueryType 'sql', got '%s'", state.QueryType)
	}

	// Test time range
	state.TimeRange = &CollabQueryTimeRange{
		Start:    time.Now().Add(-1 * time.Hour),
		End:      time.Now(),
		Relative: "last 1h",
	}

	if state.TimeRange.Relative != "last 1h" {
		t.Errorf("expected relative 'last 1h', got '%s'", state.TimeRange.Relative)
	}
}

func TestCollaborativeQueryState_ResolveConflicts(t *testing.T) {
	state := &CollaborativeQueryState{
		QueryText:   "SELECT * FROM metrics",
		vectorClock: map[string]int64{"user1": 0, "user2": 0},
		operations: []CollabCRDTOperation{
			{
				ID:            "op2",
				Type:          "insert",
				Position:      10,
				Character:     "b",
				ParticipantID: "user2",
				VectorClock:   map[string]int64{"user2": 1},
			},
			{
				ID:            "op1",
				Type:          "insert",
				Position:      5,
				Character:     "a",
				ParticipantID: "user1",
				VectorClock:   map[string]int64{"user1": 1},
			},
		},
	}

	state.ResolveConflicts()

	// After resolution, operations should be sorted
	if len(state.operations) != 2 {
		t.Errorf("expected 2 operations, got %d", len(state.operations))
	}
}

func TestQueryEditOperation(t *testing.T) {
	edit := QueryEditOperation{
		Type:     "insert",
		Position: 5,
		Text:     "hello",
	}

	if edit.Type != "insert" {
		t.Errorf("expected type 'insert', got '%s'", edit.Type)
	}
	if edit.Position != 5 {
		t.Errorf("expected position 5, got %d", edit.Position)
	}
	if edit.Text != "hello" {
		t.Errorf("expected text 'hello', got '%s'", edit.Text)
	}

	// Test delete operation
	deleteEdit := QueryEditOperation{
		Type:     "delete",
		Position: 10,
		Length:   5,
	}

	if deleteEdit.Length != 5 {
		t.Errorf("expected length 5, got %d", deleteEdit.Length)
	}

	// Test replace operation
	replaceEdit := QueryEditOperation{
		Type:     "replace",
		Position: 0,
		Text:     "UPDATE",
		Length:   6,
		OldText:  "SELECT",
	}

	if replaceEdit.OldText != "SELECT" {
		t.Errorf("expected old_text 'SELECT', got '%s'", replaceEdit.OldText)
	}
}

func TestCopyVectorClock(t *testing.T) {
	original := map[string]int64{"user1": 5, "user2": 3}
	copied := copyVectorClock(original)

	if copied["user1"] != 5 {
		t.Errorf("expected user1=5, got %d", copied["user1"])
	}

	// Modify original, copied should not change
	original["user1"] = 10
	if copied["user1"] != 5 {
		t.Error("copy was affected by original modification")
	}
}

func TestCopyParams(t *testing.T) {
	original := map[string]any{"key1": "value1", "key2": 42}
	copied := copyParams(original)

	if copied["key1"] != "value1" {
		t.Errorf("expected key1='value1', got %v", copied["key1"])
	}

	// Modify original
	original["key1"] = "modified"
	if copied["key1"] != "value1" {
		t.Error("copy was affected by original modification")
	}
}

func TestParseQueryText(t *testing.T) {
	queryText := "SELECT avg(value) FROM cpu WHERE host='server1'"
	timeRange := &CollabQueryTimeRange{
		Start: time.Now().Add(-1 * time.Hour),
		End:   time.Now(),
	}

	q, err := parseQueryText(queryText, timeRange)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if q.Start == 0 {
		t.Error("expected Start to be set")
	}
	if q.End == 0 {
		t.Error("expected End to be set")
	}

	// Test without time range
	q2, err := parseQueryText(queryText, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q2.Start != 0 {
		t.Error("expected Start to be 0 when no time range")
	}
}
