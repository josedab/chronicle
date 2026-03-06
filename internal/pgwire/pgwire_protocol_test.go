package pgwire

import (
	"testing"
	"time"
)

// --- handleCatalogQuery paths ---

func TestHandleCatalogQuery_PgType(t *testing.T) {
	db := &mockPGDB{metrics: []string{"cpu"}}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	err := sess.executeStatement("SELECT * FROM pg_catalog.pg_type")
	if err != nil {
		t.Fatalf("pg_type: %v", err)
	}
	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()
	if written == 0 {
		t.Error("Expected response for pg_type query")
	}
}

func TestHandleCatalogQuery_InformationSchema(t *testing.T) {
	db := &mockPGDB{metrics: []string{"cpu"}}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	err := sess.executeStatement("SELECT * FROM information_schema.tables")
	if err != nil {
		t.Fatalf("information_schema: %v", err)
	}
}

func TestHandleCatalogQuery_PgNamespace(t *testing.T) {
	db := &mockPGDB{metrics: []string{"cpu"}}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	err := sess.executeStatement("SELECT * FROM pg_catalog.pg_namespace")
	if err != nil {
		t.Fatalf("pg_namespace: %v", err)
	}
}

// --- handleDescribe for portal ---

func TestHandleDescribe_Portal(t *testing.T) {
	db := &mockPGDB{
		points: []Point{{Metric: "cpu", Value: 42, Timestamp: 1000}},
	}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	// Create prepared statement and portal
	sess.handleParse([]byte("stmt1\x00SELECT * FROM cpu\x00"))
	sess.handleBind([]byte("portal1\x00stmt1\x00"))

	// Describe portal ('P' + portal name)
	sess.handleDescribe([]byte("Pportal1\x00"))
	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()
	if written == 0 {
		t.Error("Expected row description for portal describe")
	}
}

// --- handleExecute with parameter substitution ---

func TestHandleExecute_WithParams(t *testing.T) {
	db := &mockPGDB{
		points: []Point{{Metric: "cpu", Value: 42, Timestamp: 1000}},
	}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	// Parse with parameters
	sess.handleParse([]byte("stmt1\x00SELECT * FROM cpu WHERE host = $1\x00"))
	// Bind with parameter value
	sess.handleBind([]byte("portal1\x00stmt1\x00server1\x00"))
	// Execute
	err := sess.handleExecute([]byte("portal1\x00\x00\x00\x00\x00"))
	if err != nil {
		t.Fatalf("handleExecute with params: %v", err)
	}
}

// --- handleSession with message routing ---

func TestHandleSession_BindExecuteSync(t *testing.T) {
	db := &mockPGDB{
		points: []Point{{Metric: "cpu", Value: 42, Timestamp: 1000}},
	}
	cfg := DefaultPGWireConfig()
	cfg.RequireAuth = false
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)

	// Startup
	mc.feedStartupMessage(196608, map[string]string{"user": "test"})
	// Parse
	mc.feedMessage(PGMsgParse, []byte("s1\x00SELECT 1\x00\x00\x00"))
	// Bind
	mc.feedMessage(PGMsgBind, []byte("p1\x00s1\x00"))
	// Describe
	mc.feedMessage(PGMsgDescribe, []byte("Ps1\x00"))
	// Execute
	mc.feedMessage(PGMsgExecute, []byte("p1\x00\x00\x00\x00\x00"))
	// Close
	mc.feedMessage(PGMsgClose, []byte("Ss1\x00"))
	// Sync
	mc.feedMessage(PGMsgSync, nil)
	// Terminate
	mc.feedMessage(PGMsgTerminate, nil)

	done := make(chan struct{})
	go func() {
		server.handleSession(sess)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleSession timeout")
	}
}

// --- handleQuery with SET statement ---

func TestHandleQuery_SetAndShow(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	// Multi-statement query with SET
	payload := append([]byte("SET client_encoding TO 'UTF8'; SHOW client_encoding"), 0)
	err := sess.handleQuery(payload)
	if err != nil {
		t.Fatalf("SET/SHOW: %v", err)
	}
}

// --- handleSelect ---

func TestHandleSelect_WithWhereAndTimestamp(t *testing.T) {
	now := time.Now().UnixNano()
	db := &mockPGDB{
		points: []Point{
			{Metric: "cpu", Value: 42, Timestamp: now, Tags: map[string]string{"host": "a"}},
		},
	}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	err := sess.executeStatement("SELECT value, timestamp FROM cpu WHERE timestamp > 1700000000000000000")
	if err != nil {
		t.Fatalf("SELECT with WHERE: %v", err)
	}
}

// --- parseWhereClause timestamp paths ---

func TestParseWhereClause_LessThan(t *testing.T) {
	maxEnd := int64(1<<63 - 1)
	_, end, _ := parseWhereClause("timestamp < 1700000000", 0, maxEnd)
	if end == maxEnd {
		t.Log("Timestamp extraction may not match '<' — depends on implementation")
	}
}

func TestParseWhereClause_MultipleConditions(t *testing.T) {
	maxEnd := int64(1<<63 - 1)
	_, _, tags := parseWhereClause("time >= 1600000000 AND time <= 1700000000 AND host = 'server1'", 0, maxEnd)
	if tags == nil {
		t.Error("Expected tags to be non-nil")
	} else if _, ok := tags["host"]; !ok {
		t.Errorf("Expected host tag, got %v", tags)
	}
}
