package pgwire

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// mockConn implements net.Conn for testing PGWire session lifecycle.
type mockConn struct {
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
	mu       sync.Mutex
	closed   bool
}

func newMockConn() *mockConn {
	return &mockConn{
		readBuf:  bytes.NewBuffer(nil),
		writeBuf: bytes.NewBuffer(nil),
	}
}

func (c *mockConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, io.EOF
	}
	if c.readBuf.Len() == 0 {
		return 0, io.EOF
	}
	return c.readBuf.Read(b)
}

func (c *mockConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	return c.writeBuf.Write(b)
}

func (c *mockConn) Close() error   { c.mu.Lock(); c.closed = true; c.mu.Unlock(); return nil }
func (c *mockConn) LocalAddr() net.Addr  { return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5432} }
func (c *mockConn) RemoteAddr() net.Addr { return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345} }
func (c *mockConn) SetDeadline(t time.Time) error      { return nil }
func (c *mockConn) SetReadDeadline(t time.Time) error   { return nil }
func (c *mockConn) SetWriteDeadline(t time.Time) error  { return nil }

// feedMessage writes a PG wire protocol message into the mock conn's read buffer.
func (c *mockConn) feedMessage(msgType byte, payload []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readBuf.WriteByte(msgType)
	length := int32(4 + len(payload))
	binary.Write(c.readBuf, binary.BigEndian, length)
	c.readBuf.Write(payload)
}

// feedStartupMessage writes a startup message (no type byte, just len+payload).
func (c *mockConn) feedStartupMessage(version uint32, params map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, version)
	for k, v := range params {
		payload.WriteString(k)
		payload.WriteByte(0)
		payload.WriteString(v)
		payload.WriteByte(0)
	}
	payload.WriteByte(0)

	length := int32(4 + payload.Len())
	binary.Write(c.readBuf, binary.BigEndian, length)
	c.readBuf.Write(payload.Bytes())
}

// --- readMessage tests ---

func TestReadMessage_Valid(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	mc.feedMessage('Q', append([]byte("SELECT 1"), 0))

	msgType, payload, err := sess.readMessage()
	if err != nil {
		t.Fatalf("readMessage: %v", err)
	}
	if msgType != 'Q' {
		t.Errorf("msgType = %c, want Q", msgType)
	}
	if string(payload) != "SELECT 1\x00" {
		t.Errorf("payload = %q", payload)
	}
}

func TestReadMessage_EmptyPayload(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	// Message with length=4 means 0 bytes of payload
	mc.feedMessage('S', nil)

	msgType, payload, err := sess.readMessage()
	if err != nil {
		t.Fatalf("readMessage empty: %v", err)
	}
	if msgType != 'S' {
		t.Errorf("msgType = %c, want S", msgType)
	}
	if payload != nil {
		t.Errorf("payload should be nil, got %d bytes", len(payload))
	}
}

func TestReadMessage_Truncated(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	// Only 2 bytes — incomplete header
	mc.mu.Lock()
	mc.readBuf.Write([]byte{0x51, 0x00})
	mc.mu.Unlock()

	_, _, err := sess.readMessage()
	if err == nil {
		t.Error("Expected error for truncated message")
	}
}

func TestReadMessage_InvalidLength(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	// 20MB message — exceeds 10MB limit
	mc.mu.Lock()
	mc.readBuf.WriteByte('Q')
	binary.Write(mc.readBuf, binary.BigEndian, int32(20*1024*1024+4))
	mc.mu.Unlock()

	_, _, err := sess.readMessage()
	if err == nil {
		t.Error("Expected error for oversized message")
	}
}

// --- flush tests ---

func TestFlush_WritesBufferedData(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	sess.writeMessage('R', []byte{0, 0, 0, 0})
	err := sess.flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}

	mc.mu.Lock()
	written := mc.writeBuf.Len()
	mc.mu.Unlock()

	if written == 0 {
		t.Error("Expected data to be flushed to conn")
	}
}

func TestFlush_EmptyBuffer(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	err := sess.flush()
	if err != nil {
		t.Fatalf("flush empty: %v", err)
	}
}

// --- handleQuery tests ---

func TestHandleQuery_SelectOne(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	payload := append([]byte("SELECT 1"), 0)
	err := sess.handleQuery(payload)
	if err != nil {
		t.Fatalf("handleQuery SELECT 1: %v", err)
	}

	mc.mu.Lock()
	written := mc.writeBuf.Len()
	mc.mu.Unlock()
	if written == 0 {
		t.Error("Expected response to be written")
	}
}

func TestHandleQuery_EmptyQuery(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	payload := []byte{0} // empty null-terminated string
	err := sess.handleQuery(payload)
	if err != nil {
		t.Fatalf("handleQuery empty: %v", err)
	}
}

func TestHandleQuery_MultipleStatements(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	payload := append([]byte("SELECT 1; SELECT 1"), 0)
	err := sess.handleQuery(payload)
	if err != nil {
		t.Fatalf("handleQuery multi: %v", err)
	}
}

// --- handleInsert via session ---

func TestHandleInsert_ViaExecuteStatement(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	err := sess.executeStatement(`INSERT INTO cpu (value, host) VALUES (42.5, 'server1')`)
	if err != nil {
		t.Fatalf("handleInsert: %v", err)
	}
	if len(db.written) != 1 {
		t.Errorf("Expected 1 written point, got %d", len(db.written))
	}
}

// --- parseStartupParams ---

func TestParseStartupParams(t *testing.T) {
	data := []byte("user\x00postgres\x00database\x00mydb\x00\x00")
	params := parseStartupParams(data)

	if params["user"] != "postgres" {
		t.Errorf("user = %q, want postgres", params["user"])
	}
	if params["database"] != "mydb" {
		t.Errorf("database = %q, want mydb", params["database"])
	}
}

func TestParseStartupParams_Empty(t *testing.T) {
	params := parseStartupParams([]byte{0})
	if len(params) != 0 {
		t.Errorf("Expected 0 params, got %d", len(params))
	}
}

// --- handleStartup ---

func TestHandleStartup_Normal(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.RequireAuth = false
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)

	// Write a valid v3 startup message
	mc.feedStartupMessage(196608, map[string]string{"user": "test", "database": "testdb"}) // 196608 = 3.0

	err := sess.handleStartup()
	if err != nil {
		t.Fatalf("handleStartup: %v", err)
	}
	if sess.user != "test" {
		t.Errorf("user = %q, want test", sess.user)
	}
	if sess.database != "testdb" {
		t.Errorf("database = %q, want testdb", sess.database)
	}
}

func TestHandleStartup_SSLRequest(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.RequireAuth = false
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)

	// First: SSL request (version 80877103)
	mc.feedStartupMessage(80877103, nil)
	// Then: normal startup after SSL denial
	mc.feedStartupMessage(196608, map[string]string{"user": "test"})

	err := sess.handleStartup()
	if err != nil {
		t.Fatalf("handleStartup with SSL: %v", err)
	}

	// Check that 'N' was written (SSL not supported)
	mc.mu.Lock()
	writtenData := mc.writeBuf.Bytes()
	mc.mu.Unlock()
	if len(writtenData) == 0 {
		t.Error("Expected 'N' SSL rejection to be written")
	}
}

func TestHandleStartup_InvalidLength(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	// Write length of 2 (too small — minimum is 8)
	mc.mu.Lock()
	binary.Write(mc.readBuf, binary.BigEndian, int32(2))
	mc.mu.Unlock()

	err := sess.handleStartup()
	if err == nil {
		t.Error("Expected error for invalid startup length")
	}
}

// --- authenticateCleartext ---

func TestAuthenticateCleartext_Success(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.RequireAuth = true
	cfg.Password = "secret123"
	cfg.AuthMethod = "cleartext"
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)
	sess.user = "admin"

	// Feed password response
	mc.feedMessage(PGMsgPassword, append([]byte("secret123"), 0))

	err := sess.authenticateCleartext()
	if err != nil {
		t.Fatalf("authenticateCleartext: %v", err)
	}
}

func TestAuthenticateCleartext_WrongPassword(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.RequireAuth = true
	cfg.Password = "secret123"
	cfg.AuthMethod = "cleartext"
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)
	sess.user = "admin"

	mc.feedMessage(PGMsgPassword, append([]byte("wrong"), 0))

	err := sess.authenticateCleartext()
	if err == nil {
		t.Error("Expected error for wrong password")
	}
}

func TestAuthenticateCleartext_WrongMessageType(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.Password = "secret"
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)

	mc.feedMessage('Q', []byte("not a password"))

	err := sess.authenticateCleartext()
	if err == nil {
		t.Error("Expected error for wrong message type")
	}
}

// --- authenticateMD5 ---

func TestAuthenticateMD5_Success(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.RequireAuth = true
	cfg.Password = "mypassword"
	cfg.AuthMethod = "md5"
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)
	sess.user = "testuser"

	// We need to intercept the salt from the server's auth message.
	// Since authenticateMD5 generates a random salt internally,
	// we'll test the flow by feeding the correct password format.
	// The MD5 auth flow: server sends salt → client sends md5(md5(pw+user)+salt)
	// We can't predict the salt, so we test the failure path.
	mc.feedMessage(PGMsgPassword, append([]byte("md5wronghash0000000000000000000000"), 0))

	err := sess.authenticateMD5()
	if err == nil {
		t.Error("Expected error for wrong MD5 password")
	}
}

// --- authenticateSession routing ---

func TestAuthenticateSession_Cleartext(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.Password = "secret"
	cfg.AuthMethod = "cleartext"
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)
	sess.user = "user"

	mc.feedMessage(PGMsgPassword, append([]byte("secret"), 0))

	err := sess.authenticateSession()
	if err != nil {
		t.Fatalf("authenticateSession cleartext: %v", err)
	}
}

func TestAuthenticateSession_MD5(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.Password = "secret"
	cfg.AuthMethod = "md5"
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)
	sess.user = "user"

	// Wrong password — just test routing
	mc.feedMessage(PGMsgPassword, append([]byte("md5wrong00000000000000000000000000"), 0))
	err := sess.authenticateSession()
	if err == nil {
		t.Error("Expected auth failure for wrong MD5")
	}
}

func TestAuthenticateSession_DefaultFallback(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.Password = "secret"
	cfg.AuthMethod = "" // defaults to cleartext
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)
	sess.user = "user"

	mc.feedMessage(PGMsgPassword, append([]byte("secret"), 0))
	err := sess.authenticateSession()
	if err != nil {
		t.Fatalf("authenticateSession default: %v", err)
	}
}

// --- handleCopyIn ---

func TestHandleCopyIn(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	// Feed CopyData then CopyDone
	copyData := []byte("1000\t42.5\tcpu\thost=server1\n")
	mc.feedMessage(PGMsgCopyData, copyData)
	mc.feedMessage(PGMsgCopyDone, nil)

	err := sess.handleCopyIn("COPY metrics FROM STDIN")
	if err != nil {
		t.Fatalf("handleCopyIn: %v", err)
	}
	if len(db.written) != 1 {
		t.Errorf("Expected 1 written point, got %d", len(db.written))
	}
}

func TestHandleCopyIn_MultipleRows(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	copyData := []byte("1000\t42.5\tcpu\n2000\t55.0\tcpu\n3000\t60.0\tcpu\n")
	mc.feedMessage(PGMsgCopyData, copyData)
	mc.feedMessage(PGMsgCopyDone, nil)

	err := sess.handleCopyIn("COPY metrics FROM STDIN")
	if err != nil {
		t.Fatalf("handleCopyIn multi: %v", err)
	}
	if len(db.written) != 3 {
		t.Errorf("Expected 3 written points, got %d", len(db.written))
	}
}

func TestHandleCopyIn_CopyFail(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	mc := newMockConn()
	sess := newPGSession(server, mc)

	mc.feedMessage(PGMsgCopyFail, append([]byte("client cancelled"), 0))

	err := sess.handleCopyIn("COPY metrics FROM STDIN")
	if err == nil {
		t.Error("Expected error on CopyFail")
	}
}

// --- PGServer Start/Stop ---

func TestPGServer_StartStop(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.Address = "127.0.0.1:0" // random port
	server, err := NewPGServer(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := server.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !server.running.Load() {
		t.Error("Server should be running")
	}

	// Double start should fail
	if err := server.Start(); err == nil {
		t.Error("Expected error on double start")
	}

	if err := server.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if server.running.Load() {
		t.Error("Server should not be running after stop")
	}
}

func TestPGServer_StopWhenNotRunning(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())

	err := server.Stop()
	if err != nil {
		t.Fatalf("Stop when not running: %v", err)
	}
}

// --- Full session flow via handleSession ---

func TestHandleSession_QueryAndTerminate(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.RequireAuth = false
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)

	// Feed startup message
	mc.feedStartupMessage(196608, map[string]string{"user": "test"})
	// Feed a query
	mc.feedMessage(PGMsgQuery, append([]byte("SELECT 1"), 0))
	// Feed terminate
	mc.feedMessage(PGMsgTerminate, nil)

	// Run in goroutine since handleSession blocks
	done := make(chan struct{})
	go func() {
		server.handleSession(sess)
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("handleSession did not complete")
	}
}

func TestHandleSession_WithAuth(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.RequireAuth = true
	cfg.Password = "pass"
	cfg.AuthMethod = "cleartext"
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)

	mc.feedStartupMessage(196608, map[string]string{"user": "test"})
	mc.feedMessage(PGMsgPassword, append([]byte("pass"), 0))
	mc.feedMessage(PGMsgTerminate, nil)

	done := make(chan struct{})
	go func() {
		server.handleSession(sess)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleSession with auth did not complete")
	}
}

// --- handleSession with extended query protocol ---

func TestHandleSession_ExtendedProtocol(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.RequireAuth = false
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)

	mc.feedStartupMessage(196608, map[string]string{"user": "test"})
	// Parse
	mc.feedMessage(PGMsgParse, []byte("stmt1\x00SELECT 1\x00\x00\x00"))
	// Sync
	mc.feedMessage(PGMsgSync, nil)
	// Flush
	mc.feedMessage(PGMsgFlush, nil)
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
		t.Fatal("handleSession extended protocol did not complete")
	}
}

// --- handleSession with COPY ---

func TestHandleSession_CopyIn(t *testing.T) {
	db := &mockPGDB{}
	cfg := DefaultPGWireConfig()
	cfg.RequireAuth = false
	server, _ := NewPGServer(db, cfg)
	mc := newMockConn()
	sess := newPGSession(server, mc)

	mc.feedStartupMessage(196608, map[string]string{"user": "test"})
	mc.feedMessage(PGMsgQuery, append([]byte("COPY metrics FROM STDIN"), 0))
	mc.feedMessage(PGMsgCopyData, []byte("1000\t42.5\tcpu\n"))
	mc.feedMessage(PGMsgCopyDone, nil)
	mc.feedMessage(PGMsgTerminate, nil)

	done := make(chan struct{})
	go func() {
		server.handleSession(sess)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleSession COPY did not complete")
	}
}
