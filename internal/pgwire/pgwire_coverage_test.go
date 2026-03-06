package pgwire

import (
	"testing"
)

func TestWriteAuthMD5Password(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	salt := [4]byte{0x01, 0x02, 0x03, 0x04}
	sess.writeAuthMD5Password(salt)

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected auth MD5 message to be written")
	}
}

func TestWriteAuthSASL(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.writeAuthSASL()

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected SASL auth message to be written")
	}
}

func TestWriteCopyInResponse(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.writeCopyInResponse("text", '\t')

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected copy in response to be written")
	}
}

func TestWriteCopyInResponse_Binary(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.writeCopyInResponse("binary", '\t')

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected binary copy in response to be written")
	}
}

func TestHandleSpecialSelect_FromTables(t *testing.T) {
	db := &mockPGDB{metrics: []string{"cpu", "mem", "disk"}}
	translator := &PGQueryTranslator{db: db}

	// Call handleSpecialSelect directly since "SELECT * FROM tables"
	// matches the normal SELECT regex in executeSelect.
	result, err := translator.handleSpecialSelect("SELECT * FROM tables")
	if err != nil {
		t.Fatalf("handleSpecialSelect FROM tables: %v", err)
	}
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
}

func TestHandleSpecialSelect_FromPgTables(t *testing.T) {
	db := &mockPGDB{metrics: []string{"cpu"}}
	translator := &PGQueryTranslator{db: db}

	result, err := translator.handleSpecialSelect("SELECT * FROM pg_tables")
	if err != nil {
		t.Fatalf("handleSpecialSelect FROM pg_tables: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

func TestScramBase64Encode(t *testing.T) {
	result := scramBase64Encode([]byte("hello"))
	if result == "" {
		t.Error("Expected non-empty base64")
	}
	// Should match standard base64
	expected := base64Encode([]byte("hello"))
	if result != expected {
		t.Errorf("scramBase64Encode = %q, want %q", result, expected)
	}
}

func TestWriteReadyForQuery(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.writeReadyForQuery()

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected ready for query message")
	}
}

func TestWriteAuthCleartextPassword(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.writeAuthCleartextPassword()

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected cleartext auth message")
	}
}

func TestSelectVersion(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	err := sess.executeStatement("SELECT VERSION()")
	if err != nil {
		t.Fatalf("SELECT VERSION(): %v", err)
	}
}

func TestSelectCurrentDatabase(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)
	sess.database = "mydb"

	err := sess.executeStatement("SELECT CURRENT_DATABASE()")
	if err != nil {
		t.Fatalf("SELECT CURRENT_DATABASE(): %v", err)
	}
}

func TestStartTransaction(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	err := sess.executeStatement("START TRANSACTION")
	if err != nil {
		t.Fatalf("START TRANSACTION: %v", err)
	}
	if sess.txState != PGTxInTx {
		t.Error("Expected transaction state")
	}
}

func TestEndTransaction(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.executeStatement("BEGIN")
	err := sess.executeStatement("END")
	if err != nil {
		t.Fatalf("END: %v", err)
	}
	if sess.txState != PGTxIdle {
		t.Error("Expected idle state after END")
	}
}
