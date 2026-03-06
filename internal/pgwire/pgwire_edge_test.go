package pgwire

import (
	"testing"
	"time"
)

// --- MD5 Password ---

func TestComputeMD5Password(t *testing.T) {
	salt := [4]byte{0x01, 0x02, 0x03, 0x04}
	result := computeMD5Password("testuser", "testpass", salt)
	if result[:3] != "md5" {
		t.Error("MD5 password should start with 'md5'")
	}
	if len(result) != 35 { // "md5" + 32 hex chars
		t.Errorf("Expected 35 chars, got %d", len(result))
	}

	// Same inputs should produce same output
	result2 := computeMD5Password("testuser", "testpass", salt)
	if result != result2 {
		t.Error("Deterministic: same inputs should produce same hash")
	}

	// Different salt should produce different output
	salt2 := [4]byte{0x05, 0x06, 0x07, 0x08}
	result3 := computeMD5Password("testuser", "testpass", salt2)
	if result == result3 {
		t.Error("Different salt should produce different hash")
	}
}

func TestVerifyMD5Password(t *testing.T) {
	db := &mockPGDB{}
	config := DefaultPGWireConfig()
	config.Password = "secretpass"
	config.Username = "admin"
	server, _ := NewPGServer(db, config)
	sess := newPGSession(server, nil)
	sess.user = "admin"

	salt := [4]byte{0xAA, 0xBB, 0xCC, 0xDD}
	correctPassword := computeMD5Password("admin", "secretpass", salt)

	if !sess.verifyMD5Password(correctPassword, salt) {
		t.Error("Correct password should verify")
	}

	if sess.verifyMD5Password("md5wronghash00000000000000000000", salt) {
		t.Error("Wrong password should not verify")
	}
}

// --- COPY statement parsing ---

func TestParseCopyStatement(t *testing.T) {
	tests := []struct {
		stmt      string
		table     string
		format    string
		delimiter byte
		wantErr   bool
	}{
		{
			stmt:      "COPY metrics FROM STDIN",
			table:     "metrics",
			format:    "text",
			delimiter: '\t',
		},
		{
			stmt:      `COPY "my_table" FROM STDIN WITH FORMAT csv DELIMITER |`,
			table:     "my_table",
			format:    "csv",
			delimiter: '|',
		},
		{
			stmt:      "COPY measurements FROM STDIN WITH (FORMAT text)",
			table:     "measurements",
			format:    "text",
			delimiter: '\t',
		},
		{
			stmt:    "NOT A COPY",
			wantErr: true,
		},
		{
			stmt:    "COPY",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		table, format, delim, err := parseCopyStatement(tc.stmt)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseCopyStatement(%q): expected error", tc.stmt)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseCopyStatement(%q): %v", tc.stmt, err)
			continue
		}
		if table != tc.table {
			t.Errorf("table = %q, want %q", table, tc.table)
		}
		if format != tc.format {
			t.Errorf("format = %q, want %q", format, tc.format)
		}
		if delim != tc.delimiter {
			t.Errorf("delimiter = %c, want %c", delim, tc.delimiter)
		}
	}
}

// --- CSV splitting ---

func TestSplitCSV(t *testing.T) {
	tests := []struct {
		line   string
		delim  byte
		expect int
	}{
		{"a,b,c", ',', 3},
		{`"hello,world",b`, ',', 2},
		{"a\tb\tc", '\t', 3},
		{"single", ',', 1},
		{"", ',', 1},
		{`"quoted""value",normal`, ',', 2},
	}

	for _, tc := range tests {
		fields := splitCSV(tc.line, tc.delim)
		if len(fields) != tc.expect {
			t.Errorf("splitCSV(%q, %c) = %d fields, want %d: %v", tc.line, tc.delim, len(fields), tc.expect, fields)
		}
	}
}

// --- processCopyData ---

func TestProcessCopyData_TextFormat(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	data := []byte("1000\t42.5\tcpu\thost=server1\n2000\t55.0\tcpu\thost=server2\n")
	n, err := sess.processCopyData("metrics", data, "text", '\t')
	if err != nil {
		t.Fatalf("processCopyData: %v", err)
	}
	if n != 2 {
		t.Errorf("Expected 2 rows, got %d", n)
	}
	if len(db.written) != 2 {
		t.Fatalf("Expected 2 written points, got %d", len(db.written))
	}
	if db.written[0].Value != 42.5 {
		t.Errorf("Expected value 42.5, got %f", db.written[0].Value)
	}
}

func TestProcessCopyData_CSVFormat(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	data := []byte("1000,42.5,cpu,host=a\n")
	n, err := sess.processCopyData("metrics", data, "csv", ',')
	if err != nil {
		t.Fatalf("processCopyData csv: %v", err)
	}
	if n != 1 {
		t.Errorf("Expected 1 row, got %d", n)
	}
}

func TestProcessCopyData_EmptyData(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	n, err := sess.processCopyData("metrics", []byte(""), "text", '\t')
	if err != nil {
		t.Fatalf("processCopyData empty: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected 0 rows for empty data, got %d", n)
	}
}

func TestProcessCopyData_EndOfData(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	data := []byte("\\.\n")
	n, err := sess.processCopyData("metrics", data, "text", '\t')
	if err != nil {
		t.Fatalf("processCopyData end-of-data: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected 0 rows for end-of-data marker, got %d", n)
	}
}

func TestProcessCopyData_InvalidTimestamp(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	// Invalid timestamp should use time.Now() fallback
	data := []byte("not-a-number\t42.5\tcpu\n")
	n, err := sess.processCopyData("metrics", data, "text", '\t')
	if err != nil {
		t.Fatalf("processCopyData invalid ts: %v", err)
	}
	if n != 1 {
		t.Errorf("Expected 1 row, got %d", n)
	}
	if db.written[0].Timestamp == 0 {
		t.Error("Timestamp should have fallback value")
	}
}

// --- SCRAM helpers ---

func TestParseSCRAMClientFirst(t *testing.T) {
	msg := "n,,n=user,r=clientnonce123"
	state, err := parseSCRAMClientFirst(msg)
	if err != nil {
		t.Fatalf("parseSCRAMClientFirst: %v", err)
	}
	if state.clientNonce != "clientnonce123" {
		t.Errorf("clientNonce = %q, want clientnonce123", state.clientNonce)
	}
}

func TestParseSCRAMClientFirst_MissingNonce(t *testing.T) {
	msg := "n,,n=user"
	_, err := parseSCRAMClientFirst(msg)
	if err == nil {
		t.Error("Expected error for missing nonce")
	}
}

func TestParseSCRAMClientFirst_InvalidFormat(t *testing.T) {
	_, err := parseSCRAMClientFirst("invalid")
	if err == nil {
		t.Error("Expected error for invalid format")
	}
}

func TestExtractSASLResponse(t *testing.T) {
	// Mechanism\0 + 4-byte length + data
	payload := []byte("SCRAM-SHA-256\x00\x00\x00\x00\x04data")
	result := extractSASLResponse(payload)
	if result != "data" {
		t.Errorf("extractSASLResponse = %q, want 'data'", result)
	}
}

func TestExtractBeforeProof(t *testing.T) {
	msg := "c=biws,r=nonce,p=proof123"
	result := extractBeforeProof(msg)
	if result != "c=biws,r=nonce" {
		t.Errorf("extractBeforeProof = %q, want 'c=biws,r=nonce'", result)
	}

	// No proof
	result2 := extractBeforeProof("no-proof-here")
	if result2 != "no-proof-here" {
		t.Errorf("extractBeforeProof (no proof) = %q", result2)
	}
}

func TestExtractProof(t *testing.T) {
	msg := "c=biws,r=nonce,p=proofdata"
	proof := extractProof(msg)
	if string(proof) != "proofdata" {
		t.Errorf("extractProof = %q, want 'proofdata'", proof)
	}

	noProof := extractProof("no proof")
	if noProof != nil {
		t.Errorf("Expected nil for no proof, got %v", noProof)
	}
}

// --- Crypto helpers ---

func TestComputeHMAC(t *testing.T) {
	key := []byte("secret")
	data := []byte("message")
	mac := computeHMAC(key, data)
	if len(mac) != 32 { // SHA-256 output
		t.Errorf("HMAC length = %d, want 32", len(mac))
	}

	// Deterministic
	mac2 := computeHMAC(key, data)
	for i := range mac {
		if mac[i] != mac2[i] {
			t.Error("HMAC should be deterministic")
			break
		}
	}
}

func TestXorBytes(t *testing.T) {
	a := []byte{0xFF, 0x00, 0xAA}
	b := []byte{0x0F, 0xF0, 0x55}
	result := xorBytes(a, b)
	expected := []byte{0xF0, 0xF0, 0xFF}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("xorBytes[%d] = %02X, want %02X", i, result[i], expected[i])
		}
	}

	// Different lengths
	short := xorBytes([]byte{0xFF, 0x00}, []byte{0x0F})
	if len(short) != 1 {
		t.Errorf("xorBytes different lengths: len=%d, want 1", len(short))
	}
}

func TestBase64Encode(t *testing.T) {
	tests := []struct {
		input    []byte
		expected string
	}{
		{[]byte{}, ""},
		{[]byte("f"), "Zg=="},
		{[]byte("fo"), "Zm8="},
		{[]byte("foo"), "Zm9v"},
		{[]byte("foobar"), "Zm9vYmFy"},
	}

	for _, tc := range tests {
		result := base64Encode(tc.input)
		if result != tc.expected {
			t.Errorf("base64Encode(%q) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

// --- Protocol message helpers ---

func TestAppendInt16(t *testing.T) {
	buf := appendInt16(nil, 256)
	if len(buf) != 2 {
		t.Errorf("Expected 2 bytes, got %d", len(buf))
	}
	if buf[0] != 1 || buf[1] != 0 {
		t.Errorf("appendInt16(256) = %v, want [1 0]", buf)
	}
}

func TestAppendInt32(t *testing.T) {
	buf := appendInt32(nil, 65536)
	if len(buf) != 4 {
		t.Errorf("Expected 4 bytes, got %d", len(buf))
	}
}

func TestAppendString(t *testing.T) {
	buf := appendString(nil, "hello")
	if len(buf) != 6 { // 5 chars + null terminator
		t.Errorf("Expected 6 bytes, got %d", len(buf))
	}
	if buf[5] != 0 {
		t.Error("String should be null-terminated")
	}
}

// --- Session message writing ---

func TestSessionWriteError(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.writeError("42P01", "relation does not exist")

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected error response to be written")
	}
}

func TestSessionWriteRowDescription(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	cols := []PGColumn{
		{Name: "id", TypeOID: PGTypeInt4, TypeLen: 4, TypeMod: -1},
		{Name: "name", TypeOID: PGTypeText, TypeLen: -1, TypeMod: -1},
	}
	sess.writeRowDescription(cols)

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected row description to be written")
	}
}

func TestSessionWriteDataRow(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.writeDataRow([]string{"1", "test", ""})

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected data row to be written")
	}
}

func TestSessionWriteCommandComplete(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.writeCommandComplete("SELECT 5")

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected command complete to be written")
	}
}

// --- Extended query protocol edge cases ---

func TestHandleParse_MalformedPayload(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	err := sess.handleParse([]byte("single-part"))
	if err == nil {
		t.Error("Expected error for malformed Parse message")
	}
}

func TestHandleDescribe_EmptyPayload(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	// Should not panic with short payload
	sess.handleDescribe([]byte{})

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected NoData response for empty describe")
	}
}

func TestHandleDescribe_UnknownStatement(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.handleDescribe([]byte("Snonexistent\x00"))

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected NoData for unknown statement")
	}
}

func TestHandleClose_Statement(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	// First create a prepared statement
	sess.handleParse([]byte("stmt1\x00SELECT 1\x00"))

	// Close it
	sess.handleClose([]byte("Sstmt1\x00"))

	sess.mu.Lock()
	_, exists := sess.preparedStmts["stmt1"]
	sess.mu.Unlock()

	if exists {
		t.Error("Statement should be closed")
	}
}

func TestHandleClose_Portal(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.handleParse([]byte("stmt1\x00SELECT 1\x00"))
	sess.handleBind([]byte("portal1\x00stmt1\x00"))

	sess.handleClose([]byte("Pportal1\x00"))

	sess.mu.Lock()
	_, exists := sess.portals["portal1"]
	sess.mu.Unlock()

	if exists {
		t.Error("Portal should be closed")
	}
}

func TestHandleClose_EmptyPayload(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	// Should not panic
	sess.handleClose([]byte{})
}

// --- Execute with nonexistent portal ---

func TestHandleExecute_NonexistentPortal(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	err := sess.handleExecute([]byte("nonexistent\x00"))
	if err != nil {
		t.Errorf("Execute nonexistent portal should not error: %v", err)
	}
}

// --- Unsupported statement ---

func TestExecuteStatement_Unsupported(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	err := sess.executeStatement("CREATE TABLE test (id INT)")
	if err == nil {
		t.Error("Expected error for unsupported DDL")
	}
}

// --- SelectFromMetrics (special SELECT) ---

func TestHandleSelectFromMetrics(t *testing.T) {
	db := &mockPGDB{
		metrics: []string{"cpu", "memory", "disk"},
		points: []Point{
			{Metric: "metrics", Value: 1, Timestamp: 100},
			{Metric: "metrics", Value: 2, Timestamp: 200},
			{Metric: "metrics", Value: 3, Timestamp: 300},
		},
	}
	translator := &PGQueryTranslator{db: db}

	result, err := translator.Execute("SELECT * FROM metrics")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
}

// --- FormatValue with time ---

func TestFormatValue_Time(t *testing.T) {
	ts := time.Date(2024, 6, 15, 12, 30, 45, 0, time.UTC)
	result := formatValue(ts)
	if result != "2024-06-15 12:30:45" {
		t.Errorf("formatValue(time) = %q", result)
	}
}

// --- WriteAuthOK ---

func TestWriteAuthOK(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.writeAuthOK()

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected auth OK to be written")
	}
}

// --- WriteParamStatus ---

func TestWriteParamStatus(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.writeParamStatus("server_version", "15.0")

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected param status to be written")
	}
}

// --- WriteBackendKeyData ---

func TestWriteBackendKeyData(t *testing.T) {
	db := &mockPGDB{}
	server, _ := NewPGServer(db, DefaultPGWireConfig())
	sess := newPGSession(server, nil)

	sess.writeBackendKeyData(12345, 67890)

	sess.mu.Lock()
	written := sess.writer.Len()
	sess.mu.Unlock()

	if written == 0 {
		t.Error("Expected backend key data to be written")
	}
}

// --- BuildRow edge cases ---

func TestBuildRow_TagLookup(t *testing.T) {
	p := Point{
		Metric:    "cpu",
		Value:     42.0,
		Timestamp: 1000,
		Tags:      map[string]string{"host": "server1", "region": "us"},
	}

	row := buildRow([]string{"host", "region", "missing"}, p)
	if row[0] != "server1" {
		t.Errorf("host = %v, want server1", row[0])
	}
	if row[1] != "us" {
		t.Errorf("region = %v, want us", row[1])
	}
	if row[2] != nil {
		t.Errorf("missing tag should be nil, got %v", row[2])
	}
}

func TestBuildRow_NilTags(t *testing.T) {
	p := Point{Metric: "cpu", Value: 42.0, Tags: nil}
	row := buildRow([]string{"host"}, p)
	if row[0] != nil {
		t.Errorf("Should handle nil tags gracefully, got %v", row[0])
	}
}
