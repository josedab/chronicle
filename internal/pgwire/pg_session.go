package pgwire

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

// readMessage reads a PostgreSQL wire protocol message (type byte + length + payload).
func (sess *PGSession) readMessage() (byte, []byte, error) {
	_ = sess.conn.SetReadDeadline(time.Now().Add(sess.server.config.QueryTimeout))

	header := make([]byte, 5)
	if _, err := io.ReadFull(sess.conn, header); err != nil {
		return 0, nil, err
	}

	msgType := header[0]
	msgLen := int(binary.BigEndian.Uint32(header[1:5])) - 4

	if msgLen < 0 || msgLen > 10*1024*1024 {
		return 0, nil, fmt.Errorf("invalid message length: %d", msgLen)
	}

	if msgLen == 0 {
		return msgType, nil, nil
	}

	payload := make([]byte, msgLen)
	if _, err := io.ReadFull(sess.conn, payload); err != nil {
		return 0, nil, err
	}

	return msgType, payload, nil
}

// writeMessage writes a single protocol message.
func (sess *PGSession) writeMessage(msgType byte, data []byte) {
	sess.mu.Lock()
	defer sess.mu.Unlock()

	sess.writer.WriteByte(msgType)
	length := int32(4 + len(data))
	_ = binary.Write(sess.writer, binary.BigEndian, length)
	if len(data) > 0 {
		sess.writer.Write(data)
	}
}

// flush sends all buffered data to the client.
func (sess *PGSession) flush() error {
	sess.mu.Lock()
	defer sess.mu.Unlock()

	if sess.writer.Len() == 0 {
		return nil
	}
	_, err := sess.conn.Write(sess.writer.Bytes())
	sess.writer.Reset()
	return err
}

// writeAuthOK sends authentication successful.
func (sess *PGSession) writeAuthOK() {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], 0) // AuthOK
	sess.writeMessage(PGMsgAuth, buf[:])
}

// writeAuthCleartextPassword requests cleartext password.
func (sess *PGSession) writeAuthCleartextPassword() {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(PGAuthCleartextPw))
	sess.writeMessage(PGMsgAuth, buf[:])
}

// writeParamStatus sends a parameter status message.
func (sess *PGSession) writeParamStatus(name, value string) {
	data := make([]byte, 0, len(name)+len(value)+2)
	data = append(data, []byte(name)...)
	data = append(data, 0)
	data = append(data, []byte(value)...)
	data = append(data, 0)
	sess.writeMessage(PGMsgParamStatus, data)
}

// writeBackendKeyData sends backend key data for cancel requests.
func (sess *PGSession) writeBackendKeyData(pid, secret int32) {
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[:4], uint32(pid))
	binary.BigEndian.PutUint32(buf[4:], uint32(secret))
	sess.writeMessage(PGMsgBackendKeyData, buf[:])
}

// writeReadyForQuery signals the server is ready for a new query.
func (sess *PGSession) writeReadyForQuery() {
	sess.writeMessage(PGMsgReadyForQuery, []byte{sess.txState})
}

// writeError sends an ErrorResponse message.
func (sess *PGSession) writeError(code, message string) {
	var buf []byte
	buf = append(buf, 'S')
	buf = append(buf, []byte("ERROR")...)
	buf = append(buf, 0)
	buf = append(buf, 'V')
	buf = append(buf, []byte("ERROR")...)
	buf = append(buf, 0)
	buf = append(buf, 'C')
	buf = append(buf, []byte(code)...)
	buf = append(buf, 0)
	buf = append(buf, 'M')
	buf = append(buf, []byte(message)...)
	buf = append(buf, 0)
	buf = append(buf, 0) // terminator
	sess.writeMessage(PGMsgErrorResponse, buf)
}

// writeRowDescription sends column metadata.
func (sess *PGSession) writeRowDescription(columns []PGColumn) {
	var buf []byte
	// Number of fields (2 bytes)
	buf = appendInt16(buf, int16(len(columns)))

	for _, col := range columns {
		buf = appendString(buf, col.Name)
		buf = appendInt32(buf, 0)          // table OID
		buf = appendInt16(buf, 0)          // column attribute number
		buf = appendInt32(buf, col.TypeOID) // type OID
		buf = appendInt16(buf, col.TypeLen) // type size
		buf = appendInt32(buf, col.TypeMod) // type modifier
		buf = appendInt16(buf, 0)          // format (0=text)
	}

	sess.writeMessage(PGMsgRowDescription, buf)
}

// writeDataRow sends a single data row.
func (sess *PGSession) writeDataRow(values []string) {
	var buf []byte
	buf = appendInt16(buf, int16(len(values)))

	for _, val := range values {
		if val == "" {
			buf = appendInt32(buf, -1) // NULL
		} else {
			buf = appendInt32(buf, int32(len(val)))
			buf = append(buf, []byte(val)...)
		}
	}

	sess.writeMessage(PGMsgDataRow, buf)
}

// writeCommandComplete sends command completion.
func (sess *PGSession) writeCommandComplete(tag string) {
	data := append([]byte(tag), 0)
	sess.writeMessage(PGMsgCommandComplete, data)
}

// handleQuery processes a simple query message.
func (sess *PGSession) handleQuery(payload []byte) error {
	query := strings.TrimRight(string(payload), "\x00")
	query = strings.TrimSpace(query)

	if query == "" {
		sess.writeMessage(PGMsgEmptyQuery, nil)
		sess.writeReadyForQuery()
		return sess.flush()
	}

	// Handle multiple statements separated by semicolons
	statements := splitStatements(query)
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := sess.executeStatement(stmt); err != nil {
			sess.writeError("42601", err.Error())
			sess.writeReadyForQuery()
			return sess.flush()
		}
	}

	sess.writeReadyForQuery()
	return sess.flush()
}

func (sess *PGSession) executeStatement(stmt string) error {
	upper := strings.ToUpper(strings.TrimSpace(stmt))

	// Handle system/catalog queries that psql and tools send
	switch {
	case upper == "SELECT 1", upper == "SELECT 1;":
		return sess.handleSelectOne()
	case strings.HasPrefix(upper, "SET "):
		sess.writeCommandComplete("SET")
		return nil
	case strings.HasPrefix(upper, "SHOW "):
		return sess.handleShow(stmt)
	case strings.HasPrefix(upper, "SELECT VERSION()"):
		return sess.handleVersion()
	case strings.HasPrefix(upper, "SELECT CURRENT_DATABASE()"):
		return sess.handleCurrentDB()
	case strings.Contains(upper, "PG_CATALOG") || strings.Contains(upper, "INFORMATION_SCHEMA"):
		return sess.handleCatalogQuery(stmt)
	case strings.HasPrefix(upper, "SELECT"):
		return sess.handleSelect(stmt)
	case strings.HasPrefix(upper, "INSERT"):
		return sess.handleInsert(stmt)
	case strings.HasPrefix(upper, "BEGIN") || strings.HasPrefix(upper, "START TRANSACTION"):
		sess.txState = PGTxInTx
		sess.writeCommandComplete("BEGIN")
		return nil
	case strings.HasPrefix(upper, "COMMIT") || strings.HasPrefix(upper, "END"):
		sess.txState = PGTxIdle
		sess.writeCommandComplete("COMMIT")
		return nil
	case strings.HasPrefix(upper, "ROLLBACK"):
		sess.txState = PGTxIdle
		sess.writeCommandComplete("ROLLBACK")
		return nil
	default:
		return fmt.Errorf("unsupported statement: %s", stmt)
	}
}

func (sess *PGSession) handleSelectOne() error {
	cols := []PGColumn{{Name: "?column?", TypeOID: PGTypeInt4, TypeLen: 4, TypeMod: -1}}
	sess.writeRowDescription(cols)
	sess.writeDataRow([]string{"1"})
	sess.writeCommandComplete("SELECT 1")
	return nil
}

func (sess *PGSession) handleVersion() error {
	cols := []PGColumn{{Name: "version", TypeOID: PGTypeText, TypeLen: -1, TypeMod: -1}}
	sess.writeRowDescription(cols)
	version := fmt.Sprintf("PostgreSQL %s on chronicle-db", sess.server.config.ServerVersion)
	sess.writeDataRow([]string{version})
	sess.writeCommandComplete("SELECT 1")
	return nil
}

func (sess *PGSession) handleCurrentDB() error {
	cols := []PGColumn{{Name: "current_database", TypeOID: PGTypeText, TypeLen: -1, TypeMod: -1}}
	sess.writeRowDescription(cols)
	sess.writeDataRow([]string{sess.database})
	sess.writeCommandComplete("SELECT 1")
	return nil
}

func (sess *PGSession) handleShow(stmt string) error {
	parts := strings.Fields(stmt)
	if len(parts) < 2 {
		return fmt.Errorf("invalid SHOW statement")
	}
	param := strings.ToLower(parts[1])
	var value string
	switch param {
	case "server_version":
		value = sess.server.config.ServerVersion
	case "server_encoding":
		value = "UTF8"
	case "client_encoding":
		value = "UTF8"
	case "timezone":
		value = "UTC"
	case "datestyle":
		value = "ISO, MDY"
	case "search_path":
		value = "\"$user\", public"
	default:
		value = ""
	}
	cols := []PGColumn{{Name: param, TypeOID: PGTypeText, TypeLen: -1, TypeMod: -1}}
	sess.writeRowDescription(cols)
	sess.writeDataRow([]string{value})
	sess.writeCommandComplete("SHOW")
	return nil
}

func (sess *PGSession) handleCatalogQuery(_ string) error {
	// Return empty result for catalog queries (psql compatibility)
	cols := []PGColumn{
		{Name: "oid", TypeOID: PGTypeInt4, TypeLen: 4, TypeMod: -1},
		{Name: "name", TypeOID: PGTypeText, TypeLen: -1, TypeMod: -1},
	}
	sess.writeRowDescription(cols)
	sess.writeCommandComplete("SELECT 0")
	return nil
}

func (sess *PGSession) handleSelect(stmt string) error {
	translator := &PGQueryTranslator{db: sess.server.db}
	result, err := translator.Execute(stmt)
	if err != nil {
		return err
	}

	sess.writeRowDescription(result.Columns)
	for _, row := range result.Rows {
		values := make([]string, len(row))
		for i, v := range row {
			values[i] = formatValue(v)
		}
		sess.writeDataRow(values)
	}
	sess.writeCommandComplete(result.Tag)
	return nil
}

func (sess *PGSession) handleInsert(stmt string) error {
	translator := &PGQueryTranslator{db: sess.server.db}
	_, err := translator.Execute(stmt)
	if err != nil {
		return err
	}
	sess.writeCommandComplete("INSERT 0 1")
	return nil
}

func splitStatements(query string) []string {
	var stmts []string
	var current strings.Builder
	inString := false
	for _, ch := range query {
		if ch == '\'' {
			inString = !inString
		}
		if ch == ';' && !inString {
			if s := current.String(); strings.TrimSpace(s) != "" {
				stmts = append(stmts, s)
			}
			current.Reset()
			continue
		}
		current.WriteRune(ch)
	}
	if s := current.String(); strings.TrimSpace(s) != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return "NaN"
		}
		return fmt.Sprintf("%g", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case time.Time:
		return val.UTC().Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", val)
	}
}

// Helper functions for building protocol messages.
func appendInt16(buf []byte, v int16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(v))
	return append(buf, b...)
}

func appendInt32(buf []byte, v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return append(buf, b...)
}

func appendString(buf []byte, s string) []byte {
	buf = append(buf, []byte(s)...)
	return append(buf, 0)
}
