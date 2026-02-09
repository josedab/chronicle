package chronicle

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ClickHouse native protocol implementation for Chronicle
// Enables direct connectivity from BI tools like DataGrip, DBeaver, Tableau, etc.

// Protocol constants
const (
	CHClientHello        uint64 = 0
	CHClientQuery        uint64 = 1
	CHClientData         uint64 = 2
	CHClientCancel       uint64 = 3
	CHClientPing         uint64 = 4
	CHClientTablesStatus uint64 = 5

	CHServerHello       uint64 = 0
	CHServerData        uint64 = 1
	CHServerException   uint64 = 2
	CHServerProgress    uint64 = 3
	CHServerPong        uint64 = 4
	CHServerEndOfStream uint64 = 5
	CHServerProfileInfo uint64 = 6
	CHServerTotals      uint64 = 7
	CHServerExtremes    uint64 = 8
	CHServerTableStatus uint64 = 9

	CHProtocolVersionMajor = 21
	CHProtocolVersionMinor = 1
	CHProtocolRevision     = 54447

	DefaultCHPort = 9000
)

// ClickHouse data types
const (
	CHTypeUInt8    = "UInt8"
	CHTypeUInt16   = "UInt16"
	CHTypeUInt32   = "UInt32"
	CHTypeUInt64   = "UInt64"
	CHTypeInt8     = "Int8"
	CHTypeInt16    = "Int16"
	CHTypeInt32    = "Int32"
	CHTypeInt64    = "Int64"
	CHTypeFloat32  = "Float32"
	CHTypeFloat64  = "Float64"
	CHTypeString   = "String"
	CHTypeDateTime = "DateTime"
	CHTypeDate     = "Date"
)

// ClickHouseProtocolConfig configures the ClickHouse protocol server
type ClickHouseProtocolConfig struct {
	// Address to listen on (default: ":9000")
	Address string

	// Database name presented to clients
	DatabaseName string

	// Server display name
	ServerName string

	// Server timezone
	Timezone string

	// Maximum concurrent connections
	MaxConnections int

	// Query timeout
	QueryTimeout time.Duration

	// Enable query logging
	QueryLogging bool

	// Enable compression (LZ4)
	EnableCompression bool

	// Read buffer size
	ReadBufferSize int

	// Write buffer size
	WriteBufferSize int
}

// DefaultClickHouseProtocolConfig returns default configuration
func DefaultClickHouseProtocolConfig() *ClickHouseProtocolConfig {
	return &ClickHouseProtocolConfig{
		Address:           ":9000",
		DatabaseName:      "chronicle",
		ServerName:        "Chronicle",
		Timezone:          "UTC",
		MaxConnections:    100,
		QueryTimeout:      30 * time.Second,
		QueryLogging:      true,
		EnableCompression: false,
		ReadBufferSize:    64 * 1024,
		WriteBufferSize:   64 * 1024,
	}
}

// CHNativeServer implements the ClickHouse native protocol
type CHNativeServer struct {
	db     *DB
	config *ClickHouseProtocolConfig

	listener net.Listener
	sessions sync.Map // connectionID -> *CHSession

	mu       sync.RWMutex
	running  bool
	shutdown chan struct{}

	// Stats
	totalConnections int64
	activeConns      int64
	totalQueries     int64
	queryErrors      int64
}

// NewCHNativeServer creates a new ClickHouse protocol server
func NewCHNativeServer(db *DB, config *ClickHouseProtocolConfig) (*CHNativeServer, error) {
	if config == nil {
		config = DefaultClickHouseProtocolConfig()
	}

	return &CHNativeServer{
		db:       db,
		config:   config,
		shutdown: make(chan struct{}),
	}, nil
}

// Start begins listening for connections
func (s *CHNativeServer) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}

	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.listener = listener
	s.running = true
	s.mu.Unlock()

	go s.acceptLoop()

	return nil
}

// Stop gracefully shuts down the server
func (s *CHNativeServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	close(s.shutdown)
	s.running = false

	if s.listener != nil {
		s.listener.Close()
	}

	// Close all active sessions
	s.sessions.Range(func(key, value interface{}) bool {
		if session, ok := value.(*CHSession); ok {
			session.Close()
		}
		return true
	})

	return nil
}

func (s *CHNativeServer) acceptLoop() {
	for {
		select {
		case <-s.shutdown:
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				return
			default:
				continue
			}
		}

		if atomic.LoadInt64(&s.activeConns) >= int64(s.config.MaxConnections) {
			conn.Close()
			continue
		}

		atomic.AddInt64(&s.totalConnections, 1)
		atomic.AddInt64(&s.activeConns, 1)

		session := newCHSession(s, conn)
		s.sessions.Store(session.id, session)

		go s.handleSession(session)
	}
}

func (s *CHNativeServer) handleSession(session *CHSession) {
	defer func() {
		session.Close()
		s.sessions.Delete(session.id)
		atomic.AddInt64(&s.activeConns, -1)
	}()

	// Handle initial handshake
	if err := session.handleHandshake(); err != nil {
		return
	}

	// Main message loop
	for {
		select {
		case <-s.shutdown:
			return
		default:
		}

		packetType, err := session.readPacketType()
		if err != nil {
			return
		}

		switch packetType {
		case CHClientPing:
			if err := session.handlePing(); err != nil {
				return
			}

		case CHClientQuery:
			atomic.AddInt64(&s.totalQueries, 1)
			if err := session.handleQuery(); err != nil {
				atomic.AddInt64(&s.queryErrors, 1)
			}

		case CHClientData:
			if err := session.handleData(); err != nil {
				return
			}

		case CHClientCancel:
			session.cancelQuery()

		default:
			return
		}
	}
}

// Stats returns server statistics
func (s *CHNativeServer) Stats() CHServerStats {
	return CHServerStats{
		TotalConnections: atomic.LoadInt64(&s.totalConnections),
		ActiveConnections: atomic.LoadInt64(&s.activeConns),
		TotalQueries:     atomic.LoadInt64(&s.totalQueries),
		QueryErrors:      atomic.LoadInt64(&s.queryErrors),
	}
}

// CHServerStats contains server statistics
type CHServerStats struct {
	TotalConnections  int64
	ActiveConnections int64
	TotalQueries      int64
	QueryErrors       int64
}

// CHSession represents a client connection session
type CHSession struct {
	id     string
	server *CHNativeServer
	conn   net.Conn
	reader *bytes.Buffer
	writer *bytes.Buffer

	clientName  string
	clientVer   uint64
	clientRev   uint64
	database    string

	ctx    context.Context
	cancel context.CancelFunc

	mu sync.Mutex
}

func newCHSession(server *CHNativeServer, conn net.Conn) *CHSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &CHSession{
		id:     fmt.Sprintf("%d", time.Now().UnixNano()),
		server: server,
		conn:   conn,
		reader: bytes.NewBuffer(make([]byte, 0, server.config.ReadBufferSize)),
		writer: bytes.NewBuffer(make([]byte, 0, server.config.WriteBufferSize)),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *CHSession) Close() {
	s.cancel()
	s.conn.Close()
}

func (s *CHSession) cancelQuery() {
	s.cancel()
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *CHSession) readPacketType() (uint64, error) {
	return s.readVarUInt()
}

func (s *CHSession) handleHandshake() error {
	// Read client hello
	packetType, err := s.readPacketType()
	if err != nil {
		return err
	}

	if packetType != CHClientHello {
		return fmt.Errorf("expected client hello, got %d", packetType)
	}

	// Read client info
	s.clientName, _ = s.readString()
	s.clientVer, _ = s.readVarUInt()
	s.clientRev, _ = s.readVarUInt()
	s.database, _ = s.readString()
	_, _ = s.readString() // user
	_, _ = s.readString() // password

	// Send server hello
	s.writeVarUInt(CHServerHello)
	s.writeString(s.server.config.ServerName)
	s.writeVarUInt(CHProtocolVersionMajor)
	s.writeVarUInt(CHProtocolVersionMinor)
	s.writeVarUInt(CHProtocolRevision)

	if s.clientRev >= 54447 {
		s.writeString(s.server.config.Timezone)
		s.writeString(s.server.config.ServerName)
	}

	return s.flush()
}

func (s *CHSession) handlePing() error {
	s.writeVarUInt(CHServerPong)
	return s.flush()
}

func (s *CHSession) handleQuery() error {
	// Read query settings
	_, _ = s.readString() // query ID

	// Read client info
	_, _ = s.readVarUInt() // query kind
	_, _ = s.readString()  // initial user
	_, _ = s.readString()  // initial query ID
	_, _ = s.readString()  // initial address
	_, _ = s.readVarUInt() // interface type
	_, _ = s.readString()  // OS user
	_, _ = s.readString()  // client hostname
	_, _ = s.readString()  // client name
	_, _ = s.readVarUInt() // client version major
	_, _ = s.readVarUInt() // client version minor
	_, _ = s.readVarUInt() // client revision

	if s.clientRev >= 54447 {
		_, _ = s.readString() // quota key
	}

	// Read settings
	for {
		key, _ := s.readString()
		if key == "" {
			break
		}
		_, _ = s.readString() // value
	}

	_, _ = s.readVarUInt() // state
	_, _ = s.readVarUInt() // compression

	query, err := s.readString()
	if err != nil {
		return s.sendException(err)
	}

	// Execute query
	return s.executeQuery(query)
}

func (s *CHSession) executeQuery(query string) error {
	query = strings.TrimSpace(query)

	if s.server.config.QueryLogging {
		fmt.Printf("[CH] Query: %s\n", query)
	}

	// Parse and route query
	translator := &CHQueryTranslator{db: s.server.db}

	result, err := translator.Execute(s.ctx, query)
	if err != nil {
		return s.sendException(err)
	}

	return s.sendResult(result)
}

func (s *CHSession) sendResult(result *CHQueryResult) error {
	// Send data block with column info
	s.writeVarUInt(CHServerData)
	s.writeString("")          // external table name
	s.writeBlockHeader(result)

	// Write data
	s.writeDataBlock(result)

	// Send end of stream
	s.writeVarUInt(CHServerEndOfStream)

	return s.flush()
}

func (s *CHSession) writeBlockHeader(result *CHQueryResult) {
	// Block info
	s.writeVarUInt(1) // field number (overflow)
	s.writeByte(0)    // is overflows
	s.writeVarUInt(2) // field number (bucket)
	s.writeInt32(-1)  // bucket num
	s.writeVarUInt(0) // end of block info

	// Columns and rows count
	s.writeVarUInt(uint64(len(result.Columns)))
	s.writeVarUInt(uint64(result.RowCount))
}

func (s *CHSession) writeDataBlock(result *CHQueryResult) {
	for i, col := range result.Columns {
		s.writeString(col.Name)
		s.writeString(col.Type)

		// Write column data
		for _, row := range result.Rows {
			if i < len(row) {
				s.writeValue(row[i], col.Type)
			}
		}
	}
}

func (s *CHSession) writeValue(value interface{}, chType string) {
	switch chType {
	case CHTypeUInt8:
		v, _ := toUint64(value)
		s.writeByte(byte(v))
	case CHTypeUInt16:
		v, _ := toUint64(value)
		s.writeUInt16(uint16(v))
	case CHTypeUInt32:
		v, _ := toUint64(value)
		s.writeUInt32(uint32(v))
	case CHTypeUInt64:
		v, _ := toUint64(value)
		s.writeUInt64(v)
	case CHTypeInt8:
		v, _ := toInt64(value)
		s.writeByte(byte(v))
	case CHTypeInt16:
		v, _ := toInt64(value)
		s.writeInt16(int16(v))
	case CHTypeInt32:
		v, _ := toInt64(value)
		s.writeInt32(int32(v))
	case CHTypeInt64:
		v, _ := toInt64(value)
		s.writeInt64(v)
	case CHTypeFloat32:
		v, _ := toFloat64(value)
		s.writeFloat32(float32(v))
	case CHTypeFloat64:
		v, _ := toFloat64(value)
		s.writeFloat64(v)
	case CHTypeString:
		s.writeString(fmt.Sprintf("%v", value))
	case CHTypeDateTime:
		v, _ := toInt64(value)
		s.writeUInt32(uint32(v))
	case CHTypeDate:
		v, _ := toInt64(value)
		s.writeUInt16(uint16(v / 86400))
	default:
		s.writeString(fmt.Sprintf("%v", value))
	}
}

func (s *CHSession) sendException(err error) error {
	s.writeVarUInt(CHServerException)
	s.writeInt32(1000)       // code
	s.writeString(err.Error())
	s.writeString("")        // stack trace
	s.writeByte(0)           // has nested
	return s.flush()
}

func (s *CHSession) handleData() error {
	// Read and discard data block (for INSERT)
	_, _ = s.readString() // table name

	// Block info
	for {
		fieldNum, _ := s.readVarUInt()
		if fieldNum == 0 {
			break
		}
		switch fieldNum {
		case 1:
			s.readByte()
		case 2:
			s.readInt32()
		}
	}

	numCols, _ := s.readVarUInt()
	numRows, _ := s.readVarUInt()

	if numCols == 0 || numRows == 0 {
		return nil
	}

	// Read column data
	for i := uint64(0); i < numCols; i++ {
		s.readString() // name
		s.readString() // type
		// Skip data for now
	}

	return nil
}

// Binary read/write helpers

func (s *CHSession) readVarUInt() (uint64, error) {
	var x uint64
	buf := make([]byte, 1)
	for i := 0; i < 9; i++ {
		_, err := io.ReadFull(s.conn, buf)
		if err != nil {
			return 0, err
		}
		b := buf[0]
		x |= uint64(b&0x7f) << (7 * i)
		if b < 0x80 {
			return x, nil
		}
	}
	return 0, fmt.Errorf("varuint too long")
}

func (s *CHSession) readString() (string, error) {
	length, err := s.readVarUInt()
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(s.conn, buf)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (s *CHSession) readByte() (byte, error) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(s.conn, buf)
	return buf[0], err
}

func (s *CHSession) readInt32() (int32, error) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(s.conn, buf)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(buf)), nil
}

func (s *CHSession) writeVarUInt(x uint64) {
	buf := make([]byte, binary.MaxVarintLen64)
	i := 0
	for x >= 0x80 {
		buf[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	buf[i] = byte(x)
	s.writer.Write(buf[:i+1])
}

func (s *CHSession) writeString(str string) {
	s.writeVarUInt(uint64(len(str)))
	s.writer.WriteString(str)
}

func (s *CHSession) writeByte(b byte) {
	s.writer.WriteByte(b)
}

func (s *CHSession) writeUInt16(v uint16) {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, v)
	s.writer.Write(buf)
}

func (s *CHSession) writeUInt32(v uint32) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	s.writer.Write(buf)
}

func (s *CHSession) writeUInt64(v uint64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, v)
	s.writer.Write(buf)
}

func (s *CHSession) writeInt16(v int16) {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(v))
	s.writer.Write(buf)
}

func (s *CHSession) writeInt32(v int32) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(v))
	s.writer.Write(buf)
}

func (s *CHSession) writeInt64(v int64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	s.writer.Write(buf)
}

func (s *CHSession) writeFloat32(v float32) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(v))
	s.writer.Write(buf)
}

func (s *CHSession) writeFloat64(v float64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	s.writer.Write(buf)
}

func (s *CHSession) flush() error {
	_, err := s.conn.Write(s.writer.Bytes())
	s.writer.Reset()
	return err
}

// CHQueryResult represents a query result
type CHQueryResult struct {
	Columns  []CHColumn
	Rows     [][]interface{}
	RowCount int
}

// CHColumn represents a result column
type CHColumn struct {
	Name string
	Type string
}

// CHQueryTranslator translates ClickHouse SQL to Chronicle queries
type CHQueryTranslator struct {
	db *DB
}

// Execute parses and executes a ClickHouse SQL query
func (t *CHQueryTranslator) Execute(ctx context.Context, query string) (*CHQueryResult, error) {
	query = strings.TrimSpace(query)
	queryLower := strings.ToLower(query)

	// System queries
	if strings.HasPrefix(queryLower, "select 1") || query == "SELECT 1" {
		return t.selectOne()
	}

	if strings.Contains(queryLower, "system.databases") {
		return t.showDatabases()
	}

	if strings.Contains(queryLower, "system.tables") {
		return t.showTables()
	}

	if strings.Contains(queryLower, "system.columns") {
		return t.showColumns(query)
	}

	if strings.HasPrefix(queryLower, "show databases") {
		return t.showDatabases()
	}

	if strings.HasPrefix(queryLower, "show tables") {
		return t.showTables()
	}

	if strings.HasPrefix(queryLower, "describe") || strings.HasPrefix(queryLower, "desc ") {
		return t.describeTable(query)
	}

	if strings.HasPrefix(queryLower, "select") {
		return t.executeSelect(ctx, query)
	}

	if strings.HasPrefix(queryLower, "insert") {
		return t.executeInsert(ctx, query)
	}

	return nil, fmt.Errorf("unsupported query: %s", query)
}

func (t *CHQueryTranslator) selectOne() (*CHQueryResult, error) {
	return &CHQueryResult{
		Columns: []CHColumn{{Name: "1", Type: CHTypeUInt8}},
		Rows:    [][]interface{}{{uint8(1)}},
		RowCount: 1,
	}, nil
}

func (t *CHQueryTranslator) showDatabases() (*CHQueryResult, error) {
	return &CHQueryResult{
		Columns: []CHColumn{{Name: "name", Type: CHTypeString}},
		Rows:    [][]interface{}{{"chronicle"}, {"system"}},
		RowCount: 2,
	}, nil
}

func (t *CHQueryTranslator) showTables() (*CHQueryResult, error) {
	// Get metrics from Chronicle
	metrics := t.db.Metrics()

	rows := make([][]interface{}, 0, len(metrics))
	for _, m := range metrics {
		rows = append(rows, []interface{}{m})
	}

	return &CHQueryResult{
		Columns: []CHColumn{{Name: "name", Type: CHTypeString}},
		Rows:    rows,
		RowCount: len(rows),
	}, nil
}

func (t *CHQueryTranslator) showColumns(query string) (*CHQueryResult, error) {
	// Extract table name from query (for future use with schema lookups)
	re := regexp.MustCompile(`table\s*=\s*'([^']+)'`)
	matches := re.FindStringSubmatch(query)

	_ = "unknown" // tableName placeholder for future schema lookups
	if len(matches) > 1 {
		_ = matches[1]
	}

	// Standard time-series columns
	return &CHQueryResult{
		Columns: []CHColumn{
			{Name: "name", Type: CHTypeString},
			{Name: "type", Type: CHTypeString},
			{Name: "default_type", Type: CHTypeString},
			{Name: "default_expression", Type: CHTypeString},
		},
		Rows: [][]interface{}{
			{"timestamp", CHTypeDateTime, "", ""},
			{"value", CHTypeFloat64, "", ""},
			{"series", CHTypeString, "", ""},
		},
		RowCount: 3,
	}, nil
}

func (t *CHQueryTranslator) describeTable(query string) (*CHQueryResult, error) {
	// Extract table name
	parts := strings.Fields(query)
	tableName := "unknown"
	if len(parts) >= 2 {
		tableName = strings.Trim(parts[len(parts)-1], "`\"")
	}

	return &CHQueryResult{
		Columns: []CHColumn{
			{Name: "name", Type: CHTypeString},
			{Name: "type", Type: CHTypeString},
			{Name: "default_type", Type: CHTypeString},
			{Name: "default_expression", Type: CHTypeString},
			{Name: "comment", Type: CHTypeString},
			{Name: "codec_expression", Type: CHTypeString},
			{Name: "ttl_expression", Type: CHTypeString},
		},
		Rows: [][]interface{}{
			{"timestamp", CHTypeDateTime, "", "", "Event timestamp", "", ""},
			{"value", CHTypeFloat64, "", "", "Metric value", "", ""},
			{"series", CHTypeString, "", "", "Series name: " + tableName, "", ""},
		},
		RowCount: 3,
	}, nil
}

func (t *CHQueryTranslator) executeSelect(ctx context.Context, query string) (*CHQueryResult, error) {
	// Parse ClickHouse SQL
	parsed, err := t.parseSelect(query)
	if err != nil {
		return nil, err
	}

	// Execute query against Chronicle
	points, err := t.executeChronicleQuery(ctx, parsed)
	if err != nil {
		return nil, err
	}

	// Build result
	return t.buildSelectResult(parsed, points)
}

type parsedSelect struct {
	columns    []string
	table      string
	where      string
	startTime  int64
	endTime    int64
	groupBy    string
	orderBy    string
	orderDesc  bool
	limit      int
	aggregates map[string]string // column -> aggregate function
}

func (t *CHQueryTranslator) parseSelect(query string) (*parsedSelect, error) {
	parsed := &parsedSelect{
		aggregates: make(map[string]string),
		limit:      1000,
		endTime:    time.Now().UnixNano(),
		startTime:  time.Now().Add(-time.Hour).UnixNano(),
	}

	queryLower := strings.ToLower(query)

	// Extract columns
	selectMatch := regexp.MustCompile(`(?i)select\s+(.+?)\s+from`).FindStringSubmatch(query)
	if len(selectMatch) > 1 {
		cols := strings.Split(selectMatch[1], ",")
		for _, col := range cols {
			col = strings.TrimSpace(col)
			parsed.columns = append(parsed.columns, col)

			// Check for aggregates
			for _, agg := range []string{"sum", "avg", "min", "max", "count"} {
				if strings.HasPrefix(strings.ToLower(col), agg+"(") {
					innerCol := regexp.MustCompile(`\(([^)]+)\)`).FindStringSubmatch(col)
					if len(innerCol) > 1 {
						parsed.aggregates[col] = agg
					}
				}
			}
		}
	}

	// Extract table
	fromMatch := regexp.MustCompile(`(?i)from\s+([^\s,;]+)`).FindStringSubmatch(query)
	if len(fromMatch) > 1 {
		parsed.table = strings.Trim(fromMatch[1], "`\"")
	}

	// Extract time range from WHERE
	if strings.Contains(queryLower, "where") {
		// Look for timestamp conditions
		timeMatch := regexp.MustCompile(`(?i)timestamp\s*>=?\s*(\d+)`).FindStringSubmatch(query)
		if len(timeMatch) > 1 {
			if ts, err := strconv.ParseInt(timeMatch[1], 10, 64); err == nil {
				parsed.startTime = ts
			}
		}

		timeMatch = regexp.MustCompile(`(?i)timestamp\s*<=?\s*(\d+)`).FindStringSubmatch(query)
		if len(timeMatch) > 1 {
			if ts, err := strconv.ParseInt(timeMatch[1], 10, 64); err == nil {
				parsed.endTime = ts
			}
		}

		// Look for toDateTime conditions
		dateMatch := regexp.MustCompile(`(?i)timestamp\s*>=?\s*toDateTime\('([^']+)'\)`).FindStringSubmatch(query)
		if len(dateMatch) > 1 {
			if t, err := time.Parse("2006-01-02 15:04:05", dateMatch[1]); err == nil {
				parsed.startTime = t.UnixNano()
			}
		}

		dateMatch = regexp.MustCompile(`(?i)timestamp\s*<=?\s*toDateTime\('([^']+)'\)`).FindStringSubmatch(query)
		if len(dateMatch) > 1 {
			if t, err := time.Parse("2006-01-02 15:04:05", dateMatch[1]); err == nil {
				parsed.endTime = t.UnixNano()
			}
		}
	}

	// Extract GROUP BY
	groupMatch := regexp.MustCompile(`(?i)group\s+by\s+([^\s;]+)`).FindStringSubmatch(query)
	if len(groupMatch) > 1 {
		parsed.groupBy = groupMatch[1]
	}

	// Extract ORDER BY
	orderMatch := regexp.MustCompile(`(?i)order\s+by\s+([^\s;]+)`).FindStringSubmatch(query)
	if len(orderMatch) > 1 {
		parsed.orderBy = orderMatch[1]
		parsed.orderDesc = strings.Contains(strings.ToLower(query), "desc")
	}

	// Extract LIMIT
	limitMatch := regexp.MustCompile(`(?i)limit\s+(\d+)`).FindStringSubmatch(query)
	if len(limitMatch) > 1 {
		if l, err := strconv.Atoi(limitMatch[1]); err == nil {
			parsed.limit = l
		}
	}

	return parsed, nil
}

func (t *CHQueryTranslator) executeChronicleQuery(ctx context.Context, parsed *parsedSelect) ([]Point, error) {
	q := &Query{
		Metric:    parsed.table,
		Start:     parsed.startTime,
		End:       parsed.endTime,
		Limit:     parsed.limit,
	}

	result, err := t.db.ExecuteContext(ctx, q)
	if err != nil {
		return nil, err
	}
	return result.Points, nil
}

func (t *CHQueryTranslator) buildSelectResult(parsed *parsedSelect, points []Point) (*CHQueryResult, error) {
	// Determine columns
	columns := make([]CHColumn, 0)
	_ = false // hasTimestamp - removed unused
	_ = false // hasValue - removed unused

	for _, col := range parsed.columns {
		colLower := strings.ToLower(col)
		if col == "*" {
			columns = append(columns,
				CHColumn{Name: "timestamp", Type: CHTypeDateTime},
				CHColumn{Name: "value", Type: CHTypeFloat64},
				CHColumn{Name: "metric", Type: CHTypeString},
			)
		} else if strings.Contains(colLower, "timestamp") || strings.Contains(colLower, "time") {
			columns = append(columns, CHColumn{Name: col, Type: CHTypeDateTime})
		} else if strings.Contains(colLower, "value") || parsed.aggregates[col] != "" {
			columns = append(columns, CHColumn{Name: col, Type: CHTypeFloat64})
		} else {
			columns = append(columns, CHColumn{Name: col, Type: CHTypeString})
		}
	}

	// Handle aggregates
	if len(parsed.aggregates) > 0 {
		return t.buildAggregateResult(parsed, points, columns)
	}

	// Build rows
	rows := make([][]interface{}, 0, len(points))
	for _, p := range points {
		row := make([]interface{}, 0, len(columns))
		for _, col := range columns {
			switch strings.ToLower(col.Name) {
			case "timestamp", "time":
				row = append(row, p.Timestamp/int64(time.Second))
			case "value":
				row = append(row, p.Value)
			case "metric", "series":
				row = append(row, p.Metric)
			case "*":
				row = append(row, p.Timestamp/int64(time.Second), p.Value, p.Metric)
			default:
				if v, ok := p.Tags[col.Name]; ok {
					row = append(row, v)
				} else {
					row = append(row, "")
				}
			}
		}
		rows = append(rows, row)
	}

	return &CHQueryResult{
		Columns:  columns,
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

func (t *CHQueryTranslator) buildAggregateResult(parsed *parsedSelect, points []Point, columns []CHColumn) (*CHQueryResult, error) {
	if len(points) == 0 {
		return &CHQueryResult{
			Columns:  columns,
			Rows:     [][]interface{}{},
			RowCount: 0,
		}, nil
	}

	// Calculate aggregates
	values := make([]float64, len(points))
	for i, p := range points {
		values[i] = p.Value
	}

	row := make([]interface{}, 0, len(columns))
	for _, col := range parsed.columns {
		colLower := strings.ToLower(col)
		if agg, ok := parsed.aggregates[col]; ok {
			var result float64
			switch agg {
			case "sum":
				for _, v := range values {
					result += v
				}
			case "avg":
				for _, v := range values {
					result += v
				}
				result /= float64(len(values))
			case "min":
				result = values[0]
				for _, v := range values[1:] {
					if v < result {
						result = v
					}
				}
			case "max":
				result = values[0]
				for _, v := range values[1:] {
					if v > result {
						result = v
					}
				}
			case "count":
				result = float64(len(values))
			}
			row = append(row, result)
		} else if strings.Contains(colLower, "timestamp") {
			row = append(row, points[0].Timestamp/int64(time.Second))
		} else {
			row = append(row, points[0].Metric)
		}
	}

	return &CHQueryResult{
		Columns:  columns,
		Rows:     [][]interface{}{row},
		RowCount: 1,
	}, nil
}

func (t *CHQueryTranslator) executeInsert(ctx context.Context, query string) (*CHQueryResult, error) {
	// Parse INSERT INTO table (columns) VALUES (values)
	re := regexp.MustCompile(`(?i)insert\s+into\s+([^\s(]+)\s*(?:\([^)]+\))?\s*values\s*(.+)`)
	matches := re.FindStringSubmatch(query)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid INSERT syntax")
	}

	table := strings.Trim(matches[1], "`\"")
	valuesStr := matches[2]

	// Parse values
	valueRe := regexp.MustCompile(`\(([^)]+)\)`)
	valueMatches := valueRe.FindAllStringSubmatch(valuesStr, -1)

	points := make([]Point, 0, len(valueMatches))
	for _, vm := range valueMatches {
		if len(vm) < 2 {
			continue
		}

		parts := strings.Split(vm[1], ",")
		if len(parts) < 2 {
			continue
		}

		timestamp := time.Now().UnixNano()
		if ts, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64); err == nil {
			timestamp = ts * int64(time.Second)
		}

		value := 0.0
		if v, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64); err == nil {
			value = v
		}

		points = append(points, Point{
			Metric:    table,
			Timestamp: timestamp,
			Value:     value,
		})
	}

	// Write points
	for _, p := range points {
		if err := t.db.Write(p); err != nil {
			return nil, fmt.Errorf("write failed: %w", err)
		}
	}

	return &CHQueryResult{
		Columns:  []CHColumn{},
		Rows:     [][]interface{}{},
		RowCount: 0,
	}, nil
}

// Conversion helpers
func toUint64(v interface{}) (uint64, error) {
	switch val := v.(type) {
	case uint64:
		return val, nil
	case uint32:
		return uint64(val), nil
	case uint16:
		return uint64(val), nil
	case uint8:
		return uint64(val), nil
	case int64:
		return uint64(val), nil
	case int32:
		return uint64(val), nil
	case int:
		return uint64(val), nil
	case float64:
		return uint64(val), nil
	case float32:
		return uint64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", v)
	}
}

func toInt64(v interface{}) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int32:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int:
		return int64(val), nil
	case uint64:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case float32:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}
