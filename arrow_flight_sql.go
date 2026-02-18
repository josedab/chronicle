package chronicle

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// FlightSQLCommand represents Arrow Flight SQL command types.
type FlightSQLCommand int

const (
	// CommandGetCatalogs requests the list of available catalogs.
	CommandGetCatalogs FlightSQLCommand = iota + 100
	// CommandGetSchemas requests the list of schemas in a catalog.
	CommandGetSchemas
	// CommandGetTables requests the list of tables in a schema.
	CommandGetTables
	// CommandGetTableTypes requests the list of table types.
	CommandGetTableTypes
	// CommandStatementQuery executes a SQL statement that returns results.
	CommandStatementQuery
	// CommandStatementUpdate executes a SQL statement that modifies data.
	CommandStatementUpdate
	// CommandPreparedStatementQuery executes a prepared SQL statement.
	CommandPreparedStatementQuery
)

// FlightSQLConfig holds configuration for the Flight SQL server.
type FlightSQLConfig struct {
	BindAddr             string        `json:"bind_addr"`
	MaxBatchRows         int           `json:"max_batch_rows"`
	MaxConcurrentStreams int           `json:"max_concurrent_streams"`
	EnableSQL            bool          `json:"enable_sql"`
	Catalogs             []string      `json:"catalogs"`
	Schemas              []string      `json:"schemas"`
	SQLTimeout           time.Duration `json:"sql_timeout"`
}

// DefaultFlightSQLConfig returns a default Flight SQL configuration.
func DefaultFlightSQLConfig() FlightSQLConfig {
	return FlightSQLConfig{
		BindAddr:             "127.0.0.1:8816",
		MaxBatchRows:         65536,
		MaxConcurrentStreams: 64,
		EnableSQL:            true,
		Catalogs:             []string{"chronicle"},
		Schemas:              []string{"default"},
		SQLTimeout:           30 * time.Second,
	}
}

// FlightSQLServer implements the Arrow Flight SQL protocol for BI tool connectivity.
type FlightSQLServer struct {
	db       *DB
	config   FlightSQLConfig
	mu       sync.RWMutex
	listener net.Listener
	running  bool
	stopCh   chan struct{}
	wg       sync.WaitGroup

	// Session and prepared statement management.
	sessions           map[string]*flightSQLSession
	preparedStatements map[string]*preparedStatement
	sessionMu          sync.RWMutex

	// Concurrency limiter.
	streamSem chan struct{}
}

type flightSQLSession struct {
	id        string
	createdAt time.Time
	lastUsed  time.Time
}

type preparedStatement struct {
	id      string
	sql     string
	schema  ArrowSchema
	created time.Time
}

// Flight SQL wire protocol message types (offset from base Flight types).
const (
	flightSQLMessageGetCatalogs      uint32 = 100
	flightSQLMessageGetSchemas       uint32 = 101
	flightSQLMessageGetTables        uint32 = 102
	flightSQLMessageGetTableTypes    uint32 = 103
	flightSQLMessageStatementQuery   uint32 = 104
	flightSQLMessageStatementUpdate  uint32 = 105
	flightSQLMessagePreparedQuery    uint32 = 106
	flightSQLMessageGetFlightInfo    uint32 = 107
	flightSQLMessageDoGet            uint32 = 108
	flightSQLMessageDoPut            uint32 = 109
	flightSQLMessageResponse         uint32 = 200
	flightSQLMessageError            uint32 = 255
)

// NewFlightSQLServer creates a new Flight SQL server.
func NewFlightSQLServer(db *DB, config FlightSQLConfig) *FlightSQLServer {
	return &FlightSQLServer{
		db:                 db,
		config:             config,
		stopCh:             make(chan struct{}),
		sessions:           make(map[string]*flightSQLSession),
		preparedStatements: make(map[string]*preparedStatement),
		streamSem:          make(chan struct{}, config.MaxConcurrentStreams),
	}
}

// Start starts the Flight SQL server.
func (s *FlightSQLServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("flight sql server already running")
	}

	listener, err := net.Listen("tcp", s.config.BindAddr)
	if err != nil {
		return fmt.Errorf("failed to bind Flight SQL server: %w", err)
	}
	s.listener = listener
	s.running = true
	s.stopCh = make(chan struct{})

	s.wg.Add(1)
	go s.serve()

	slog.Info("Flight SQL server started", "addr", s.config.BindAddr)
	return nil
}

// Stop stops the Flight SQL server.
func (s *FlightSQLServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	close(s.stopCh)
	if s.listener != nil {
		_ = s.listener.Close() //nolint:errcheck // best-effort cleanup during shutdown
	}
	s.wg.Wait()
	s.running = false

	slog.Info("Flight SQL server stopped")
	return nil
}

func (s *FlightSQLServer) serve() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				slog.Warn("Flight SQL accept error", "err", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *FlightSQLServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		// Read 8-byte header: [4 bytes msgType][4 bytes msgLen]
		header := make([]byte, 8)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}

		msgType := binary.BigEndian.Uint32(header[:4])
		msgLen := binary.BigEndian.Uint32(header[4:])

		if msgLen > 64*1024*1024 {
			s.sendErrorMsg(conn, fmt.Errorf("message too large: %d bytes", msgLen))
			return
		}

		body := make([]byte, msgLen)
		if msgLen > 0 {
			if _, err := io.ReadFull(conn, body); err != nil {
				return
			}
		}

		// Acquire stream semaphore for concurrency control.
		select {
		case s.streamSem <- struct{}{}:
		case <-s.stopCh:
			return
		}

		s.dispatchMessage(conn, msgType, body)
		<-s.streamSem
	}
}

func (s *FlightSQLServer) dispatchMessage(conn net.Conn, msgType uint32, body []byte) {
	switch msgType {
	case flightSQLMessageGetCatalogs:
		s.handleGetCatalogsMsg(conn)
	case flightSQLMessageGetSchemas:
		s.handleGetSchemasMsg(conn, body)
	case flightSQLMessageGetTables:
		s.handleGetTablesMsg(conn, body)
	case flightSQLMessageGetTableTypes:
		s.handleGetTableTypesMsg(conn)
	case flightSQLMessageStatementQuery:
		s.handleStatementQueryMsg(conn, body)
	case flightSQLMessageStatementUpdate:
		s.handleStatementUpdateMsg(conn, body)
	case flightSQLMessagePreparedQuery:
		s.handlePreparedQueryMsg(conn, body)
	case flightSQLMessageGetFlightInfo:
		s.handleGetFlightInfoMsg(conn, body)
	case flightSQLMessageDoGet:
		s.handleDoGetMsg(conn, body)
	case flightSQLMessageDoPut:
		s.handleDoPutMsg(conn, body)
	default:
		s.sendErrorMsg(conn, fmt.Errorf("unknown Flight SQL message type: %d", msgType))
	}
}

// --- Flight SQL Handlers ---

// HandleGetFlightInfo returns FlightInfo for a given descriptor.
func (s *FlightSQLServer) HandleGetFlightInfo(descriptor FlightDescriptor) (*FlightInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var query FlightQuery
	switch descriptor.Type {
	case FlightDescriptorCmd:
		if err := json.Unmarshal(descriptor.Cmd, &query); err != nil {
			return nil, fmt.Errorf("invalid flight query command: %w", err)
		}
	case FlightDescriptorPath:
		if len(descriptor.Path) == 0 {
			return nil, fmt.Errorf("empty descriptor path")
		}
		query.Metric = descriptor.Path[0]
		query.Start = time.Now().Add(-time.Hour).UnixNano()
		query.End = time.Now().UnixNano()
	default:
		return nil, fmt.Errorf("unsupported descriptor type: %d", descriptor.Type)
	}

	ticketData, _ := json.Marshal(descriptor)
	info := &FlightInfo{
		Schema: ChronicleSchema(),
		Endpoints: []FlightEndpoint{
			{
				Ticket:    FlightTicket{Ticket: ticketData},
				Locations: []FlightLocation{{URI: "grpc://" + s.config.BindAddr}},
			},
		},
		TotalRows:  -1,
		TotalBytes: -1,
	}
	return info, nil
}

func (s *FlightSQLServer) handleGetFlightInfoMsg(conn net.Conn, body []byte) {
	var desc FlightDescriptor
	if err := json.Unmarshal(body, &desc); err != nil {
		s.sendErrorMsg(conn, err)
		return
	}
	info, err := s.HandleGetFlightInfo(desc)
	if err != nil {
		s.sendErrorMsg(conn, err)
		return
	}
	s.sendResponseMsg(conn, flightSQLMessageResponse, info)
}

// HandleDoGet streams record batches for a given ticket.
func (s *FlightSQLServer) HandleDoGet(ticket FlightTicket) ([]ArrowRecordBatch, error) {
	var desc FlightDescriptor
	if err := json.Unmarshal(ticket.Ticket, &desc); err != nil {
		return nil, fmt.Errorf("invalid ticket: %w", err)
	}

	var query FlightQuery
	switch desc.Type {
	case FlightDescriptorCmd:
		if err := json.Unmarshal(desc.Cmd, &query); err != nil {
			return nil, fmt.Errorf("invalid query in ticket: %w", err)
		}
	case FlightDescriptorPath:
		if len(desc.Path) > 0 {
			query.Metric = desc.Path[0]
			query.Start = time.Now().Add(-time.Hour).UnixNano()
			query.End = time.Now().UnixNano()
		}
	}

	chronicleQuery := flightQueryToChronicle(query)
	ctx, cancel := context.WithTimeout(context.Background(), s.config.SQLTimeout)
	defer cancel()

	result, err := s.db.ExecuteContext(ctx, chronicleQuery)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	return buildBatches(result.Points, s.config.MaxBatchRows), nil
}

func (s *FlightSQLServer) handleDoGetMsg(conn net.Conn, body []byte) {
	var ticket FlightTicket
	if err := json.Unmarshal(body, &ticket); err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	batches, err := s.HandleDoGet(ticket)
	if err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	for _, batch := range batches {
		if err := s.sendResponseMsg(conn, flightSQLMessageDoGet, batch); err != nil {
			return
		}
	}
	_ = s.sendResponseMsg(conn, flightSQLMessageDoGet, map[string]any{"complete": true}) //nolint:errcheck // final completion marker, no recovery action
}

// HandleDoPut ingests record batches into the database.
func (s *FlightSQLServer) HandleDoPut(schema ArrowSchema, batches []ArrowRecordBatch) (int, error) {
	totalWritten := 0
	for _, batch := range batches {
		points, err := recordBatchToPoints(batch)
		if err != nil {
			return totalWritten, fmt.Errorf("batch conversion failed: %w", err)
		}
		if err := s.db.WriteBatch(points); err != nil {
			return totalWritten, fmt.Errorf("write failed: %w", err)
		}
		totalWritten += len(points)
	}
	return totalWritten, nil
}

func (s *FlightSQLServer) handleDoPutMsg(conn net.Conn, body []byte) {
	var req struct {
		Schema  ArrowSchema      `json:"schema"`
		Batches []ArrowRecordBatch `json:"batches"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	written, err := s.HandleDoPut(req.Schema, req.Batches)
	if err != nil {
		s.sendErrorMsg(conn, err)
		return
	}
	s.sendResponseMsg(conn, flightSQLMessageResponse, map[string]any{"rows_written": written})
}

// HandleGetCatalogs returns available catalogs.
func (s *FlightSQLServer) HandleGetCatalogs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	catalogs := make([]string, len(s.config.Catalogs))
	copy(catalogs, s.config.Catalogs)
	return catalogs
}

func (s *FlightSQLServer) handleGetCatalogsMsg(conn net.Conn) {
	catalogs := s.HandleGetCatalogs()
	batch := catalogsToRecordBatch(catalogs)
	s.sendResponseMsg(conn, flightSQLMessageResponse, batch)
}

// HandleGetSchemas returns schemas for a catalog.
func (s *FlightSQLServer) HandleGetSchemas(catalog string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, c := range s.config.Catalogs {
		if c == catalog || catalog == "" {
			schemas := make([]string, len(s.config.Schemas))
			copy(schemas, s.config.Schemas)
			return schemas
		}
	}
	return nil
}

func (s *FlightSQLServer) handleGetSchemasMsg(conn net.Conn, body []byte) {
	var req struct {
		Catalog string `json:"catalog"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req) //nolint:errcheck // optional request body; defaults used on parse failure
	}
	schemas := s.HandleGetSchemas(req.Catalog)
	batch := schemasToRecordBatch(req.Catalog, schemas)
	s.sendResponseMsg(conn, flightSQLMessageResponse, batch)
}

// HandleGetTables returns tables (metrics) for a given catalog and schema.
func (s *FlightSQLServer) HandleGetTables(catalog, schema string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	found := false
	for _, c := range s.config.Catalogs {
		if c == catalog || catalog == "" {
			found = true
			break
		}
	}
	if !found {
		return nil
	}

	return s.db.Metrics()
}

func (s *FlightSQLServer) handleGetTablesMsg(conn net.Conn, body []byte) {
	var req struct {
		Catalog string `json:"catalog"`
		Schema  string `json:"schema"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req) //nolint:errcheck // optional request body; defaults used on parse failure
	}
	tables := s.HandleGetTables(req.Catalog, req.Schema)
	batch := tablesToRecordBatch(req.Catalog, req.Schema, tables)
	s.sendResponseMsg(conn, flightSQLMessageResponse, batch)
}

func (s *FlightSQLServer) handleGetTableTypesMsg(conn net.Conn) {
	batch := ArrowRecordBatch{
		Schema: ArrowSchema{
			Fields: []ArrowField{
				{Name: "table_type", Type: ArrowTypeString, Nullable: false},
			},
		},
		Columns: []ArrowColumn{
			{Name: "table_type", Type: ArrowTypeString, Data: []any{"TABLE"}},
		},
		Length: 1,
	}
	s.sendResponseMsg(conn, flightSQLMessageResponse, batch)
}

// HandleStatementQuery executes a SQL query and returns results as record batches.
func (s *FlightSQLServer) HandleStatementQuery(sql string) ([]ArrowRecordBatch, error) {
	if !s.config.EnableSQL {
		return nil, fmt.Errorf("SQL queries are disabled")
	}

	translator := &SQLToQueryTranslator{}
	query, err := translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL translation failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.SQLTimeout)
	defer cancel()

	result, err := s.db.ExecuteContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	return buildBatches(result.Points, s.config.MaxBatchRows), nil
}

func (s *FlightSQLServer) handleStatementQueryMsg(conn net.Conn, body []byte) {
	var req struct {
		SQL string `json:"sql"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	batches, err := s.HandleStatementQuery(req.SQL)
	if err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	for _, batch := range batches {
		if err := s.sendResponseMsg(conn, flightSQLMessageDoGet, batch); err != nil {
			return
		}
	}
	//nolint:errcheck // final completion marker, no recovery action
	_ = s.sendResponseMsg(conn, flightSQLMessageResponse, map[string]any{
		"complete": true,
		"batches":  len(batches),
	})
}

func (s *FlightSQLServer) handleStatementUpdateMsg(conn net.Conn, body []byte) {
	var req struct {
		SQL string `json:"sql"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.sendErrorMsg(conn, err)
		return
	}
	// Chronicle is append-only; updates are not supported.
	s.sendErrorMsg(conn, fmt.Errorf("statement updates are not supported on a time-series database"))
}

// --- Standard Flight SQL RPC Methods for Client Interop ---

// GetFlightInfoStatement returns FlightInfo for a SQL statement (DBeaver, ADBC, DataFusion).
func (s *FlightSQLServer) GetFlightInfoStatement(sql string) (*FlightInfo, error) {
	if !s.config.EnableSQL {
		return nil, fmt.Errorf("SQL queries are disabled")
	}
	if sql == "" {
		return nil, fmt.Errorf("empty SQL statement")
	}

	// Generate a unique ticket for this query
	ticketID := fmt.Sprintf("stmt-%d", time.Now().UnixNano())

	// Store the prepared statement for later DoGet
	s.sessionMu.Lock()
	s.preparedStatements[ticketID] = &preparedStatement{
		id:      ticketID,
		sql:     sql,
		schema:  ChronicleSchema(),
		created: time.Now(),
	}
	s.sessionMu.Unlock()

	return &FlightInfo{
		Schema: ChronicleSchema(),
		Endpoints: []FlightEndpoint{{
			Ticket: FlightTicket{Ticket: []byte(ticketID)},
			Locations: []FlightLocation{{URI: s.config.BindAddr}},
		}},
		TotalRows:  -1, // unknown until executed
		TotalBytes: -1,
	}, nil
}

// DoGetStatement executes a SQL statement and returns results as Arrow batches.
func (s *FlightSQLServer) DoGetStatement(ticketID string) ([]ArrowRecordBatch, error) {
	s.sessionMu.RLock()
	ps, exists := s.preparedStatements[ticketID]
	s.sessionMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unknown ticket: %s", ticketID)
	}

	return s.HandleStatementQuery(ps.sql)
}

// CreatePreparedStatement creates a server-side prepared statement.
func (s *FlightSQLServer) CreatePreparedStatement(sql string) (string, *ArrowSchema, error) {
	if !s.config.EnableSQL {
		return "", nil, fmt.Errorf("SQL queries are disabled")
	}

	stmtID := fmt.Sprintf("prepared-%d", time.Now().UnixNano())
	schema := ChronicleSchema()

	s.sessionMu.Lock()
	s.preparedStatements[stmtID] = &preparedStatement{
		id:      stmtID,
		sql:     sql,
		schema:  schema,
		created: time.Now(),
	}
	s.sessionMu.Unlock()

	return stmtID, &schema, nil
}

// ClosePreparedStatement closes a previously created prepared statement.
func (s *FlightSQLServer) ClosePreparedStatement(stmtID string) error {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	if _, exists := s.preparedStatements[stmtID]; !exists {
		return fmt.Errorf("prepared statement not found: %s", stmtID)
	}
	delete(s.preparedStatements, stmtID)
	return nil
}

// GetSqlInfo returns SQL information for client capability detection.
func (s *FlightSQLServer) GetSqlInfo() map[string]any {
	return map[string]any{
		"flight_sql_server_name":          "Chronicle Flight SQL",
		"flight_sql_server_version":       "1.0.0",
		"flight_sql_server_arrow_version": "15.0.0",
		"flight_sql_server_read_only":     false,
		"sql_ddl_catalog":                 false,
		"sql_ddl_schema":                  false,
		"sql_ddl_table":                   false,
		"sql_identifier_case":             "case_insensitive",
		"sql_identifier_quote_char":       "\"",
		"sql_quoted_identifier_case":      "case_sensitive",
		"sql_all_tables_are_selectable":   true,
	}
}

func (s *FlightSQLServer) handlePreparedQueryMsg(conn net.Conn, body []byte) {
	var req struct {
		StatementID string `json:"statement_id"`
		SQL         string `json:"sql"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	// Create or retrieve prepared statement.
	s.sessionMu.Lock()
	ps, exists := s.preparedStatements[req.StatementID]
	if !exists && req.SQL != "" {
		ps = &preparedStatement{
			id:      req.StatementID,
			sql:     req.SQL,
			schema:  ChronicleSchema(),
			created: time.Now(),
		}
		s.preparedStatements[req.StatementID] = ps
	}
	s.sessionMu.Unlock()

	if ps == nil {
		s.sendErrorMsg(conn, fmt.Errorf("prepared statement not found: %s", req.StatementID))
		return
	}

	batches, err := s.HandleStatementQuery(ps.sql)
	if err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	for _, batch := range batches {
		if err := s.sendResponseMsg(conn, flightSQLMessageDoGet, batch); err != nil {
			return
		}
	}
	//nolint:errcheck // final completion marker, no recovery action
	_ = s.sendResponseMsg(conn, flightSQLMessageResponse, map[string]any{
		"complete":     true,
		"statement_id": ps.id,
	})
}

// --- Wire protocol helpers ---

func (s *FlightSQLServer) sendResponseMsg(conn net.Conn, msgType uint32, data any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return s.sendMessage(conn, msgType, body)
}

func (s *FlightSQLServer) sendErrorMsg(conn net.Conn, err error) {
	resp := map[string]string{"error": err.Error()}
	body, _ := json.Marshal(resp)                           //nolint:errcheck // simple map marshal cannot fail
	_ = s.sendMessage(conn, flightSQLMessageError, body) //nolint:errcheck // error path: best-effort send
}

func (s *FlightSQLServer) sendMessage(conn net.Conn, msgType uint32, body []byte) error {
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], msgType)
	binary.BigEndian.PutUint32(header[4:], uint32(len(body)))

	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(body)
	return err
}

// --- SQLToQueryTranslator ---

// SQLToQueryTranslator translates basic SQL SELECT statements to Chronicle queries.
type SQLToQueryTranslator struct{}

// Translate parses a SQL SELECT statement and converts it to a Chronicle Query.
// Supports: SELECT ... FROM metric WHERE time > X AND tag = 'val' ORDER BY timestamp LIMIT N
func (t *SQLToQueryTranslator) Translate(sql string) (*Query, error) {
	normalized := strings.TrimSpace(sql)
	if normalized == "" {
		return nil, fmt.Errorf("empty SQL query")
	}

	upper := strings.ToUpper(normalized)
	if !strings.HasPrefix(upper, "SELECT") {
		return nil, fmt.Errorf("only SELECT statements are supported")
	}

	query := &Query{}

	// Extract FROM clause to get metric name.
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return nil, fmt.Errorf("missing FROM clause")
	}

	afterFrom := strings.TrimSpace(normalized[fromIdx+4:])
	metric, rest := extractIdentifier(afterFrom)
	if metric == "" {
		return nil, fmt.Errorf("missing table name in FROM clause")
	}
	// Strip surrounding quotes if present.
	metric = strings.Trim(metric, "\"'`")
	query.Metric = metric

	// Default time range: last hour.
	query.Start = time.Now().Add(-time.Hour).UnixNano()
	query.End = time.Now().UnixNano()

	restUpper := strings.ToUpper(rest)

	// Parse WHERE clause.
	if whereIdx := strings.Index(restUpper, "WHERE"); whereIdx >= 0 {
		wherePart := rest[whereIdx+5:]
		// Find end of WHERE clause (ORDER BY or LIMIT).
		endIdx := len(wherePart)
		for _, kw := range []string{"ORDER BY", "LIMIT"} {
			idx := strings.Index(strings.ToUpper(wherePart), kw)
			if idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		conditions := strings.TrimSpace(wherePart[:endIdx])
		rest = wherePart[endIdx:]
		restUpper = strings.ToUpper(rest)

		if err := parseWhereConditions(conditions, query); err != nil {
			return nil, err
		}
	}

	// Parse ORDER BY clause.
	if orderIdx := strings.Index(restUpper, "ORDER BY"); orderIdx >= 0 {
		afterOrder := rest[orderIdx+8:]
		limitIdx := strings.Index(strings.ToUpper(afterOrder), "LIMIT")
		if limitIdx >= 0 {
			rest = afterOrder[limitIdx:]
			restUpper = strings.ToUpper(rest)
		} else {
			rest = ""
			restUpper = ""
		}
	}

	// Parse LIMIT clause.
	if limitIdx := strings.Index(restUpper, "LIMIT"); limitIdx >= 0 {
		limitPart := strings.TrimSpace(rest[limitIdx+5:])
		var limit int
		if _, err := fmt.Sscanf(limitPart, "%d", &limit); err == nil && limit > 0 {
			query.Limit = limit
		}
	}

	return query, nil
}

func extractIdentifier(s string) (string, string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}

	// Handle quoted identifiers.
	if s[0] == '"' || s[0] == '\'' || s[0] == '`' {
		quote := s[0]
		end := strings.IndexByte(s[1:], quote)
		if end >= 0 {
			return s[1 : end+1], strings.TrimSpace(s[end+2:])
		}
	}

	// Unquoted: ends at whitespace or keywords.
	for i, ch := range s {
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == ';' {
			return s[:i], strings.TrimSpace(s[i:])
		}
	}
	return s, ""
}

func parseWhereConditions(conditions string, query *Query) error {
	if strings.TrimSpace(conditions) == "" {
		return nil
	}

	// Split on AND (case-insensitive).
	parts := splitOnAND(conditions)
	if query.Tags == nil {
		query.Tags = make(map[string]string)
	}

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		partUpper := strings.ToUpper(part)

		// Time range conditions.
		if strings.Contains(partUpper, "TIME") || strings.Contains(partUpper, "TIMESTAMP") {
			if ts, ok := parseTimeCondition(part); ok {
				if strings.Contains(part, ">") {
					query.Start = ts
				} else if strings.Contains(part, "<") {
					query.End = ts
				}
				continue
			}
		}

		// Tag equality: key = 'value'
		if eqIdx := strings.Index(part, "="); eqIdx > 0 && !strings.Contains(part, "!=") {
			key := strings.TrimSpace(part[:eqIdx])
			val := strings.TrimSpace(part[eqIdx+1:])
			val = strings.Trim(val, "'\"")
			key = strings.Trim(key, "\"'`")
			query.Tags[key] = val
		}
	}

	return nil
}

func splitOnAND(s string) []string {
	var parts []string
	upper := strings.ToUpper(s)
	for {
		idx := strings.Index(upper, " AND ")
		if idx < 0 {
			parts = append(parts, strings.TrimSpace(s))
			break
		}
		parts = append(parts, strings.TrimSpace(s[:idx]))
		s = s[idx+5:]
		upper = upper[idx+5:]
	}
	return parts
}

func parseTimeCondition(cond string) (int64, bool) {
	// Try to find a numeric timestamp.
	for _, op := range []string{">=", "<=", ">", "<", "="} {
		idx := strings.Index(cond, op)
		if idx < 0 {
			continue
		}
		val := strings.TrimSpace(cond[idx+len(op):])
		val = strings.Trim(val, "'\"")

		// Try parsing as unix nanoseconds.
		var ts int64
		if _, err := fmt.Sscanf(val, "%d", &ts); err == nil {
			return ts, true
		}

		// Try RFC3339.
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t.UnixNano(), true
		}

		// Try common SQL timestamp format.
		if t, err := time.Parse("2006-01-02 15:04:05", val); err == nil {
			return t.UnixNano(), true
		}

		break
	}
	return 0, false
}

// --- RecordBatchBuilder ---

// RecordBatchBuilder builds Arrow record batches from Points.
type RecordBatchBuilder struct {
	points []Point
	schema ArrowSchema
}

// NewRecordBatchBuilder creates a new RecordBatchBuilder.
func NewRecordBatchBuilder() *RecordBatchBuilder {
	return &RecordBatchBuilder{}
}

// AddPoint adds a point to the batch.
func (b *RecordBatchBuilder) AddPoint(p Point) {
	b.points = append(b.points, p)
}

// Build constructs an ArrowRecordBatch from the accumulated points.
// Schema is inferred from the Point data structure.
func (b *RecordBatchBuilder) Build() ArrowRecordBatch {
	n := len(b.points)
	if n == 0 {
		return ArrowRecordBatch{
			Schema: ChronicleSchema(),
			Length: 0,
		}
	}

	// Infer schema: base fields + any extra tag keys found across all points.
	tagKeys := make(map[string]struct{})
	for _, p := range b.points {
		for k := range p.Tags {
			tagKeys[k] = struct{}{}
		}
	}

	schema := b.inferSchema(tagKeys)

	timestamps := make([]any, n)
	metrics := make([]any, n)
	values := make([]any, n)
	tags := make([]any, n)

	for i, p := range b.points {
		timestamps[i] = p.Timestamp
		metrics[i] = p.Metric
		values[i] = p.Value
		tags[i] = p.Tags
	}

	columns := []ArrowColumn{
		{Name: "timestamp", Type: ArrowTypeTimestamp, Data: timestamps},
		{Name: "metric", Type: ArrowTypeString, Data: metrics},
		{Name: "value", Type: ArrowTypeFloat64, Data: values},
		{Name: "tags", Type: ArrowTypeMap, Data: tags},
	}

	return ArrowRecordBatch{
		Schema:  schema,
		Columns: columns,
		Length:  n,
	}
}

func (b *RecordBatchBuilder) inferSchema(tagKeys map[string]struct{}) ArrowSchema {
	fields := []ArrowField{
		{Name: "timestamp", Type: ArrowTypeTimestamp, Nullable: false},
		{Name: "metric", Type: ArrowTypeString, Nullable: false},
		{Name: "value", Type: ArrowTypeFloat64, Nullable: false},
		{Name: "tags", Type: ArrowTypeMap, Nullable: true},
	}
	return ArrowSchema{Fields: fields}
}

// --- Batch conversion helpers ---

func buildBatches(points []Point, maxBatchRows int) []ArrowRecordBatch {
	if maxBatchRows <= 0 {
		maxBatchRows = 65536
	}

	var batches []ArrowRecordBatch
	for i := 0; i < len(points); i += maxBatchRows {
		end := i + maxBatchRows
		if end > len(points) {
			end = len(points)
		}

		builder := NewRecordBatchBuilder()
		for _, p := range points[i:end] {
			builder.AddPoint(p)
		}
		batches = append(batches, builder.Build())
	}

	if len(batches) == 0 {
		batches = append(batches, NewRecordBatchBuilder().Build())
	}

	return batches
}

func recordBatchToPoints(batch ArrowRecordBatch) ([]Point, error) {
	if batch.Length == 0 {
		return nil, nil
	}

	var tsCol, metricCol, valueCol, tagsCol *ArrowColumn
	for i := range batch.Columns {
		switch batch.Columns[i].Name {
		case "timestamp":
			tsCol = &batch.Columns[i]
		case "metric":
			metricCol = &batch.Columns[i]
		case "value":
			valueCol = &batch.Columns[i]
		case "tags":
			tagsCol = &batch.Columns[i]
		}
	}

	if tsCol == nil || metricCol == nil || valueCol == nil {
		return nil, fmt.Errorf("missing required columns (timestamp, metric, value)")
	}

	tsData, ok := tsCol.Data.([]any)
	if !ok {
		return nil, fmt.Errorf("invalid timestamp column data")
	}
	metricData, ok := metricCol.Data.([]any)
	if !ok {
		return nil, fmt.Errorf("invalid metric column data")
	}
	valueData, ok := valueCol.Data.([]any)
	if !ok {
		return nil, fmt.Errorf("invalid value column data")
	}

	var tagsData []any
	if tagsCol != nil {
		tagsData, _ = tagsCol.Data.([]any) //nolint:errcheck // tags column is optional; nil tagsData is handled below
	}

	points := make([]Point, 0, batch.Length)
	for i := 0; i < batch.Length && i < len(tsData); i++ {
		p := Point{}

		switch v := tsData[i].(type) {
		case float64:
			p.Timestamp = int64(v)
		case int64:
			p.Timestamp = v
		case json.Number:
			n, _ := v.Int64()
			p.Timestamp = n
		}

		if m, ok := metricData[i].(string); ok {
			p.Metric = m
		}

		switch v := valueData[i].(type) {
		case float64:
			p.Value = v
		case json.Number:
			f, _ := v.Float64()
			p.Value = f
		}

		if tagsData != nil && i < len(tagsData) {
			if t, ok := tagsData[i].(map[string]any); ok {
				p.Tags = make(map[string]string, len(t))
				for k, v := range t {
					p.Tags[k] = fmt.Sprintf("%v", v)
				}
			} else if t, ok := tagsData[i].(map[string]string); ok {
				p.Tags = t
			}
		}

		points = append(points, p)
	}

	return points, nil
}

func flightQueryToChronicle(fq FlightQuery) *Query {
	q := &Query{
		Metric: fq.Metric,
		Tags:   fq.Tags,
		Start:  fq.Start,
		End:    fq.End,
		Limit:  fq.Limit,
	}
	if q.Start == 0 {
		q.Start = time.Now().Add(-time.Hour).UnixNano()
	}
	if q.End == 0 {
		q.End = time.Now().UnixNano()
	}
	return q
}

// --- Metadata record batch builders ---

func catalogsToRecordBatch(catalogs []string) ArrowRecordBatch {
	data := make([]any, len(catalogs))
	for i, c := range catalogs {
		data[i] = c
	}
	return ArrowRecordBatch{
		Schema: ArrowSchema{
			Fields: []ArrowField{
				{Name: "catalog_name", Type: ArrowTypeString, Nullable: false},
			},
		},
		Columns: []ArrowColumn{
			{Name: "catalog_name", Type: ArrowTypeString, Data: data},
		},
		Length: len(catalogs),
	}
}

func schemasToRecordBatch(catalog string, schemas []string) ArrowRecordBatch {
	catalogData := make([]any, len(schemas))
	schemaData := make([]any, len(schemas))
	for i, s := range schemas {
		catalogData[i] = catalog
		schemaData[i] = s
	}
	return ArrowRecordBatch{
		Schema: ArrowSchema{
			Fields: []ArrowField{
				{Name: "catalog_name", Type: ArrowTypeString, Nullable: true},
				{Name: "schema_name", Type: ArrowTypeString, Nullable: false},
			},
		},
		Columns: []ArrowColumn{
			{Name: "catalog_name", Type: ArrowTypeString, Data: catalogData},
			{Name: "schema_name", Type: ArrowTypeString, Data: schemaData},
		},
		Length: len(schemas),
	}
}

func tablesToRecordBatch(catalog, schema string, tables []string) ArrowRecordBatch {
	catalogData := make([]any, len(tables))
	schemaData := make([]any, len(tables))
	tableData := make([]any, len(tables))
	typeData := make([]any, len(tables))
	for i, t := range tables {
		catalogData[i] = catalog
		schemaData[i] = schema
		tableData[i] = t
		typeData[i] = "TABLE"
	}
	return ArrowRecordBatch{
		Schema: ArrowSchema{
			Fields: []ArrowField{
				{Name: "catalog_name", Type: ArrowTypeString, Nullable: true},
				{Name: "schema_name", Type: ArrowTypeString, Nullable: true},
				{Name: "table_name", Type: ArrowTypeString, Nullable: false},
				{Name: "table_type", Type: ArrowTypeString, Nullable: false},
			},
		},
		Columns: []ArrowColumn{
			{Name: "catalog_name", Type: ArrowTypeString, Data: catalogData},
			{Name: "schema_name", Type: ArrowTypeString, Data: schemaData},
			{Name: "table_name", Type: ArrowTypeString, Data: tableData},
			{Name: "table_type", Type: ArrowTypeString, Data: typeData},
		},
		Length: len(tables),
	}
}

// --- HTTP Routes ---

// setupFlightSQLRoutes registers HTTP routes for REST-based Flight SQL access.
func setupFlightSQLRoutes(mux *http.ServeMux, db *DB, wrap middlewareWrapper, server *FlightSQLServer) {
	mux.HandleFunc("/api/v1/flight-sql/catalogs", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		catalogs := server.HandleGetCatalogs()
		writeJSON(w, map[string]any{"catalogs": catalogs})
	}))

	mux.HandleFunc("/api/v1/flight-sql/schemas", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		catalog := r.URL.Query().Get("catalog")
		schemas := server.HandleGetSchemas(catalog)
		writeJSON(w, map[string]any{"catalog": catalog, "schemas": schemas})
	}))

	mux.HandleFunc("/api/v1/flight-sql/tables", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		catalog := r.URL.Query().Get("catalog")
		schema := r.URL.Query().Get("schema")
		tables := server.HandleGetTables(catalog, schema)
		writeJSON(w, map[string]any{
			"catalog": catalog,
			"schema":  schema,
			"tables":  tables,
		})
	}))

	mux.HandleFunc("/api/v1/flight-sql/query", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			SQL string `json:"sql"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if req.SQL == "" {
			writeError(w, "missing sql field", http.StatusBadRequest)
			return
		}

		batches, err := server.HandleStatementQuery(req.SQL)
		if err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}

		totalRows := 0
		for _, b := range batches {
			totalRows += b.Length
		}

		writeJSON(w, map[string]any{
			"batches":    batches,
			"total_rows": totalRows,
		})
	}))

	mux.HandleFunc("/api/v1/flight-sql/status", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		server.mu.RLock()
		running := server.running
		server.mu.RUnlock()

		server.sessionMu.RLock()
		sessionCount := len(server.sessions)
		preparedCount := len(server.preparedStatements)
		server.sessionMu.RUnlock()

		writeJSON(w, map[string]any{
			"running":              running,
			"bind_addr":            server.config.BindAddr,
			"sql_enabled":          server.config.EnableSQL,
			"max_batch_rows":       server.config.MaxBatchRows,
			"max_concurrent_streams": server.config.MaxConcurrentStreams,
			"active_sessions":      sessionCount,
			"prepared_statements":  preparedCount,
		})
	}))
}
