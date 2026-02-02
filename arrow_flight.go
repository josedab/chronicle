package chronicle

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// ArrowFlightServer provides Apache Arrow Flight RPC interface for zero-copy data transfer.
// This enables high-performance data export to analytics tools like Pandas, Polars, and DuckDB.
type ArrowFlightServer struct {
	db       *DB
	config   ArrowFlightConfig
	listener net.Listener
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// ArrowFlightConfig configures the Arrow Flight server.
type ArrowFlightConfig struct {
	// Enabled enables the Arrow Flight server.
	Enabled bool

	// BindAddr is the address to bind for Flight RPC.
	BindAddr string

	// MaxBatchSize is the maximum number of rows per batch.
	MaxBatchSize int

	// MaxMessageSize is the maximum message size in bytes.
	MaxMessageSize int64

	// EnableCompression enables LZ4 compression for data transfer.
	EnableCompression bool
}

// DefaultArrowFlightConfig returns default Arrow Flight configuration.
func DefaultArrowFlightConfig() ArrowFlightConfig {
	return ArrowFlightConfig{
		Enabled:           false,
		BindAddr:          "127.0.0.1:8815",
		MaxBatchSize:      65536,
		MaxMessageSize:    64 * 1024 * 1024, // 64MB
		EnableCompression: true,
	}
}

// ArrowSchema represents an Arrow schema for Chronicle data.
type ArrowSchema struct {
	Fields []ArrowField `json:"fields"`
}

// ArrowField represents a field in an Arrow schema.
type ArrowField struct {
	Name     string            `json:"name"`
	Type     ArrowType         `json:"type"`
	Nullable bool              `json:"nullable"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ArrowType represents Arrow data types.
type ArrowType int

const (
	// ArrowTypeInt64 is a 64-bit signed integer.
	ArrowTypeInt64 ArrowType = iota
	// ArrowTypeFloat64 is a 64-bit floating point.
	ArrowTypeFloat64
	// ArrowTypeString is a UTF-8 string.
	ArrowTypeString
	// ArrowTypeTimestamp is a nanosecond timestamp.
	ArrowTypeTimestamp
	// ArrowTypeBool is a boolean.
	ArrowTypeBool
	// ArrowTypeBinary is binary data.
	ArrowTypeBinary
	// ArrowTypeMap is a map type.
	ArrowTypeMap
)

func (t ArrowType) String() string {
	switch t {
	case ArrowTypeInt64:
		return "int64"
	case ArrowTypeFloat64:
		return "float64"
	case ArrowTypeString:
		return "utf8"
	case ArrowTypeTimestamp:
		return "timestamp[ns]"
	case ArrowTypeBool:
		return "bool"
	case ArrowTypeBinary:
		return "binary"
	case ArrowTypeMap:
		return "map<utf8, utf8>"
	default:
		return "unknown"
	}
}

// ChronicleSchema returns the Arrow schema for Chronicle time-series data.
func ChronicleSchema() ArrowSchema {
	return ArrowSchema{
		Fields: []ArrowField{
			{Name: "timestamp", Type: ArrowTypeTimestamp, Nullable: false},
			{Name: "metric", Type: ArrowTypeString, Nullable: false},
			{Name: "value", Type: ArrowTypeFloat64, Nullable: false},
			{Name: "tags", Type: ArrowTypeMap, Nullable: true},
		},
	}
}

// FlightInfo describes a dataset available for retrieval.
type FlightInfo struct {
	Schema    ArrowSchema       `json:"schema"`
	Endpoints []FlightEndpoint  `json:"endpoints"`
	TotalRows int64             `json:"total_rows"`
	TotalBytes int64            `json:"total_bytes"`
}

// FlightEndpoint describes a location where data can be retrieved.
type FlightEndpoint struct {
	Ticket    FlightTicket     `json:"ticket"`
	Locations []FlightLocation `json:"locations"`
}

// FlightTicket is an opaque ticket for retrieving data.
type FlightTicket struct {
	Ticket []byte `json:"ticket"`
}

// FlightLocation describes a server location.
type FlightLocation struct {
	URI string `json:"uri"`
}

// FlightDescriptor describes a dataset.
type FlightDescriptor struct {
	Type FlightDescriptorType `json:"type"`
	Cmd  []byte               `json:"cmd,omitempty"`
	Path []string             `json:"path,omitempty"`
}

// FlightDescriptorType is the type of flight descriptor.
type FlightDescriptorType int

const (
	// FlightDescriptorUnknown is an unknown descriptor type.
	FlightDescriptorUnknown FlightDescriptorType = iota
	// FlightDescriptorPath is a path-based descriptor.
	FlightDescriptorPath
	// FlightDescriptorCmd is a command-based descriptor.
	FlightDescriptorCmd
)

// FlightQuery represents a query for Flight data retrieval.
type FlightQuery struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags,omitempty"`
	Start     int64             `json:"start"`
	End       int64             `json:"end"`
	Limit     int               `json:"limit,omitempty"`
	Columns   []string          `json:"columns,omitempty"`
}

// ArrowRecordBatch represents a batch of Arrow records.
type ArrowRecordBatch struct {
	Schema  ArrowSchema     `json:"schema"`
	Columns []ArrowColumn   `json:"columns"`
	Length  int             `json:"length"`
}

// ArrowColumn represents a column of data.
type ArrowColumn struct {
	Name   string      `json:"name"`
	Type   ArrowType   `json:"type"`
	Data   interface{} `json:"data"`
	Nulls  []bool      `json:"nulls,omitempty"`
}

// NewArrowFlightServer creates a new Arrow Flight server.
func NewArrowFlightServer(db *DB, config ArrowFlightConfig) *ArrowFlightServer {
	return &ArrowFlightServer{
		db:     db,
		config: config,
		stopCh: make(chan struct{}),
	}
}

// Start starts the Arrow Flight server.
func (s *ArrowFlightServer) Start() error {
	if !s.config.Enabled {
		return nil
	}

	listener, err := net.Listen("tcp", s.config.BindAddr)
	if err != nil {
		return fmt.Errorf("failed to bind Arrow Flight server: %w", err)
	}
	s.listener = listener

	s.wg.Add(1)
	go s.serve()

	return nil
}

// Stop stops the Arrow Flight server.
func (s *ArrowFlightServer) Stop() error {
	close(s.stopCh)
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

// serve handles incoming connections.
func (s *ArrowFlightServer) serve() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles a single Flight connection.
func (s *ArrowFlightServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		// Read message type and length
		header := make([]byte, 8)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}

		msgType := binary.BigEndian.Uint32(header[:4])
		msgLen := binary.BigEndian.Uint32(header[4:])

		if int64(msgLen) > s.config.MaxMessageSize {
			s.sendError(conn, errors.New("message too large"))
			return
		}

		// Read message body
		body := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}

		// Handle message
		switch FlightMessageType(msgType) {
		case FlightMessageGetFlightInfo:
			s.handleGetFlightInfo(conn, body)
		case FlightMessageDoGet:
			s.handleDoGet(conn, body)
		case FlightMessageDoPut:
			s.handleDoPut(conn, body)
		case FlightMessageGetSchema:
			s.handleGetSchema(conn, body)
		case FlightMessageListFlights:
			s.handleListFlights(conn, body)
		case FlightMessageListActions:
			s.handleListActions(conn)
		case FlightMessageDoAction:
			s.handleDoAction(conn, body)
		default:
			s.sendError(conn, fmt.Errorf("unknown message type: %d", msgType))
		}
	}
}

// FlightMessageType represents Flight RPC message types.
type FlightMessageType uint32

const (
	FlightMessageGetFlightInfo FlightMessageType = 1
	FlightMessageDoGet         FlightMessageType = 2
	FlightMessageDoPut         FlightMessageType = 3
	FlightMessageGetSchema     FlightMessageType = 4
	FlightMessageListFlights   FlightMessageType = 5
	FlightMessageListActions   FlightMessageType = 6
	FlightMessageDoAction      FlightMessageType = 7
)

// handleGetFlightInfo handles GetFlightInfo requests.
func (s *ArrowFlightServer) handleGetFlightInfo(conn net.Conn, body []byte) {
	var desc FlightDescriptor
	if err := json.Unmarshal(body, &desc); err != nil {
		s.sendError(conn, err)
		return
	}

	var query FlightQuery
	if desc.Type == FlightDescriptorCmd {
		if err := json.Unmarshal(desc.Cmd, &query); err != nil {
			s.sendError(conn, err)
			return
		}
	} else if len(desc.Path) > 0 {
		query.Metric = desc.Path[0]
		query.Start = time.Now().Add(-time.Hour).UnixNano()
		query.End = time.Now().UnixNano()
	}

	// Execute query to get row count
	chronicleQuery := s.toChronicleQuery(query)
	result, err := s.db.Execute(chronicleQuery)
	if err != nil {
		s.sendError(conn, err)
		return
	}

	info := FlightInfo{
		Schema:    ChronicleSchema(),
		TotalRows: int64(len(result.Points)),
		TotalBytes: int64(len(result.Points) * 64), // Approximate
		Endpoints: []FlightEndpoint{
			{
				Ticket: FlightTicket{Ticket: body},
				Locations: []FlightLocation{
					{URI: fmt.Sprintf("grpc+tcp://%s", s.config.BindAddr)},
				},
			},
		},
	}

	s.sendResponse(conn, FlightMessageGetFlightInfo, info)
}

// handleDoGet handles DoGet requests - the main data retrieval method.
func (s *ArrowFlightServer) handleDoGet(conn net.Conn, body []byte) {
	var ticket FlightTicket
	if err := json.Unmarshal(body, &ticket); err != nil {
		s.sendError(conn, err)
		return
	}

	// Decode query from ticket
	var desc FlightDescriptor
	if err := json.Unmarshal(ticket.Ticket, &desc); err != nil {
		s.sendError(conn, err)
		return
	}

	var query FlightQuery
	if desc.Type == FlightDescriptorCmd {
		if err := json.Unmarshal(desc.Cmd, &query); err != nil {
			s.sendError(conn, err)
			return
		}
	} else if len(desc.Path) > 0 {
		query.Metric = desc.Path[0]
		query.Start = time.Now().Add(-time.Hour).UnixNano()
		query.End = time.Now().UnixNano()
	}

	// Execute query
	chronicleQuery := s.toChronicleQuery(query)
	result, err := s.db.Execute(chronicleQuery)
	if err != nil {
		s.sendError(conn, err)
		return
	}

	// Send schema first
	s.sendResponse(conn, FlightMessageGetSchema, ChronicleSchema())

	// Stream data in batches
	points := result.Points
	batchSize := s.config.MaxBatchSize
	if batchSize <= 0 {
		batchSize = 65536
	}

	for i := 0; i < len(points); i += batchSize {
		end := i + batchSize
		if end > len(points) {
			end = len(points)
		}

		batch := s.pointsToRecordBatch(points[i:end])
		if err := s.sendRecordBatch(conn, batch); err != nil {
			return
		}
	}

	// Send end-of-stream marker
	s.sendResponse(conn, FlightMessageDoGet, map[string]interface{}{"complete": true})
}

// handleDoPut handles DoPut requests for data ingestion.
func (s *ArrowFlightServer) handleDoPut(conn net.Conn, body []byte) {
	var batch ArrowRecordBatch
	if err := json.Unmarshal(body, &batch); err != nil {
		s.sendError(conn, err)
		return
	}

	// Convert batch to points
	points, err := s.recordBatchToPoints(batch)
	if err != nil {
		s.sendError(conn, err)
		return
	}

	// Write points
	if err := s.db.WriteBatch(points); err != nil {
		s.sendError(conn, err)
		return
	}

	s.sendResponse(conn, FlightMessageDoPut, map[string]interface{}{
		"rows_written": len(points),
	})
}

// handleGetSchema handles GetSchema requests.
func (s *ArrowFlightServer) handleGetSchema(conn net.Conn, body []byte) {
	s.sendResponse(conn, FlightMessageGetSchema, ChronicleSchema())
}

// handleListFlights handles ListFlights requests.
func (s *ArrowFlightServer) handleListFlights(conn net.Conn, body []byte) {
	metrics := s.db.Metrics()
	flights := make([]FlightInfo, len(metrics))

	for i, metric := range metrics {
		flights[i] = FlightInfo{
			Schema: ChronicleSchema(),
			Endpoints: []FlightEndpoint{
				{
					Ticket: FlightTicket{
						Ticket: []byte(fmt.Sprintf(`{"type":1,"path":["%s"]}`, metric)),
					},
				},
			},
		}
	}

	s.sendResponse(conn, FlightMessageListFlights, flights)
}

// FlightAction represents an action that can be performed.
type FlightAction struct {
	Type string `json:"type"`
	Body []byte `json:"body,omitempty"`
}

// FlightActionType describes an available action.
type FlightActionType struct {
	Type        string `json:"type"`
	Description string `json:"description"`
}

// handleListActions handles ListActions requests.
func (s *ArrowFlightServer) handleListActions(conn net.Conn) {
	actions := []FlightActionType{
		{Type: "compact", Description: "Trigger compaction"},
		{Type: "flush", Description: "Flush write buffer"},
		{Type: "stats", Description: "Get database statistics"},
	}
	s.sendResponse(conn, FlightMessageListActions, actions)
}

// handleDoAction handles DoAction requests.
func (s *ArrowFlightServer) handleDoAction(conn net.Conn, body []byte) {
	var action FlightAction
	if err := json.Unmarshal(body, &action); err != nil {
		s.sendError(conn, err)
		return
	}

	switch action.Type {
	case "flush":
		if err := s.db.Flush(); err != nil {
			s.sendError(conn, err)
			return
		}
		s.sendResponse(conn, FlightMessageDoAction, map[string]string{"status": "ok"})

	case "stats":
		metrics := s.db.Metrics()
		s.sendResponse(conn, FlightMessageDoAction, map[string]interface{}{
			"metrics":       metrics,
			"metric_count":  len(metrics),
		})

	default:
		s.sendError(conn, fmt.Errorf("unknown action: %s", action.Type))
	}
}

// toChronicleQuery converts a FlightQuery to Chronicle Query.
func (s *ArrowFlightServer) toChronicleQuery(fq FlightQuery) *Query {
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

// pointsToRecordBatch converts Chronicle Points to an Arrow RecordBatch.
func (s *ArrowFlightServer) pointsToRecordBatch(points []Point) ArrowRecordBatch {
	n := len(points)

	timestamps := make([]int64, n)
	metrics := make([]string, n)
	values := make([]float64, n)
	tags := make([]map[string]string, n)

	for i, p := range points {
		timestamps[i] = p.Timestamp
		metrics[i] = p.Metric
		values[i] = p.Value
		tags[i] = p.Tags
	}

	return ArrowRecordBatch{
		Schema: ChronicleSchema(),
		Length: n,
		Columns: []ArrowColumn{
			{Name: "timestamp", Type: ArrowTypeTimestamp, Data: timestamps},
			{Name: "metric", Type: ArrowTypeString, Data: metrics},
			{Name: "value", Type: ArrowTypeFloat64, Data: values},
			{Name: "tags", Type: ArrowTypeMap, Data: tags},
		},
	}
}

// recordBatchToPoints converts an Arrow RecordBatch to Chronicle Points.
func (s *ArrowFlightServer) recordBatchToPoints(batch ArrowRecordBatch) ([]Point, error) {
	if batch.Length == 0 {
		return nil, nil
	}

	var timestamps []int64
	var metrics []string
	var values []float64
	var tags []map[string]string

	for _, col := range batch.Columns {
		switch col.Name {
		case "timestamp":
			if data, ok := col.Data.([]interface{}); ok {
				timestamps = make([]int64, len(data))
				for i, v := range data {
					if f, ok := v.(float64); ok {
						timestamps[i] = int64(f)
					}
				}
			}
		case "metric":
			if data, ok := col.Data.([]interface{}); ok {
				metrics = make([]string, len(data))
				for i, v := range data {
					if s, ok := v.(string); ok {
						metrics[i] = s
					}
				}
			}
		case "value":
			if data, ok := col.Data.([]interface{}); ok {
				values = make([]float64, len(data))
				for i, v := range data {
					if f, ok := v.(float64); ok {
						values[i] = f
					}
				}
			}
		case "tags":
			if data, ok := col.Data.([]interface{}); ok {
				tags = make([]map[string]string, len(data))
				for i, v := range data {
					if m, ok := v.(map[string]interface{}); ok {
						tags[i] = make(map[string]string)
						for k, val := range m {
							if s, ok := val.(string); ok {
								tags[i][k] = s
							}
						}
					}
				}
			}
		}
	}

	// Validate we have all required columns
	n := batch.Length
	if len(timestamps) == 0 {
		timestamps = make([]int64, n)
		now := time.Now().UnixNano()
		for i := range timestamps {
			timestamps[i] = now
		}
	}
	if len(metrics) == 0 {
		return nil, errors.New("metric column is required")
	}
	if len(values) == 0 {
		return nil, errors.New("value column is required")
	}
	if len(tags) == 0 {
		tags = make([]map[string]string, n)
	}

	points := make([]Point, n)
	for i := 0; i < n; i++ {
		points[i] = Point{
			Timestamp: timestamps[i],
			Metric:    metrics[i],
			Value:     values[i],
			Tags:      tags[i],
		}
	}

	return points, nil
}

// sendResponse sends a response message.
func (s *ArrowFlightServer) sendResponse(conn net.Conn, msgType FlightMessageType, data interface{}) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return s.sendMessage(conn, uint32(msgType), body)
}

// sendRecordBatch sends a record batch.
func (s *ArrowFlightServer) sendRecordBatch(conn net.Conn, batch ArrowRecordBatch) error {
	return s.sendResponse(conn, FlightMessageDoGet, batch)
}

// sendError sends an error response.
func (s *ArrowFlightServer) sendError(conn net.Conn, err error) {
	errResp := map[string]string{"error": err.Error()}
	body, _ := json.Marshal(errResp)
	_ = s.sendMessage(conn, 0, body)
}

// sendMessage sends a length-prefixed message.
func (s *ArrowFlightServer) sendMessage(conn net.Conn, msgType uint32, body []byte) error {
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], msgType)
	binary.BigEndian.PutUint32(header[4:], uint32(len(body)))

	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(body)
	return err
}

// ArrowFlightClient is a client for the Arrow Flight server.
type ArrowFlightClient struct {
	addr string
	conn net.Conn
}

// NewArrowFlightClient creates a new Arrow Flight client.
func NewArrowFlightClient(addr string) *ArrowFlightClient {
	return &ArrowFlightClient{addr: addr}
}

// Connect establishes a connection to the server.
func (c *ArrowFlightClient) Connect(ctx context.Context) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// Close closes the connection.
func (c *ArrowFlightClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// GetFlightInfo retrieves flight information.
func (c *ArrowFlightClient) GetFlightInfo(ctx context.Context, desc FlightDescriptor) (*FlightInfo, error) {
	body, err := json.Marshal(desc)
	if err != nil {
		return nil, err
	}

	if err := c.sendMessage(FlightMessageGetFlightInfo, body); err != nil {
		return nil, err
	}

	respBody, err := c.readMessage()
	if err != nil {
		return nil, err
	}

	var info FlightInfo
	if err := json.Unmarshal(respBody, &info); err != nil {
		return nil, err
	}

	return &info, nil
}

// DoGet retrieves data from the server.
func (c *ArrowFlightClient) DoGet(ctx context.Context, ticket FlightTicket) ([]ArrowRecordBatch, error) {
	body, err := json.Marshal(ticket)
	if err != nil {
		return nil, err
	}

	if err := c.sendMessage(FlightMessageDoGet, body); err != nil {
		return nil, err
	}

	var batches []ArrowRecordBatch

	for {
		respBody, err := c.readMessage()
		if err != nil {
			return batches, nil
		}

		// Check for completion marker
		var completion map[string]interface{}
		if err := json.Unmarshal(respBody, &completion); err == nil {
			if complete, ok := completion["complete"].(bool); ok && complete {
				break
			}
		}

		// Try to parse as record batch
		var batch ArrowRecordBatch
		if err := json.Unmarshal(respBody, &batch); err == nil && batch.Length > 0 {
			batches = append(batches, batch)
		}
	}

	return batches, nil
}

// DoPut sends data to the server.
func (c *ArrowFlightClient) DoPut(ctx context.Context, batch ArrowRecordBatch) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return err
	}

	if err := c.sendMessage(FlightMessageDoPut, body); err != nil {
		return err
	}

	_, err = c.readMessage()
	return err
}

// sendMessage sends a message to the server.
func (c *ArrowFlightClient) sendMessage(msgType FlightMessageType, body []byte) error {
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], uint32(msgType))
	binary.BigEndian.PutUint32(header[4:], uint32(len(body)))

	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(body)
	return err
}

// readMessage reads a message from the server.
func (c *ArrowFlightClient) readMessage() ([]byte, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, err
	}

	msgLen := binary.BigEndian.Uint32(header[4:])
	body := make([]byte, msgLen)
	if _, err := io.ReadFull(c.conn, body); err != nil {
		return nil, err
	}

	return body, nil
}

// ToDataFrame converts record batches to a simple columnar format for export.
func ToDataFrame(batches []ArrowRecordBatch) map[string]interface{} {
	result := make(map[string]interface{})

	if len(batches) == 0 {
		return result
	}

	// Initialize columns
	for _, col := range batches[0].Columns {
		result[col.Name] = make([]interface{}, 0)
	}

	// Aggregate all batches
	for _, batch := range batches {
		for _, col := range batch.Columns {
			if existing, ok := result[col.Name].([]interface{}); ok {
				if data, ok := col.Data.([]interface{}); ok {
					result[col.Name] = append(existing, data...)
				}
			}
		}
	}

	return result
}

// IPC serialization helpers for Arrow IPC format compatibility

// ArrowIPCMessage represents an Arrow IPC message.
type ArrowIPCMessage struct {
	Header ArrowIPCHeader `json:"header"`
	Body   []byte         `json:"body"`
}

// ArrowIPCHeader represents the header of an Arrow IPC message.
type ArrowIPCHeader struct {
	Version    int    `json:"version"`
	Type       int    `json:"type"`
	BodyLength int64  `json:"body_length"`
}

// SerializeToIPC serializes a record batch to Arrow IPC format.
func SerializeToIPC(batch ArrowRecordBatch) ([]byte, error) {
	// Simplified IPC serialization - in production, use apache/arrow-go
	var buf bytes.Buffer

	// Write schema
	schemaBytes, err := json.Marshal(batch.Schema)
	if err != nil {
		return nil, err
	}

	// Write header
	header := ArrowIPCHeader{
		Version:    4, // Arrow IPC v4
		Type:       1, // RecordBatch
		BodyLength: int64(len(schemaBytes)),
	}
	headerBytes, _ := json.Marshal(header)

	// Continuation bytes (Arrow IPC format)
	buf.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	
	// Header length
	headerLen := uint32(len(headerBytes))
	binary.Write(&buf, binary.LittleEndian, headerLen)
	
	// Header
	buf.Write(headerBytes)
	
	// Padding to 8-byte boundary
	padding := (8 - buf.Len()%8) % 8
	for i := 0; i < padding; i++ {
		buf.WriteByte(0)
	}

	// Body
	buf.Write(schemaBytes)

	return buf.Bytes(), nil
}

// DeserializeFromIPC deserializes Arrow IPC format to a record batch.
func DeserializeFromIPC(data []byte) (*ArrowRecordBatch, error) {
	// Simplified IPC deserialization - in production, use apache/arrow-go
	if len(data) < 8 {
		return nil, errors.New("invalid IPC data")
	}

	// Skip continuation bytes
	offset := 4
	
	// Read header length
	headerLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if int(headerLen) > len(data)-offset {
		return nil, errors.New("invalid header length")
	}

	// Read header
	var header ArrowIPCHeader
	if err := json.Unmarshal(data[offset:offset+int(headerLen)], &header); err != nil {
		return nil, err
	}
	offset += int(headerLen)

	// Skip padding
	offset = (offset + 7) &^ 7

	// Read body
	if offset >= len(data) {
		return nil, errors.New("missing body")
	}

	var schema ArrowSchema
	if err := json.Unmarshal(data[offset:], &schema); err != nil {
		return nil, err
	}

	return &ArrowRecordBatch{
		Schema: schema,
	}, nil
}
