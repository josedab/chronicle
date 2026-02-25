package chronicle

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// ArrowFlightServer provides a simplified Arrow Flight-like RPC interface for data transfer.
// NOTE: This is a custom binary protocol inspired by Apache Arrow Flight, not a fully
// compliant Arrow Flight implementation. It does not use gRPC or the Arrow IPC format and
// is not interoperable with standard Arrow Flight clients (e.g., pyarrow.flight).
//
// EXPERIMENTAL: This API is unstable and may change without notice.
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
	Schema     ArrowSchema      `json:"schema"`
	Endpoints  []FlightEndpoint `json:"endpoints"`
	TotalRows  int64            `json:"total_rows"`
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
	Metric  string            `json:"metric"`
	Tags    map[string]string `json:"tags,omitempty"`
	Start   int64             `json:"start"`
	End     int64             `json:"end"`
	Limit   int               `json:"limit,omitempty"`
	Columns []string          `json:"columns,omitempty"`
}

// ArrowRecordBatch represents a batch of Arrow records.
type ArrowRecordBatch struct {
	Schema  ArrowSchema   `json:"schema"`
	Columns []ArrowColumn `json:"columns"`
	Length  int           `json:"length"`
}

// ArrowColumn represents a column of data.
type ArrowColumn struct {
	Name  string    `json:"name"`
	Type  ArrowType `json:"type"`
	Data  any       `json:"data"`
	Nulls []bool    `json:"nulls,omitempty"`
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
		closeQuietly(s.listener)
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
		Schema:     ChronicleSchema(),
		TotalRows:  int64(len(result.Points)),
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
	s.sendResponse(conn, FlightMessageDoGet, map[string]any{"complete": true})
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

	s.sendResponse(conn, FlightMessageDoPut, map[string]any{
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
