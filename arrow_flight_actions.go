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
	"time"
)

// Flight action handling, query conversion, and record batch serialization.

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
		s.sendResponse(conn, FlightMessageDoAction, map[string]any{
			"metrics":      metrics,
			"metric_count": len(metrics),
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
			if data, ok := col.Data.([]any); ok {
				timestamps = make([]int64, len(data))
				for i, v := range data {
					if f, ok := v.(float64); ok {
						timestamps[i] = int64(f)
					}
				}
			}
		case "metric":
			if data, ok := col.Data.([]any); ok {
				metrics = make([]string, len(data))
				for i, v := range data {
					if s, ok := v.(string); ok {
						metrics[i] = s
					}
				}
			}
		case "value":
			if data, ok := col.Data.([]any); ok {
				values = make([]float64, len(data))
				for i, v := range data {
					if f, ok := v.(float64); ok {
						values[i] = f
					}
				}
			}
		case "tags":
			if data, ok := col.Data.([]any); ok {
				tags = make([]map[string]string, len(data))
				for i, v := range data {
					if m, ok := v.(map[string]any); ok {
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
func (s *ArrowFlightServer) sendResponse(conn net.Conn, msgType FlightMessageType, data any) error {
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
	_ = s.sendMessage(conn, 0, body) //nolint:errcheck // best-effort final message
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
		var completion map[string]any
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
func ToDataFrame(batches []ArrowRecordBatch) map[string]any {
	result := make(map[string]any)

	if len(batches) == 0 {
		return result
	}

	// Initialize columns
	for _, col := range batches[0].Columns {
		result[col.Name] = make([]any, 0)
	}

	// Aggregate all batches
	for _, batch := range batches {
		for _, col := range batch.Columns {
			if existing, ok := result[col.Name].([]any); ok {
				if data, ok := col.Data.([]any); ok {
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
	Version    int   `json:"version"`
	Type       int   `json:"type"`
	BodyLength int64 `json:"body_length"`
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
