package chronicle

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestArrowSchema(t *testing.T) {
	schema := ChronicleSchema()

	if len(schema.Fields) != 4 {
		t.Errorf("expected 4 fields, got %d", len(schema.Fields))
	}

	expected := []struct {
		name     string
		nullable bool
	}{
		{"timestamp", false},
		{"metric", false},
		{"value", false},
		{"tags", true},
	}

	for i, exp := range expected {
		if schema.Fields[i].Name != exp.name {
			t.Errorf("field %d: expected name %s, got %s", i, exp.name, schema.Fields[i].Name)
		}
		if schema.Fields[i].Nullable != exp.nullable {
			t.Errorf("field %d: expected nullable %v, got %v", i, exp.nullable, schema.Fields[i].Nullable)
		}
	}
}

func TestArrowType_String(t *testing.T) {
	tests := []struct {
		typ      ArrowType
		expected string
	}{
		{ArrowTypeInt64, "int64"},
		{ArrowTypeFloat64, "float64"},
		{ArrowTypeString, "utf8"},
		{ArrowTypeTimestamp, "timestamp[ns]"},
		{ArrowTypeBool, "bool"},
		{ArrowTypeBinary, "binary"},
		{ArrowTypeMap, "map<utf8, utf8>"},
		{ArrowType(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.expected {
			t.Errorf("ArrowType(%d).String() = %s, want %s", tt.typ, got, tt.expected)
		}
	}
}

func TestArrowFlightServer_Creation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultArrowFlightConfig()
	server := NewArrowFlightServer(db, config)

	if server == nil {
		t.Fatal("expected non-nil server")
	}
	if server.db != db {
		t.Error("server db reference mismatch")
	}
}

func TestArrowFlightServer_StartStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultArrowFlightConfig()
	config.Enabled = true
	config.BindAddr = "127.0.0.1:18815"

	server := NewArrowFlightServer(db, config)

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	// Give it time to start
	time.Sleep(50 * time.Millisecond)

	if err := server.Stop(); err != nil {
		t.Errorf("failed to stop server: %v", err)
	}
}

func TestArrowFlightServer_DisabledNoStart(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultArrowFlightConfig()
	config.Enabled = false

	server := NewArrowFlightServer(db, config)

	// Should return nil when disabled
	if err := server.Start(); err != nil {
		t.Errorf("disabled server should not error on start: %v", err)
	}
}

func TestPointsToRecordBatch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewArrowFlightServer(db, DefaultArrowFlightConfig())

	points := []Point{
		{Timestamp: 1000000000, Metric: "cpu", Value: 50.0, Tags: map[string]string{"host": "a"}},
		{Timestamp: 2000000000, Metric: "cpu", Value: 60.0, Tags: map[string]string{"host": "b"}},
		{Timestamp: 3000000000, Metric: "mem", Value: 70.0, Tags: map[string]string{"host": "a"}},
	}

	batch := server.pointsToRecordBatch(points)

	if batch.Length != 3 {
		t.Errorf("expected length 3, got %d", batch.Length)
	}

	if len(batch.Columns) != 4 {
		t.Errorf("expected 4 columns, got %d", len(batch.Columns))
	}

	// Verify timestamp column
	for _, col := range batch.Columns {
		if col.Name == "timestamp" {
			ts := col.Data.([]int64)
			if len(ts) != 3 {
				t.Errorf("timestamp column: expected 3 values, got %d", len(ts))
			}
			if ts[0] != 1000000000 {
				t.Errorf("first timestamp mismatch")
			}
		}
	}
}

func TestRecordBatchToPoints(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewArrowFlightServer(db, DefaultArrowFlightConfig())

	batch := ArrowRecordBatch{
		Schema: ChronicleSchema(),
		Length: 2,
		Columns: []ArrowColumn{
			{Name: "timestamp", Type: ArrowTypeTimestamp, Data: []interface{}{float64(1000), float64(2000)}},
			{Name: "metric", Type: ArrowTypeString, Data: []interface{}{"cpu", "mem"}},
			{Name: "value", Type: ArrowTypeFloat64, Data: []interface{}{float64(50), float64(60)}},
			{Name: "tags", Type: ArrowTypeMap, Data: []interface{}{
				map[string]interface{}{"host": "a"},
				map[string]interface{}{"host": "b"},
			}},
		},
	}

	points, err := server.recordBatchToPoints(batch)
	if err != nil {
		t.Fatalf("failed to convert batch: %v", err)
	}

	if len(points) != 2 {
		t.Errorf("expected 2 points, got %d", len(points))
	}

	if points[0].Metric != "cpu" {
		t.Errorf("first point metric mismatch: got %s", points[0].Metric)
	}
	if points[1].Metric != "mem" {
		t.Errorf("second point metric mismatch: got %s", points[1].Metric)
	}
}

func TestRecordBatchToPoints_MissingMetric(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewArrowFlightServer(db, DefaultArrowFlightConfig())

	batch := ArrowRecordBatch{
		Length: 1,
		Columns: []ArrowColumn{
			{Name: "value", Type: ArrowTypeFloat64, Data: []interface{}{float64(50)}},
		},
	}

	_, err := server.recordBatchToPoints(batch)
	if err == nil {
		t.Error("expected error for missing metric column")
	}
}

func TestFlightQuery_Conversion(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewArrowFlightServer(db, DefaultArrowFlightConfig())

	fq := FlightQuery{
		Metric: "temperature",
		Tags:   map[string]string{"room": "living"},
		Start:  1000000000,
		End:    2000000000,
		Limit:  100,
	}

	q := server.toChronicleQuery(fq)

	if q.Metric != "temperature" {
		t.Errorf("metric mismatch")
	}
	if q.Tags["room"] != "living" {
		t.Errorf("tags mismatch")
	}
	if q.Start != 1000000000 {
		t.Errorf("start mismatch")
	}
	if q.End != 2000000000 {
		t.Errorf("end mismatch")
	}
	if q.Limit != 100 {
		t.Errorf("limit mismatch")
	}
}

func TestFlightQuery_DefaultTimes(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewArrowFlightServer(db, DefaultArrowFlightConfig())

	fq := FlightQuery{
		Metric: "test",
		// Start and End are zero
	}

	q := server.toChronicleQuery(fq)

	if q.Start == 0 {
		t.Error("expected non-zero start time")
	}
	if q.End == 0 {
		t.Error("expected non-zero end time")
	}
	if q.End <= q.Start {
		t.Error("end should be after start")
	}
}

func TestFlightInfo_Serialization(t *testing.T) {
	info := FlightInfo{
		Schema:     ChronicleSchema(),
		TotalRows:  1000,
		TotalBytes: 64000,
		Endpoints: []FlightEndpoint{
			{
				Ticket:    FlightTicket{Ticket: []byte("test-ticket")},
				Locations: []FlightLocation{{URI: "grpc+tcp://localhost:8815"}},
			},
		},
	}

	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded FlightInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.TotalRows != info.TotalRows {
		t.Errorf("total rows mismatch")
	}
	if len(decoded.Endpoints) != 1 {
		t.Errorf("endpoints mismatch")
	}
}

func TestFlightDescriptor_Serialization(t *testing.T) {
	tests := []struct {
		name string
		desc FlightDescriptor
	}{
		{
			name: "path descriptor",
			desc: FlightDescriptor{
				Type: FlightDescriptorPath,
				Path: []string{"metrics", "cpu"},
			},
		},
		{
			name: "cmd descriptor",
			desc: FlightDescriptor{
				Type: FlightDescriptorCmd,
				Cmd:  []byte(`{"metric":"cpu","start":0,"end":0}`),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.desc)
			if err != nil {
				t.Fatalf("failed to marshal: %v", err)
			}

			var decoded FlightDescriptor
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}

			if decoded.Type != tt.desc.Type {
				t.Errorf("type mismatch")
			}
		})
	}
}

func TestToDataFrame(t *testing.T) {
	batches := []ArrowRecordBatch{
		{
			Schema: ChronicleSchema(),
			Length: 2,
			Columns: []ArrowColumn{
				{Name: "timestamp", Data: []interface{}{float64(1), float64(2)}},
				{Name: "value", Data: []interface{}{float64(10), float64(20)}},
			},
		},
		{
			Schema: ChronicleSchema(),
			Length: 1,
			Columns: []ArrowColumn{
				{Name: "timestamp", Data: []interface{}{float64(3)}},
				{Name: "value", Data: []interface{}{float64(30)}},
			},
		},
	}

	df := ToDataFrame(batches)

	if len(df) != 2 {
		t.Errorf("expected 2 columns, got %d", len(df))
	}

	timestamps, ok := df["timestamp"].([]interface{})
	if !ok {
		t.Fatal("timestamp column not found or wrong type")
	}
	if len(timestamps) != 3 {
		t.Errorf("expected 3 timestamps, got %d", len(timestamps))
	}
}

func TestToDataFrame_Empty(t *testing.T) {
	df := ToDataFrame([]ArrowRecordBatch{})
	if len(df) != 0 {
		t.Errorf("expected empty dataframe")
	}
}

func TestDefaultArrowFlightConfig(t *testing.T) {
	config := DefaultArrowFlightConfig()

	if config.MaxBatchSize <= 0 {
		t.Error("MaxBatchSize should be positive")
	}
	if config.MaxMessageSize <= 0 {
		t.Error("MaxMessageSize should be positive")
	}
	if config.BindAddr == "" {
		t.Error("BindAddr should not be empty")
	}
}

func TestArrowFlightClient_Creation(t *testing.T) {
	client := NewArrowFlightClient("localhost:8815")
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.addr != "localhost:8815" {
		t.Errorf("address mismatch")
	}
}

func TestArrowIPCMessage_Serialization(t *testing.T) {
	msg := ArrowIPCMessage{
		Header: ArrowIPCHeader{
			Version:    4,
			Type:       1,
			BodyLength: 100,
		},
		Body: []byte("test body"),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded ArrowIPCMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.Header.Version != 4 {
		t.Errorf("version mismatch")
	}
	if decoded.Header.BodyLength != 100 {
		t.Errorf("body length mismatch")
	}
}

func TestSerializeToIPC(t *testing.T) {
	batch := ArrowRecordBatch{
		Schema: ChronicleSchema(),
		Length: 0,
	}

	data, err := SerializeToIPC(batch)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	if len(data) == 0 {
		t.Error("expected non-empty IPC data")
	}

	// Check continuation bytes
	if data[0] != 0xFF || data[1] != 0xFF || data[2] != 0xFF || data[3] != 0xFF {
		t.Error("invalid IPC continuation bytes")
	}
}

func TestDeserializeFromIPC_Invalid(t *testing.T) {
	_, err := DeserializeFromIPC([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for invalid IPC data")
	}
}

func TestArrowFlightServer_ClientServer(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write test data
	_ = db.Write(Point{
		Metric:    "test_metric",
		Value:     42.0,
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"env": "test"},
	})
	_ = db.Flush()

	config := DefaultArrowFlightConfig()
	config.Enabled = true
	config.BindAddr = "127.0.0.1:18816"

	server := NewArrowFlightServer(db, config)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	client := NewArrowFlightClient(config.BindAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer client.Close()

	// Test GetFlightInfo
	desc := FlightDescriptor{
		Type: FlightDescriptorPath,
		Path: []string{"test_metric"},
	}

	info, err := client.GetFlightInfo(ctx, desc)
	if err != nil {
		t.Fatalf("GetFlightInfo failed: %v", err)
	}

	if info == nil {
		t.Fatal("expected non-nil flight info")
	}
}
