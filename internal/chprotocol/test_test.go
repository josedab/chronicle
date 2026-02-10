package chprotocol

import (
	"context"
	"testing"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

func TestNewCHNativeServer(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultClickHouseProtocolConfig()
	server, err := NewCHNativeServer(db, config)
	if err != nil {
		t.Fatalf("NewCHNativeServer() error = %v", err)
	}

	if server.db != db {
		t.Error("db reference not set correctly")
	}
	if server.config.Address != ":9000" {
		t.Errorf("Address = %s, want :9000", server.config.Address)
	}
}

func TestClickHouseProtocolConfig(t *testing.T) {
	config := DefaultClickHouseProtocolConfig()

	if config.Address != ":9000" {
		t.Errorf("Address = %s, want :9000", config.Address)
	}
	if config.DatabaseName != "chronicle" {
		t.Errorf("DatabaseName = %s, want chronicle", config.DatabaseName)
	}
	if config.MaxConnections != 100 {
		t.Errorf("MaxConnections = %d, want 100", config.MaxConnections)
	}
}

func TestCHQueryTranslatorSelectOne(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	translator := &CHQueryTranslator{db: db}
	ctx := context.Background()

	result, err := translator.Execute(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", result.RowCount)
	}
	if len(result.Columns) != 1 {
		t.Errorf("Columns count = %d, want 1", len(result.Columns))
	}
	if result.Rows[0][0] != uint8(1) {
		t.Errorf("Value = %v, want 1", result.Rows[0][0])
	}
}

func TestCHQueryTranslatorShowDatabases(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	translator := &CHQueryTranslator{db: db}
	ctx := context.Background()

	result, err := translator.Execute(ctx, "SHOW DATABASES")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if result.RowCount < 1 {
		t.Error("Expected at least one database")
	}
	if result.Columns[0].Name != "name" {
		t.Errorf("Column name = %s, want name", result.Columns[0].Name)
	}
}

func TestCHQueryTranslatorShowTables(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Write some data to create series
	db.Write(chronicle.Point{Metric: "cpu_usage", Timestamp: 1000000000, Value: 50.0})
	db.Write(chronicle.Point{Metric: "memory_usage", Timestamp: 1000000000, Value: 70.0})
	db.Flush()

	translator := &CHQueryTranslator{db: db}
	ctx := context.Background()

	result, err := translator.Execute(ctx, "SHOW TABLES")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if result.RowCount < 2 {
		t.Errorf("Expected at least 2 tables, got %d", result.RowCount)
	}
}

func TestCHQueryTranslatorDescribe(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	translator := &CHQueryTranslator{db: db}
	ctx := context.Background()

	result, err := translator.Execute(ctx, "DESCRIBE TABLE cpu_usage")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if result.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3 (timestamp, value, series)", result.RowCount)
	}

	// Check column structure
	foundTimestamp := false
	foundValue := false
	for _, row := range result.Rows {
		if row[0] == "timestamp" {
			foundTimestamp = true
		}
		if row[0] == "value" {
			foundValue = true
		}
	}

	if !foundTimestamp {
		t.Error("Expected timestamp column in describe")
	}
	if !foundValue {
		t.Error("Expected value column in describe")
	}
}

func TestCHQueryTranslatorSelect(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Write test data
	now := time.Now().UnixNano()
	db.Write(chronicle.Point{Metric: "test_metric", Timestamp: now - 1000000000, Value: 10.0})
	db.Write(chronicle.Point{Metric: "test_metric", Timestamp: now - 500000000, Value: 20.0})
	db.Write(chronicle.Point{Metric: "test_metric", Timestamp: now, Value: 30.0})
	db.Flush()

	translator := &CHQueryTranslator{db: db}
	ctx := context.Background()

	result, err := translator.Execute(ctx, "SELECT * FROM test_metric LIMIT 10")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if result.RowCount < 1 {
		t.Error("Expected at least one row")
	}
}

func TestCHQueryTranslatorSelectAggregate(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Write test data
	now := time.Now().UnixNano()
	db.Write(chronicle.Point{Metric: "agg_metric", Timestamp: now - 1000000000, Value: 10.0})
	db.Write(chronicle.Point{Metric: "agg_metric", Timestamp: now - 500000000, Value: 20.0})
	db.Write(chronicle.Point{Metric: "agg_metric", Timestamp: now, Value: 30.0})
	db.Flush()

	translator := &CHQueryTranslator{db: db}
	ctx := context.Background()

	// Test SUM
	result, err := translator.Execute(ctx, "SELECT sum(value) FROM agg_metric")
	if err != nil {
		t.Fatalf("Execute(sum) error = %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", result.RowCount)
	}

	// Test AVG
	result, err = translator.Execute(ctx, "SELECT avg(value) FROM agg_metric")
	if err != nil {
		t.Fatalf("Execute(avg) error = %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", result.RowCount)
	}

	// Test COUNT
	result, err = translator.Execute(ctx, "SELECT count(value) FROM agg_metric")
	if err != nil {
		t.Fatalf("Execute(count) error = %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", result.RowCount)
	}
}

func TestCHQueryTranslatorInsert(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	translator := &CHQueryTranslator{db: db}
	ctx := context.Background()

	_, err2 := translator.Execute(ctx, "INSERT INTO insert_test VALUES (1000, 42.5)")
	if err2 != nil {
		t.Fatalf("Execute(INSERT) error = %v", err2)
	}
	db.Flush()

	// Verify data was written using chronicle.DB directly (INSERT timestamp is epoch-relative
	// and falls outside the CH translator's default 1-hour query window)
	result, err := db.Execute(&chronicle.Query{
		Metric: "insert_test",
		Start:  0,
		End:    time.Now().UnixNano() + int64(time.Hour),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if len(result.Points) < 1 {
		t.Error("Expected inserted data to be queryable")
	}
}

func TestParseSelect(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	translator := &CHQueryTranslator{db: db}

	tests := []struct {
		query       string
		expectTable string
		expectLimit int
	}{
		{
			query:       "SELECT * FROM metrics LIMIT 100",
			expectTable: "metrics",
			expectLimit: 100,
		},
		{
			query:       "SELECT timestamp, value FROM cpu_usage",
			expectTable: "cpu_usage",
			expectLimit: 1000,
		},
		{
			query:       "SELECT sum(value) FROM memory GROUP BY host",
			expectTable: "memory",
			expectLimit: 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			parsed, err := translator.parseSelect(tt.query)
			if err != nil {
				t.Fatalf("parseSelect() error = %v", err)
			}
			if parsed.table != tt.expectTable {
				t.Errorf("table = %s, want %s", parsed.table, tt.expectTable)
			}
			if parsed.limit != tt.expectLimit {
				t.Errorf("limit = %d, want %d", parsed.limit, tt.expectLimit)
			}
		})
	}
}

func TestParseSelectWithTime(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	translator := &CHQueryTranslator{db: db}

	query := "SELECT * FROM metrics WHERE timestamp >= 1000 AND timestamp <= 2000"
	parsed, err := translator.parseSelect(query)
	if err != nil {
		t.Fatalf("parseSelect() error = %v", err)
	}

	if parsed.startTime != 1000 {
		t.Errorf("startTime = %d, want 1000", parsed.startTime)
	}
	if parsed.endTime != 2000 {
		t.Errorf("endTime = %d, want 2000", parsed.endTime)
	}
}

func TestTypeConversions(t *testing.T) {
	// Test toUint64
	tests := []struct {
		input    any
		expected uint64
	}{
		{uint64(100), 100},
		{uint32(200), 200},
		{int64(300), 300},
		{int(400), 400},
		{float64(500.5), 500},
	}

	for _, tt := range tests {
		result, err := toUint64(tt.input)
		if err != nil {
			t.Errorf("toUint64(%v) error = %v", tt.input, err)
		}
		if result != tt.expected {
			t.Errorf("toUint64(%v) = %d, want %d", tt.input, result, tt.expected)
		}
	}

	// Test toInt64
	i64Tests := []struct {
		input    any
		expected int64
	}{
		{int64(-100), -100},
		{int32(-200), -200},
		{int(-300), -300},
		{float64(-400.5), -400},
	}

	for _, tt := range i64Tests {
		result, err := toInt64(tt.input)
		if err != nil {
			t.Errorf("toInt64(%v) error = %v", tt.input, err)
		}
		if result != tt.expected {
			t.Errorf("toInt64(%v) = %d, want %d", tt.input, result, tt.expected)
		}
	}

	// Test toFloat64
	f64Tests := []struct {
		input    any
		expected float64
	}{
		{float64(1.5), 1.5},
		{float32(2.5), 2.5},
		{int64(100), 100.0},
		{int(200), 200.0},
	}

	for _, tt := range f64Tests {
		result, err := toFloat64(tt.input)
		if err != nil {
			t.Errorf("toFloat64(%v) error = %v", tt.input, err)
		}
		if result != tt.expected {
			t.Errorf("toFloat64(%v) = %f, want %f", tt.input, result, tt.expected)
		}
	}
}

func TestCHServerStats(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultClickHouseProtocolConfig()
	server, _ := NewCHNativeServer(db, config)

	stats := server.Stats()
	if stats.TotalConnections != 0 {
		t.Errorf("TotalConnections = %d, want 0", stats.TotalConnections)
	}
	if stats.ActiveConnections != 0 {
		t.Errorf("ActiveConnections = %d, want 0", stats.ActiveConnections)
	}
}

func TestVarUIntEncoding(t *testing.T) {
	// Test VarUInt encoding
	tests := []struct {
		value    uint64
		expected int // expected byte length
	}{
		{0, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
	}

	for _, tt := range tests {
		var buf []byte
		x := tt.value
		for {
			if x < 0x80 {
				buf = append(buf, byte(x))
				break
			}
			buf = append(buf, byte(x)|0x80)
			x >>= 7
		}

		if len(buf) != tt.expected {
			t.Errorf("VarUInt(%d) length = %d, want %d", tt.value, len(buf), tt.expected)
		}
	}
}

func TestSystemQueries(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	translator := &CHQueryTranslator{db: db}
	ctx := context.Background()

	// Test system.databases query
	result, err := translator.Execute(ctx, "SELECT * FROM system.databases")
	if err != nil {
		t.Fatalf("system.databases query error = %v", err)
	}
	if result.RowCount < 1 {
		t.Error("Expected at least one database")
	}

	// Test system.tables query
	result, err = translator.Execute(ctx, "SELECT * FROM system.tables")
	if err != nil {
		t.Fatalf("system.tables query error = %v", err)
	}

	// Test system.columns query
	result, err = translator.Execute(ctx, "SELECT * FROM system.columns WHERE table = 'test'")
	if err != nil {
		t.Fatalf("system.columns query error = %v", err)
	}
	if result.RowCount != 3 {
		t.Errorf("Expected 3 columns, got %d", result.RowCount)
	}
}

func TestUnsupportedQuery(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	translator := &CHQueryTranslator{db: db}
	ctx := context.Background()

	_, err2 := translator.Execute(ctx, "DROP TABLE test")
	if err2 == nil {
		t.Error("Expected error for unsupported query")
	}
}

func TestSelectWithOrderBy(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	translator := &CHQueryTranslator{db: db}

	query := "SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 10"
	parsed, err := translator.parseSelect(query)
	if err != nil {
		t.Fatalf("parseSelect() error = %v", err)
	}

	if parsed.orderBy != "timestamp" {
		t.Errorf("orderBy = %s, want timestamp", parsed.orderBy)
	}
	if !parsed.orderDesc {
		t.Error("orderDesc should be true")
	}
}
