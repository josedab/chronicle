package pgwire

import (
	"math"
	"testing"
	"time"
)

// mockPGDB implements PGDB for testing.
type mockPGDB struct {
	metrics []string
	points  []Point
	written []Point
}

func (m *mockPGDB) Execute(metric string, start, end int64, tags map[string]string, limit int) (*QueryResult, error) {
	var pts []Point
	for _, p := range m.points {
		if p.Metric == metric {
			pts = append(pts, p)
		}
	}
	if limit > 0 && len(pts) > limit {
		pts = pts[:limit]
	}
	return &QueryResult{Points: pts}, nil
}
func (m *mockPGDB) Write(p Point) error {
	m.written = append(m.written, p)
	return nil
}
func (m *mockPGDB) Metrics() []string { return m.metrics }

func TestDefaultPGWireConfig(t *testing.T) {
	cfg := DefaultPGWireConfig()
	if cfg.Address != ":5432" {
		t.Errorf("expected :5432, got %s", cfg.Address)
	}
	if cfg.DatabaseName != "chronicle" {
		t.Errorf("expected chronicle, got %s", cfg.DatabaseName)
	}
	if cfg.MaxConnections != 100 {
		t.Errorf("expected 100, got %d", cfg.MaxConnections)
	}
	if cfg.QueryTimeout != 30*time.Second {
		t.Errorf("expected 30s, got %v", cfg.QueryTimeout)
	}
}

func TestNewPGServer(t *testing.T) {
	db := &mockPGDB{}
	srv, err := NewPGServer(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
}

func TestPGServerStats(t *testing.T) {
	db := &mockPGDB{}
	srv, _ := NewPGServer(db, nil)
	stats := srv.Stats()
	if stats.TotalConnections != 0 || stats.ActiveConnections != 0 {
		t.Error("expected zero stats for new server")
	}
}

func TestPGQueryTranslator_Select(t *testing.T) {
	db := &mockPGDB{
		points: []Point{
			{Metric: "cpu", Value: 42.0, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"host": "a"}},
		},
	}
	translator := &PGQueryTranslator{db: db}
	result, err := translator.Execute("SELECT * FROM cpu")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("expected 1 row, got %d", result.RowCount)
	}
}

func TestPGQueryTranslator_SelectWithLimit(t *testing.T) {
	db := &mockPGDB{
		points: []Point{
			{Metric: "cpu", Value: 1},
			{Metric: "cpu", Value: 2},
			{Metric: "cpu", Value: 3},
		},
	}
	translator := &PGQueryTranslator{db: db}
	result, err := translator.Execute("SELECT * FROM cpu LIMIT 2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount != 2 {
		t.Errorf("expected 2 rows, got %d", result.RowCount)
	}
}

func TestPGQueryTranslator_Insert(t *testing.T) {
	db := &mockPGDB{}
	translator := &PGQueryTranslator{db: db}
	result, err := translator.Execute(`INSERT INTO cpu (value, host) VALUES (42.5, 'server1')`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("expected RowCount 1, got %d", result.RowCount)
	}
	if len(db.written) != 1 {
		t.Fatalf("expected 1 written point, got %d", len(db.written))
	}
	if db.written[0].Tags["host"] != "server1" {
		t.Errorf("expected host=server1, got %s", db.written[0].Tags["host"])
	}
}

func TestPGQueryTranslator_Unsupported(t *testing.T) {
	db := &mockPGDB{}
	translator := &PGQueryTranslator{db: db}
	_, err := translator.Execute("DELETE FROM cpu")
	if err == nil {
		t.Error("expected error for unsupported SQL")
	}
}

func TestPGQueryTranslator_SelectFromMetric(t *testing.T) {
	db := &mockPGDB{
		metrics: []string{"cpu", "mem"},
		points: []Point{
			{Metric: "cpu", Value: 1, Timestamp: 100, Tags: map[string]string{"host": "a"}},
		},
	}
	translator := &PGQueryTranslator{db: db}
	result, err := translator.Execute("SELECT value, host FROM cpu")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("expected 1 row, got %d", result.RowCount)
	}
	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"SELECT 1", 1},
		{"SELECT 1; SELECT 2", 2},
		{"SELECT 'a;b'", 1},
		{"", 0},
	}
	for _, tt := range tests {
		got := splitStatements(tt.input)
		if len(got) != tt.want {
			t.Errorf("splitStatements(%q) = %d stmts, want %d", tt.input, len(got), tt.want)
		}
	}
}

func TestSplitTrimmed(t *testing.T) {
	result := splitTrimmed("a, b, c", ",")
	if len(result) != 3 {
		t.Errorf("expected 3 parts, got %d", len(result))
	}
	if result[0] != "a" || result[1] != "b" || result[2] != "c" {
		t.Errorf("unexpected result: %v", result)
	}
}

func TestParseColumnList(t *testing.T) {
	star := parseColumnList("*")
	if len(star) != 4 {
		t.Errorf("expected 4 columns for *, got %d", len(star))
	}
	custom := parseColumnList("value, timestamp")
	if len(custom) != 2 {
		t.Errorf("expected 2 columns, got %d", len(custom))
	}
}

func TestBuildPGColumns(t *testing.T) {
	cols := buildPGColumns([]string{"timestamp", "value", "metric"})
	if len(cols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(cols))
	}
	if cols[0].TypeOID != PGTypeTimestamp {
		t.Errorf("expected timestamp type for first col")
	}
	if cols[1].TypeOID != PGTypeFloat8 {
		t.Errorf("expected float8 type for value col")
	}
	if cols[2].TypeOID != PGTypeText {
		t.Errorf("expected text type for metric col")
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input any
		want  string
	}{
		{nil, ""},
		{42.5, "42.5"},
		{int64(100), "100"},
		{math.NaN(), "NaN"},
		{"hello", "hello"},
	}
	for _, tt := range tests {
		got := formatValue(tt.input)
		if got != tt.want {
			t.Errorf("formatValue(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseWhereClause(t *testing.T) {
	start, end, tags := parseWhereClause("host = server1", 0, 1000)
	if start != 0 || end != 1000 {
		t.Errorf("unexpected time range: %d-%d", start, end)
	}
	if tags == nil || tags["host"] != "server1" {
		t.Errorf("expected host=server1, got %v", tags)
	}
}

func TestExtractTimestamp(t *testing.T) {
	ts := extractTimestamp("timestamp > 1700000000")
	if ts != 1700000000000000000 {
		t.Errorf("expected seconds-to-nanos conversion, got %d", ts)
	}

	tsNano := extractTimestamp("time > 1700000000000000000")
	if tsNano != 1700000000000000000 {
		t.Errorf("expected nano passthrough, got %d", tsNano)
	}

	if extractTimestamp("no number here") != 0 {
		t.Error("expected 0 for no timestamp")
	}
}
