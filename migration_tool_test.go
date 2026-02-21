package chronicle

import (
	"strings"
	"testing"
)

func TestImportEngine(t *testing.T) {
	db := setupTestDB(t)

	t.Run("import influx line protocol", func(t *testing.T) {
		e := NewImportEngine(db, DefaultImportConfig())
		input := `cpu,host=server01,region=us-east usage=42.5 1000000000
cpu,host=server02,region=eu-west usage=55.1 1000000001
memory,host=server01 used=8192 1000000002
# comment line

disk,host=server01 free=500.0i 1000000003`

		result, err := e.ImportInfluxLineProtocol(strings.NewReader(input))
		if err != nil { t.Fatal(err) }
		if result.PointsRead != 4 { t.Errorf("expected 4 read, got %d", result.PointsRead) }
		if result.PointsWritten != 4 { t.Errorf("expected 4 written, got %d", result.PointsWritten) }
		if len(result.Metrics) < 3 { t.Errorf("expected 3+ metrics, got %d", len(result.Metrics)) }
	})

	t.Run("import csv", func(t *testing.T) {
		e := NewImportEngine(db, DefaultImportConfig())
		input := `metric,value,timestamp
temperature,22.5,1000000000,room=kitchen
temperature,23.1,1000000001,room=living
humidity,65.0,1000000002,room=kitchen`

		result, err := e.ImportCSV(strings.NewReader(input))
		if err != nil { t.Fatal(err) }
		if result.PointsRead != 3 { t.Errorf("expected 3 read, got %d", result.PointsRead) }
		if result.PointsWritten != 3 { t.Errorf("expected 3 written, got %d", result.PointsWritten) }
	})

	t.Run("dry run", func(t *testing.T) {
		cfg := DefaultImportConfig()
		cfg.DryRun = true
		e := NewImportEngine(db, cfg)
		input := "cpu,host=a value=1 1000\ncpu,host=b value=2 2000\n"
		result, _ := e.ImportInfluxLineProtocol(strings.NewReader(input))
		if !result.DryRun { t.Error("expected dry run") }
		if result.PointsWritten != 2 { t.Errorf("expected 2 counted, got %d", result.PointsWritten) }
	})

	t.Run("invalid lines skipped", func(t *testing.T) {
		e := NewImportEngine(db, DefaultImportConfig())
		input := "bad line\ncpu value=1 1000\nalso bad\n"
		result, _ := e.ImportInfluxLineProtocol(strings.NewReader(input))
		if result.PointsSkipped < 2 { t.Errorf("expected 2+ skipped, got %d", result.PointsSkipped) }
	})

	t.Run("csv with missing fields skipped", func(t *testing.T) {
		e := NewImportEngine(db, DefaultImportConfig())
		input := "metric,value\nonly,two\ncpu,42,1000\n"
		result, _ := e.ImportCSV(strings.NewReader(input))
		if result.PointsSkipped < 1 { t.Error("expected skipped points") }
	})

	t.Run("parse influx line", func(t *testing.T) {
		p, err := parseInfluxLine("cpu,host=a,region=us value=42.5 1000000000")
		if err != nil { t.Fatal(err) }
		if p.Metric != "cpu" { t.Error("wrong metric") }
		if p.Tags["host"] != "a" { t.Error("wrong host tag") }
		if p.Value != 42.5 { t.Error("wrong value") }
		if p.Timestamp != 1000000000 { t.Error("wrong timestamp") }
	})

	t.Run("parse influx line invalid", func(t *testing.T) {
		_, err := parseInfluxLine("x")
		if err == nil { t.Error("expected error for invalid line") }
	})

	t.Run("stats tracking", func(t *testing.T) {
		e := NewImportEngine(db, DefaultImportConfig())
		e.ImportInfluxLineProtocol(strings.NewReader("cpu value=1 1000\n"))
		stats := e.GetStats()
		if stats.TotalMigrations != 1 { t.Error("expected 1 migration") }
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewImportEngine(db, DefaultImportConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})
}
