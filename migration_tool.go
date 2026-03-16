package chronicle

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ImportConfig configures the migration tool.
type ImportConfig struct {
	Enabled       bool
	BatchSize     int
	MaxErrors     int
	SkipInvalid   bool
	DryRun        bool
}

// DefaultImportConfig returns sensible defaults.
func DefaultImportConfig() ImportConfig {
	return ImportConfig{
		Enabled:   true,
		BatchSize: 10000,
		MaxErrors: 100,
		SkipInvalid: true,
		DryRun:    false,
	}
}

// ImportResult captures the outcome of a migration.
type ImportResult struct {
	Source        string        `json:"source"`
	Format       string        `json:"format"`
	PointsRead   int64         `json:"points_read"`
	PointsWritten int64        `json:"points_written"`
	PointsSkipped int64        `json:"points_skipped"`
	Errors       []string      `json:"errors,omitempty"`
	Duration     time.Duration `json:"duration"`
	Metrics      []string      `json:"metrics_imported"`
	DryRun       bool          `json:"dry_run"`
}

// ImportStats holds engine statistics.
type ImportStats struct {
	TotalMigrations int64 `json:"total_migrations"`
	TotalPointsIn   int64 `json:"total_points_imported"`
	TotalErrors     int64 `json:"total_errors"`
}

// ImportEngine handles importing data from various formats.
type ImportEngine struct {
	db     *DB
	config ImportConfig
	mu     sync.RWMutex
	running bool
	stopCh  chan struct{}
	stats   ImportStats
}

// NewImportEngine creates a new migration engine.
func NewImportEngine(db *DB, cfg ImportConfig) *ImportEngine {
	return &ImportEngine{db: db, config: cfg, stopCh: make(chan struct{})}
}

func (e *ImportEngine) Start() {
	e.mu.Lock(); if e.running { e.mu.Unlock(); return }; e.running = true; e.mu.Unlock()
}

func (e *ImportEngine) Stop() {
	e.mu.Lock(); defer e.mu.Unlock(); if !e.running { return }; e.running = false; select { case <-e.stopCh: default: close(e.stopCh) }
}

// ImportInfluxLineProtocol imports data in InfluxDB line protocol format.
// Format: <measurement>,<tag_key>=<tag_value> <field_key>=<field_value> <timestamp>
func (e *ImportEngine) ImportInfluxLineProtocol(reader io.Reader) (*ImportResult, error) {
	result := &ImportResult{Source: "influx_line_protocol", Format: "influx", DryRun: e.config.DryRun}
	start := time.Now()
	metricsSet := make(map[string]bool)

	scanner := bufio.NewScanner(reader)
	var batch []Point

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		p, err := parseInfluxLine(line)
		if err != nil {
			result.PointsSkipped++
			if len(result.Errors) < e.config.MaxErrors {
				result.Errors = append(result.Errors, err.Error())
			}
			if !e.config.SkipInvalid {
				return result, err
			}
			continue
		}

		result.PointsRead++
		metricsSet[p.Metric] = true
		batch = append(batch, p)

		if len(batch) >= e.config.BatchSize {
			if !e.config.DryRun {
				if err := e.db.WriteBatch(batch); err != nil {
					result.Errors = append(result.Errors, err.Error())
				} else {
					result.PointsWritten += int64(len(batch))
				}
			} else {
				result.PointsWritten += int64(len(batch))
			}
			batch = batch[:0]
		}
	}

	// Flush remaining
	if len(batch) > 0 && !e.config.DryRun {
		if err := e.db.WriteBatch(batch); err != nil {
			result.Errors = append(result.Errors, err.Error())
		} else {
			result.PointsWritten += int64(len(batch))
		}
	} else if len(batch) > 0 {
		result.PointsWritten += int64(len(batch))
	}

	for m := range metricsSet {
		result.Metrics = append(result.Metrics, m)
	}
	result.Duration = time.Since(start)

	e.mu.Lock()
	e.stats.TotalMigrations++
	e.stats.TotalPointsIn += result.PointsWritten
	e.mu.Unlock()

	return result, nil
}

// ImportCSV imports data from CSV format.
// Expected columns: metric,value,timestamp[,tag_key=tag_value,...]
func (e *ImportEngine) ImportCSV(reader io.Reader) (*ImportResult, error) {
	result := &ImportResult{Source: "csv", Format: "csv", DryRun: e.config.DryRun}
	start := time.Now()
	metricsSet := make(map[string]bool)

	scanner := bufio.NewScanner(reader)
	var batch []Point
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "metric,") {
			continue // skip header
		}

		parts := strings.Split(line, ",")
		if len(parts) < 3 {
			result.PointsSkipped++
			continue
		}

		value, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if err != nil {
			result.PointsSkipped++
			continue
		}

		ts, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 64)
		if err != nil {
			result.PointsSkipped++
			continue
		}

		tags := make(map[string]string)
		for _, tagPair := range parts[3:] {
			kv := strings.SplitN(strings.TrimSpace(tagPair), "=", 2)
			if len(kv) == 2 {
				tags[kv[0]] = kv[1]
			}
		}

		p := Point{Metric: strings.TrimSpace(parts[0]), Value: value, Timestamp: ts, Tags: tags}
		result.PointsRead++
		metricsSet[p.Metric] = true
		batch = append(batch, p)

		if len(batch) >= e.config.BatchSize {
			if !e.config.DryRun {
				if err := e.db.WriteBatch(batch); err != nil {
					result.Errors = append(result.Errors, err.Error())
				} else {
					result.PointsWritten += int64(len(batch))
				}
			} else {
				result.PointsWritten += int64(len(batch))
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if !e.config.DryRun {
			if err := e.db.WriteBatch(batch); err != nil {
				result.Errors = append(result.Errors, err.Error())
			} else {
				result.PointsWritten += int64(len(batch))
			}
		} else {
			result.PointsWritten += int64(len(batch))
		}
	}

	for m := range metricsSet {
		result.Metrics = append(result.Metrics, m)
	}
	result.Duration = time.Since(start)

	e.mu.Lock()
	e.stats.TotalMigrations++
	e.stats.TotalPointsIn += result.PointsWritten
	e.mu.Unlock()

	return result, nil
}

// parseInfluxLine parses a single InfluxDB line protocol line.
func parseInfluxLine(line string) (Point, error) {
	// Format: measurement,tag1=val1,tag2=val2 field1=val1 timestamp
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 2 {
		return Point{}, fmt.Errorf("invalid line protocol: %q", line)
	}

	// Parse measurement and tags
	measurementAndTags := strings.Split(parts[0], ",")
	metric := measurementAndTags[0]
	tags := make(map[string]string)
	for _, tag := range measurementAndTags[1:] {
		kv := strings.SplitN(tag, "=", 2)
		if len(kv) == 2 {
			tags[kv[0]] = kv[1]
		}
	}

	// Parse field value (take first field)
	fields := strings.Split(parts[1], ",")
	var value float64
	fieldParsed := false
	for _, field := range fields {
		kv := strings.SplitN(field, "=", 2)
		if len(kv) == 2 {
			v := strings.TrimSuffix(kv[1], "i") // InfluxDB integer suffix
			var err error
			value, err = strconv.ParseFloat(v, 64)
			if err == nil {
				fieldParsed = true
				break
			}
		}
	}
	if !fieldParsed {
		return Point{}, fmt.Errorf("no valid field in line: %q", line)
	}

	// Parse timestamp
	var ts int64
	if len(parts) == 3 {
		var err error
		ts, err = strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 64)
		if err != nil {
			ts = time.Now().UnixNano()
		}
	} else {
		ts = time.Now().UnixNano()
	}

	return Point{Metric: metric, Value: value, Tags: tags, Timestamp: ts}, nil
}

// GetStats returns migration stats.
func (e *ImportEngine) GetStats() ImportStats {
	e.mu.RLock(); defer e.mu.RUnlock(); return e.stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *ImportEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/migrate/influx", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		result, err := e.ImportInfluxLineProtocol(r.Body)
		if err != nil { http.Error(w, "bad request", http.StatusBadRequest); return }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("/api/v1/migrate/csv", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		result, err := e.ImportCSV(r.Body)
		if err != nil { http.Error(w, "bad request", http.StatusBadRequest); return }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("/api/v1/migrate/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
