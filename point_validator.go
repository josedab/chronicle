package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sync"
	"time"
)

// PointValidatorConfig configures the point validator.
type PointValidatorConfig struct {
	Enabled          bool          `json:"enabled"`
	RejectNaN        bool          `json:"reject_nan"`
	RejectInf        bool          `json:"reject_inf"`
	MaxTagKeys       int           `json:"max_tag_keys"`
	MaxTagKeyLen     int           `json:"max_tag_key_len"`
	MaxTagValueLen   int           `json:"max_tag_value_len"`
	MaxMetricLen     int           `json:"max_metric_len"`
	MaxTimestampSkew time.Duration `json:"max_timestamp_skew"`
	ValidateFormat   bool          `json:"validate_format"`
}

// DefaultPointValidatorConfig returns sensible defaults.
func DefaultPointValidatorConfig() PointValidatorConfig {
	return PointValidatorConfig{
		Enabled:          true,
		RejectNaN:        true,
		RejectInf:        true,
		MaxTagKeys:       64,
		MaxTagKeyLen:     128,
		MaxTagValueLen:   256,
		MaxMetricLen:     256,
		MaxTimestampSkew: 24 * time.Hour,
		ValidateFormat:   true,
	}
}

// metricNameRe matches valid metric names: alphanumeric, dots, underscores,
// hyphens, forward slashes, colons. Must start with a letter or underscore.
var metricNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9._\-/:]*$`)

// tagKeyRe matches valid tag keys: alphanumeric, underscores, hyphens, dots.
var tagKeyRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9._\-]*$`)

// PointValidationError describes a validation issue.
type PointValidationError struct {
	Field    string `json:"field"`
	Message  string `json:"message"`
	Severity string `json:"severity"`
}

// PointValidatorStats holds validation statistics.
type PointValidatorStats struct {
	TotalValidated int64            `json:"total_validated"`
	TotalRejected  int64            `json:"total_rejected"`
	ErrorsByType   map[string]int64 `json:"errors_by_type"`
}

// PointValidatorEngine validates points before ingestion.
type PointValidatorEngine struct {
	db      *DB
	config  PointValidatorConfig
	mu      sync.RWMutex
	stats   PointValidatorStats
	running bool
	stopCh  chan struct{}
}

// NewPointValidatorEngine creates a new engine.
func NewPointValidatorEngine(db *DB, cfg PointValidatorConfig) *PointValidatorEngine {
	return &PointValidatorEngine{
		db:     db,
		config: cfg,
		stats: PointValidatorStats{
			ErrorsByType: make(map[string]int64),
		},
		stopCh: make(chan struct{}),
	}
}

// Start starts the engine.
func (e *PointValidatorEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop stops the engine.
func (e *PointValidatorEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	select {
	case <-e.stopCh:
	default:
		close(e.stopCh)
	}
}

// Validate checks a point and returns any validation errors.
func (e *PointValidatorEngine) Validate(p Point) []PointValidationError {
	var errors []PointValidationError

	if p.Metric == "" {
		errors = append(errors, PointValidationError{
			Field:    "metric",
			Message:  "metric name is empty",
			Severity: "error",
		})
	}

	if e.config.MaxMetricLen > 0 && len(p.Metric) > e.config.MaxMetricLen {
		errors = append(errors, PointValidationError{
			Field:    "metric",
			Message:  fmt.Sprintf("metric name exceeds max length of %d", e.config.MaxMetricLen),
			Severity: "error",
		})
	}

	if e.config.ValidateFormat && p.Metric != "" && !metricNameRe.MatchString(p.Metric) {
		errors = append(errors, PointValidationError{
			Field:    "metric",
			Message:  "metric name contains invalid characters (must match [a-zA-Z_][a-zA-Z0-9._-/:]*)",
			Severity: "error",
		})
	}

	if e.config.RejectNaN && math.IsNaN(p.Value) {
		errors = append(errors, PointValidationError{
			Field:    "value",
			Message:  "value is NaN",
			Severity: "error",
		})
	}

	if e.config.RejectInf && math.IsInf(p.Value, 0) {
		errors = append(errors, PointValidationError{
			Field:    "value",
			Message:  "value is Inf",
			Severity: "error",
		})
	}

	if e.config.MaxTagKeys > 0 && len(p.Tags) > e.config.MaxTagKeys {
		errors = append(errors, PointValidationError{
			Field:    "tags",
			Message:  fmt.Sprintf("too many tag keys: %d > %d", len(p.Tags), e.config.MaxTagKeys),
			Severity: "error",
		})
	}

	if e.config.MaxTagValueLen > 0 || e.config.MaxTagKeyLen > 0 || e.config.ValidateFormat {
		for k, v := range p.Tags {
			if k == "" {
				errors = append(errors, PointValidationError{
					Field:    "tags",
					Message:  "empty tag key",
					Severity: "error",
				})
				continue
			}
			if e.config.MaxTagKeyLen > 0 && len(k) > e.config.MaxTagKeyLen {
				errors = append(errors, PointValidationError{
					Field:    "tags." + k,
					Message:  fmt.Sprintf("tag key exceeds max length of %d", e.config.MaxTagKeyLen),
					Severity: "error",
				})
			}
			if e.config.ValidateFormat && k != "" && !tagKeyRe.MatchString(k) {
				errors = append(errors, PointValidationError{
					Field:    "tags." + k,
					Message:  "tag key contains invalid characters (must match [a-zA-Z_][a-zA-Z0-9._-]*)",
					Severity: "error",
				})
			}
			if e.config.MaxTagValueLen > 0 && len(v) > e.config.MaxTagValueLen {
				errors = append(errors, PointValidationError{
					Field:    "tags." + k,
					Message:  fmt.Sprintf("tag value exceeds max length of %d", e.config.MaxTagValueLen),
					Severity: "warning",
				})
			}
		}
	}

	if p.Timestamp <= 0 {
		errors = append(errors, PointValidationError{
			Field:    "timestamp",
			Message:  "timestamp must be a positive Unix nanosecond value",
			Severity: "warning",
		})
	} else if e.config.MaxTimestampSkew > 0 {
		now := time.Now().UnixNano()
		skew := now - p.Timestamp
		if skew < 0 {
			skew = -skew
		}
		if time.Duration(skew) > e.config.MaxTimestampSkew {
			errors = append(errors, PointValidationError{
				Field:    "timestamp",
				Message:  "timestamp skew exceeds maximum",
				Severity: "warning",
			})
		}
	}

	e.mu.Lock()
	if e.stats.ErrorsByType == nil {
		e.stats.ErrorsByType = make(map[string]int64)
	}
	e.stats.TotalValidated++
	if len(errors) > 0 {
		e.stats.TotalRejected++
		for _, ve := range errors {
			e.stats.ErrorsByType[ve.Field]++
		}
	}
	e.mu.Unlock()

	return errors
}

// GetStats returns validation statistics.
func (e *PointValidatorEngine) GetStats() PointValidatorStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cp := PointValidatorStats{
		TotalValidated: e.stats.TotalValidated,
		TotalRejected:  e.stats.TotalRejected,
		ErrorsByType:   make(map[string]int64),
	}
	for k, v := range e.stats.ErrorsByType {
		cp.ErrorsByType[k] = v
	}
	return cp
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *PointValidatorEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/validator/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
	mux.HandleFunc("/api/v1/validator/validate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var p Point
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeError(w, "bad request", http.StatusBadRequest)
			return
		}
		errors := e.Validate(p)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"valid":  len(errors) == 0,
			"errors": errors,
		})
	})
}
