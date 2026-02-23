package chronicle

import (
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// StreamingSQLV2Config configures the SQL:2016 streaming engine with windowed
// operations, stream-to-stream joins, watermark-based late data handling, and
// exactly-once processing semantics.
type StreamingSQLV2Config struct {
	Enabled              bool          `json:"enabled"`
	MaxWatermarkLag      time.Duration `json:"max_watermark_lag"`
	LateDataGracePeriod  time.Duration `json:"late_data_grace_period"`
	ExactlyOnceEnabled   bool          `json:"exactly_once_enabled"`
	CheckpointInterval   time.Duration `json:"checkpoint_interval"`
	CheckpointDir        string        `json:"checkpoint_dir"`
	MaxWindowsPerQuery   int           `json:"max_windows_per_query"`
	WindowRetention      time.Duration `json:"window_retention"`
	JoinBufferSize       int           `json:"join_buffer_size"`
	JoinTimeout          time.Duration `json:"join_timeout"`
	MaxConcurrentQueries int           `json:"max_concurrent_queries"`
}

// DefaultStreamingSQLV2Config returns sensible defaults.
func DefaultStreamingSQLV2Config() StreamingSQLV2Config {
	return StreamingSQLV2Config{
		Enabled:              true,
		MaxWatermarkLag:      5 * time.Second,
		LateDataGracePeriod:  10 * time.Second,
		ExactlyOnceEnabled:   true,
		CheckpointInterval:   30 * time.Second,
		CheckpointDir:        "/tmp/chronicle-checkpoints",
		MaxWindowsPerQuery:   10000,
		WindowRetention:      time.Hour,
		JoinBufferSize:       50000,
		JoinTimeout:          time.Minute,
		MaxConcurrentQueries: 100,
	}
}

// ---------------------------------------------------------------------------
// Watermark – tracks event-time progress per query
// ---------------------------------------------------------------------------

// StreamWatermark tracks the progress of event time within a streaming query,
// allowing the engine to know when a window can be closed and results emitted.
type StreamWatermark struct {
	mu                sync.Mutex
	currentWatermark  int64 // nanosecond epoch
	maxOutOfOrderness time.Duration
	gracePeriod       time.Duration
	lastAdvance       time.Time
}

// NewStreamWatermark creates a watermark tracker.
func NewStreamWatermark(maxOutOfOrderness, gracePeriod time.Duration) *StreamWatermark {
	return &StreamWatermark{
		maxOutOfOrderness: maxOutOfOrderness,
		gracePeriod:       gracePeriod,
		lastAdvance:       time.Now(),
	}
}

// Advance moves the watermark forward based on the observed event time.
func (w *StreamWatermark) Advance(eventTime int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	candidate := eventTime - w.maxOutOfOrderness.Nanoseconds()
	if candidate > w.currentWatermark {
		w.currentWatermark = candidate
		w.lastAdvance = time.Now()
	}
}

// IsLate returns true when the event arrived after the watermark plus the
// configured grace period.
func (w *StreamWatermark) IsLate(eventTime int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	deadline := w.currentWatermark - w.gracePeriod.Nanoseconds()
	return eventTime < deadline
}

// GetWatermark returns the current watermark value.
func (w *StreamWatermark) GetWatermark() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentWatermark
}
