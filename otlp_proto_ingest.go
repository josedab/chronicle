package chronicle

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// OTLPProtoConfig configures the OTLP proto ingestion engine.
type OTLPProtoConfig struct {
	Enabled                     bool `json:"enabled"`
	MaxBatchSize                int  `json:"max_batch_size"`
	EnableHistograms            bool `json:"enable_histograms"`
	EnableSummaries             bool `json:"enable_summaries"`
	EnableExponentialHistograms bool `json:"enable_exponential_histograms"`
}

// DefaultOTLPProtoConfig returns sensible defaults.
func DefaultOTLPProtoConfig() OTLPProtoConfig {
	return OTLPProtoConfig{
		Enabled:                     true,
		MaxBatchSize:                10000,
		EnableHistograms:            true,
		EnableSummaries:             true,
		EnableExponentialHistograms: false,
	}
}

// ProtoMetricPoint represents a metric data point in proto format.
type ProtoMetricPoint struct {
	Name      string            `json:"name"`
	Value     float64           `json:"value"`
	Timestamp int64             `json:"timestamp"`
	Labels    map[string]string `json:"labels"`
	Type      string            `json:"type"` // gauge, sum, histogram, summary
}

// ProtoHistogramPoint represents a histogram data point.
type ProtoHistogramPoint struct {
	Name      string            `json:"name"`
	Buckets   []ProtoBucket     `json:"buckets"`
	Sum       float64           `json:"sum"`
	Count     uint64            `json:"count"`
	Timestamp int64             `json:"timestamp"`
	Labels    map[string]string `json:"labels"`
}

// ProtoBucket represents a single histogram bucket.
type ProtoBucket struct {
	UpperBound float64 `json:"upper_bound"`
	Count      uint64  `json:"count"`
}

// ProtoIngestResult reports the outcome of an ingestion call.
type ProtoIngestResult struct {
	PointsAccepted      int           `json:"points_accepted"`
	PointsRejected      int           `json:"points_rejected"`
	HistogramsProcessed  int           `json:"histograms_processed"`
	SummariesProcessed   int           `json:"summaries_processed"`
	Duration             time.Duration `json:"duration"`
}

// OTLPProtoStats holds engine statistics.
type OTLPProtoStats struct {
	TotalIngested       int64 `json:"total_ingested"`
	TotalRejected       int64 `json:"total_rejected"`
	HistogramsProcessed int64 `json:"histograms_processed"`
	SummariesProcessed  int64 `json:"summaries_processed"`
}

// OTLPProtoEngine handles OTLP proto metric ingestion.
type OTLPProtoEngine struct {
	db      *DB
	config  OTLPProtoConfig
	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}
	stats   OTLPProtoStats
}

// NewOTLPProtoEngine creates a new OTLP proto ingestion engine.
func NewOTLPProtoEngine(db *DB, cfg OTLPProtoConfig) *OTLPProtoEngine {
	return &OTLPProtoEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

func (e *OTLPProtoEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

func (e *OTLPProtoEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// IngestMetrics processes a batch of proto metric points.
func (e *OTLPProtoEngine) IngestMetrics(points []ProtoMetricPoint) (*ProtoIngestResult, error) {
	start := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()

	result := &ProtoIngestResult{}

	for _, p := range points {
		if result.PointsAccepted+result.PointsRejected >= e.config.MaxBatchSize {
			result.PointsRejected += len(points) - (result.PointsAccepted + result.PointsRejected)
			break
		}
		if p.Name == "" {
			result.PointsRejected++
			e.stats.TotalRejected++
			continue
		}
		if p.Type == "summary" && !e.config.EnableSummaries {
			result.PointsRejected++
			e.stats.TotalRejected++
			continue
		}
		if p.Type == "histogram" && !e.config.EnableHistograms {
			result.PointsRejected++
			e.stats.TotalRejected++
			continue
		}

		result.PointsAccepted++
		e.stats.TotalIngested++

		if p.Type == "histogram" {
			result.HistogramsProcessed++
			e.stats.HistogramsProcessed++
		}
		if p.Type == "summary" {
			result.SummariesProcessed++
			e.stats.SummariesProcessed++
		}
	}

	result.Duration = time.Since(start)
	return result, nil
}

// IngestHistograms processes a batch of proto histogram points.
func (e *OTLPProtoEngine) IngestHistograms(histograms []ProtoHistogramPoint) (*ProtoIngestResult, error) {
	start := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.config.EnableHistograms {
		return &ProtoIngestResult{
			PointsRejected: len(histograms),
			Duration:       time.Since(start),
		}, nil
	}

	result := &ProtoIngestResult{}

	for _, h := range histograms {
		if result.PointsAccepted+result.PointsRejected >= e.config.MaxBatchSize {
			result.PointsRejected += len(histograms) - (result.PointsAccepted + result.PointsRejected)
			break
		}
		if h.Name == "" {
			result.PointsRejected++
			e.stats.TotalRejected++
			continue
		}
		result.PointsAccepted++
		result.HistogramsProcessed++
		e.stats.TotalIngested++
		e.stats.HistogramsProcessed++
	}

	result.Duration = time.Since(start)
	return result, nil
}

// ConvertToPoints converts proto metric points to Chronicle Points.
func (e *OTLPProtoEngine) ConvertToPoints(proto []ProtoMetricPoint) []Point {
	e.mu.RLock()
	defer e.mu.RUnlock()

	points := make([]Point, 0, len(proto))
	for _, p := range proto {
		if p.Name == "" {
			continue
		}
		tags := make(map[string]string, len(p.Labels)+1)
		for k, v := range p.Labels {
			tags[k] = v
		}
		tags["__type__"] = p.Type

		points = append(points, Point{
			Metric:    p.Name,
			Value:     p.Value,
			Timestamp: p.Timestamp,
			Tags:      tags,
		})
	}
	return points
}

// GetStats returns engine statistics.
func (e *OTLPProtoEngine) GetStats() OTLPProtoStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *OTLPProtoEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/otlp-proto/ingest", func(w http.ResponseWriter, r *http.Request) {
		var points []ProtoMetricPoint
		if err := json.NewDecoder(r.Body).Decode(&points); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		result, err := e.IngestMetrics(points)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("/api/v1/otlp-proto/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
