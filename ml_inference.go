package chronicle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// MLInferenceConfig configures the real-time ML inference pipeline.
type MLInferenceConfig struct {
	// Enabled enables the ML inference engine.
	Enabled bool `json:"enabled"`

	// MaxModels is the maximum number of loaded models.
	MaxModels int `json:"max_models"`

	// MaxModelSizeBytes is the maximum model size in bytes.
	MaxModelSizeBytes int64 `json:"max_model_size_bytes"`

	// InferenceTimeoutMs is the timeout for inference operations.
	InferenceTimeoutMs int64 `json:"inference_timeout_ms"`

	// BatchSize for batch inference operations.
	BatchSize int `json:"batch_size"`

	// FeatureWindowSize is the default window size for feature extraction.
	FeatureWindowSize int `json:"feature_window_size"`

	// EnableRealTimeScoring enables scoring on write path.
	EnableRealTimeScoring bool `json:"enable_real_time_scoring"`

	// AnomalyScoreThreshold is the default threshold for anomaly detection.
	AnomalyScoreThreshold float64 `json:"anomaly_score_threshold"`

	// ModelStorePath is where models are persisted.
	ModelStorePath string `json:"model_store_path"`
}

// DefaultMLInferenceConfig returns default configuration.
func DefaultMLInferenceConfig() MLInferenceConfig {
	return MLInferenceConfig{
		Enabled:               true,
		MaxModels:             100,
		MaxModelSizeBytes:     50 * 1024 * 1024, // 50MB
		InferenceTimeoutMs:    1000,
		BatchSize:             1000,
		FeatureWindowSize:     10,
		EnableRealTimeScoring: false,
		AnomalyScoreThreshold: 2.5,
	}
}

// MLInferencePipeline provides real-time ML inference for time-series data.
type MLInferencePipeline struct {
	db     *DB
	config MLInferenceConfig

	// Model registry
	models   map[string]*InferenceModel
	modelsMu sync.RWMutex

	// Feature extractors
	extractors   map[string]FeatureExtractor
	extractorsMu sync.RWMutex

	// Inference hooks for real-time scoring
	hooks   []InferenceHook
	hooksMu sync.RWMutex

	// Background processing
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	scoreCh  chan *ScoreRequest
	resultCh chan *ScoreResult

	// Statistics
	stats   MLInferenceStats
	statsMu sync.RWMutex
}

// MLInferenceStats tracks inference statistics.
type MLInferenceStats struct {
	TotalInferences      int64     `json:"total_inferences"`
	SuccessfulInferences int64     `json:"successful_inferences"`
	FailedInferences     int64     `json:"failed_inferences"`
	TotalLatencyNs       int64     `json:"total_latency_ns"`
	AnomaliesDetected    int64     `json:"anomalies_detected"`
	ModelsLoaded         int       `json:"models_loaded"`
	LastInferenceTime    time.Time `json:"last_inference_time"`
}

// InferenceModel wraps an ML model with metadata and inference state.
type InferenceModel struct {
	// Metadata
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	Version     string             `json:"version"`
	Type        InferenceModelType `json:"type"`
	Description string             `json:"description"`
	Created     time.Time          `json:"created"`
	Updated     time.Time          `json:"updated"`

	// Model configuration
	InputShape   []int              `json:"input_shape"`
	OutputShape  []int              `json:"output_shape"`
	FeatureNames []string           `json:"feature_names"`
	Hyperparams  map[string]any     `json:"hyperparams"`
	Metrics      map[string]float64 `json:"metrics"`

	// Runtime
	model     MLModel
	extractor FeatureExtractor
	mu        sync.RWMutex
}

// InferenceModelType identifies the model type.
type InferenceModelType string

const (
	InferenceModelTypeAnomalyDetector InferenceModelType = "anomaly_detector"
	InferenceModelTypeForecaster      InferenceModelType = "forecaster"
	InferenceModelTypeClassifier      InferenceModelType = "classifier"
	InferenceModelTypeRegressor       InferenceModelType = "regressor"
	InferenceModelTypeAutoEncoder     InferenceModelType = "autoencoder"
	InferenceModelTypeTransformer     InferenceModelType = "transformer"
)

// FeatureExtractor extracts features from time-series data.
type FeatureExtractor interface {
	// Name returns the extractor name.
	Name() string
	// Extract extracts features from values.
	Extract(values []float64, timestamps []int64) ([]float64, error)
	// FeatureNames returns the names of extracted features.
	FeatureNames() []string
}

// InferenceHook is called during real-time scoring.
type InferenceHook interface {
	// OnScore is called when a point is scored.
	OnScore(point Point, score float64, anomaly bool)
}

// ScoreRequest represents a scoring request.
type ScoreRequest struct {
	ModelID    string
	Points     []Point
	ResultChan chan *ScoreResult
}

// ScoreResult contains scoring results.
type ScoreResult struct {
	ModelID   string         `json:"model_id"`
	Scores    []PointScore   `json:"scores"`
	Anomalies []AnomalyPoint `json:"anomalies"`
	Latency   time.Duration  `json:"latency"`
	Error     error          `json:"error,omitempty"`
}

// PointScore contains the score for a single point.
type PointScore struct {
	Timestamp int64             `json:"timestamp"`
	Score     float64           `json:"score"`
	IsAnomaly bool              `json:"is_anomaly"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// NewMLInferencePipeline creates a new ML inference pipeline.
func NewMLInferencePipeline(db *DB, config MLInferenceConfig) *MLInferencePipeline {
	ctx, cancel := context.WithCancel(context.Background())

	p := &MLInferencePipeline{
		db:         db,
		config:     config,
		models:     make(map[string]*InferenceModel),
		extractors: make(map[string]FeatureExtractor),
		ctx:        ctx,
		cancel:     cancel,
		scoreCh:    make(chan *ScoreRequest, 100),
		resultCh:   make(chan *ScoreResult, 100),
	}

	// Register default extractors
	p.RegisterExtractor(NewStatisticalExtractor())
	p.RegisterExtractor(NewTemporalExtractor())
	p.RegisterExtractor(NewRollingWindowExtractor(10))

	return p
}

// Start begins background processing.
func (p *MLInferencePipeline) Start() {
	p.wg.Add(1)
	go p.processScoreRequests()
}

// Stop stops background processing.
func (p *MLInferencePipeline) Stop() {
	p.cancel()
	p.wg.Wait()
}

func (p *MLInferencePipeline) processScoreRequests() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case req := <-p.scoreCh:
			result := p.scorePoints(req.ModelID, req.Points)
			if req.ResultChan != nil {
				req.ResultChan <- result
			}
		}
	}
}

// RegisterModel registers a model with the pipeline.
func (p *MLInferencePipeline) RegisterModel(model *InferenceModel) error {
	p.modelsMu.Lock()
	defer p.modelsMu.Unlock()

	if len(p.models) >= p.config.MaxModels {
		return errors.New("maximum models reached")
	}

	if model.ID == "" {
		return errors.New("model ID is required")
	}

	model.Created = time.Now()
	model.Updated = time.Now()
	p.models[model.ID] = model

	p.statsMu.Lock()
	p.stats.ModelsLoaded = len(p.models)
	p.statsMu.Unlock()

	return nil
}

// UnregisterModel removes a model from the pipeline.
func (p *MLInferencePipeline) UnregisterModel(modelID string) {
	p.modelsMu.Lock()
	defer p.modelsMu.Unlock()
	delete(p.models, modelID)

	p.statsMu.Lock()
	p.stats.ModelsLoaded = len(p.models)
	p.statsMu.Unlock()
}

// GetModel returns a model by ID.
func (p *MLInferencePipeline) GetModel(modelID string) (*InferenceModel, bool) {
	p.modelsMu.RLock()
	defer p.modelsMu.RUnlock()
	model, ok := p.models[modelID]
	return model, ok
}

// ListModels returns all registered models.
func (p *MLInferencePipeline) ListModels() []*InferenceModel {
	p.modelsMu.RLock()
	defer p.modelsMu.RUnlock()

	models := make([]*InferenceModel, 0, len(p.models))
	for _, m := range p.models {
		models = append(models, m)
	}
	return models
}

// RegisterExtractor registers a feature extractor.
func (p *MLInferencePipeline) RegisterExtractor(extractor FeatureExtractor) {
	p.extractorsMu.Lock()
	defer p.extractorsMu.Unlock()
	p.extractors[extractor.Name()] = extractor
}

// AddHook adds an inference hook for real-time scoring.
func (p *MLInferencePipeline) AddHook(hook InferenceHook) {
	p.hooksMu.Lock()
	defer p.hooksMu.Unlock()
	p.hooks = append(p.hooks, hook)
}

// Score performs inference on points using a specific model.
func (p *MLInferencePipeline) Score(ctx context.Context, modelID string, points []Point) (*ScoreResult, error) {
	resultChan := make(chan *ScoreResult, 1)

	req := &ScoreRequest{
		ModelID:    modelID,
		Points:     points,
		ResultChan: resultChan,
	}

	select {
	case p.scoreCh <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case result := <-resultChan:
		return result, result.Error
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *MLInferencePipeline) scorePoints(modelID string, points []Point) *ScoreResult {
	start := time.Now()
	result := &ScoreResult{
		ModelID: modelID,
	}

	model, ok := p.GetModel(modelID)
	if !ok {
		result.Error = fmt.Errorf("model not found: %s", modelID)
		p.statsMu.Lock()
		p.stats.FailedInferences++
		p.statsMu.Unlock()
		return result
	}

	// Extract values
	values := make([]float64, len(points))
	timestamps := make([]int64, len(points))
	for i, pt := range points {
		values[i] = pt.Value
		timestamps[i] = pt.Timestamp
	}

	// Extract features
	var features []float64
	var err error

	if model.extractor != nil {
		features, err = model.extractor.Extract(values, timestamps)
	} else {
		// Use default rolling window features
		extractor := NewRollingWindowExtractor(p.config.FeatureWindowSize)
		features, err = extractor.Extract(values, timestamps)
	}

	if err != nil {
		result.Error = fmt.Errorf("feature extraction failed: %w", err)
		return result
	}

	// Run inference
	if model.model == nil {
		result.Error = errors.New("model not initialized")
		return result
	}

	scores, err := model.model.Predict(features)
	if err != nil {
		result.Error = fmt.Errorf("inference failed: %w", err)
		p.statsMu.Lock()
		p.stats.FailedInferences++
		p.statsMu.Unlock()
		return result
	}

	// Build results
	result.Scores = make([]PointScore, len(points))
	for i, pt := range points {
		score := 0.0
		if i < len(scores) {
			score = scores[i]
		}
		isAnomaly := score > p.config.AnomalyScoreThreshold

		result.Scores[i] = PointScore{
			Timestamp: pt.Timestamp,
			Score:     score,
			IsAnomaly: isAnomaly,
			Labels:    pt.Tags,
		}

		if isAnomaly {
			result.Anomalies = append(result.Anomalies, AnomalyPoint{
				Timestamp:     pt.Timestamp,
				ActualValue:   pt.Value,
				ExpectedValue: 0, // Would need model to provide this
				Deviation:     score,
				Score:         score,
			})
		}

		// Call hooks
		p.hooksMu.RLock()
		for _, hook := range p.hooks {
			hook.OnScore(pt, score, isAnomaly)
		}
		p.hooksMu.RUnlock()
	}

	result.Latency = time.Since(start)

	// Update stats
	p.statsMu.Lock()
	p.stats.TotalInferences++
	p.stats.SuccessfulInferences++
	p.stats.TotalLatencyNs += result.Latency.Nanoseconds()
	p.stats.AnomaliesDetected += int64(len(result.Anomalies))
	p.stats.LastInferenceTime = time.Now()
	p.statsMu.Unlock()

	return result
}

// ScoreMetric scores all data for a metric using a model.
func (p *MLInferencePipeline) ScoreMetric(ctx context.Context, modelID, metric string, start, end int64) (*ScoreResult, error) {
	// Query data
	result, err := p.db.Execute(&Query{
		Metric: metric,
		Start:  start,
		End:    end,
	})
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	if len(result.Points) == 0 {
		return &ScoreResult{ModelID: modelID}, nil
	}

	return p.Score(ctx, modelID, result.Points)
}

// TrainModel trains a model on historical data.
