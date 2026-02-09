package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
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
	TotalInferences     int64         `json:"total_inferences"`
	SuccessfulInferences int64        `json:"successful_inferences"`
	FailedInferences    int64         `json:"failed_inferences"`
	TotalLatencyNs      int64         `json:"total_latency_ns"`
	AnomaliesDetected   int64         `json:"anomalies_detected"`
	ModelsLoaded        int           `json:"models_loaded"`
	LastInferenceTime   time.Time     `json:"last_inference_time"`
}

// InferenceModel wraps an ML model with metadata and inference state.
type InferenceModel struct {
	// Metadata
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Version     string            `json:"version"`
	Type        InferenceModelType `json:"type"`
	Description string            `json:"description"`
	Created     time.Time         `json:"created"`
	Updated     time.Time         `json:"updated"`

	// Model configuration
	InputShape    []int             `json:"input_shape"`
	OutputShape   []int             `json:"output_shape"`
	FeatureNames  []string          `json:"feature_names"`
	Hyperparams   map[string]interface{} `json:"hyperparams"`
	Metrics       map[string]float64 `json:"metrics"`

	// Runtime
	model    MLModel
	extractor FeatureExtractor
	mu       sync.RWMutex
}

// InferenceModelType identifies the model type.
type InferenceModelType string

const (
	InferenceModelTypeAnomalyDetector  InferenceModelType = "anomaly_detector"
	InferenceModelTypeForecaster       InferenceModelType = "forecaster"
	InferenceModelTypeClassifier       InferenceModelType = "classifier"
	InferenceModelTypeRegressor        InferenceModelType = "regressor"
	InferenceModelTypeAutoEncoder      InferenceModelType = "autoencoder"
	InferenceModelTypeTransformer      InferenceModelType = "transformer"
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
	ModelID   string           `json:"model_id"`
	Scores    []PointScore     `json:"scores"`
	Anomalies []AnomalyPoint   `json:"anomalies"`
	Latency   time.Duration    `json:"latency"`
	Error     error            `json:"error,omitempty"`
}

// PointScore contains the score for a single point.
type PointScore struct {
	Timestamp int64   `json:"timestamp"`
	Score     float64 `json:"score"`
	IsAnomaly bool    `json:"is_anomaly"`
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
func (p *MLInferencePipeline) TrainModel(ctx context.Context, modelID, metric string, start, end int64, modelType InferenceModelType) (*InferenceModel, error) {
	// Query training data
	result, err := p.db.Execute(&Query{
		Metric: metric,
		Start:  start,
		End:    end,
	})
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	if len(result.Points) < 100 {
		return nil, errors.New("insufficient training data (need at least 100 points)")
	}

	// Extract values
	values := make([]float64, len(result.Points))
	for i, pt := range result.Points {
		values[i] = pt.Value
	}

	// Create and train model based on type
	var model MLModel
	switch modelType {
	case InferenceModelTypeAnomalyDetector:
		model = NewIsolationForestModel(modelID, 100, 256)
	case InferenceModelTypeForecaster:
		model = NewSimpleExponentialSmoothingModel(modelID, 0.3)
	case InferenceModelTypeClassifier:
		model = NewKMeansModel(modelID, 5, 100)
	default:
		model = NewStatisticalAnomalyDetector(modelID, 2.0)
	}

	if err := model.Train(values); err != nil {
		return nil, fmt.Errorf("training failed: %w", err)
	}

	// Create inference model wrapper
	infModel := &InferenceModel{
		ID:          modelID,
		Name:        modelID,
		Type:        modelType,
		Description: fmt.Sprintf("Auto-trained %s model for metric %s", modelType, metric),
		InputShape:  []int{len(values)},
		OutputShape: []int{len(values)},
		Hyperparams: map[string]interface{}{
			"metric":       metric,
			"training_start": start,
			"training_end":   end,
			"training_points": len(result.Points),
		},
		model: model,
	}

	// Register the model
	if err := p.RegisterModel(infModel); err != nil {
		return nil, err
	}

	return infModel, nil
}

// AutoDetectAnomalies automatically detects anomalies using an appropriate model.
func (p *MLInferencePipeline) AutoDetectAnomalies(ctx context.Context, metric string, start, end int64) (*ScoreResult, error) {
	// Create a temporary model ID
	modelID := fmt.Sprintf("auto_%s_%d", metric, time.Now().UnixNano())

	// Train model
	_, err := p.TrainModel(ctx, modelID, metric, start, end, InferenceModelTypeAnomalyDetector)
	if err != nil {
		return nil, err
	}
	defer p.UnregisterModel(modelID)

	// Score the same data
	return p.ScoreMetric(ctx, modelID, metric, start, end)
}

// GetStats returns inference statistics.
func (p *MLInferencePipeline) GetStats() MLInferenceStats {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	return p.stats
}

// --- Feature Extractors ---

// StatisticalExtractor extracts statistical features.
type StatisticalExtractor struct{}

func NewStatisticalExtractor() *StatisticalExtractor {
	return &StatisticalExtractor{}
}

func (e *StatisticalExtractor) Name() string { return "statistical" }

func (e *StatisticalExtractor) Extract(values []float64, _ []int64) ([]float64, error) {
	if len(values) == 0 {
		return nil, errors.New("no values provided")
	}

	// Calculate statistics
	n := float64(len(values))
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / n

	variance := 0.0
	for _, v := range values {
		diff := v - mean
		variance += diff * diff
	}
	variance /= n
	stdDev := math.Sqrt(variance)

	// Sort for percentiles
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	min := sorted[0]
	max := sorted[len(sorted)-1]
	median := sorted[len(sorted)/2]
	p25 := sorted[len(sorted)/4]
	p75 := sorted[3*len(sorted)/4]

	// Calculate skewness and kurtosis
	skewness := 0.0
	kurtosis := 0.0
	if stdDev > 0 {
		for _, v := range values {
			z := (v - mean) / stdDev
			skewness += z * z * z
			kurtosis += z * z * z * z
		}
		skewness /= n
		kurtosis = kurtosis/n - 3 // Excess kurtosis
	}

	return []float64{
		mean, stdDev, min, max, median, p25, p75, skewness, kurtosis,
	}, nil
}

func (e *StatisticalExtractor) FeatureNames() []string {
	return []string{"mean", "std_dev", "min", "max", "median", "p25", "p75", "skewness", "kurtosis"}
}

// TemporalExtractor extracts temporal features.
type TemporalExtractor struct{}

func NewTemporalExtractor() *TemporalExtractor {
	return &TemporalExtractor{}
}

func (e *TemporalExtractor) Name() string { return "temporal" }

func (e *TemporalExtractor) Extract(values []float64, timestamps []int64) ([]float64, error) {
	if len(values) < 2 {
		return nil, errors.New("need at least 2 values for temporal features")
	}

	// Calculate first differences (velocity)
	diffs := make([]float64, len(values)-1)
	for i := 1; i < len(values); i++ {
		diffs[i-1] = values[i] - values[i-1]
	}

	// Calculate second differences (acceleration)
	accel := make([]float64, len(diffs)-1)
	for i := 1; i < len(diffs); i++ {
		accel[i-1] = diffs[i] - diffs[i-1]
	}

	// Calculate mean velocity and acceleration
	meanVel := 0.0
	for _, d := range diffs {
		meanVel += d
	}
	meanVel /= float64(len(diffs))

	meanAccel := 0.0
	if len(accel) > 0 {
		for _, a := range accel {
			meanAccel += a
		}
		meanAccel /= float64(len(accel))
	}

	// Calculate autocorrelation at lag 1
	autocorr := 0.0
	if len(values) > 1 {
		mean := 0.0
		for _, v := range values {
			mean += v
		}
		mean /= float64(len(values))

		var num, denom float64
		for i := 0; i < len(values)-1; i++ {
			num += (values[i] - mean) * (values[i+1] - mean)
		}
		for _, v := range values {
			denom += (v - mean) * (v - mean)
		}
		if denom > 0 {
			autocorr = num / denom
		}
	}

	// Calculate trend using linear regression
	trend := 0.0
	if len(values) > 1 {
		n := float64(len(values))
		sumX, sumY, sumXY, sumX2 := 0.0, 0.0, 0.0, 0.0
		for i, v := range values {
			x := float64(i)
			sumX += x
			sumY += v
			sumXY += x * v
			sumX2 += x * x
		}
		denom := n*sumX2 - sumX*sumX
		if denom != 0 {
			trend = (n*sumXY - sumX*sumY) / denom
		}
	}

	// Calculate time interval statistics
	meanInterval := int64(0)
	if len(timestamps) > 1 {
		totalInterval := int64(0)
		for i := 1; i < len(timestamps); i++ {
			totalInterval += timestamps[i] - timestamps[i-1]
		}
		meanInterval = totalInterval / int64(len(timestamps)-1)
	}

	return []float64{
		meanVel, meanAccel, autocorr, trend, float64(meanInterval),
	}, nil
}

func (e *TemporalExtractor) FeatureNames() []string {
	return []string{"mean_velocity", "mean_acceleration", "autocorrelation", "trend", "mean_interval_ns"}
}

// RollingWindowExtractor extracts rolling window features.
type RollingWindowExtractor struct {
	windowSize int
}

func NewRollingWindowExtractor(windowSize int) *RollingWindowExtractor {
	if windowSize < 2 {
		windowSize = 10
	}
	return &RollingWindowExtractor{windowSize: windowSize}
}

func (e *RollingWindowExtractor) Name() string { return "rolling_window" }

func (e *RollingWindowExtractor) Extract(values []float64, _ []int64) ([]float64, error) {
	if len(values) < e.windowSize {
		return values, nil // Return raw values if not enough data
	}

	// Create rolling window features
	features := make([]float64, len(values))

	for i := range values {
		start := i - e.windowSize + 1
		if start < 0 {
			start = 0
		}
		window := values[start : i+1]

		// Calculate rolling mean
		sum := 0.0
		for _, v := range window {
			sum += v
		}
		mean := sum / float64(len(window))

		// Calculate deviation from rolling mean (z-score like)
		variance := 0.0
		for _, v := range window {
			diff := v - mean
			variance += diff * diff
		}
		variance /= float64(len(window))
		stdDev := math.Sqrt(variance)

		if stdDev > 0 {
			features[i] = (values[i] - mean) / stdDev
		} else {
			features[i] = 0
		}
	}

	return features, nil
}

func (e *RollingWindowExtractor) FeatureNames() []string {
	return []string{"rolling_zscore"}
}

// --- AutoML Model Selection ---

// AutoMLSelector automatically selects the best model for the data.
type AutoMLSelector struct {
	config InferenceAutoMLConfig
}

// InferenceAutoMLConfig configures automatic model selection for inference.
type InferenceAutoMLConfig struct {
	// CandidateModels lists models to try.
	CandidateModels []InferenceModelType
	// ValidationSplit is the fraction of data for validation.
	ValidationSplit float64
	// MaxTrainingTime limits training time.
	MaxTrainingTime time.Duration
	// OptimizeFor specifies the metric to optimize.
	OptimizeFor string
}

// DefaultInferenceAutoMLConfig returns default AutoML configuration for inference.
func DefaultInferenceAutoMLConfig() InferenceAutoMLConfig {
	return InferenceAutoMLConfig{
		CandidateModels: []InferenceModelType{
			InferenceModelTypeAnomalyDetector,
			InferenceModelTypeForecaster,
		},
		ValidationSplit: 0.2,
		MaxTrainingTime: 5 * time.Minute,
		OptimizeFor:     "accuracy",
	}
}

// NewAutoMLSelector creates a new AutoML selector.
func NewAutoMLSelector(config InferenceAutoMLConfig) *AutoMLSelector {
	return &AutoMLSelector{config: config}
}

// SelectBestModel trains multiple models and returns the best one.
func (s *AutoMLSelector) SelectBestModel(values []float64, modelName string) (MLModel, map[string]float64, error) {
	if len(values) < 100 {
		return nil, nil, errors.New("need at least 100 data points for AutoML")
	}

	// Split data
	splitIdx := int(float64(len(values)) * (1 - s.config.ValidationSplit))
	trainData := values[:splitIdx]
	valData := values[splitIdx:]

	bestModel := MLModel(nil)
	bestScore := -math.MaxFloat64
	bestMetrics := make(map[string]float64)

	for _, modelType := range s.config.CandidateModels {
		var model MLModel
		switch modelType {
		case InferenceModelTypeAnomalyDetector:
			model = NewIsolationForestModel(modelName+"_if", 100, 256)
		case InferenceModelTypeForecaster:
			model = NewSimpleExponentialSmoothingModel(modelName+"_ses", 0.3)
		default:
			model = NewStatisticalAnomalyDetector(modelName+"_stats", 2.0)
		}

		// Train
		if err := model.Train(trainData); err != nil {
			continue
		}

		// Validate
		scores, err := model.Predict(valData)
		if err != nil {
			continue
		}

		// Calculate accuracy (for anomaly detection, we use a simplified metric)
		score := s.calculateScore(scores, valData)

		if score > bestScore {
			bestScore = score
			bestModel = model
			bestMetrics["score"] = score
			// Store model type index as float (0=anomaly_detector, 1=forecaster, etc.)
			for i, mt := range s.config.CandidateModels {
				if mt == modelType {
					bestMetrics["model_type_index"] = float64(i)
					break
				}
			}
		}
	}

	if bestModel == nil {
		return nil, nil, errors.New("no model could be trained successfully")
	}

	return bestModel, bestMetrics, nil
}

func (s *AutoMLSelector) calculateScore(predictions, actual []float64) float64 {
	if len(predictions) == 0 || len(actual) == 0 {
		return 0
	}

	// Use negative RMSE as score (higher is better)
	sumSq := 0.0
	n := min(len(predictions), len(actual))
	for i := 0; i < n; i++ {
		// For anomaly scores, we want lower scores for normal data
		diff := predictions[i]
		sumSq += diff * diff
	}
	rmse := math.Sqrt(sumSq / float64(n))

	// Invert and scale
	return 1.0 / (1.0 + rmse)
}

// --- Model Serialization ---

// ModelRegistry persists and loads models.
type ModelRegistry struct {
	path   string
	models map[string][]byte
	mu     sync.RWMutex
}

// NewModelRegistry creates a new model registry.
func NewModelRegistry(path string) *ModelRegistry {
	return &ModelRegistry{
		path:   path,
		models: make(map[string][]byte),
	}
}

// Save persists a model.
func (r *ModelRegistry) Save(id string, model MLModel) error {
	data, err := model.Serialize()
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.models[id] = data
	r.mu.Unlock()

	return nil
}

// Load loads a model by ID.
func (r *ModelRegistry) Load(id string, model MLModel) error {
	r.mu.RLock()
	data, ok := r.models[id]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("model not found: %s", id)
	}

	return model.Deserialize(data)
}

// List returns all model IDs.
func (r *ModelRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.models))
	for id := range r.models {
		ids = append(ids, id)
	}
	return ids
}

// SerializeModel serializes a model to JSON.
func SerializeModel(model *InferenceModel) ([]byte, error) {
	return json.Marshal(model)
}

// DeserializeModel deserializes a model from JSON.
func DeserializeModel(data []byte) (*InferenceModel, error) {
	var model InferenceModel
	if err := json.Unmarshal(data, &model); err != nil {
		return nil, err
	}
	return &model, nil
}
