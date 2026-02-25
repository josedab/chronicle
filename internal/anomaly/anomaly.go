package anomaly

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// AnomalyConfig configures the AI-powered anomaly detection engine.
type AnomalyConfig struct {
	// Model is the anomaly detection model to use.
	Model AnomalyModel

	// Sensitivity controls anomaly detection sensitivity (0-1, higher = more sensitive).
	Sensitivity float64

	// MinDataPoints is the minimum data points required for training.
	MinDataPoints int

	// WindowSize is the sliding window size for LSTM/Transformer models.
	WindowSize int

	// HiddenSize is the hidden layer size for neural models.
	HiddenSize int

	// NumLayers is the number of layers for neural models.
	NumLayers int

	// LearningRate for online learning.
	LearningRate float64

	// AutoTrain enables automatic model retraining.
	AutoTrain bool

	// RetrainInterval is how often to retrain the model.
	RetrainInterval time.Duration

	// EnableSeasonality enables seasonal decomposition.
	EnableSeasonality bool

	// SeasonalPeriod is the expected seasonal period.
	SeasonalPeriod int
}

// AnomalyModel identifies the anomaly detection algorithm.
type AnomalyModel int

const (
	// AnomalyModelIsolationForest uses isolation forest for anomaly detection.
	AnomalyModelIsolationForest AnomalyModel = iota
	// AnomalyModelLSTM uses LSTM neural network for sequence anomaly detection.
	AnomalyModelLSTM
	// AnomalyModelAutoencoder uses autoencoder for reconstruction-based detection.
	AnomalyModelAutoencoder
	// AnomalyModelTransformer uses transformer architecture for anomaly detection.
	AnomalyModelTransformer
	// AnomalyModelEnsemble uses ensemble of multiple models.
	AnomalyModelEnsemble
	// AnomalyModelStatistical uses statistical methods (Z-score, IQR, etc.).
	AnomalyModelStatistical
)

func (m AnomalyModel) String() string {
	switch m {
	case AnomalyModelIsolationForest:
		return "isolation_forest"
	case AnomalyModelLSTM:
		return "lstm"
	case AnomalyModelAutoencoder:
		return "autoencoder"
	case AnomalyModelTransformer:
		return "transformer"
	case AnomalyModelEnsemble:
		return "ensemble"
	case AnomalyModelStatistical:
		return "statistical"
	default:
		return "unknown"
	}
}

// DefaultAnomalyConfig returns default anomaly detection configuration.
func DefaultAnomalyConfig() AnomalyConfig {
	return AnomalyConfig{
		Model:             AnomalyModelEnsemble,
		Sensitivity:       0.95,
		MinDataPoints:     100,
		WindowSize:        24,
		HiddenSize:        64,
		NumLayers:         2,
		LearningRate:      0.001,
		AutoTrain:         true,
		RetrainInterval:   time.Hour,
		EnableSeasonality: true,
		SeasonalPeriod:    24,
	}
}

// AnomalyDetector provides AI-powered anomaly detection.
type AnomalyDetector struct {
	ds     DataSource
	config AnomalyConfig

	// Models
	isolationForest *IsolationForest
	lstmModel       *LSTMModel
	autoencoder     *Autoencoder
	transformer     *TransformerModel
	statistical     *StatisticalModel

	// State
	mu          sync.RWMutex
	trained     bool
	lastTrain   time.Time
	trainData   []float64
	predictions []Prediction

	// Background training
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Prediction represents a model prediction.
type Prediction struct {
	Timestamp int64   `json:"timestamp"`
	Predicted float64 `json:"predicted"`
	Actual    float64 `json:"actual"`
	IsAnomaly bool    `json:"is_anomaly"`
	Score     float64 `json:"score"`
	Model     string  `json:"model"`
}

// AnomalyResult contains the results of anomaly detection.
type AnomalyResult struct {
	// IsAnomaly indicates if the point is anomalous.
	IsAnomaly bool `json:"is_anomaly"`

	// Score is the anomaly score (0-1, higher = more anomalous).
	Score float64 `json:"score"`

	// ExpectedValue is the predicted normal value.
	ExpectedValue float64 `json:"expected_value"`

	// Deviation is the deviation from expected.
	Deviation float64 `json:"deviation"`

	// Confidence is the confidence in the prediction.
	Confidence float64 `json:"confidence"`

	// ContributingFactors lists factors contributing to anomaly score.
	ContributingFactors []ContributingFactor `json:"contributing_factors,omitempty"`

	// ModelScores contains scores from individual models (for ensemble).
	ModelScores map[string]float64 `json:"model_scores,omitempty"`
}

// ContributingFactor describes a factor contributing to anomaly detection.
type ContributingFactor struct {
	Name        string  `json:"name"`
	Importance  float64 `json:"importance"`
	Description string  `json:"description"`
}

// NewAnomalyDetector creates a new AI-powered anomaly detector.
func NewAnomalyDetector(ds DataSource, config AnomalyConfig) *AnomalyDetector {
	if config.Sensitivity <= 0 || config.Sensitivity > 1 {
		config.Sensitivity = 0.95
	}
	if config.MinDataPoints <= 0 {
		config.MinDataPoints = 100
	}
	if config.WindowSize <= 0 {
		config.WindowSize = 24
	}
	if config.HiddenSize <= 0 {
		config.HiddenSize = 64
	}

	ctx, cancel := context.WithCancel(context.Background())

	ad := &AnomalyDetector{
		ds:              ds,
		config:          config,
		isolationForest: NewIsolationForest(100, 256),
		lstmModel:       NewLSTMModel(config.WindowSize, config.HiddenSize, config.NumLayers),
		autoencoder:     NewAutoencoder(config.WindowSize, config.HiddenSize),
		transformer:     NewTransformerModel(config.WindowSize, config.HiddenSize, 4),
		statistical:     NewStatisticalModel(config.Sensitivity),
		trainData:       make([]float64, 0),
		ctx:             ctx,
		cancel:          cancel,
	}

	return ad
}

// Start starts the anomaly detector with auto-training.
func (ad *AnomalyDetector) Start() {
	if ad.config.AutoTrain {
		ad.wg.Add(1)
		go ad.autoTrainLoop()
	}
}

// Stop stops the anomaly detector.
func (ad *AnomalyDetector) Stop() {
	ad.cancel()
	ad.wg.Wait()
}

func (ad *AnomalyDetector) autoTrainLoop() {
	defer ad.wg.Done()

	ticker := time.NewTicker(ad.config.RetrainInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ad.ctx.Done():
			return
		case <-ticker.C:
			ad.retrain()
		}
	}
}

// Train trains the anomaly detection models on historical data.
func (ad *AnomalyDetector) Train(data TimeSeriesData) error {
	if len(data.Values) < ad.config.MinDataPoints {
		return fmt.Errorf("insufficient data: need %d points, have %d",
			ad.config.MinDataPoints, len(data.Values))
	}

	ad.mu.Lock()
	defer ad.mu.Unlock()

	// Normalize data
	normalized, mean, std := normalize(data.Values)

	// Train each model based on configuration
	switch ad.config.Model {
	case AnomalyModelIsolationForest:
		ad.isolationForest.Train(normalized)
	case AnomalyModelLSTM:
		ad.lstmModel.Train(normalized)
	case AnomalyModelAutoencoder:
		ad.autoencoder.Train(normalized)
	case AnomalyModelTransformer:
		ad.transformer.Train(normalized)
	case AnomalyModelStatistical:
		ad.statistical.Train(data.Values)
	case AnomalyModelEnsemble:
		// Train all models
		ad.isolationForest.Train(normalized)
		ad.lstmModel.Train(normalized)
		ad.autoencoder.Train(normalized)
		ad.statistical.Train(data.Values)
	}

	ad.trainData = data.Values
	ad.trained = true
	ad.lastTrain = time.Now()

	// Store normalization parameters
	ad.statistical.mean = mean
	ad.statistical.std = std

	return nil
}

func (ad *AnomalyDetector) retrain() {
	// Get recent data from the database
	if ad.ds == nil {
		return
	}

	// Query recent metrics for training
	end := time.Now().UnixNano()
	start := time.Now().Add(-24 * time.Hour).UnixNano()

	metrics := ad.ds.Metrics()
	if len(metrics) == 0 {
		return
	}

	// Train on the first metric (simplified)
	points, err := ad.ds.QueryRange(metrics[0], start, end)
	if err != nil || len(points) < ad.config.MinDataPoints {
		return
	}

	values := make([]float64, len(points))
	timestamps := make([]int64, len(points))
	for i, p := range points {
		values[i] = p.Value
		timestamps[i] = p.Timestamp
	}

	_ = ad.Train(TimeSeriesData{ //nolint:errcheck // best-effort training on new data
		Timestamps: timestamps,
		Values:     values,
	})
}

// Detect performs anomaly detection on a single point.
func (ad *AnomalyDetector) Detect(value float64, context []float64) (*AnomalyResult, error) {
	ad.mu.RLock()
	defer ad.mu.RUnlock()

	if !ad.trained && ad.config.Model != AnomalyModelStatistical {
		return nil, errors.New("model not trained")
	}

	result := &AnomalyResult{
		ModelScores: make(map[string]float64),
	}

	switch ad.config.Model {
	case AnomalyModelIsolationForest:
		score := ad.isolationForest.Score(value)
		result.Score = score
		result.ModelScores["isolation_forest"] = score

	case AnomalyModelLSTM:
		if len(context) < ad.config.WindowSize {
			return nil, errors.New("insufficient context for LSTM")
		}
		predicted := ad.lstmModel.Predict(context)
		score := math.Abs(value-predicted) / (ad.statistical.std + 1e-10)
		result.Score = sigmoid(score - 2) // Normalize to 0-1
		result.ExpectedValue = predicted
		result.ModelScores["lstm"] = result.Score

	case AnomalyModelAutoencoder:
		if len(context) < ad.config.WindowSize {
			return nil, errors.New("insufficient context for autoencoder")
		}
		reconstructed := ad.autoencoder.Reconstruct(context)
		score := reconstructionError(context, reconstructed)
		result.Score = score
		result.ModelScores["autoencoder"] = score

	case AnomalyModelTransformer:
		if len(context) < ad.config.WindowSize {
			return nil, errors.New("insufficient context for transformer")
		}
		attention, predicted := ad.transformer.Predict(context)
		score := math.Abs(value-predicted) / (ad.statistical.std + 1e-10)
		result.Score = sigmoid(score - 2)
		result.ExpectedValue = predicted
		result.ModelScores["transformer"] = result.Score
		result.ContributingFactors = ad.interpretAttention(attention)

	case AnomalyModelStatistical:
		score := ad.statistical.Score(value)
		result.Score = score
		result.ExpectedValue = ad.statistical.mean
		result.ModelScores["statistical"] = score

	case AnomalyModelEnsemble:
		result = ad.ensembleDetect(value, context)
	}

	// Determine if anomaly based on sensitivity threshold
	threshold := 1 - ad.config.Sensitivity
	result.IsAnomaly = result.Score > threshold
	result.Deviation = value - result.ExpectedValue
	result.Confidence = ad.calculateConfidence(result.Score)

	return result, nil
}

func (ad *AnomalyDetector) ensembleDetect(value float64, context []float64) *AnomalyResult {
	result := &AnomalyResult{
		ModelScores: make(map[string]float64),
	}

	var scores []float64
	var weights []float64

	// Isolation Forest
	ifScore := ad.isolationForest.Score(value)
	scores = append(scores, ifScore)
	weights = append(weights, 0.25)
	result.ModelScores["isolation_forest"] = ifScore

	// Statistical
	statScore := ad.statistical.Score(value)
	scores = append(scores, statScore)
	weights = append(weights, 0.25)
	result.ModelScores["statistical"] = statScore
	result.ExpectedValue = ad.statistical.mean

	// LSTM (if context available)
	if len(context) >= ad.config.WindowSize {
		predicted := ad.lstmModel.Predict(context)
		lstmScore := math.Abs(value-predicted) / (ad.statistical.std + 1e-10)
		lstmScore = sigmoid(lstmScore - 2)
		scores = append(scores, lstmScore)
		weights = append(weights, 0.3)
		result.ModelScores["lstm"] = lstmScore
		result.ExpectedValue = predicted

		// Autoencoder
		reconstructed := ad.autoencoder.Reconstruct(context)
		aeScore := reconstructionError(context, reconstructed)
		scores = append(scores, aeScore)
		weights = append(weights, 0.2)
		result.ModelScores["autoencoder"] = aeScore
	} else {
		// Adjust weights when no context
		weights[0] = 0.4
		weights[1] = 0.6
	}

	// Weighted average
	var totalWeight float64
	for _, w := range weights {
		totalWeight += w
	}
	var weightedSum float64
	for i, s := range scores {
		weightedSum += s * weights[i]
	}
	result.Score = weightedSum / totalWeight

	return result
}

func (ad *AnomalyDetector) interpretAttention(attention []float64) []ContributingFactor {
	factors := make([]ContributingFactor, 0)

	// Find top attention weights
	type indexedWeight struct {
		index  int
		weight float64
	}
	indexed := make([]indexedWeight, len(attention))
	for i, w := range attention {
		indexed[i] = indexedWeight{i, w}
	}
	sort.Slice(indexed, func(i, j int) bool {
		return indexed[i].weight > indexed[j].weight
	})

	// Top 3 contributing time steps
	for i := 0; i < 3 && i < len(indexed); i++ {
		factors = append(factors, ContributingFactor{
			Name:        fmt.Sprintf("timestep_%d", indexed[i].index),
			Importance:  indexed[i].weight,
			Description: fmt.Sprintf("High attention on %d steps ago", len(attention)-indexed[i].index),
		})
	}

	return factors
}

func (ad *AnomalyDetector) calculateConfidence(score float64) float64 {
	// Higher score -> lower confidence in normalcy
	// But we want confidence in the prediction itself
	if score < 0.3 {
		return 0.95 // High confidence it's normal
	} else if score > 0.7 {
		return 0.9 // High confidence it's anomaly
	}
	return 0.6 + 0.3*(1-math.Abs(score-0.5)*2) // Medium confidence in gray zone
}

// DetectBatch performs anomaly detection on multiple points.
func (ad *AnomalyDetector) DetectBatch(data TimeSeriesData) ([]AnomalyResult, error) {
	results := make([]AnomalyResult, len(data.Values))

	for i, value := range data.Values {
		// Build context from previous values
		contextStart := i - ad.config.WindowSize
		if contextStart < 0 {
			contextStart = 0
		}
		context := data.Values[contextStart:i]

		result, err := ad.Detect(value, context)
		if err != nil {
			// Use statistical fallback
			score := ad.statistical.Score(value)
			results[i] = AnomalyResult{
				Score:         score,
				IsAnomaly:     score > (1 - ad.config.Sensitivity),
				ExpectedValue: ad.statistical.mean,
				Deviation:     value - ad.statistical.mean,
			}
		} else {
			results[i] = *result
		}
	}

	return results, nil
}

// Stats returns anomaly detector statistics.
func (ad *AnomalyDetector) Stats() AnomalyStats {
	ad.mu.RLock()
	defer ad.mu.RUnlock()

	return AnomalyStats{
		Model:         ad.config.Model.String(),
		Trained:       ad.trained,
		LastTrainTime: ad.lastTrain,
		TrainDataSize: len(ad.trainData),
		Sensitivity:   ad.config.Sensitivity,
		WindowSize:    ad.config.WindowSize,
	}
}

// AnomalyStats contains anomaly detector statistics.
type AnomalyStats struct {
	Model         string    `json:"model"`
	Trained       bool      `json:"trained"`
	LastTrainTime time.Time `json:"last_train_time"`
	TrainDataSize int       `json:"train_data_size"`
	Sensitivity   float64   `json:"sensitivity"`
	WindowSize    int       `json:"window_size"`
}
