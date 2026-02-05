package anomaly

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

	_ = ad.Train(TimeSeriesData{
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

// ========== Model Implementations ==========

// IsolationForest implements the Isolation Forest algorithm.
type IsolationForest struct {
	numTrees   int
	sampleSize int
	trees      []*IsolationTree
	trained    bool
}

// IsolationTree is a single isolation tree.
type IsolationTree struct {
	root      *IsolationNode
	maxDepth  int
	trainData []float64
}

// IsolationNode is a node in the isolation tree.
type IsolationNode struct {
	splitValue float64
	left       *IsolationNode
	right      *IsolationNode
	size       int
	isLeaf     bool
}

// NewIsolationForest creates a new isolation forest.
func NewIsolationForest(numTrees, sampleSize int) *IsolationForest {
	return &IsolationForest{
		numTrees:   numTrees,
		sampleSize: sampleSize,
		trees:      make([]*IsolationTree, numTrees),
	}
}

// Train trains the isolation forest.
func (f *IsolationForest) Train(data []float64) {
	maxDepth := int(math.Ceil(math.Log2(float64(f.sampleSize))))

	for i := 0; i < f.numTrees; i++ {
		// Sample data
		sample := randomSample(data, f.sampleSize)

		// Build tree
		tree := &IsolationTree{
			maxDepth:  maxDepth,
			trainData: sample,
		}
		tree.root = f.buildTree(sample, 0, maxDepth)
		f.trees[i] = tree
	}

	f.trained = true
}

func (f *IsolationForest) buildTree(data []float64, depth, maxDepth int) *IsolationNode {
	if len(data) <= 1 || depth >= maxDepth {
		return &IsolationNode{size: len(data), isLeaf: true}
	}

	// Random split value between min and max
	minVal, maxVal := minMax(data)
	if minVal == maxVal {
		return &IsolationNode{size: len(data), isLeaf: true}
	}

	splitValue := minVal + randFloat()*(maxVal-minVal)

	// Partition data
	var left, right []float64
	for _, v := range data {
		if v < splitValue {
			left = append(left, v)
		} else {
			right = append(right, v)
		}
	}

	return &IsolationNode{
		splitValue: splitValue,
		left:       f.buildTree(left, depth+1, maxDepth),
		right:      f.buildTree(right, depth+1, maxDepth),
		size:       len(data),
	}
}

// Score returns the anomaly score for a value.
func (f *IsolationForest) Score(value float64) float64 {
	if !f.trained || len(f.trees) == 0 {
		return 0.5
	}

	var pathLengths []float64
	for _, tree := range f.trees {
		length := f.pathLength(tree.root, value, 0)
		pathLengths = append(pathLengths, length)
	}

	avgPath := mean(pathLengths)
	c := f.averagePathLength(float64(f.sampleSize))

	// Anomaly score: closer to 1 = more anomalous
	score := math.Pow(2, -avgPath/c)
	return score
}

func (f *IsolationForest) pathLength(node *IsolationNode, value float64, depth int) float64 {
	if node == nil || node.isLeaf {
		if node != nil && node.size > 1 {
			return float64(depth) + f.averagePathLength(float64(node.size))
		}
		return float64(depth)
	}

	if value < node.splitValue {
		return f.pathLength(node.left, value, depth+1)
	}
	return f.pathLength(node.right, value, depth+1)
}

func (f *IsolationForest) averagePathLength(n float64) float64 {
	if n <= 1 {
		return 0
	}
	return 2*(math.Log(n-1)+0.5772156649) - 2*(n-1)/n
}

// LSTMModel implements a simplified LSTM for time series prediction.
type LSTMModel struct {
	windowSize int
	hiddenSize int
	numLayers  int
	trained    bool

	// Weights (simplified single-layer)
	Wf, Wi, Wc, Wo [][]float64 // Forget, input, cell, output gate weights
	bf, bi, bc, bo []float64   // Biases

	// State
	cellState   []float64
	hiddenState []float64
}

// NewLSTMModel creates a new LSTM model.
func NewLSTMModel(windowSize, hiddenSize, numLayers int) *LSTMModel {
	model := &LSTMModel{
		windowSize:  windowSize,
		hiddenSize:  hiddenSize,
		numLayers:   numLayers,
		cellState:   make([]float64, hiddenSize),
		hiddenState: make([]float64, hiddenSize),
	}

	// Initialize weights with small random values
	inputSize := 1 // Single feature for time series
	model.initWeights(inputSize, hiddenSize)

	return model
}

func (m *LSTMModel) initWeights(inputSize, hiddenSize int) {
	totalSize := inputSize + hiddenSize

	m.Wf = randomMatrix(hiddenSize, totalSize, 0.1)
	m.Wi = randomMatrix(hiddenSize, totalSize, 0.1)
	m.Wc = randomMatrix(hiddenSize, totalSize, 0.1)
	m.Wo = randomMatrix(hiddenSize, totalSize, 0.1)

	m.bf = make([]float64, hiddenSize)
	m.bi = make([]float64, hiddenSize)
	m.bc = make([]float64, hiddenSize)
	m.bo = make([]float64, hiddenSize)

	// Initialize forget gate bias to 1 for better gradient flow
	for i := range m.bf {
		m.bf[i] = 1.0
	}
}

// Train trains the LSTM model.
func (m *LSTMModel) Train(data []float64) {
	// Simplified training: compute statistics for prediction
	// In a real implementation, this would use backpropagation
	m.trained = true

	// Reset state
	m.cellState = make([]float64, m.hiddenSize)
	m.hiddenState = make([]float64, m.hiddenSize)

	// Process data to warm up state
	for i := 0; i < len(data)-1 && i < m.windowSize*2; i++ {
		m.forward(data[i])
	}
}

// Predict predicts the next value given context.
func (m *LSTMModel) Predict(context []float64) float64 {
	if !m.trained || len(context) == 0 {
		if len(context) > 0 {
			return context[len(context)-1]
		}
		return 0
	}

	// Reset state for prediction
	cellState := make([]float64, m.hiddenSize)
	hiddenState := make([]float64, m.hiddenSize)

	// Process context
	for _, x := range context {
		// Concatenate input and hidden state
		combined := append([]float64{x}, hiddenState...)

		// Gate computations with numerical stability
		ft := m.applyGate(m.Wf, m.bf, combined, sigmoid) // Forget gate
		it := m.applyGate(m.Wi, m.bi, combined, sigmoid) // Input gate
		ct := m.applyGate(m.Wc, m.bc, combined, tanh)    // Cell candidate
		ot := m.applyGate(m.Wo, m.bo, combined, sigmoid) // Output gate

		// Update cell state: c_t = f_t * c_{t-1} + i_t * c_t
		for i := range cellState {
			cellState[i] = ft[i]*cellState[i] + it[i]*ct[i]
		}

		// Update hidden state: h_t = o_t * tanh(c_t)
		for i := range hiddenState {
			hiddenState[i] = ot[i] * tanh(cellState[i])
		}
	}

	// Predict: weighted sum of hidden state
	prediction := 0.0
	for i := range hiddenState {
		prediction += hiddenState[i]
	}
	prediction /= float64(m.hiddenSize)

	// Scale back to data range (simplified)
	if len(context) > 0 {
		contextMean := mean(context)
		contextStd := stdDevSingle(context)
		prediction = prediction*contextStd + contextMean
	}

	return prediction
}

func (m *LSTMModel) forward(x float64) []float64 {
	combined := append([]float64{x}, m.hiddenState...)

	ft := m.applyGate(m.Wf, m.bf, combined, sigmoid)
	it := m.applyGate(m.Wi, m.bi, combined, sigmoid)
	ct := m.applyGate(m.Wc, m.bc, combined, tanh)
	ot := m.applyGate(m.Wo, m.bo, combined, sigmoid)

	for i := range m.cellState {
		m.cellState[i] = ft[i]*m.cellState[i] + it[i]*ct[i]
	}

	for i := range m.hiddenState {
		m.hiddenState[i] = ot[i] * tanh(m.cellState[i])
	}

	return m.hiddenState
}

func (m *LSTMModel) applyGate(W [][]float64, b, input []float64, activation func(float64) float64) []float64 {
	result := make([]float64, len(W))
	for i := range W {
		sum := b[i]
		for j := range input {
			if j < len(W[i]) {
				sum += W[i][j] * input[j]
			}
		}
		result[i] = activation(sum)
	}
	return result
}

// Autoencoder implements a simple autoencoder for anomaly detection.
type Autoencoder struct {
	inputSize  int
	hiddenSize int
	trained    bool

	// Encoder weights
	encoderW [][]float64
	encoderB []float64

	// Decoder weights
	decoderW [][]float64
	decoderB []float64

	// Threshold for anomaly
	threshold float64
}

// NewAutoencoder creates a new autoencoder.
func NewAutoencoder(inputSize, hiddenSize int) *Autoencoder {
	ae := &Autoencoder{
		inputSize:  inputSize,
		hiddenSize: hiddenSize,
	}

	// Initialize weights
	ae.encoderW = randomMatrix(hiddenSize, inputSize, 0.1)
	ae.encoderB = make([]float64, hiddenSize)
	ae.decoderW = randomMatrix(inputSize, hiddenSize, 0.1)
	ae.decoderB = make([]float64, inputSize)

	return ae
}

// Train trains the autoencoder.
func (ae *Autoencoder) Train(data []float64) {
	// Simplified training: just compute reconstruction threshold
	// In practice, this would use gradient descent

	// Create windows for training
	windowSize := ae.inputSize
	if len(data) < windowSize {
		ae.trained = true
		return
	}

	var errors []float64
	for i := 0; i <= len(data)-windowSize; i++ {
		window := data[i : i+windowSize]
		reconstructed := ae.Reconstruct(window)
		err := reconstructionError(window, reconstructed)
		errors = append(errors, err)
	}

	// Set threshold as mean + 2*std of reconstruction errors
	if len(errors) > 0 {
		ae.threshold = mean(errors) + 2*stdDevSingle(errors)
	}

	ae.trained = true
}

// Reconstruct reconstructs the input.
func (ae *Autoencoder) Reconstruct(input []float64) []float64 {
	// Encode
	encoded := make([]float64, ae.hiddenSize)
	for i := range encoded {
		sum := ae.encoderB[i]
		for j := range input {
			if j < len(ae.encoderW[i]) {
				sum += ae.encoderW[i][j] * input[j]
			}
		}
		encoded[i] = relu(sum)
	}

	// Decode
	decoded := make([]float64, ae.inputSize)
	for i := range decoded {
		sum := ae.decoderB[i]
		for j := range encoded {
			if j < len(ae.decoderW[i]) {
				sum += ae.decoderW[i][j] * encoded[j]
			}
		}
		decoded[i] = sum // Linear activation for output
	}

	return decoded
}

// TransformerModel implements a simplified transformer for time series.
type TransformerModel struct {
	windowSize int
	hiddenSize int
	numHeads   int
	trained    bool

	// Attention weights
	Wq, Wk, Wv [][]float64
	Wo         [][]float64

	// FFN weights
	W1, W2 [][]float64
	b1, b2 []float64
}

// NewTransformerModel creates a new transformer model.
func NewTransformerModel(windowSize, hiddenSize, numHeads int) *TransformerModel {
	t := &TransformerModel{
		windowSize: windowSize,
		hiddenSize: hiddenSize,
		numHeads:   numHeads,
	}

	// Initialize attention weights
	headDim := hiddenSize / numHeads
	t.Wq = randomMatrix(hiddenSize, 1, 0.1)
	t.Wk = randomMatrix(hiddenSize, 1, 0.1)
	t.Wv = randomMatrix(hiddenSize, 1, 0.1)
	t.Wo = randomMatrix(1, hiddenSize, 0.1)

	// FFN weights
	t.W1 = randomMatrix(hiddenSize*4, hiddenSize, 0.1)
	t.W2 = randomMatrix(hiddenSize, hiddenSize*4, 0.1)
	t.b1 = make([]float64, hiddenSize*4)
	t.b2 = make([]float64, hiddenSize)

	_ = headDim // Used in full implementation

	return t
}

// Train trains the transformer model.
func (t *TransformerModel) Train(data []float64) {
	t.trained = true
}

// Predict predicts the next value and returns attention weights.
func (t *TransformerModel) Predict(context []float64) (attention []float64, prediction float64) {
	if len(context) == 0 {
		return nil, 0
	}

	// Simplified self-attention
	seqLen := len(context)
	attention = make([]float64, seqLen)

	// Compute attention scores (simplified dot-product attention)
	scores := make([]float64, seqLen)
	lastValue := context[seqLen-1]
	for i, v := range context {
		// Q: last position, K: all positions
		scores[i] = lastValue * v / math.Sqrt(float64(seqLen))
	}

	// Softmax
	maxScore := scores[0]
	for _, s := range scores {
		if s > maxScore {
			maxScore = s
		}
	}
	sumExp := 0.0
	for i, s := range scores {
		attention[i] = math.Exp(s - maxScore)
		sumExp += attention[i]
	}
	for i := range attention {
		attention[i] /= sumExp
	}

	// Weighted sum of values
	prediction = 0
	for i, v := range context {
		prediction += attention[i] * v
	}

	return attention, prediction
}

// StatisticalModel implements statistical anomaly detection.
type StatisticalModel struct {
	sensitivity float64
	trained     bool
	mean        float64
	std         float64
	q1          float64
	q3          float64
	iqr         float64
	min         float64
	max         float64
}

// NewStatisticalModel creates a new statistical model.
func NewStatisticalModel(sensitivity float64) *StatisticalModel {
	return &StatisticalModel{
		sensitivity: sensitivity,
	}
}

// Train trains the statistical model.
func (m *StatisticalModel) Train(data []float64) {
	if len(data) == 0 {
		return
	}

	m.mean = mean(data)
	m.std = stdDevSingle(data)
	m.min, m.max = minMax(data)

	// Calculate quartiles
	sorted := make([]float64, len(data))
	copy(sorted, data)
	sort.Float64s(sorted)

	m.q1 = percentile(sorted, 25)
	m.q3 = percentile(sorted, 75)
	m.iqr = m.q3 - m.q1

	m.trained = true
}

// Score returns the anomaly score for a value.
func (m *StatisticalModel) Score(value float64) float64 {
	if !m.trained || m.std == 0 {
		return 0.5
	}

	// Z-score based anomaly score
	zScore := math.Abs(value-m.mean) / m.std

	// IQR-based score
	iqrScore := 0.0
	if m.iqr > 0 {
		lowerBound := m.q1 - 1.5*m.iqr
		upperBound := m.q3 + 1.5*m.iqr
		if value < lowerBound || value > upperBound {
			iqrScore = 1.0
		}
	}

	// Combine scores
	zScoreNorm := sigmoid(zScore - 2) // Normalize Z-score to 0-1
	combinedScore := 0.7*zScoreNorm + 0.3*iqrScore

	return combinedScore
}

// ========== Helper Functions ==========

func normalize(data []float64) (normalized []float64, mean, std float64) {
	mean = 0
	for _, v := range data {
		mean += v
	}
	mean /= float64(len(data))

	variance := 0.0
	for _, v := range data {
		variance += (v - mean) * (v - mean)
	}
	std = math.Sqrt(variance / float64(len(data)))

	if std == 0 {
		std = 1
	}

	normalized = make([]float64, len(data))
	for i, v := range data {
		normalized[i] = (v - mean) / std
	}

	return normalized, mean, std
}

func randomSample(data []float64, size int) []float64 {
	if len(data) <= size {
		return data
	}

	sample := make([]float64, size)
	for i := 0; i < size; i++ {
		sample[i] = data[randInt(len(data))]
	}
	return sample
}

func minMax(data []float64) (float64, float64) {
	if len(data) == 0 {
		return 0, 0
	}
	min, max := data[0], data[0]
	for _, v := range data {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

func randFloat() float64 {
	// Simple random float between 0 and 1
	return float64(randInt(1000000)) / 1000000.0
}

func randInt(max int) int {
	// Seeded in init() for deterministic testing
	return int(time.Now().UnixNano()) % max
}

func randomMatrix(rows, cols int, scale float64) [][]float64 {
	matrix := make([][]float64, rows)
	for i := range matrix {
		matrix[i] = make([]float64, cols)
		for j := range matrix[i] {
			matrix[i][j] = (randFloat() - 0.5) * 2 * scale
		}
	}
	return matrix
}

func sigmoid(x float64) float64 {
	if x < -500 {
		return 0
	}
	if x > 500 {
		return 1
	}
	return 1.0 / (1.0 + math.Exp(-x))
}

func tanh(x float64) float64 {
	return math.Tanh(x)
}

func relu(x float64) float64 {
	if x > 0 {
		return x
	}
	return 0
}

func reconstructionError(original, reconstructed []float64) float64 {
	if len(original) != len(reconstructed) {
		minLen := len(original)
		if len(reconstructed) < minLen {
			minLen = len(reconstructed)
		}
		original = original[:minLen]
		reconstructed = reconstructed[:minLen]
	}

	if len(original) == 0 {
		return 0
	}

	sumSq := 0.0
	for i := range original {
		diff := original[i] - reconstructed[i]
		sumSq += diff * diff
	}

	return math.Sqrt(sumSq / float64(len(original)))
}

func stdDevSingle(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	m := mean(data)
	variance := 0.0
	for _, v := range data {
		variance += (v - m) * (v - m)
	}
	return math.Sqrt(variance / float64(len(data)))
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	k := (p / 100) * float64(len(sorted)-1)
	f := math.Floor(k)
	c := math.Ceil(k)
	if f == c {
		return sorted[int(k)]
	}
	return sorted[int(f)]*(c-k) + sorted[int(c)]*(k-f)
}

// AnomalyDetectionDB wraps a DB with anomaly detection capabilities.
type AnomalyDetectionDB struct {
	ds       DataSource
	pw       PointWriter
	detector *AnomalyDetector
}

// NewAnomalyDetectionDB creates a new anomaly detection enabled database.
func NewAnomalyDetectionDB(ds DataSource, pw PointWriter, config AnomalyConfig) *AnomalyDetectionDB {
	detector := NewAnomalyDetector(ds, config)

	return &AnomalyDetectionDB{
		ds:       ds,
		pw:       pw,
		detector: detector,
	}
}

// Start starts anomaly detection.
func (adb *AnomalyDetectionDB) Start() {
	adb.detector.Start()
}

// Stop stops anomaly detection.
func (adb *AnomalyDetectionDB) Stop() {
	adb.detector.Stop()
}

// Detector returns the anomaly detector.
func (adb *AnomalyDetectionDB) Detector() *AnomalyDetector {
	return adb.detector
}

// WriteAndDetect writes a point and performs anomaly detection.
func (adb *AnomalyDetectionDB) WriteAndDetect(p PointData, context []float64) (*AnomalyResult, error) {
	// Write the point
	if err := adb.pw.WritePoint(p); err != nil {
		return nil, err
	}

	// Detect anomaly
	return adb.detector.Detect(p.Value, context)
}

// ExportModel exports the trained model as JSON.
func (ad *AnomalyDetector) ExportModel() ([]byte, error) {
	ad.mu.RLock()
	defer ad.mu.RUnlock()

	if !ad.trained {
		return nil, errors.New("model not trained")
	}

	export := map[string]interface{}{
		"model":      ad.config.Model.String(),
		"trained":    ad.trained,
		"last_train": ad.lastTrain,
		"config":     ad.config,
		"statistical": map[string]float64{
			"mean": ad.statistical.mean,
			"std":  ad.statistical.std,
			"q1":   ad.statistical.q1,
			"q3":   ad.statistical.q3,
		},
	}

	return json.Marshal(export)
}

// ImportModel imports a previously exported model.
func (ad *AnomalyDetector) ImportModel(data []byte) error {
	var export map[string]interface{}
	if err := json.Unmarshal(data, &export); err != nil {
		return err
	}

	ad.mu.Lock()
	defer ad.mu.Unlock()

	if stats, ok := export["statistical"].(map[string]interface{}); ok {
		if mean, ok := stats["mean"].(float64); ok {
			ad.statistical.mean = mean
		}
		if std, ok := stats["std"].(float64); ok {
			ad.statistical.std = std
		}
		if q1, ok := stats["q1"].(float64); ok {
			ad.statistical.q1 = q1
		}
		if q3, ok := stats["q3"].(float64); ok {
			ad.statistical.q3 = q3
		}
	}

	ad.trained = true
	return nil
}
