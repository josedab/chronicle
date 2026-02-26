//go:build !nostubs

package chronicle

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
)

// EXPERIMENTAL: This API is unstable and may change without notice.
// TinyMLEngine provides embedded machine learning inference for time-series data.
// It supports on-device anomaly detection, forecasting, and pattern recognition
// without requiring external ML infrastructure.
type TinyMLEngine struct {
	db      *DB
	config  TinyMLConfig
	models  map[string]MLModel
	modelMu sync.RWMutex
}

// TinyMLConfig configures the TinyML inference engine.
type TinyMLConfig struct {
	// Enabled enables the TinyML engine.
	Enabled bool

	// MaxModelSize is the maximum model size in bytes.
	MaxModelSize int64

	// MaxModels is the maximum number of loaded models.
	MaxModels int

	// DefaultThreshold is the default anomaly detection threshold.
	DefaultThreshold float64

	// InferenceTimeout is the timeout for inference operations.
	InferenceTimeout int64 // milliseconds
}

// DefaultTinyMLConfig returns default TinyML configuration.
func DefaultTinyMLConfig() TinyMLConfig {
	return TinyMLConfig{
		Enabled:          true,
		MaxModelSize:     10 * 1024 * 1024, // 10MB
		MaxModels:        100,
		DefaultThreshold: 2.0,  // 2 standard deviations
		InferenceTimeout: 1000, // 1 second
	}
}

// MLModel is the interface for machine learning models.
type MLModel interface {
	// Name returns the model name.
	Name() string
	// Type returns the model type.
	Type() MLModelType
	// Predict runs inference on input data.
	Predict(input []float64) ([]float64, error)
	// Train trains the model on data.
	Train(data []float64) error
	// Serialize returns the model as bytes.
	Serialize() ([]byte, error)
	// Deserialize loads the model from bytes.
	Deserialize(data []byte) error
}

// MLModelType represents the type of ML model.
type MLModelType int

const (
	// MLModelTypeAnomalyDetector detects anomalies in time-series.
	MLModelTypeAnomalyDetector MLModelType = iota
	// MLModelTypeForecaster predicts future values.
	MLModelTypeForecaster
	// MLModelTypeClassifier classifies patterns.
	MLModelTypeClassifier
	// MLModelTypeRegressor performs regression.
	MLModelTypeRegressor
)

func (t MLModelType) String() string {
	switch t {
	case MLModelTypeAnomalyDetector:
		return "anomaly_detector"
	case MLModelTypeForecaster:
		return "forecaster"
	case MLModelTypeClassifier:
		return "classifier"
	case MLModelTypeRegressor:
		return "regressor"
	default:
		return "unknown"
	}
}

// NewTinyMLEngine creates a new TinyML engine.
func NewTinyMLEngine(db *DB, config TinyMLConfig) *TinyMLEngine {
	return &TinyMLEngine{
		db:     db,
		config: config,
		models: make(map[string]MLModel),
	}
}

// RegisterModel registers a model with the engine.
func (e *TinyMLEngine) RegisterModel(model MLModel) error {
	e.modelMu.Lock()
	defer e.modelMu.Unlock()

	if len(e.models) >= e.config.MaxModels {
		return errors.New("maximum models reached")
	}

	e.models[model.Name()] = model
	return nil
}

// UnregisterModel removes a model from the engine.
func (e *TinyMLEngine) UnregisterModel(name string) {
	e.modelMu.Lock()
	defer e.modelMu.Unlock()
	delete(e.models, name)
}

// GetModel returns a model by name.
func (e *TinyMLEngine) GetModel(name string) (MLModel, bool) {
	e.modelMu.RLock()
	defer e.modelMu.RUnlock()
	model, ok := e.models[name]
	return model, ok
}

// ListModels returns all registered models.
func (e *TinyMLEngine) ListModels() []string {
	e.modelMu.RLock()
	defer e.modelMu.RUnlock()

	names := make([]string, 0, len(e.models))
	for name := range e.models {
		names = append(names, name)
	}
	return names
}

// Infer runs inference using a specific model.
func (e *TinyMLEngine) Infer(modelName string, input []float64) ([]float64, error) {
	model, ok := e.GetModel(modelName)
	if !ok {
		return nil, fmt.Errorf("model not found: %s", modelName)
	}
	return model.Predict(input)
}

// DetectAnomalies runs anomaly detection on a metric.
func (e *TinyMLEngine) DetectAnomalies(metric string, start, end int64) (*TinyMLAnomalyResult, error) {
	// Query data
	result, err := e.db.Execute(&Query{
		Metric: metric,
		Start:  start,
		End:    end,
	})
	if err != nil {
		return nil, err
	}

	if len(result.Points) < 10 {
		return &TinyMLAnomalyResult{}, nil
	}

	values := make([]float64, len(result.Points))
	for i, p := range result.Points {
		values[i] = p.Value
	}

	// Run anomaly detection
	detector := NewIsolationForestModel("auto_detector", 100, 256)
	if err := detector.Train(values); err != nil {
		return nil, err
	}

	scores, err := detector.Predict(values)
	if err != nil {
		return nil, err
	}

	// Identify anomalies
	threshold := e.config.DefaultThreshold
	anomalies := make([]TinyMLAnomalyPoint, 0)

	for i, score := range scores {
		if score > threshold {
			anomalies = append(anomalies, TinyMLAnomalyPoint{
				Timestamp: result.Points[i].Timestamp,
				Value:     result.Points[i].Value,
				Score:     score,
				Tags:      result.Points[i].Tags,
			})
		}
	}

	return &TinyMLAnomalyResult{
		Metric:    metric,
		Start:     start,
		End:       end,
		Total:     len(result.Points),
		Anomalies: anomalies,
		Threshold: threshold,
	}, nil
}

// TinyMLAnomalyResult represents anomaly detection results from TinyML models.
type TinyMLAnomalyResult struct {
	Metric    string               `json:"metric"`
	Start     int64                `json:"start"`
	End       int64                `json:"end"`
	Total     int                  `json:"total"`
	Anomalies []TinyMLAnomalyPoint `json:"anomalies"`
	Threshold float64              `json:"threshold"`
}

// TinyMLAnomalyPoint represents an anomalous data point detected by TinyML.
type TinyMLAnomalyPoint struct {
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Score     float64           `json:"score"`
	Tags      map[string]string `json:"tags,omitempty"`
}

// IsolationForestModel implements isolation forest for anomaly detection.
type IsolationForestModel struct {
	name       string
	numTrees   int
	sampleSize int
	trees      []*isolationTree
	trained    bool
}

// isolationTree represents a single isolation tree.
type isolationTree struct {
	root *isolationNode
}

// isolationNode represents a node in an isolation tree.
type isolationNode struct {
	splitFeature int
	splitValue   float64
	left         *isolationNode
	right        *isolationNode
	size         int
	isLeaf       bool
}

// NewIsolationForestModel creates a new Isolation Forest model.
func NewIsolationForestModel(name string, numTrees, sampleSize int) *IsolationForestModel {
	return &IsolationForestModel{
		name:       name,
		numTrees:   numTrees,
		sampleSize: sampleSize,
	}
}

func (m *IsolationForestModel) Name() string      { return m.name }
func (m *IsolationForestModel) Type() MLModelType { return MLModelTypeAnomalyDetector }

// Train trains the isolation forest on data.
func (m *IsolationForestModel) Train(data []float64) error {
	if len(data) < 2 {
		return errors.New("insufficient training data")
	}

	// Convert to 2D for multi-feature support (here we use sliding window)
	windowSize := 5
	features := m.createFeatures(data, windowSize)
	if len(features) < 10 {
		return errors.New("insufficient data for windowed features")
	}

	maxDepth := int(math.Ceil(math.Log2(float64(m.sampleSize))))
	m.trees = make([]*isolationTree, m.numTrees)

	for i := 0; i < m.numTrees; i++ {
		sample := m.subsample(features, m.sampleSize)
		m.trees[i] = m.buildTree(sample, 0, maxDepth)
	}

	m.trained = true
	return nil
}

// Predict returns anomaly scores for each point.
func (m *IsolationForestModel) Predict(input []float64) ([]float64, error) {
	if !m.trained {
		return nil, errors.New("model not trained")
	}

	windowSize := 5
	features := m.createFeatures(input, windowSize)
	scores := make([]float64, len(input))

	// First windowSize-1 points get score 0
	for i := 0; i < windowSize-1 && i < len(scores); i++ {
		scores[i] = 0
	}

	// Calculate anomaly scores for each feature vector
	for i, feat := range features {
		avgPathLength := 0.0
		for _, tree := range m.trees {
			avgPathLength += m.pathLength(feat, tree.root, 0)
		}
		avgPathLength /= float64(len(m.trees))

		// Normalize score
		c := m.avgPathLength(float64(m.sampleSize))
		scores[i+windowSize-1] = math.Pow(2, -avgPathLength/c)
	}

	return scores, nil
}

// createFeatures creates sliding window features from time-series data.
func (m *IsolationForestModel) createFeatures(data []float64, windowSize int) [][]float64 {
	if len(data) < windowSize {
		return nil
	}

	features := make([][]float64, len(data)-windowSize+1)
	for i := 0; i <= len(data)-windowSize; i++ {
		features[i] = make([]float64, windowSize)
		copy(features[i], data[i:i+windowSize])
	}
	return features
}

// subsample randomly samples from features.
func (m *IsolationForestModel) subsample(features [][]float64, size int) [][]float64 {
	if len(features) <= size {
		return features
	}

	// Simple random sampling without replacement
	indices := make([]int, len(features))
	for i := range indices {
		indices[i] = i
	}

	// Fisher-Yates shuffle (first size elements)
	for i := 0; i < size; i++ {
		j := i + int(float64(len(features)-i)*pseudoRandom(i))
		indices[i], indices[j] = indices[j], indices[i]
	}

	sample := make([][]float64, size)
	for i := 0; i < size; i++ {
		sample[i] = features[indices[i]]
	}
	return sample
}

// pseudoRandom generates a deterministic pseudo-random number for reproducibility.
func pseudoRandom(seed int) float64 {
	x := uint32(seed + 1)
	x ^= x << 13
	x ^= x >> 17
	x ^= x << 5
	return float64(x) / float64(math.MaxUint32)
}

// buildTree builds an isolation tree.
func (m *IsolationForestModel) buildTree(data [][]float64, depth, maxDepth int) *isolationTree {
	root := m.buildNode(data, depth, maxDepth)
	return &isolationTree{root: root}
}

// buildNode recursively builds a node.
func (m *IsolationForestModel) buildNode(data [][]float64, depth, maxDepth int) *isolationNode {
	if len(data) <= 1 || depth >= maxDepth {
		return &isolationNode{
			isLeaf: true,
			size:   len(data),
		}
	}

	numFeatures := len(data[0])
	feature := int(pseudoRandom(depth*1000+len(data))) * numFeatures % numFeatures
	if feature < 0 {
		feature = 0
	}

	// Find min and max for the selected feature
	minVal, maxVal := data[0][feature], data[0][feature]
	for _, d := range data {
		if d[feature] < minVal {
			minVal = d[feature]
		}
		if d[feature] > maxVal {
			maxVal = d[feature]
		}
	}

	if minVal == maxVal {
		return &isolationNode{
			isLeaf: true,
			size:   len(data),
		}
	}

	// Random split point
	splitValue := minVal + pseudoRandom(depth*2000+len(data))*(maxVal-minVal)

	// Split data
	var leftData, rightData [][]float64
	for _, d := range data {
		if d[feature] < splitValue {
			leftData = append(leftData, d)
		} else {
			rightData = append(rightData, d)
		}
	}

	// Ensure we actually split
	if len(leftData) == 0 || len(rightData) == 0 {
		return &isolationNode{
			isLeaf: true,
			size:   len(data),
		}
	}

	return &isolationNode{
		splitFeature: feature,
		splitValue:   splitValue,
		left:         m.buildNode(leftData, depth+1, maxDepth),
		right:        m.buildNode(rightData, depth+1, maxDepth),
		size:         len(data),
		isLeaf:       false,
	}
}

// pathLength calculates the path length for a sample.
func (m *IsolationForestModel) pathLength(sample []float64, node *isolationNode, depth int) float64 {
	if node == nil || node.isLeaf {
		if node != nil && node.size > 1 {
			return float64(depth) + m.avgPathLength(float64(node.size))
		}
		return float64(depth)
	}

	if sample[node.splitFeature] < node.splitValue {
		return m.pathLength(sample, node.left, depth+1)
	}
	return m.pathLength(sample, node.right, depth+1)
}

// avgPathLength calculates the average path length for n samples.
func (m *IsolationForestModel) avgPathLength(n float64) float64 {
	if n <= 1 {
		return 0
	}
	if n == 2 {
		return 1
	}
	// H(n-1) approximation using Euler's constant
	return 2*(math.Log(n-1)+0.5772156649) - 2*(n-1)/n
}

// Serialize serializes the model.
func (m *IsolationForestModel) Serialize() ([]byte, error) {
	return json.Marshal(map[string]any{
		"name":        m.name,
		"num_trees":   m.numTrees,
		"sample_size": m.sampleSize,
		"trained":     m.trained,
	})
}

// Deserialize deserializes the model.
func (m *IsolationForestModel) Deserialize(data []byte) error {
	var state map[string]any
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if name, ok := state["name"].(string); ok {
		m.name = name
	}
	if numTrees, ok := state["num_trees"].(float64); ok {
		m.numTrees = int(numTrees)
	}
	if sampleSize, ok := state["sample_size"].(float64); ok {
		m.sampleSize = int(sampleSize)
	}
	return nil
}
