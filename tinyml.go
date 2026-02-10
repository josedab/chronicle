package chronicle

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
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

// SimpleExponentialSmoothingModel implements exponential smoothing forecasting.
type SimpleExponentialSmoothingModel struct {
	name     string
	alpha    float64
	level    float64
	trained  bool
	lastData []float64
}

// NewSimpleExponentialSmoothingModel creates a new SES model.
func NewSimpleExponentialSmoothingModel(name string, alpha float64) *SimpleExponentialSmoothingModel {
	if alpha <= 0 || alpha >= 1 {
		alpha = 0.3 // Default smoothing factor
	}
	return &SimpleExponentialSmoothingModel{
		name:  name,
		alpha: alpha,
	}
}

func (m *SimpleExponentialSmoothingModel) Name() string      { return m.name }
func (m *SimpleExponentialSmoothingModel) Type() MLModelType { return MLModelTypeForecaster }

// Train trains the model on historical data.
func (m *SimpleExponentialSmoothingModel) Train(data []float64) error {
	if len(data) < 2 {
		return errors.New("insufficient training data")
	}

	m.lastData = make([]float64, len(data))
	copy(m.lastData, data)

	// Initialize level with first observation
	m.level = data[0]

	// Update level through all observations
	for i := 1; i < len(data); i++ {
		m.level = m.alpha*data[i] + (1-m.alpha)*m.level
	}

	m.trained = true
	return nil
}

// Predict returns forecasted values.
func (m *SimpleExponentialSmoothingModel) Predict(input []float64) ([]float64, error) {
	if !m.trained {
		return nil, errors.New("model not trained")
	}

	// Input represents the number of periods to forecast
	periods := 1
	if len(input) > 0 {
		periods = int(input[0])
	}
	if periods <= 0 {
		periods = 1
	}

	forecasts := make([]float64, periods)
	for i := 0; i < periods; i++ {
		forecasts[i] = m.level
	}

	return forecasts, nil
}

// Serialize serializes the model.
func (m *SimpleExponentialSmoothingModel) Serialize() ([]byte, error) {
	return json.Marshal(map[string]any{
		"name":    m.name,
		"alpha":   m.alpha,
		"level":   m.level,
		"trained": m.trained,
	})
}

// Deserialize deserializes the model.
func (m *SimpleExponentialSmoothingModel) Deserialize(data []byte) error {
	var state map[string]any
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if name, ok := state["name"].(string); ok {
		m.name = name
	}
	if alpha, ok := state["alpha"].(float64); ok {
		m.alpha = alpha
	}
	if level, ok := state["level"].(float64); ok {
		m.level = level
	}
	if trained, ok := state["trained"].(bool); ok {
		m.trained = trained
	}
	return nil
}

// KMeansModel implements k-means clustering for pattern classification.
type KMeansModel struct {
	name      string
	k         int
	maxIter   int
	centroids [][]float64
	trained   bool
}

// NewKMeansModel creates a new K-Means model.
func NewKMeansModel(name string, k, maxIter int) *KMeansModel {
	if k <= 0 {
		k = 3
	}
	if maxIter <= 0 {
		maxIter = 100
	}
	return &KMeansModel{
		name:    name,
		k:       k,
		maxIter: maxIter,
	}
}

func (m *KMeansModel) Name() string      { return m.name }
func (m *KMeansModel) Type() MLModelType { return MLModelTypeClassifier }

// Train trains the k-means model.
func (m *KMeansModel) Train(data []float64) error {
	if len(data) < m.k {
		return errors.New("insufficient training data")
	}

	// Convert to 2D (using sliding window)
	windowSize := 3
	features := make([][]float64, 0)
	for i := 0; i <= len(data)-windowSize; i++ {
		feat := make([]float64, windowSize)
		copy(feat, data[i:i+windowSize])
		features = append(features, feat)
	}

	if len(features) < m.k {
		return errors.New("insufficient features after windowing")
	}

	// Initialize centroids randomly
	m.centroids = make([][]float64, m.k)
	step := len(features) / m.k
	for i := 0; i < m.k; i++ {
		m.centroids[i] = make([]float64, windowSize)
		copy(m.centroids[i], features[i*step])
	}

	// Run k-means iterations
	for iter := 0; iter < m.maxIter; iter++ {
		// Assign points to clusters
		clusters := make([][]int, m.k)
		for i := range clusters {
			clusters[i] = make([]int, 0)
		}

		for i, feat := range features {
			minDist := math.MaxFloat64
			minCluster := 0
			for j, centroid := range m.centroids {
				dist := tinyMLEuclideanDistance(feat, centroid)
				if dist < minDist {
					minDist = dist
					minCluster = j
				}
			}
			clusters[minCluster] = append(clusters[minCluster], i)
		}

		// Update centroids
		newCentroids := make([][]float64, m.k)
		for i := range newCentroids {
			newCentroids[i] = make([]float64, windowSize)
			if len(clusters[i]) > 0 {
				for _, idx := range clusters[i] {
					for j := 0; j < windowSize; j++ {
						newCentroids[i][j] += features[idx][j]
					}
				}
				for j := 0; j < windowSize; j++ {
					newCentroids[i][j] /= float64(len(clusters[i]))
				}
			} else {
				copy(newCentroids[i], m.centroids[i])
			}
		}
		m.centroids = newCentroids
	}

	m.trained = true
	return nil
}

// Predict returns cluster assignments.
func (m *KMeansModel) Predict(input []float64) ([]float64, error) {
	if !m.trained {
		return nil, errors.New("model not trained")
	}

	windowSize := len(m.centroids[0])
	if len(input) < windowSize {
		return nil, errors.New("input too short")
	}

	results := make([]float64, len(input)-windowSize+1)
	for i := 0; i <= len(input)-windowSize; i++ {
		feat := input[i : i+windowSize]
		minDist := math.MaxFloat64
		minCluster := 0
		for j, centroid := range m.centroids {
			dist := tinyMLEuclideanDistance(feat, centroid)
			if dist < minDist {
				minDist = dist
				minCluster = j
			}
		}
		results[i] = float64(minCluster)
	}

	return results, nil
}

// tinyMLEuclideanDistance calculates Euclidean distance between two vectors.
func tinyMLEuclideanDistance(a, b []float64) float64 {
	sum := 0.0
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return math.Sqrt(sum)
}

// Serialize serializes the model.
func (m *KMeansModel) Serialize() ([]byte, error) {
	return json.Marshal(map[string]any{
		"name":      m.name,
		"k":         m.k,
		"max_iter":  m.maxIter,
		"centroids": m.centroids,
		"trained":   m.trained,
	})
}

// Deserialize deserializes the model.
func (m *KMeansModel) Deserialize(data []byte) error {
	var state map[string]any
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if name, ok := state["name"].(string); ok {
		m.name = name
	}
	if k, ok := state["k"].(float64); ok {
		m.k = int(k)
	}
	if maxIter, ok := state["max_iter"].(float64); ok {
		m.maxIter = int(maxIter)
	}
	if trained, ok := state["trained"].(bool); ok {
		m.trained = trained
	}
	return nil
}

// StatisticalAnomalyDetector uses statistical methods for anomaly detection.
type StatisticalAnomalyDetector struct {
	name      string
	mean      float64
	stdDev    float64
	threshold float64
	trained   bool
}

// NewStatisticalAnomalyDetector creates a statistical anomaly detector.
func NewStatisticalAnomalyDetector(name string, threshold float64) *StatisticalAnomalyDetector {
	if threshold <= 0 {
		threshold = 2.0
	}
	return &StatisticalAnomalyDetector{
		name:      name,
		threshold: threshold,
	}
}

func (m *StatisticalAnomalyDetector) Name() string      { return m.name }
func (m *StatisticalAnomalyDetector) Type() MLModelType { return MLModelTypeAnomalyDetector }

// Train calculates mean and standard deviation.
func (m *StatisticalAnomalyDetector) Train(data []float64) error {
	if len(data) < 2 {
		return errors.New("insufficient training data")
	}

	// Calculate mean
	sum := 0.0
	for _, v := range data {
		sum += v
	}
	m.mean = sum / float64(len(data))

	// Calculate standard deviation
	sumSq := 0.0
	for _, v := range data {
		diff := v - m.mean
		sumSq += diff * diff
	}
	m.stdDev = math.Sqrt(sumSq / float64(len(data)))

	if m.stdDev == 0 {
		m.stdDev = 1 // Avoid division by zero
	}

	m.trained = true
	return nil
}

// Predict returns z-scores for each point.
func (m *StatisticalAnomalyDetector) Predict(input []float64) ([]float64, error) {
	if !m.trained {
		return nil, errors.New("model not trained")
	}

	scores := make([]float64, len(input))
	for i, v := range input {
		zScore := math.Abs(v-m.mean) / m.stdDev
		scores[i] = zScore
	}

	return scores, nil
}

// Serialize serializes the model.
func (m *StatisticalAnomalyDetector) Serialize() ([]byte, error) {
	return json.Marshal(map[string]any{
		"name":      m.name,
		"mean":      m.mean,
		"std_dev":   m.stdDev,
		"threshold": m.threshold,
		"trained":   m.trained,
	})
}

// Deserialize deserializes the model.
func (m *StatisticalAnomalyDetector) Deserialize(data []byte) error {
	var state map[string]any
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if name, ok := state["name"].(string); ok {
		m.name = name
	}
	if mean, ok := state["mean"].(float64); ok {
		m.mean = mean
	}
	if stdDev, ok := state["std_dev"].(float64); ok {
		m.stdDev = stdDev
	}
	if threshold, ok := state["threshold"].(float64); ok {
		m.threshold = threshold
	}
	if trained, ok := state["trained"].(bool); ok {
		m.trained = trained
	}
	return nil
}

// MedianAbsoluteDeviationDetector uses MAD for robust anomaly detection.
type MedianAbsoluteDeviationDetector struct {
	name      string
	median    float64
	mad       float64
	threshold float64
	trained   bool
}

// NewMADDetector creates a MAD-based anomaly detector.
func NewMADDetector(name string, threshold float64) *MedianAbsoluteDeviationDetector {
	if threshold <= 0 {
		threshold = 3.0
	}
	return &MedianAbsoluteDeviationDetector{
		name:      name,
		threshold: threshold,
	}
}

func (m *MedianAbsoluteDeviationDetector) Name() string      { return m.name }
func (m *MedianAbsoluteDeviationDetector) Type() MLModelType { return MLModelTypeAnomalyDetector }

// Train calculates median and MAD.
func (m *MedianAbsoluteDeviationDetector) Train(data []float64) error {
	if len(data) < 2 {
		return errors.New("insufficient training data")
	}

	// Calculate median
	sorted := make([]float64, len(data))
	copy(sorted, data)
	sort.Float64s(sorted)
	m.median = sorted[len(sorted)/2]

	// Calculate MAD
	deviations := make([]float64, len(data))
	for i, v := range data {
		deviations[i] = math.Abs(v - m.median)
	}
	sort.Float64s(deviations)
	m.mad = deviations[len(deviations)/2]

	if m.mad == 0 {
		m.mad = 1 // Avoid division by zero
	}

	m.trained = true
	return nil
}

// Predict returns modified z-scores.
func (m *MedianAbsoluteDeviationDetector) Predict(input []float64) ([]float64, error) {
	if !m.trained {
		return nil, errors.New("model not trained")
	}

	// Modified z-score = 0.6745 * (x - median) / MAD
	scores := make([]float64, len(input))
	for i, v := range input {
		scores[i] = math.Abs(0.6745 * (v - m.median) / m.mad)
	}

	return scores, nil
}

// Serialize serializes the model.
func (m *MedianAbsoluteDeviationDetector) Serialize() ([]byte, error) {
	return json.Marshal(map[string]any{
		"name":      m.name,
		"median":    m.median,
		"mad":       m.mad,
		"threshold": m.threshold,
		"trained":   m.trained,
	})
}

// Deserialize deserializes the model.
func (m *MedianAbsoluteDeviationDetector) Deserialize(data []byte) error {
	var state map[string]any
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if name, ok := state["name"].(string); ok {
		m.name = name
	}
	if median, ok := state["median"].(float64); ok {
		m.median = median
	}
	if mad, ok := state["mad"].(float64); ok {
		m.mad = mad
	}
	if threshold, ok := state["threshold"].(float64); ok {
		m.threshold = threshold
	}
	if trained, ok := state["trained"].(bool); ok {
		m.trained = trained
	}
	return nil
}
