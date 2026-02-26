//go:build !nostubs

package chronicle

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
)

// Exponential smoothing and K-Means model implementations for TinyML.

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
