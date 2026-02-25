package chronicle

import (
	"encoding/json"
	"math"
	"net/http"
	"sync"
	"time"
)

// DBSCAN, Isolation Forest, online learning, and multi-channel alert routing algorithms for anomaly detection v2.

// --- DBSCAN Multivariate Anomaly Detection ---

// DBSCANConfig configures the DBSCAN clustering algorithm.
type DBSCANConfig struct {
	Epsilon   float64 // neighborhood radius
	MinPoints int     // minimum points to form a cluster
}

// DBSCANResult holds the output of a DBSCAN clustering run.
type DBSCANResult struct {
	Clusters    [][]int // indices of points in each cluster
	Noise       []int   // indices of noise points (anomalies)
	NumClusters int
}

// DBSCAN performs density-based spatial clustering for multivariate anomaly detection.
// Points not belonging to any cluster are classified as anomalies.
func DBSCAN(points [][]float64, config DBSCANConfig) *DBSCANResult {
	n := len(points)
	if n == 0 {
		return &DBSCANResult{}
	}

	labels := make([]int, n) // -1 = unvisited, 0 = noise, >0 = cluster ID
	for i := range labels {
		labels[i] = -1
	}

	clusterID := 0
	for i := 0; i < n; i++ {
		if labels[i] != -1 {
			continue
		}

		neighbors := regionQuery(points, i, config.Epsilon)
		if len(neighbors) < config.MinPoints {
			labels[i] = 0 // noise
			continue
		}

		clusterID++
		labels[i] = clusterID
		seeds := make([]int, len(neighbors))
		copy(seeds, neighbors)

		for j := 0; j < len(seeds); j++ {
			q := seeds[j]
			if q == i {
				continue
			}
			if labels[q] == 0 {
				labels[q] = clusterID
			}
			if labels[q] != -1 {
				continue
			}
			labels[q] = clusterID

			qNeighbors := regionQuery(points, q, config.Epsilon)
			if len(qNeighbors) >= config.MinPoints {
				seeds = append(seeds, qNeighbors...)
			}
		}
	}

	// Build result
	result := &DBSCANResult{NumClusters: clusterID}
	clusterMap := make(map[int][]int)
	for i, label := range labels {
		if label == 0 {
			result.Noise = append(result.Noise, i)
		} else if label > 0 {
			clusterMap[label] = append(clusterMap[label], i)
		}
	}
	for _, indices := range clusterMap {
		result.Clusters = append(result.Clusters, indices)
	}
	return result
}

func regionQuery(points [][]float64, idx int, epsilon float64) []int {
	var neighbors []int
	for i := range points {
		if euclideanDistF64(points[idx], points[i]) <= epsilon {
			neighbors = append(neighbors, i)
		}
	}
	return neighbors
}

func euclideanDistF64(a, b []float64) float64 {
	var sum float64
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		d := a[i] - b[i]
		sum += d * d
	}
	return math.Sqrt(sum)
}

// --- Isolation Forest for High-Cardinality Anomaly Detection ---

// IForestDetector implements the Isolation Forest algorithm for anomaly detection.
type IForestDetector struct {
	iTrees      []*iforestTree
	iNumTrees   int
	iSampleSize int
	iThreshold  float64 // anomaly score threshold (0-1)
}

type iforestTree struct {
	left      *iforestTree
	right     *iforestTree
	splitAttr int
	splitVal  float64
	size      int
	depth     int
}

// NewIsolationForest creates a new Isolation Forest.
func NewIForestDetector(numTrees, sampleSize int, threshold float64) *IForestDetector {
	if numTrees <= 0 {
		numTrees = 100
	}
	if sampleSize <= 0 {
		sampleSize = 256
	}
	if threshold <= 0 || threshold > 1 {
		threshold = 0.6
	}
	return &IForestDetector{
		iNumTrees:   numTrees,
		iSampleSize: sampleSize,
		iThreshold:  threshold,
	}
}

// Fit builds the isolation forest from training data.
func (f *IForestDetector) Fit(data [][]float64) {
	if len(data) == 0 {
		return
	}
	maxDepth := int(math.Ceil(math.Log2(float64(f.iSampleSize))))
	f.iTrees = make([]*iforestTree, f.iNumTrees)

	for i := 0; i < f.iNumTrees; i++ {
		// Subsample
		sample := subsample(data, f.iSampleSize)
		f.iTrees[i] = buildITree(sample, 0, maxDepth)
	}
}

// Score computes the anomaly score for a point (higher = more anomalous).
func (f *IForestDetector) Score(point []float64) float64 {
	if len(f.iTrees) == 0 {
		return 0
	}

	var totalDepth float64
	for _, tree := range f.iTrees {
		totalDepth += float64(pathLength(tree, point, 0))
	}
	avgDepth := totalDepth / float64(len(f.iTrees))

	// Normalize using the expected path length
	n := float64(f.iSampleSize)
	c := 2.0*(math.Log(n-1.0)+0.5772156649) - (2.0 * (n - 1.0) / n)
	if c == 0 {
		return 0
	}
	return math.Pow(2.0, -avgDepth/c)
}

// IsAnomaly returns true if the point's anomaly score exceeds the threshold.
func (f *IForestDetector) IsAnomaly(point []float64) bool {
	return f.Score(point) > f.iThreshold
}

func buildITree(data [][]float64, depth, maxDepth int) *iforestTree {
	node := &iforestTree{size: len(data), depth: depth}
	if len(data) <= 1 || depth >= maxDepth {
		return node
	}

	dims := len(data[0])
	if dims == 0 {
		return node
	}

	// Pick random attribute and split value
	attr := depth % dims // deterministic for reproducibility
	minVal, maxVal := data[0][attr], data[0][attr]
	for _, d := range data[1:] {
		if attr < len(d) {
			if d[attr] < minVal {
				minVal = d[attr]
			}
			if d[attr] > maxVal {
				maxVal = d[attr]
			}
		}
	}

	if minVal == maxVal {
		return node
	}

	splitVal := (minVal + maxVal) / 2.0
	node.splitAttr = attr
	node.splitVal = splitVal

	var left, right [][]float64
	for _, d := range data {
		if attr < len(d) && d[attr] < splitVal {
			left = append(left, d)
		} else {
			right = append(right, d)
		}
	}

	if len(left) > 0 {
		node.left = buildITree(left, depth+1, maxDepth)
	}
	if len(right) > 0 {
		node.right = buildITree(right, depth+1, maxDepth)
	}
	return node
}

func pathLength(node *iforestTree, point []float64, depth int) int {
	if node == nil || (node.left == nil && node.right == nil) {
		if node != nil && node.size > 1 {
			return depth + int(math.Ceil(math.Log2(float64(node.size))))
		}
		return depth
	}

	if node.splitAttr < len(point) && point[node.splitAttr] < node.splitVal {
		return pathLength(node.left, point, depth+1)
	}
	return pathLength(node.right, point, depth+1)
}

func subsample(data [][]float64, n int) [][]float64 {
	if len(data) <= n {
		result := make([][]float64, len(data))
		copy(result, data)
		return result
	}
	// Take first n items (deterministic for reproducibility)
	result := make([][]float64, n)
	copy(result, data[:n])
	return result
}

// --- Online Learning for Drift Adaptation ---

// OnlineLearner adapts anomaly thresholds in real-time based on incoming data.
type OnlineLearner struct {
	mu            sync.RWMutex
	mean          float64
	variance      float64
	count         int64
	learningRate  float64
	driftDetector *DriftDetector
}

// DriftDetector monitors for concept drift in streaming data.
type DriftDetector struct {
	windowSize   int
	recentErrors []float64
	baselineErr  float64
	driftCount   int64
}

// NewOnlineLearner creates a new online learning component.
func NewOnlineLearner(learningRate float64) *OnlineLearner {
	if learningRate <= 0 || learningRate > 1 {
		learningRate = 0.01
	}
	return &OnlineLearner{
		learningRate: learningRate,
		driftDetector: &DriftDetector{
			windowSize: 100,
		},
	}
}

// Update incorporates a new data point using exponential weighted moving average.
func (ol *OnlineLearner) Update(value float64) {
	ol.mu.Lock()
	defer ol.mu.Unlock()

	ol.count++
	if ol.count == 1 {
		ol.mean = value
		ol.variance = 0
		return
	}

	// Welford's online algorithm
	delta := value - ol.mean
	ol.mean += ol.learningRate * delta
	delta2 := value - ol.mean
	ol.variance = (1-ol.learningRate)*ol.variance + ol.learningRate*delta*delta2
}

// Predict returns the expected value and standard deviation.
func (ol *OnlineLearner) Predict() (mean, stddev float64) {
	ol.mu.RLock()
	defer ol.mu.RUnlock()
	return ol.mean, math.Sqrt(math.Max(0, ol.variance))
}

// DetectDrift checks if concept drift has occurred.
func (ol *OnlineLearner) DetectDrift(predictionError float64) bool {
	ol.mu.Lock()
	defer ol.mu.Unlock()

	dd := ol.driftDetector
	dd.recentErrors = append(dd.recentErrors, math.Abs(predictionError))
	if len(dd.recentErrors) > dd.windowSize {
		dd.recentErrors = dd.recentErrors[1:]
	}

	if len(dd.recentErrors) < dd.windowSize/2 {
		return false
	}

	// Compare recent error rate to baseline
	var recentSum float64
	for _, e := range dd.recentErrors {
		recentSum += e
	}
	recentAvg := recentSum / float64(len(dd.recentErrors))

	if dd.baselineErr == 0 {
		dd.baselineErr = recentAvg
		return false
	}

	// Drift detected if recent error is 2x baseline
	if recentAvg > 2*dd.baselineErr {
		dd.driftCount++
		dd.baselineErr = recentAvg // adapt
		return true
	}

	// Slowly adapt baseline
	dd.baselineErr = 0.95*dd.baselineErr + 0.05*recentAvg
	return false
}

// DriftCount returns the number of drift events detected.
func (ol *OnlineLearner) DriftCount() int64 {
	ol.mu.RLock()
	defer ol.mu.RUnlock()
	return ol.driftDetector.driftCount
}

// --- Multi-Channel Alert Routing ---

// AlertChannel defines a destination for anomaly alerts.
type AlertChannel struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // "webhook", "email", "slack", "pagerduty"
	Endpoint string `json:"endpoint"`
	Severity string `json:"min_severity"` // minimum severity to trigger
	Enabled  bool   `json:"enabled"`
}

// MultiChannelRouter routes anomaly alerts to multiple channels based on severity.
type MultiChannelRouter struct {
	mu       sync.RWMutex
	channels []AlertChannel
	stats    MultiChannelStats
}

// MultiChannelStats tracks alert routing metrics.
type MultiChannelStats struct {
	TotalAlerts      int64            `json:"total_alerts"`
	AlertsByChannel  map[string]int64 `json:"alerts_by_channel"`
	AlertsBySeverity map[string]int64 `json:"alerts_by_severity"`
}

// NewMultiChannelRouter creates a new multi-channel alert router.
func NewMultiChannelRouter(channels []AlertChannel) *MultiChannelRouter {
	return &MultiChannelRouter{
		channels: channels,
		stats: MultiChannelStats{
			AlertsByChannel:  make(map[string]int64),
			AlertsBySeverity: make(map[string]int64),
		},
	}
}

// Route sends an anomaly alert to all matching channels.
func (r *MultiChannelRouter) Route(anomaly *AnomalyV2) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.stats.TotalAlerts++
	r.stats.AlertsBySeverity[anomaly.Severity]++

	var routed []string
	severityOrder := map[string]int{"info": 0, "warning": 1, "critical": 2}
	anomalySev := severityOrder[anomaly.Severity]

	for _, ch := range r.channels {
		if !ch.Enabled {
			continue
		}
		channelMinSev := severityOrder[ch.Severity]
		if anomalySev >= channelMinSev {
			r.stats.AlertsByChannel[ch.Name]++
			routed = append(routed, ch.Name)
		}
	}
	return routed
}

// Stats returns alert routing statistics.
func (r *MultiChannelRouter) Stats() MultiChannelStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stats
}

func (e *AnomalyDetectionV2Engine) detectionLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			// Periodic threshold recalibration based on feedback
			e.mu.Lock()
			for metric, threshold := range e.thresholds {
				total := threshold.TruePositives + threshold.FalsePositives
				if total > 10 {
					fpRate := float64(threshold.FalsePositives) / float64(total)
					if fpRate > e.config.MaxFalsePositiveRate {
						threshold.Upper *= 1.05
						threshold.Lower *= 0.95
						threshold.Confidence = math.Max(0.1, threshold.Confidence-0.05)
					}
				}
				_ = metric //nolint:errcheck // metric used for iteration
			}
			e.mu.Unlock()
		}
	}
}

// AsPostWriteHook returns a WriteHook that feeds points into the anomaly detector.
func (e *AnomalyDetectionV2Engine) AsPostWriteHook() WriteHook {
	return WriteHook{
		Name:  "anomaly-detection-v2",
		Phase: "post",
		Handler: func(p Point) (Point, error) {
			e.Ingest(p.Metric, p.Value, p.Tags, time.Now())
			return p, nil // pass-through: don't modify the point
		},
	}
}

// RegisterHTTPHandlers registers HTTP endpoints for the anomaly v2 engine.
func (e *AnomalyDetectionV2Engine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/anomaly/v2/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric string            `json:"metric"`
			Value  float64           `json:"value"`
			Tags   map[string]string `json:"tags"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		result := e.Ingest(req.Metric, req.Value, req.Tags, time.Now())
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"anomaly": result,
		})
	})

	mux.HandleFunc("/api/v1/anomaly/v2/anomalies", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListAnomalies(100))
	})

	mux.HandleFunc("/api/v1/anomaly/v2/feedback", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var fb AnomalyFeedback
		if err := json.NewDecoder(r.Body).Decode(&fb); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := e.SubmitFeedback(fb); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/api/v1/anomaly/v2/baselines", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := r.URL.Query().Get("metric")
		if metric != "" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e.GetBaseline(metric))
			return
		}
		e.mu.RLock()
		baselines := make([]*MetricBaseline, 0, len(e.baselines))
		for _, b := range e.baselines {
			cp := *b
			cp.Values = nil // Don't expose raw values
			baselines = append(baselines, &cp)
		}
		e.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(baselines)
	})

	mux.HandleFunc("/api/v1/anomaly/v2/correlations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.CorrelateAnomalies())
	})

	mux.HandleFunc("/api/v1/anomaly/v2/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})

	mux.HandleFunc("/api/v1/anomaly/v2/decompose", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			http.Error(w, "metric parameter required", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Decompose(metric))
	})
}
