package anomaly

import (
	"math"
	"sort"
)

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
