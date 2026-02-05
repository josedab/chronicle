package chronicle

// adaptive_compression.go implements V1 of the adaptive compression engine.
// V2 (adaptive_compression_v2.go) adds per-column codec profiling and selection.
// V3 (adaptive_compression_v3.go) adds multi-armed bandit online codec selection.
// New callers should prefer the V3 API (BanditCompressor) for best results.

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"time"
)

// AdaptiveCompressionConfig configures the adaptive compression engine.
type AdaptiveCompressionConfig struct {
	// Enabled enables adaptive compression.
	Enabled bool

	// AnalysisWindow is the number of points to analyze for codec selection.
	AnalysisWindow int

	// ReanalysisInterval is how often to reanalyze data patterns.
	ReanalysisInterval time.Duration

	// MinCompressionRatio is the minimum acceptable compression ratio.
	MinCompressionRatio float64

	// PreferSpeed prioritizes compression speed over ratio.
	PreferSpeed bool

	// EnableLearning enables ML-based codec selection.
	EnableLearning bool

	// MaxMemoryUsage limits memory usage for compression.
	MaxMemoryUsage int64
}

// DefaultAdaptiveCompressionConfig returns default configuration.
func DefaultAdaptiveCompressionConfig() AdaptiveCompressionConfig {
	return AdaptiveCompressionConfig{
		Enabled:             true,
		AnalysisWindow:      1000,
		ReanalysisInterval:  time.Hour,
		MinCompressionRatio: 1.5,
		PreferSpeed:         false,
		EnableLearning:      true,
		MaxMemoryUsage:      64 * 1024 * 1024,
	}
}

// CodecType identifies a compression codec.
type CodecType int

const (
	// CodecNone uses no compression.
	CodecNone CodecType = iota
	// CodecGorilla uses Facebook's Gorilla XOR compression for floats.
	CodecGorilla
	// CodecDeltaDelta uses delta-of-delta encoding for timestamps.
	CodecDeltaDelta
	// CodecDictionary uses dictionary encoding for tags.
	CodecDictionary
	// CodecRLE uses run-length encoding for repeated values.
	CodecRLE
	// CodecZSTD uses Zstandard compression.
	CodecZSTD
	// CodecLZ4 uses LZ4 compression.
	CodecLZ4
	// CodecSnappy uses Snappy compression.
	CodecSnappy
	// CodecGzip uses Gzip compression.
	CodecGzip
	// CodecBitPacking uses bit-packing for small integers.
	CodecBitPacking
	// CodecFloatXOR uses XOR-based float compression.
	CodecFloatXOR
)

func (c CodecType) String() string {
	switch c {
	case CodecNone:
		return "none"
	case CodecGorilla:
		return "gorilla"
	case CodecDeltaDelta:
		return "delta-delta"
	case CodecDictionary:
		return "dictionary"
	case CodecRLE:
		return "rle"
	case CodecZSTD:
		return "zstd"
	case CodecLZ4:
		return "lz4"
	case CodecSnappy:
		return "snappy"
	case CodecGzip:
		return "gzip"
	case CodecBitPacking:
		return "bit-packing"
	case CodecFloatXOR:
		return "float-xor"
	default:
		return "unknown"
	}
}

// DataCharacteristics describes the characteristics of data for codec selection.
type DataCharacteristics struct {
	// Type of data (float, int, string, timestamp)
	DataType string

	// Entropy estimate (0-1, lower = more compressible)
	Entropy float64

	// Cardinality (number of unique values)
	Cardinality int

	// Monotonicity (-1 = decreasing, 0 = random, 1 = increasing)
	Monotonicity float64

	// Periodicity (detected period, 0 if none)
	Periodicity int

	// MeanDelta is the average difference between consecutive values
	MeanDelta float64

	// DeltaVariance is the variance of deltas
	DeltaVariance float64

	// RepeatRatio is the ratio of repeated consecutive values
	RepeatRatio float64

	// Range is max - min
	Range float64

	// BitsRequired is the bits needed to represent values
	BitsRequired int
}

// AdaptiveCompressionEngine provides ML-driven adaptive compression.
type AdaptiveCompressionEngine struct {
	config AdaptiveCompressionConfig

	// Per-column codec selection
	codecSelection   map[string]CodecType
	codecSelectionMu sync.RWMutex

	// Codec performance history
	history   map[string][]CodecPerformance
	historyMu sync.RWMutex

	// ML model for codec selection
	model *CodecSelectionModel

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// CodecPerformance records codec performance metrics.
type CodecPerformance struct {
	Codec            CodecType
	CompressionRatio float64
	CompressTime     time.Duration
	DecompressTime   time.Duration
	InputSize        int64
	OutputSize       int64
	Timestamp        time.Time
}

// NewAdaptiveCompressionEngine creates a new adaptive compression engine.
func NewAdaptiveCompressionEngine(config AdaptiveCompressionConfig) *AdaptiveCompressionEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &AdaptiveCompressionEngine{
		config:         config,
		codecSelection: make(map[string]CodecType),
		history:        make(map[string][]CodecPerformance),
		model:          NewCodecSelectionModel(),
		ctx:            ctx,
		cancel:         cancel,
	}

	return engine
}

// Start starts the adaptive compression engine.
func (e *AdaptiveCompressionEngine) Start() error {
	if e.config.ReanalysisInterval > 0 {
		e.wg.Add(1)
		go e.reanalysisLoop()
	}
	return nil
}

// Stop stops the adaptive compression engine.
func (e *AdaptiveCompressionEngine) Stop() error {
	e.cancel()
	e.wg.Wait()
	return nil
}

func (e *AdaptiveCompressionEngine) reanalysisLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.ReanalysisInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.reanalyzeAllColumns()
		}
	}
}

func (e *AdaptiveCompressionEngine) reanalyzeAllColumns() {
	e.codecSelectionMu.RLock()
	columns := make([]string, 0, len(e.codecSelection))
	for col := range e.codecSelection {
		columns = append(columns, col)
	}
	e.codecSelectionMu.RUnlock()

	for _, col := range columns {
		e.historyMu.RLock()
		history := e.history[col]
		e.historyMu.RUnlock()

		if len(history) > 0 {
			// Use ML model to select best codec
			bestCodec := e.model.PredictBestCodec(history)
			e.codecSelectionMu.Lock()
			e.codecSelection[col] = bestCodec
			e.codecSelectionMu.Unlock()
		}
	}
}

// AnalyzeAndCompress analyzes data and compresses with the best codec.
func (e *AdaptiveCompressionEngine) AnalyzeAndCompress(columnID string, data []byte, dataType string) ([]byte, CodecType, error) {
	// Get or select codec
	codec := e.selectCodec(columnID, data, dataType)

	// Compress with selected codec
	compressed, err := e.Compress(data, codec)
	if err != nil {
		return nil, CodecNone, err
	}

	// Record performance
	e.recordPerformance(columnID, codec, len(data), len(compressed))

	return compressed, codec, nil
}

func (e *AdaptiveCompressionEngine) selectCodec(columnID string, data []byte, dataType string) CodecType {
	// Check if we have a cached selection
	e.codecSelectionMu.RLock()
	if codec, ok := e.codecSelection[columnID]; ok {
		e.codecSelectionMu.RUnlock()
		return codec
	}
	e.codecSelectionMu.RUnlock()

	// Analyze data characteristics
	characteristics := e.analyzeData(data, dataType)

	// Select codec based on characteristics
	codec := e.selectCodecFromCharacteristics(characteristics)

	// If learning is enabled, try multiple codecs and compare
	if e.config.EnableLearning {
		codec = e.trialAndSelect(data, characteristics)
	}

	// Cache the selection
	e.codecSelectionMu.Lock()
	e.codecSelection[columnID] = codec
	e.codecSelectionMu.Unlock()

	return codec
}

func (e *AdaptiveCompressionEngine) analyzeData(data []byte, dataType string) DataCharacteristics {
	chars := DataCharacteristics{
		DataType: dataType,
	}

	if len(data) == 0 {
		return chars
	}

	switch dataType {
	case "float64":
		chars = e.analyzeFloat64Data(data)
	case "int64":
		chars = e.analyzeInt64Data(data)
	case "string":
		chars = e.analyzeStringData(data)
	default:
		chars = e.analyzeGenericData(data)
	}

	chars.DataType = dataType
	return chars
}

func (e *AdaptiveCompressionEngine) analyzeFloat64Data(data []byte) DataCharacteristics {
	chars := DataCharacteristics{}

	if len(data) < 8 {
		return chars
	}

	// Parse float64 values
	values := make([]float64, len(data)/8)
	for i := 0; i < len(values); i++ {
		bits := binary.LittleEndian.Uint64(data[i*8:])
		values[i] = math.Float64frombits(bits)
	}

	if len(values) == 0 {
		return chars
	}

	// Calculate statistics
	var min, max, sum float64
	min = values[0]
	max = values[0]
	unique := make(map[float64]bool)

	for _, v := range values {
		sum += v
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
		unique[v] = true
	}

	chars.Range = max - min
	chars.Cardinality = len(unique)

	// Calculate deltas
	if len(values) > 1 {
		deltas := make([]float64, len(values)-1)
		var deltaSum float64
		for i := 1; i < len(values); i++ {
			deltas[i-1] = values[i] - values[i-1]
			deltaSum += deltas[i-1]
		}
		chars.MeanDelta = deltaSum / float64(len(deltas))

		// Delta variance
		var variance float64
		for _, d := range deltas {
			diff := d - chars.MeanDelta
			variance += diff * diff
		}
		chars.DeltaVariance = variance / float64(len(deltas))

		// Monotonicity
		increasing := 0
		decreasing := 0
		for _, d := range deltas {
			if d > 0 {
				increasing++
			} else if d < 0 {
				decreasing++
			}
		}
		chars.Monotonicity = float64(increasing-decreasing) / float64(len(deltas))
	}

	// Repeat ratio
	repeats := 0
	for i := 1; i < len(values); i++ {
		if values[i] == values[i-1] {
			repeats++
		}
	}
	chars.RepeatRatio = float64(repeats) / float64(len(values)-1)

	// Entropy estimate (simplified)
	chars.Entropy = float64(chars.Cardinality) / float64(len(values))

	return chars
}

func (e *AdaptiveCompressionEngine) analyzeInt64Data(data []byte) DataCharacteristics {
	chars := DataCharacteristics{}

	if len(data) < 8 {
		return chars
	}

	values := make([]int64, len(data)/8)
	for i := 0; i < len(values); i++ {
		values[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
	}

	if len(values) == 0 {
		return chars
	}

	// Calculate statistics
	var min, max int64
	min = values[0]
	max = values[0]
	unique := make(map[int64]bool)

	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
		unique[v] = true
	}

	chars.Range = float64(max - min)
	chars.Cardinality = len(unique)
	chars.BitsRequired = bitsRequired(uint64(max - min))

	// Monotonicity for timestamps
	if len(values) > 1 {
		increasing := 0
		for i := 1; i < len(values); i++ {
			if values[i] >= values[i-1] {
				increasing++
			}
		}
		chars.Monotonicity = float64(increasing) / float64(len(values)-1)
	}

	return chars
}

func (e *AdaptiveCompressionEngine) analyzeStringData(data []byte) DataCharacteristics {
	chars := DataCharacteristics{}

	// Simple byte-level analysis
	unique := make(map[byte]bool)
	for _, b := range data {
		unique[b] = true
	}

	chars.Cardinality = len(unique)
	chars.Entropy = float64(len(unique)) / 256.0

	return chars
}

func (e *AdaptiveCompressionEngine) analyzeGenericData(data []byte) DataCharacteristics {
	chars := DataCharacteristics{}

	unique := make(map[byte]bool)
	repeats := 0
	for i, b := range data {
		unique[b] = true
		if i > 0 && data[i] == data[i-1] {
			repeats++
		}
	}

	chars.Cardinality = len(unique)
	chars.Entropy = float64(len(unique)) / 256.0
	if len(data) > 1 {
		chars.RepeatRatio = float64(repeats) / float64(len(data)-1)
	}

	return chars
}

func (e *AdaptiveCompressionEngine) selectCodecFromCharacteristics(chars DataCharacteristics) CodecType {
	switch chars.DataType {
	case "float64":
		// For floats with low delta variance, use Gorilla
		if chars.DeltaVariance < 0.1 {
			return CodecGorilla
		}
		// For highly compressible floats
		if chars.RepeatRatio > 0.5 {
			return CodecRLE
		}
		return CodecFloatXOR

	case "int64":
		// Timestamps are usually monotonic
		if chars.Monotonicity > 0.9 {
			return CodecDeltaDelta
		}
		// Low cardinality integers
		if chars.Cardinality < 100 && chars.BitsRequired <= 8 {
			return CodecBitPacking
		}
		return CodecDeltaDelta

	case "string":
		// Low cardinality strings benefit from dictionary encoding
		if chars.Cardinality < 1000 {
			return CodecDictionary
		}
		// High entropy strings use general compression
		return CodecSnappy

	default:
		// High repeat ratio suggests RLE
		if chars.RepeatRatio > 0.3 {
			return CodecRLE
		}
		// Low entropy suggests dictionary
		if chars.Entropy < 0.3 {
			return CodecDictionary
		}
		// Default to Snappy for speed
		if e.config.PreferSpeed {
			return CodecSnappy
		}
		return CodecGzip
	}
}

func (e *AdaptiveCompressionEngine) trialAndSelect(data []byte, chars DataCharacteristics) CodecType {
	candidates := e.getCandidateCodecs(chars)

	bestCodec := CodecNone
	bestScore := 0.0

	for _, codec := range candidates {
		compressed, err := e.Compress(data, codec)
		if err != nil {
			continue
		}

		ratio := float64(len(data)) / float64(len(compressed))
		score := e.calculateScore(ratio, codec)

		if score > bestScore {
			bestScore = score
			bestCodec = codec
		}
	}

	return bestCodec
}

func (e *AdaptiveCompressionEngine) getCandidateCodecs(chars DataCharacteristics) []CodecType {
	switch chars.DataType {
	case "float64":
		return []CodecType{CodecGorilla, CodecFloatXOR, CodecSnappy, CodecGzip}
	case "int64":
		return []CodecType{CodecDeltaDelta, CodecBitPacking, CodecRLE, CodecSnappy}
	case "string":
		return []CodecType{CodecDictionary, CodecSnappy, CodecGzip}
	default:
		return []CodecType{CodecSnappy, CodecGzip, CodecRLE}
	}
}

func (e *AdaptiveCompressionEngine) calculateScore(ratio float64, codec CodecType) float64 {
	// Base score is compression ratio
	score := ratio

	// Adjust for speed preference
	if e.config.PreferSpeed {
		switch codec {
		case CodecSnappy, CodecLZ4:
			score *= 1.2 // Boost fast codecs
		case CodecGzip, CodecZSTD:
			score *= 0.8 // Penalize slow codecs
		}
	}

	return score
}

func (e *AdaptiveCompressionEngine) recordPerformance(columnID string, codec CodecType, inputSize, outputSize int) {
	perf := CodecPerformance{
		Codec:            codec,
		CompressionRatio: float64(inputSize) / float64(outputSize),
		InputSize:        int64(inputSize),
		OutputSize:       int64(outputSize),
		Timestamp:        time.Now(),
	}

	e.historyMu.Lock()
	e.history[columnID] = append(e.history[columnID], perf)

	// Limit history size
	if len(e.history[columnID]) > 100 {
		e.history[columnID] = e.history[columnID][len(e.history[columnID])-100:]
	}
	e.historyMu.Unlock()
}

// Compress compresses data with the specified codec.
func (e *AdaptiveCompressionEngine) Compress(data []byte, codec CodecType) ([]byte, error) {
	switch codec {
	case CodecNone:
		return data, nil

	case CodecGorilla:
		return compressGorilla(data)

	case CodecDeltaDelta:
		return compressDeltaDelta(data)

	case CodecRLE:
		return compressRLE(data)

	case CodecSnappy:
		return compressSnappy(data)

	case CodecGzip:
		return compressGzip(data)

	case CodecDictionary:
		return compressDictionary(data)

	case CodecBitPacking:
		return compressBitPacking(data)

	case CodecFloatXOR:
		return compressFloatXOR(data)

	default:
		return nil, errors.New("unsupported codec")
	}
}

// Decompress decompresses data with the specified codec.
func (e *AdaptiveCompressionEngine) Decompress(data []byte, codec CodecType) ([]byte, error) {
	switch codec {
	case CodecNone:
		return data, nil

	case CodecGorilla:
		return decompressGorilla(data)

	case CodecDeltaDelta:
		return decompressDeltaDelta(data)

	case CodecRLE:
		return decompressRLE(data)

	case CodecSnappy:
		return decompressSnappy(data)

	case CodecGzip:
		return decompressGzip(data)

	case CodecDictionary:
		return decompressDictionary(data)

	case CodecBitPacking:
		return decompressBitPacking(data)

	case CodecFloatXOR:
		return decompressFloatXOR(data)

	default:
		return nil, errors.New("unsupported codec")
	}
}

// GetSelectedCodec returns the selected codec for a column.
func (e *AdaptiveCompressionEngine) GetSelectedCodec(columnID string) CodecType {
	e.codecSelectionMu.RLock()
	defer e.codecSelectionMu.RUnlock()
	return e.codecSelection[columnID]
}

// SetCodec manually sets the codec for a column.
func (e *AdaptiveCompressionEngine) SetCodec(columnID string, codec CodecType) {
	e.codecSelectionMu.Lock()
	e.codecSelection[columnID] = codec
	e.codecSelectionMu.Unlock()
}

// Stats returns compression statistics.
func (e *AdaptiveCompressionEngine) Stats() CompressionStats {
	e.codecSelectionMu.RLock()
	codecCounts := make(map[string]int)
	for _, codec := range e.codecSelection {
		codecCounts[codec.String()]++
	}
	e.codecSelectionMu.RUnlock()

	e.historyMu.RLock()
	var totalRatio float64
	var count int
	for _, history := range e.history {
		for _, perf := range history {
			totalRatio += perf.CompressionRatio
			count++
		}
	}
	e.historyMu.RUnlock()

	avgRatio := 0.0
	if count > 0 {
		avgRatio = totalRatio / float64(count)
	}

	return CompressionStats{
		ColumnCount:          len(e.codecSelection),
		CodecDistribution:    codecCounts,
		AverageCompressionRatio: avgRatio,
		HistorySize:          count,
	}
}

// CompressionStats contains compression statistics.
type CompressionStats struct {
	ColumnCount             int            `json:"column_count"`
	CodecDistribution       map[string]int `json:"codec_distribution"`
	AverageCompressionRatio float64        `json:"average_compression_ratio"`
	HistorySize             int            `json:"history_size"`
}

// CodecSelectionModel is an ML model for codec selection.
type CodecSelectionModel struct {
	weights map[CodecType]float64
	mu      sync.RWMutex
}

// NewCodecSelectionModel creates a new codec selection model.
func NewCodecSelectionModel() *CodecSelectionModel {
	return &CodecSelectionModel{
		weights: make(map[CodecType]float64),
	}
}

// PredictBestCodec predicts the best codec based on history.
func (m *CodecSelectionModel) PredictBestCodec(history []CodecPerformance) CodecType {
	if len(history) == 0 {
		return CodecSnappy
	}

	// Simple weighted average of compression ratios
	codecScores := make(map[CodecType]float64)
	codecCounts := make(map[CodecType]int)

	for i, perf := range history {
		// Weight recent performance more heavily
		weight := float64(i+1) / float64(len(history))
		codecScores[perf.Codec] += perf.CompressionRatio * weight
		codecCounts[perf.Codec]++
	}

	// Find best codec
	bestCodec := CodecSnappy
	bestScore := 0.0

	for codec, totalScore := range codecScores {
		avgScore := totalScore / float64(codecCounts[codec])
		if avgScore > bestScore {
			bestScore = avgScore
			bestCodec = codec
		}
	}

	return bestCodec
}

// ========== Codec Implementations ==========

func compressGorilla(data []byte) ([]byte, error) {
	// Simplified Gorilla XOR compression
	if len(data) < 8 {
		return data, nil
	}

	values := make([]uint64, len(data)/8)
	for i := 0; i < len(values); i++ {
		values[i] = binary.LittleEndian.Uint64(data[i*8:])
	}

	var buf bytes.Buffer
	// Write first value uncompressed
	binary.Write(&buf, binary.LittleEndian, values[0])

	// XOR encode subsequent values
	prev := values[0]
	for i := 1; i < len(values); i++ {
		xor := values[i] ^ prev
		binary.Write(&buf, binary.LittleEndian, xor)
		prev = values[i]
	}

	return buf.Bytes(), nil
}

func decompressGorilla(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return data, nil
	}

	var buf bytes.Buffer
	values := make([]uint64, len(data)/8)

	// Read first value
	values[0] = binary.LittleEndian.Uint64(data[0:8])

	// XOR decode subsequent values
	for i := 1; i < len(values); i++ {
		xor := binary.LittleEndian.Uint64(data[i*8:])
		values[i] = values[i-1] ^ xor
	}

	for _, v := range values {
		binary.Write(&buf, binary.LittleEndian, v)
	}

	return buf.Bytes(), nil
}

func compressDeltaDelta(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return data, nil
	}

	values := make([]int64, len(data)/8)
	for i := 0; i < len(values); i++ {
		values[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
	}

	var buf bytes.Buffer
	// Write first value
	binary.Write(&buf, binary.LittleEndian, values[0])

	if len(values) < 2 {
		return buf.Bytes(), nil
	}

	// Write first delta
	firstDelta := values[1] - values[0]
	binary.Write(&buf, binary.LittleEndian, firstDelta)

	// Write delta-of-deltas
	prevDelta := firstDelta
	for i := 2; i < len(values); i++ {
		delta := values[i] - values[i-1]
		dod := delta - prevDelta
		binary.Write(&buf, binary.LittleEndian, dod)
		prevDelta = delta
	}

	return buf.Bytes(), nil
}

func decompressDeltaDelta(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return data, nil
	}

	numValues := len(data) / 8
	var buf bytes.Buffer

	if numValues == 0 {
		return buf.Bytes(), nil
	}

	// Read first value
	value := int64(binary.LittleEndian.Uint64(data[0:8]))
	binary.Write(&buf, binary.LittleEndian, value)

	if numValues == 1 {
		return buf.Bytes(), nil
	}

	// Read first delta
	delta := int64(binary.LittleEndian.Uint64(data[8:16]))
	value += delta
	binary.Write(&buf, binary.LittleEndian, value)

	// Decode delta-of-deltas
	for i := 2; i < numValues; i++ {
		dod := int64(binary.LittleEndian.Uint64(data[i*8:]))
		delta += dod
		value += delta
		binary.Write(&buf, binary.LittleEndian, value)
	}

	return buf.Bytes(), nil
}

func compressRLE(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	var buf bytes.Buffer
	i := 0
	for i < len(data) {
		count := 1
		for i+count < len(data) && data[i] == data[i+count] && count < 255 {
			count++
		}
		buf.WriteByte(byte(count))
		buf.WriteByte(data[i])
		i += count
	}

	return buf.Bytes(), nil
}

func decompressRLE(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	for i := 0; i < len(data)-1; i += 2 {
		count := int(data[i])
		value := data[i+1]
		for j := 0; j < count; j++ {
			buf.WriteByte(value)
		}
	}
	return buf.Bytes(), nil
}

func compressSnappy(data []byte) ([]byte, error) {
	// Use snappy from the imported package
	// For now, fall back to gzip
	return compressGzip(data)
}

func decompressSnappy(data []byte) ([]byte, error) {
	return decompressGzip(data)
}

func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompressGzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func compressDictionary(data []byte) ([]byte, error) {
	// Build dictionary
	dict := make(map[byte]int)
	for _, b := range data {
		if _, ok := dict[b]; !ok {
			dict[b] = len(dict)
		}
	}

	var buf bytes.Buffer
	// Write dictionary size
	buf.WriteByte(byte(len(dict)))
	// Write dictionary
	for b := range dict {
		buf.WriteByte(b)
	}
	// Write data using dictionary indices
	for _, b := range data {
		buf.WriteByte(byte(dict[b]))
	}

	return buf.Bytes(), nil
}

func decompressDictionary(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	dictSize := int(data[0])
	if len(data) < 1+dictSize {
		return nil, errors.New("invalid dictionary data")
	}

	// Read dictionary
	dict := data[1 : 1+dictSize]

	// Decompress
	var buf bytes.Buffer
	for i := 1 + dictSize; i < len(data); i++ {
		idx := int(data[i])
		if idx < len(dict) {
			buf.WriteByte(dict[idx])
		}
	}

	return buf.Bytes(), nil
}

func compressBitPacking(data []byte) ([]byte, error) {
	// Simple bit packing for bytes
	return data, nil // Simplified
}

func decompressBitPacking(data []byte) ([]byte, error) {
	return data, nil // Simplified
}

func compressFloatXOR(data []byte) ([]byte, error) {
	return compressGorilla(data) // Same as Gorilla for floats
}

func decompressFloatXOR(data []byte) ([]byte, error) {
	return decompressGorilla(data)
}

func bitsRequired(n uint64) int {
	if n == 0 {
		return 1
	}
	bits := 0
	for n > 0 {
		bits++
		n >>= 1
	}
	return bits
}
