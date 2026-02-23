package compression

// adaptive_compression.go implements V1 of the adaptive compression engine.
// V2 (adaptive_compression_v2.go) adds per-column codec profiling and selection.
// V3 (adaptive_compression_v3.go) adds multi-armed bandit online codec selection.
//
// Deprecated: New callers should prefer the V3 API (BanditCompressor) for best results.
// This file will be removed in a future major version.

import (
	"context"
	"encoding/binary"
	"math"
	"sync"
	"time"
)

// AdaptiveCompressionConfig configures the adaptive compression engine.
//
// Deprecated: Use [AdaptiveCompressionV3Config] instead.
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
//
// Deprecated: Use [DefaultAdaptiveCompressionV3Config] instead.
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
//
// Deprecated: Use [AdaptiveCompressorV3] (BanditCompressor) instead.
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
//
// Deprecated: Use [NewAdaptiveCompressorV3] instead.
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
	codec := e.SelectCodec(columnID, data, dataType)

	// Compress with selected codec
	compressed, err := e.Compress(data, codec)
	if err != nil {
		return nil, CodecNone, err
	}

	// Record performance
	e.recordPerformance(columnID, codec, len(data), len(compressed))

	return compressed, codec, nil
}

func (e *AdaptiveCompressionEngine) SelectCodec(columnID string, data []byte, dataType string) CodecType {
	// Check if we have a cached selection
	e.codecSelectionMu.RLock()
	if codec, ok := e.codecSelection[columnID]; ok {
		e.codecSelectionMu.RUnlock()
		return codec
	}
	e.codecSelectionMu.RUnlock()

	// Analyze data characteristics
	characteristics := e.AnalyzeData(data, dataType)

	// Select codec based on characteristics
	codec := e.SelectCodecFromCharacteristics(characteristics)

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

func (e *AdaptiveCompressionEngine) AnalyzeData(data []byte, dataType string) DataCharacteristics {
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
	chars.BitsRequired = BitsRequired(uint64(max - min))

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

func (e *AdaptiveCompressionEngine) SelectCodecFromCharacteristics(chars DataCharacteristics) CodecType {
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
