package compression

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
)

// Trial-and-select codec evaluation and scoring for adaptive compression.

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
		return CompressGorilla(data)

	case CodecDeltaDelta:
		return CompressDeltaDelta(data)

	case CodecRLE:
		return CompressRLE(data)

	case CodecSnappy:
		return CompressSnappy(data)

	case CodecGzip:
		return CompressGzip(data)

	case CodecDictionary:
		return CompressDictionary(data)

	case CodecBitPacking:
		return CompressBitPacking(data)

	case CodecFloatXOR:
		return CompressFloatXOR(data)

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
		return DecompressGorilla(data)

	case CodecDeltaDelta:
		return DecompressDeltaDelta(data)

	case CodecRLE:
		return DecompressRLE(data)

	case CodecSnappy:
		return DecompressSnappy(data)

	case CodecGzip:
		return DecompressGzip(data)

	case CodecDictionary:
		return DecompressDictionary(data)

	case CodecBitPacking:
		return DecompressBitPacking(data)

	case CodecFloatXOR:
		return DecompressFloatXOR(data)

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
		ColumnCount:             len(e.codecSelection),
		CodecDistribution:       codecCounts,
		AverageCompressionRatio: avgRatio,
		HistorySize:             count,
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

func CompressGorilla(data []byte) ([]byte, error) {
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

func DecompressGorilla(data []byte) ([]byte, error) {
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

func CompressDeltaDelta(data []byte) ([]byte, error) {
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

func DecompressDeltaDelta(data []byte) ([]byte, error) {
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

func CompressRLE(data []byte) ([]byte, error) {
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

func DecompressRLE(data []byte) ([]byte, error) {
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

func CompressSnappy(data []byte) ([]byte, error) {
	// Use snappy from the imported package
	// For now, fall back to gzip
	return CompressGzip(data)
}

func DecompressSnappy(data []byte) ([]byte, error) {
	return DecompressGzip(data)
}

func CompressGzip(data []byte) ([]byte, error) {
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

func DecompressGzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func CompressDictionary(data []byte) ([]byte, error) {
	// Build dictionary
	dict := make(map[byte]int)
	for _, b := range data {
		if _, ok := dict[b]; !ok {
			dict[b] = len(dict)
		}
	}

	// Build index-ordered dictionary for deterministic output
	ordered := make([]byte, len(dict))
	for b, idx := range dict {
		ordered[idx] = b
	}

	var buf bytes.Buffer
	// Write dictionary size
	buf.WriteByte(byte(len(dict)))
	// Write dictionary in index order
	buf.Write(ordered)
	// Write data using dictionary indices
	for _, b := range data {
		buf.WriteByte(byte(dict[b]))
	}

	return buf.Bytes(), nil
}

func DecompressDictionary(data []byte) ([]byte, error) {
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

func CompressBitPacking(data []byte) ([]byte, error) {
	// Simple bit packing for bytes
	return data, nil // Simplified
}

func DecompressBitPacking(data []byte) ([]byte, error) {
	return data, nil // Simplified
}

func CompressFloatXOR(data []byte) ([]byte, error) {
	return CompressGorilla(data) // Same as Gorilla for floats
}

func DecompressFloatXOR(data []byte) ([]byte, error) {
	return DecompressGorilla(data)
}

func BitsRequired(n uint64) int {
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
