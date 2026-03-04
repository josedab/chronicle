// Bridge: adaptive_compression.go
//
// Re-exports types from internal/compression/ into the public chronicle package.
// Pattern: internal/compression/ (implementation) → adaptive_compression*.go (public API)

package chronicle

import "github.com/chronicle-db/chronicle/internal/compression"

// AdaptiveCompressionConfig holds configuration for the adaptive compression engine.
type AdaptiveCompressionConfig = compression.AdaptiveCompressionConfig

// AdaptiveCompressionEngine selects optimal codecs based on data characteristics.
type AdaptiveCompressionEngine = compression.AdaptiveCompressionEngine

// CodecType identifies a compression codec (gorilla, delta-delta, zstd, etc.).
type CodecType = compression.CodecType

// DataCharacteristics describes the statistical properties of a data column.
type DataCharacteristics = compression.DataCharacteristics

// CompressionStats contains compression performance statistics.
type CompressionStats = compression.CompressionStats

// CodecPerformance records the measured performance of a codec on a workload.
type CodecPerformance = compression.CodecPerformance

// ColumnProfile describes the data profile of a column for codec selection.
type ColumnProfile = compression.ColumnProfile

// ColumnType classifies a column's data distribution (monotonic, sparse, gaussian, etc.).
type ColumnType = compression.ColumnType

// AutoProfiler automatically profiles columns to determine their data characteristics.
type AutoProfiler = compression.AutoProfiler

// CodecRecommendation is a recommended codec for a column based on profiling.
type CodecRecommendation = compression.CodecRecommendation

// CodecRegistry manages available compression codecs.
type CodecRegistry = compression.CodecRegistry

// CodecEncoder encodes and decodes data using a specific codec.
type CodecEncoder = compression.CodecEncoder

// PartitionCodecHeader stores per-partition codec metadata.
type PartitionCodecHeader = compression.PartitionCodecHeader

// CodecSelectionModel uses machine learning to select optimal codecs.
type CodecSelectionModel = compression.CodecSelectionModel

// BanditStrategy identifies the multi-armed bandit strategy for codec selection.
type BanditStrategy = compression.BanditStrategy

// ColumnBandit applies bandit algorithms to learn the best codec per column.
type ColumnBandit = compression.ColumnBandit

// WorkloadLearner learns workload patterns to improve codec selection over time.
type WorkloadLearner = compression.WorkloadLearner

// AdaptiveCompressionV3Config holds configuration for the v3 adaptive compressor.
type AdaptiveCompressionV3Config = compression.AdaptiveCompressionV3Config

// AdaptiveCompressorV3 is the third-generation adaptive compression engine.
type AdaptiveCompressorV3 = compression.AdaptiveCompressorV3

// AdaptiveCompressionV3Stats contains statistics for the v3 compression engine.
type AdaptiveCompressionV3Stats = compression.AdaptiveCompressionV3Stats

// Codec constants from internal/compression.
const (
CodecNone       = compression.CodecNone
CodecGorilla    = compression.CodecGorilla
CodecDeltaDelta = compression.CodecDeltaDelta
CodecDictionary = compression.CodecDictionary
CodecRLE        = compression.CodecRLE
CodecZSTD       = compression.CodecZSTD
CodecLZ4        = compression.CodecLZ4
CodecSnappy     = compression.CodecSnappy
CodecGzip       = compression.CodecGzip
CodecBitPacking = compression.CodecBitPacking
CodecFloatXOR   = compression.CodecFloatXOR
)

// Constructor wrappers from internal/compression.
var (
DefaultAdaptiveCompressionConfig   = compression.DefaultAdaptiveCompressionConfig
NewAdaptiveCompressionEngine       = compression.NewAdaptiveCompressionEngine
ProfileValues                      = compression.ProfileValues
RecommendCodec                     = compression.RecommendCodec
NewAutoProfiler                    = compression.NewAutoProfiler
NewCodecRegistry                   = compression.NewCodecRegistry
NewPartitionCodecHeader            = compression.NewPartitionCodecHeader
NewCodecSelectionModel             = compression.NewCodecSelectionModel
NewWorkloadLearner                 = compression.NewWorkloadLearner
DefaultAdaptiveCompressionV3Config = compression.DefaultAdaptiveCompressionV3Config
NewAdaptiveCompressorV3            = compression.NewAdaptiveCompressorV3
)

// ColumnType constants from internal/compression.
const (
	ColumnTypeUnknown        = compression.ColumnTypeUnknown
	ColumnTypeMonotonic      = compression.ColumnTypeMonotonic
	ColumnTypeLowCardinality = compression.ColumnTypeLowCardinality
	ColumnTypeSparse         = compression.ColumnTypeSparse
	ColumnTypeGaussian       = compression.ColumnTypeGaussian
	ColumnTypeRandom         = compression.ColumnTypeRandom
	ColumnTypeConstant       = compression.ColumnTypeConstant
)

// BanditStrategy constants from internal/compression.
const (
	BanditUCB1             = compression.BanditUCB1
	BanditThompsonSampling = compression.BanditThompsonSampling
	BanditEpsilonGreedy    = compression.BanditEpsilonGreedy
)

// Compression helper functions from internal/compression.
var (
compressGorilla    = compression.CompressGorilla
compressDeltaDelta = compression.CompressDeltaDelta
compressRLE        = compression.CompressRLE
compressSnappy     = compression.CompressSnappy
compressGzip       = compression.CompressGzip
compressDictionary = compression.CompressDictionary
compressBitPacking = compression.CompressBitPacking
compressFloatXOR   = compression.CompressFloatXOR

decompressGorilla    = compression.DecompressGorilla
decompressDeltaDelta = compression.DecompressDeltaDelta
decompressRLE        = compression.DecompressRLE
decompressSnappy     = compression.DecompressSnappy
decompressGzip       = compression.DecompressGzip
decompressDictionary = compression.DecompressDictionary
decompressBitPacking = compression.DecompressBitPacking
decompressFloatXOR   = compression.DecompressFloatXOR
)

// bitsRequired re-exported for internal use.
var bitsRequired = compression.BitsRequired
