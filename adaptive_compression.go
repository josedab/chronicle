// Bridge: adaptive_compression.go
//
// Re-exports types from internal/compression/ into the public chronicle package.
// Pattern: internal/compression/ (implementation) → adaptive_compression*.go (public API)

package chronicle

import "github.com/chronicle-db/chronicle/internal/compression"

// Type aliases from internal/compression.
type AdaptiveCompressionConfig = compression.AdaptiveCompressionConfig
type AdaptiveCompressionEngine = compression.AdaptiveCompressionEngine
type CodecType = compression.CodecType
type DataCharacteristics = compression.DataCharacteristics
type CompressionStats = compression.CompressionStats
type CodecPerformance = compression.CodecPerformance
type ColumnProfile = compression.ColumnProfile
type ColumnType = compression.ColumnType
type AutoProfiler = compression.AutoProfiler
type CodecRecommendation = compression.CodecRecommendation
type CodecRegistry = compression.CodecRegistry
type CodecEncoder = compression.CodecEncoder
type PartitionCodecHeader = compression.PartitionCodecHeader
type CodecSelectionModel = compression.CodecSelectionModel
type BanditStrategy = compression.BanditStrategy
type ColumnBandit = compression.ColumnBandit
type WorkloadLearner = compression.WorkloadLearner
type AdaptiveCompressionV3Config = compression.AdaptiveCompressionV3Config
type AdaptiveCompressorV3 = compression.AdaptiveCompressorV3
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
