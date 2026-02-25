// Bridge: zero_copy_query.go
//
// This file bridges internal/zerocopy/ into the public chronicle package.
// It re-exports types and provides adapter constructors so that callers
// use the top-level chronicle API while implementation stays private.
//
// Pattern: internal/zerocopy/ (implementation) → zero_copy_query.go (public API)

package chronicle

import (
"github.com/chronicle-db/chronicle/internal/zerocopy"
)

// Type aliases re-exported from internal/zerocopy.
type VectorAggOp = zerocopy.VectorAggOp
type VectorizedAggregator = zerocopy.VectorizedAggregator
type MmapPartition = zerocopy.MmapPartition
type ZeroCopyQueryPlan = zerocopy.ZeroCopyQueryPlan
type ZeroCopyQueryPlanner = zerocopy.ZeroCopyQueryPlanner
type ZeroCopyScan = zerocopy.ZeroCopyScan
type ZeroCopyScanResult = zerocopy.ZeroCopyScanResult
type ParallelVectorizedExecutor = zerocopy.ParallelVectorizedExecutor
type AdaptiveExecutor = zerocopy.AdaptiveExecutor
type AdaptiveExecutorStats = zerocopy.AdaptiveExecutorStats
type ExecutionPath = zerocopy.ExecutionPath

// Constants re-exported from internal/zerocopy.
const (
VectorSum   = zerocopy.VectorSum
VectorMin   = zerocopy.VectorMin
VectorMax   = zerocopy.VectorMax
VectorAvg   = zerocopy.VectorAvg
VectorCount = zerocopy.VectorCount

PathRowOriented  = zerocopy.PathRowOriented
PathColumnar     = zerocopy.PathColumnar
PathVectorized   = zerocopy.PathVectorized
PathParallelScan = zerocopy.PathParallelScan
)

// Constructor wrappers re-exported from internal/zerocopy.
var (
NewVectorizedAggregator       = zerocopy.NewVectorizedAggregator
NewMmapPartition              = zerocopy.NewMmapPartition
NewZeroCopyQueryPlanner       = zerocopy.NewZeroCopyQueryPlanner
NewParallelVectorizedExecutor = zerocopy.NewParallelVectorizedExecutor
NewAdaptiveExecutor           = zerocopy.NewAdaptiveExecutor
)
