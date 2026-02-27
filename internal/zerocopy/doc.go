// Package zerocopy implements a vectorized query execution engine.
//
// It supports columnar scan operations and parallelized aggregations
// optimized with SIMD instructions where available. The engine minimizes
// memory copies by operating directly on memory-mapped column buffers.
package zerocopy
