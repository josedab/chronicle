// Package compression implements adaptive compression engines for time-series data.
//
// It provides multiple codec selection strategies including multi-armed bandit
// approaches that automatically choose the best compression algorithm based on
// data characteristics. Supported codecs include Gorilla XOR, delta encoding,
// dictionary compression, and columnar encoding.
package compression
