// Package encoding provides compression codecs for time-series data.
//
// The package implements several encoding schemes optimized for time-series data:
//   - Gorilla: Float compression using XOR-based encoding (Facebook Gorilla paper)
//   - Delta: Integer compression using delta-of-delta encoding
//   - Dictionary: String compression using dictionary encoding
//   - Column: Per-column encoding selection based on data type
package encoding
