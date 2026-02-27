// Package gpucompression provides GPU-accelerated data compression.
//
// It supports batch and streaming compression modes with automatic backend
// detection for CUDA, HIP, and Metal. When no GPU is available, it falls
// back to CPU-based codecs transparently.
package gpucompression
