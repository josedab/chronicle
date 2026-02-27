// Package retry provides configurable retry logic with exponential backoff.
//
// It supports jitter, maximum attempt limits, and per-error retry decisions.
// Use [Do] to wrap any fallible operation with automatic retry behavior.
package retry
