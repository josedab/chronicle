// Package chaos implements a chaos engineering framework for Chronicle.
//
// It provides fault injection primitives including network partitions, latency
// injection, disk failures, and process crashes. Each scenario defines
// invariant checks that are validated during and after fault injection.
package chaos
