// Bridge: retry.go
//
// Re-exports types from internal/retry/ into the public chronicle package.
// Pattern: internal/retry/ (implementation) → retry.go (public API)

package chronicle

import "github.com/chronicle-db/chronicle/internal/retry"

// Type aliases from internal/retry.
type RetryConfig = retry.RetryConfig
type Retryer = retry.Retryer
type RetryResult = retry.RetryResult
type CircuitBreaker = retry.CircuitBreaker

// Constructor wrappers from internal/retry.
var (
DefaultRetryConfig = retry.DefaultRetryConfig
NewRetryer         = retry.NewRetryer
Retry              = retry.Retry
RetryWithBackoff   = retry.RetryWithBackoff
IsRetryable        = retry.IsRetryable
NewCircuitBreaker  = retry.NewCircuitBreaker
)

// Sentinel errors from internal/retry.
var ErrCircuitOpen = retry.ErrCircuitOpen

// computeBackoff re-exported for internal use.
var computeBackoff = retry.ComputeBackoff
