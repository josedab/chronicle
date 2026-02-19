package chronicle

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// RetryConfig configures retry behavior.
type RetryConfig struct {
	// MaxAttempts is the maximum number of attempts (including the first).
	// Default: 3
	MaxAttempts int

	// InitialBackoff is the initial delay before the first retry.
	// Default: 100ms
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay between retries.
	// Default: 30s
	MaxBackoff time.Duration

	// BackoffMultiplier is multiplied to the backoff after each retry.
	// Default: 2.0
	BackoffMultiplier float64

	// Jitter adds randomness to backoff to prevent thundering herd.
	// Value between 0 and 1, where 0.1 means ±10% jitter.
	// Default: 0.1
	Jitter float64

	// RetryIf determines if an error should be retried.
	// If nil, all errors are retried.
	RetryIf func(error) bool
}

// DefaultRetryConfig returns a retry configuration with sensible defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:       3,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        30 * time.Second,
		BackoffMultiplier: 2.0,
		Jitter:            0.1,
	}
}

// Retryer performs operations with automatic retry on failure.
type Retryer struct {
	config RetryConfig
}

// NewRetryer creates a new retryer with the given configuration.
func NewRetryer(config RetryConfig) *Retryer {
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 3
	}
	if config.InitialBackoff <= 0 {
		config.InitialBackoff = 100 * time.Millisecond
	}
	if config.MaxBackoff <= 0 {
		config.MaxBackoff = 30 * time.Second
	}
	if config.BackoffMultiplier <= 0 {
		config.BackoffMultiplier = 2.0
	}
	if config.Jitter < 0 || config.Jitter > 1 {
		config.Jitter = 0.1
	}
	return &Retryer{config: config}
}

// RetryResult contains the result of a retry operation.
type RetryResult struct {
	Attempts int
	LastErr  error
}

// Do executes the operation with retries.
// Returns the result of the last attempt and retry metadata.
func (r *Retryer) Do(ctx context.Context, op func() error) RetryResult {
	var lastErr error
	backoff := r.config.InitialBackoff

	for attempt := 1; attempt <= r.config.MaxAttempts; attempt++ {
		lastErr = op()
		if lastErr == nil {
			return RetryResult{Attempts: attempt}
		}

		// Check if we should retry this error
		if r.config.RetryIf != nil && !r.config.RetryIf(lastErr) {
			return RetryResult{Attempts: attempt, LastErr: lastErr}
		}

		// Don't sleep after the last attempt
		if attempt == r.config.MaxAttempts {
			break
		}

		// Calculate sleep duration with jitter
		sleepDuration := r.addJitter(backoff)

		// Wait or check for context cancellation
		select {
		case <-ctx.Done():
			return RetryResult{Attempts: attempt, LastErr: ctx.Err()}
		case <-time.After(sleepDuration):
		}

		// Increase backoff for next iteration
		backoff = time.Duration(float64(backoff) * r.config.BackoffMultiplier)
		if backoff > r.config.MaxBackoff {
			backoff = r.config.MaxBackoff
		}
	}

	return RetryResult{Attempts: r.config.MaxAttempts, LastErr: lastErr}
}

// DoWithResult executes an operation that returns a value with retries.
func (r *Retryer) DoWithResult(ctx context.Context, op func() (any, error)) (any, RetryResult) {
	var result any
	var lastErr error
	backoff := r.config.InitialBackoff

	for attempt := 1; attempt <= r.config.MaxAttempts; attempt++ {
		result, lastErr = op()
		if lastErr == nil {
			return result, RetryResult{Attempts: attempt}
		}

		// Check if we should retry this error
		if r.config.RetryIf != nil && !r.config.RetryIf(lastErr) {
			return nil, RetryResult{Attempts: attempt, LastErr: lastErr}
		}

		// Don't sleep after the last attempt
		if attempt == r.config.MaxAttempts {
			break
		}

		// Calculate sleep duration with jitter
		sleepDuration := r.addJitter(backoff)

		// Wait or check for context cancellation
		select {
		case <-ctx.Done():
			return nil, RetryResult{Attempts: attempt, LastErr: ctx.Err()}
		case <-time.After(sleepDuration):
		}

		// Increase backoff for next iteration
		backoff = time.Duration(float64(backoff) * r.config.BackoffMultiplier)
		if backoff > r.config.MaxBackoff {
			backoff = r.config.MaxBackoff
		}
	}

	return nil, RetryResult{Attempts: r.config.MaxAttempts, LastErr: lastErr}
}

func (r *Retryer) addJitter(d time.Duration) time.Duration {
	if r.config.Jitter == 0 {
		return d
	}
	// Add random jitter: d * (1 ± jitter)
	jitterRange := float64(d) * r.config.Jitter
	jitter := (rand.Float64()*2 - 1) * jitterRange
	return time.Duration(float64(d) + jitter)
}

// Retry is a convenience function for simple retry operations.
func Retry(ctx context.Context, maxAttempts int, op func() error) error {
	r := NewRetryer(RetryConfig{MaxAttempts: maxAttempts})
	result := r.Do(ctx, op)
	return result.LastErr
}

// RetryWithBackoff is a convenience function with configurable backoff.
func RetryWithBackoff(ctx context.Context, maxAttempts int, initialBackoff time.Duration, op func() error) error {
	r := NewRetryer(RetryConfig{
		MaxAttempts:    maxAttempts,
		InitialBackoff: initialBackoff,
	})
	result := r.Do(ctx, op)
	return result.LastErr
}

// IsRetryable checks if an error is typically retryable (transient).
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Context errors are not retryable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Check for common retryable error patterns
	errStr := err.Error()
	retryablePatterns := []string{
		"connection refused",
		"connection reset",
		"timeout",
		"temporary failure",
		"service unavailable",
		"too many requests",
		"rate limit",
		"503",
		"502",
		"504",
		"429",
	}

	for _, pattern := range retryablePatterns {
		if containsIgnoreCase(errStr, pattern) {
			return true
		}
	}

	return false
}

func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			len(s) > len(substr) &&
				(indexIgnoreCase(s, substr) >= 0))
}

func indexIgnoreCase(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(s) < len(substr) {
		return -1
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			c1, c2 := s[i+j], substr[j]
			if c1 != c2 && toLower(c1) != toLower(c2) {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

func toLower(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + 32
	}
	return c
}

// CircuitBreaker implements a simple circuit breaker pattern.
// It is safe for concurrent use.
type CircuitBreaker struct {
	mu           sync.Mutex
	maxFailures  int
	resetTimeout time.Duration
	failures     int
	lastFailure  time.Time
	state        circuitState
}

type circuitState int

const (
	circuitClosed circuitState = iota
	circuitOpen
	circuitHalfOpen
)

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
		state:        circuitClosed,
	}
}

// ErrCircuitOpen is returned when the circuit breaker is open.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// Execute runs the operation through the circuit breaker.
func (cb *CircuitBreaker) Execute(op func() error) error {
	cb.mu.Lock()
	allowed := cb.allowRequestLocked()
	cb.mu.Unlock()

	if !allowed {
		return ErrCircuitOpen
	}

	err := op()

	cb.mu.Lock()
	cb.recordResultLocked(err)
	cb.mu.Unlock()

	return err
}

func (cb *CircuitBreaker) allowRequestLocked() bool {
	switch cb.state {
	case circuitClosed:
		return true
	case circuitOpen:
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = circuitHalfOpen
			return true
		}
		return false
	case circuitHalfOpen:
		return true
	}
	return true
}

func (cb *CircuitBreaker) recordResultLocked(err error) {
	if err == nil {
		cb.failures = 0
		cb.state = circuitClosed
		return
	}

	cb.failures++
	cb.lastFailure = time.Now()

	if cb.failures >= cb.maxFailures {
		cb.state = circuitOpen
	}
}

// State returns the current circuit breaker state as a string.
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case circuitClosed:
		return "closed"
	case circuitOpen:
		return "open"
	case circuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Failures returns the current failure count.
func (cb *CircuitBreaker) Failures() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.failures
}

// computeBackoff calculates exponential backoff duration.
func computeBackoff(attempt int, initial, max time.Duration, multiplier float64) time.Duration {
	if attempt <= 0 {
		return initial
	}
	backoff := float64(initial) * math.Pow(multiplier, float64(attempt-1))
	if backoff > float64(max) {
		return max
	}
	return time.Duration(backoff)
}
