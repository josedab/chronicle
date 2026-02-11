package chronicle

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryerSuccess(t *testing.T) {
	r := NewRetryer(DefaultRetryConfig())

	calls := 0
	result := r.Do(context.Background(), func() error {
		calls++
		return nil
	})

	if result.Attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", result.Attempts)
	}
	if result.LastErr != nil {
		t.Errorf("expected no error, got %v", result.LastErr)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestRetryerFailureThenSuccess(t *testing.T) {
	r := NewRetryer(RetryConfig{
		MaxAttempts:    5,
		InitialBackoff: time.Millisecond,
	})

	calls := 0
	result := r.Do(context.Background(), func() error {
		calls++
		if calls < 3 {
			return errors.New("transient error")
		}
		return nil
	})

	if result.Attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", result.Attempts)
	}
	if result.LastErr != nil {
		t.Errorf("expected no error, got %v", result.LastErr)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestRetryerAllFailures(t *testing.T) {
	r := NewRetryer(RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
	})

	expectedErr := errors.New("persistent error")
	calls := 0
	result := r.Do(context.Background(), func() error {
		calls++
		return expectedErr
	})

	if result.Attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", result.Attempts)
	}
	if result.LastErr != expectedErr {
		t.Errorf("expected %v, got %v", expectedErr, result.LastErr)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestRetryerContextCancellation(t *testing.T) {
	r := NewRetryer(RetryConfig{
		MaxAttempts:    10,
		InitialBackoff: time.Second, // Long backoff
	})

	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	done := make(chan RetryResult)
	go func() {
		done <- r.Do(ctx, func() error {
			calls++
			return errors.New("error")
		})
	}()

	// Let it fail once, then cancel
	time.Sleep(10 * time.Millisecond)
	cancel()

	result := <-done
	if !errors.Is(result.LastErr, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", result.LastErr)
	}
}

func TestRetryerRetryIf(t *testing.T) {
	retryableErr := errors.New("retryable")
	nonRetryableErr := errors.New("non-retryable")

	r := NewRetryer(RetryConfig{
		MaxAttempts:    5,
		InitialBackoff: time.Millisecond,
		RetryIf: func(err error) bool {
			return err == retryableErr
		},
	})

	// Test retryable error
	calls := 0
	result := r.Do(context.Background(), func() error {
		calls++
		if calls < 3 {
			return retryableErr
		}
		return nil
	})
	if result.Attempts != 3 {
		t.Errorf("expected 3 attempts for retryable, got %d", result.Attempts)
	}

	// Test non-retryable error
	calls = 0
	result = r.Do(context.Background(), func() error {
		calls++
		return nonRetryableErr
	})
	if result.Attempts != 1 {
		t.Errorf("expected 1 attempt for non-retryable, got %d", result.Attempts)
	}
	if result.LastErr != nonRetryableErr {
		t.Errorf("expected non-retryable error")
	}
}

func TestRetryerWithResult(t *testing.T) {
	r := NewRetryer(RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
	})

	calls := 0
	val, result := r.DoWithResult(context.Background(), func() (any, error) {
		calls++
		if calls < 2 {
			return nil, errors.New("error")
		}
		return "success", nil
	})

	if result.Attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", result.Attempts)
	}
	if val != "success" {
		t.Errorf("expected 'success', got %v", val)
	}
}

func TestRetryConvenienceFunctions(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), 3, func() error {
		calls++
		if calls < 2 {
			return errors.New("error")
		}
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}

	// Test RetryWithBackoff
	calls = 0
	err = RetryWithBackoff(context.Background(), 3, time.Millisecond, func() error {
		calls++
		if calls < 2 {
			return errors.New("error")
		}
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context canceled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"connection refused", errors.New("connection refused"), true},
		{"timeout", errors.New("request timeout"), true},
		{"503", errors.New("status 503"), true},
		{"429", errors.New("429 Too Many Requests"), true},
		{"rate limit", errors.New("rate limit exceeded"), true},
		{"generic error", errors.New("something went wrong"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRetryable(tt.err); got != tt.want {
				t.Errorf("IsRetryable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(3, 100*time.Millisecond)

	// Initial state should be closed
	if cb.State() != "closed" {
		t.Errorf("expected closed state, got %s", cb.State())
	}

	// Successful calls keep circuit closed
	err := cb.Execute(func() error { return nil })
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if cb.State() != "closed" {
		t.Errorf("expected closed after success")
	}

	// Failures should accumulate
	testErr := errors.New("test error")
	for i := 0; i < 3; i++ {
		err = cb.Execute(func() error { return testErr })
		if err != testErr {
			t.Errorf("expected test error")
		}
	}

	// Circuit should now be open
	if cb.State() != "open" {
		t.Errorf("expected open state after failures, got %s", cb.State())
	}
	if cb.Failures() != 3 {
		t.Errorf("expected 3 failures, got %d", cb.Failures())
	}

	// Requests should be rejected when open
	err = cb.Execute(func() error { return nil })
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}

	// Wait for reset timeout
	time.Sleep(150 * time.Millisecond)

	// Should now be half-open and allow a request
	err = cb.Execute(func() error { return nil })
	if err != nil {
		t.Errorf("expected success in half-open state, got %v", err)
	}

	// Success should close the circuit
	if cb.State() != "closed" {
		t.Errorf("expected closed after success in half-open, got %s", cb.State())
	}
}

func TestComputeBackoff(t *testing.T) {
	initial := 100 * time.Millisecond
	max := 10 * time.Second
	multiplier := 2.0

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, initial},
		{1, initial},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{10, max}, // Should cap at max
	}

	for _, tt := range tests {
		got := computeBackoff(tt.attempt, initial, max, multiplier)
		if got != tt.want {
			t.Errorf("computeBackoff(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestCircuitBreakerConcurrent(t *testing.T) {
	cb := NewCircuitBreaker(100, 100*time.Millisecond)

	// Run many goroutines concurrently to detect race conditions
	const numGoroutines = 50
	const opsPerGoroutine = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < opsPerGoroutine; j++ {
				// Mix of successful and failing operations
				if j%3 == 0 {
					_ = cb.Execute(func() error {
						return errors.New("intentional failure")
					})
				} else {
					_ = cb.Execute(func() error {
						return nil
					})
				}
				// Also read state concurrently
				_ = cb.State()
				_ = cb.Failures()
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Basic sanity check - circuit should be in a valid state
	state := cb.State()
	if state != "closed" && state != "open" && state != "half-open" {
		t.Errorf("unexpected state: %s", state)
	}
}

func TestCircuitBreakerHalfOpenFailure(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond)

	// Trip the circuit
	testErr := errors.New("error")
	for i := 0; i < 2; i++ {
		_ = cb.Execute(func() error { return testErr })
	}

	if cb.State() != "open" {
		t.Fatalf("expected open state, got %s", cb.State())
	}

	// Wait for reset timeout
	time.Sleep(60 * time.Millisecond)

	// Fail in half-open state - should go back to open
	err := cb.Execute(func() error { return testErr })
	if err != testErr {
		t.Errorf("expected test error, got %v", err)
	}

	// Should be open again after failure in half-open
	if cb.State() != "open" {
		t.Errorf("expected open state after half-open failure, got %s", cb.State())
	}
}
