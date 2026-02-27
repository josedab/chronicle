package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDefaultRetryConfig(t *testing.T) {
	cfg := DefaultRetryConfig()
	if cfg.MaxAttempts != 3 {
		t.Errorf("expected 3, got %d", cfg.MaxAttempts)
	}
	if cfg.InitialBackoff != 100*time.Millisecond {
		t.Errorf("expected 100ms, got %v", cfg.InitialBackoff)
	}
}

func TestRetryer_SucceedsImmediately(t *testing.T) {
	r := NewRetryer(DefaultRetryConfig())
	result := r.Do(context.Background(), func() error { return nil })
	if result.LastErr != nil {
		t.Errorf("expected nil error, got %v", result.LastErr)
	}
	if result.Attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", result.Attempts)
	}
}

func TestRetryer_RetriesOnError(t *testing.T) {
	r := NewRetryer(RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Millisecond,
	})
	calls := 0
	result := r.Do(context.Background(), func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if result.LastErr != nil {
		t.Errorf("expected nil error, got %v", result.LastErr)
	}
	if result.Attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", result.Attempts)
	}
}

func TestRetryer_ExhaustsAttempts(t *testing.T) {
	r := NewRetryer(RetryConfig{
		MaxAttempts:    2,
		InitialBackoff: 1 * time.Millisecond,
	})
	result := r.Do(context.Background(), func() error {
		return errors.New("permanent")
	})
	if result.LastErr == nil {
		t.Error("expected error after exhausting attempts")
	}
	if result.Attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", result.Attempts)
	}
}

func TestRetryer_ContextCanceled(t *testing.T) {
	r := NewRetryer(RetryConfig{
		MaxAttempts:    10,
		InitialBackoff: time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := r.Do(ctx, func() error {
		return errors.New("fail")
	})
	if !errors.Is(result.LastErr, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", result.LastErr)
	}
}

func TestRetry_Convenience(t *testing.T) {
	err := Retry(context.Background(), 1, func() error { return nil })
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestCircuitBreaker_ClosedByDefault(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Minute)
	if cb.State() != "closed" {
		t.Errorf("expected closed, got %s", cb.State())
	}
	if cb.Failures() != 0 {
		t.Errorf("expected 0 failures, got %d", cb.Failures())
	}
}

func TestCircuitBreaker_OpensAfterFailures(t *testing.T) {
	cb := NewCircuitBreaker(2, time.Minute)
	testErr := errors.New("fail")
	for i := 0; i < 2; i++ {
		cb.Execute(func() error { return testErr })
	}
	if cb.State() != "open" {
		t.Errorf("expected open, got %s", cb.State())
	}
	err := cb.Execute(func() error { return nil })
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestIsRetryable(t *testing.T) {
	if IsRetryable(nil) {
		t.Error("nil should not be retryable")
	}
	if IsRetryable(context.Canceled) {
		t.Error("context.Canceled should not be retryable")
	}
	if !IsRetryable(errors.New("connection refused")) {
		t.Error("connection refused should be retryable")
	}
	if !IsRetryable(errors.New("timeout")) {
		t.Error("timeout should be retryable")
	}
}

func TestComputeBackoff(t *testing.T) {
	initial := 100 * time.Millisecond
	max := 10 * time.Second
	b := ComputeBackoff(0, initial, max, 2.0)
	if b != initial {
		t.Errorf("attempt 0: expected %v, got %v", initial, b)
	}
	b = ComputeBackoff(1, initial, max, 2.0)
	if b != initial {
		t.Errorf("attempt 1: expected %v, got %v", initial, b)
	}
	b = ComputeBackoff(100, initial, max, 2.0)
	if b != max {
		t.Errorf("attempt 100: expected capped at %v, got %v", max, b)
	}
}
