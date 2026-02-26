package chronicle

import (
	"testing"
	"time"
)

func TestHttpCore(t *testing.T) {
	t.Run("rate_limiter_creation", func(t *testing.T) {
		rl := newRateLimiter(100, time.Second)
		if rl == nil {
			t.Fatal("expected non-nil rateLimiter")
		}
		defer rl.Stop()
	})

	t.Run("rate_limiter_allows_within_limit", func(t *testing.T) {
		rl := newRateLimiter(5, time.Minute)
		defer rl.Stop()

		for i := 0; i < 5; i++ {
			if !rl.allow("10.0.0.1") {
				t.Errorf("request %d should be allowed", i+1)
			}
		}
	})

	t.Run("rate_limiter_blocks_over_limit", func(t *testing.T) {
		rl := newRateLimiter(3, time.Minute)
		defer rl.Stop()

		for i := 0; i < 3; i++ {
			rl.allow("10.0.0.2")
		}
		if rl.allow("10.0.0.2") {
			t.Error("4th request should be blocked")
		}
	})

	t.Run("rate_limiter_independent_ips", func(t *testing.T) {
		rl := newRateLimiter(1, time.Minute)
		defer rl.Stop()

		if !rl.allow("10.0.0.3") {
			t.Error("first IP should be allowed")
		}
		if !rl.allow("10.0.0.4") {
			t.Error("second IP should be allowed independently")
		}
	})

	t.Run("rate_limiter_stop_idempotent", func(t *testing.T) {
		rl := newRateLimiter(10, time.Second)
		rl.Stop()
		rl.Stop() // should not panic
	})
}
