package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ChaosFaultInjector defines a pluggable fault that can be injected into the system.
type ChaosFaultInjector interface {
	Name() string
	Inject() error
	Remove() error
	IsActive() bool
}

// DiskFullFault simulates a disk full condition.
type DiskFullFault struct {
	active bool
	mu     sync.Mutex
}

func (f *DiskFullFault) Name() string { return "disk_full" }

func (f *DiskFullFault) Inject() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = true
	return nil
}

func (f *DiskFullFault) Remove() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = false
	return nil
}

func (f *DiskFullFault) IsActive() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active
}

// Check returns an error if disk full is simulated.
func (f *DiskFullFault) Check() error {
	if f.IsActive() {
		return fmt.Errorf("simulated disk full: no space left on device")
	}
	return nil
}

// SlowIOFault injects latency into I/O operations.
type SlowIOFault struct {
	latency time.Duration
	active  bool
	mu      sync.Mutex
}

// NewSlowIOFault creates a slow I/O fault with configurable latency.
func NewSlowIOFault(latency time.Duration) *SlowIOFault {
	return &SlowIOFault{latency: latency}
}

func (f *SlowIOFault) Name() string { return "slow_io" }

func (f *SlowIOFault) Inject() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = true
	return nil
}

func (f *SlowIOFault) Remove() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = false
	return nil
}

func (f *SlowIOFault) IsActive() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active
}

// MaybeDelay adds latency if the fault is active.
func (f *SlowIOFault) MaybeDelay() {
	f.MaybeDelayContext(context.Background())
}

// MaybeDelayContext adds latency if the fault is active, respecting context cancellation.
func (f *SlowIOFault) MaybeDelayContext(ctx context.Context) {
	if f.IsActive() {
		select {
		case <-time.After(f.latency):
		case <-ctx.Done():
		}
	}
}

// CorruptWriteFault randomly corrupts write data.
type CorruptWriteFault struct {
	probability float64 // 0-1
	rng         *rand.Rand
	active      bool
	mu          sync.Mutex
}

// NewCorruptWriteFault creates a corruption fault with given probability.
func NewCorruptWriteFault(probability float64, seed int64) *CorruptWriteFault {
	return &CorruptWriteFault{
		probability: probability,
		rng:         rand.New(rand.NewSource(seed)),
	}
}

func (f *CorruptWriteFault) Name() string { return "corrupt_write" }

func (f *CorruptWriteFault) Inject() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = true
	return nil
}

func (f *CorruptWriteFault) Remove() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = false
	return nil
}

func (f *CorruptWriteFault) IsActive() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active
}

// ShouldCorrupt returns true if this write should be corrupted.
func (f *CorruptWriteFault) ShouldCorrupt() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.active {
		return false
	}
	return f.rng.Float64() < f.probability
}

// CorruptBytes randomly flips bits in the given byte slice.
func (f *CorruptWriteFault) CorruptBytes(data []byte) []byte {
	if !f.ShouldCorrupt() || len(data) == 0 {
		return data
	}
	corrupted := make([]byte, len(data))
	copy(corrupted, data)
	// Flip a random bit
	f.mu.Lock()
	idx := f.rng.Intn(len(corrupted))
	bit := byte(1 << uint(f.rng.Intn(8)))
	f.mu.Unlock()
	corrupted[idx] ^= bit
	return corrupted
}

// OOMFault simulates out-of-memory conditions.
type OOMFault struct {
	active bool
	mu     sync.Mutex
}

func (f *OOMFault) Name() string { return "oom" }

func (f *OOMFault) Inject() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = true
	return nil
}

func (f *OOMFault) Remove() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = false
	return nil
}

func (f *OOMFault) IsActive() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active
}

// CheckAllocation returns an error if OOM is simulated and allocation exceeds threshold.
func (f *OOMFault) CheckAllocation(bytes int64) error {
	if f.IsActive() && bytes > 1024*1024 { // Reject allocations > 1MB
		return fmt.Errorf("simulated OOM: allocation of %d bytes rejected", bytes)
	}
	return nil
}

// ClockSkewFault shifts time readings for testing time-dependent logic.
type ClockSkewFault struct {
	skew   time.Duration
	active bool
	mu     sync.Mutex
}

// NewClockSkewFault creates a clock skew fault.
func NewClockSkewFault(skew time.Duration) *ClockSkewFault {
	return &ClockSkewFault{skew: skew}
}

func (f *ClockSkewFault) Name() string { return "clock_skew" }

func (f *ClockSkewFault) Inject() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = true
	return nil
}

func (f *ClockSkewFault) Remove() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = false
	return nil
}

func (f *ClockSkewFault) IsActive() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active
}

// Now returns the current time, adjusted by skew if active.
func (f *ClockSkewFault) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active {
		return time.Now().Add(f.skew)
	}
	return time.Now()
}

// RandomFaultInjector injects random faults based on a seed for reproducibility.
type RandomFaultInjector struct {
	seed   int64
	rng    *rand.Rand
	faults []ChaosFaultInjector
	mu     sync.Mutex
}

// NewRandomFaultInjector creates a seed-based random fault injector.
func NewRandomFaultInjector(seed int64, faults []ChaosFaultInjector) *RandomFaultInjector {
	return &RandomFaultInjector{
		seed:   seed,
		rng:    rand.New(rand.NewSource(seed)),
		faults: faults,
	}
}

// InjectRandom randomly activates one of the registered faults.
func (r *RandomFaultInjector) InjectRandom() (ChaosFaultInjector, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.faults) == 0 {
		return nil, fmt.Errorf("no faults registered")
	}

	idx := r.rng.Intn(len(r.faults))
	fault := r.faults[idx]
	if err := fault.Inject(); err != nil {
		return nil, fmt.Errorf("failed to inject %s: %w", fault.Name(), err)
	}
	return fault, nil
}

// RemoveAll removes all active faults.
func (r *RandomFaultInjector) RemoveAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, f := range r.faults {
		if f.IsActive() {
			f.Remove()
		}
	}
}

// Reset resets the RNG to the original seed for reproducibility.
func (r *RandomFaultInjector) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rng = rand.New(rand.NewSource(r.seed))
}
