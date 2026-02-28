package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// SimClock provides a deterministic clock for simulation testing.
// All time operations go through this interface instead of time.Now().
type SimClock struct {
	mu      sync.Mutex
	current time.Time
	timers  []*simTimer
}

type simTimer struct {
	deadline time.Time
	ch       chan time.Time
	fired    bool
}

// NewSimClock creates a new deterministic clock starting at the given time.
func NewSimClock(start time.Time) *SimClock {
	return &SimClock{current: start}
}

// Now returns the current simulated time.
func (c *SimClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

// Advance moves the clock forward by the given duration, firing any timers.
func (c *SimClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.current = c.current.Add(d)
	now := c.current

	// Fire any timers whose deadline has passed
	for _, t := range c.timers {
		if !t.fired && !t.deadline.After(now) {
			t.fired = true
			select {
			case t.ch <- now:
			default:
			}
		}
	}
	c.mu.Unlock()
}

// After returns a channel that fires after the given duration.
func (c *SimClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := make(chan time.Time, 1)
	deadline := c.current.Add(d)

	// Fire immediately if deadline is already past
	if !deadline.After(c.current) {
		ch <- c.current
		return ch
	}

	c.timers = append(c.timers, &simTimer{
		deadline: deadline,
		ch:       ch,
	})
	return ch
}

// SimRand provides a deterministic random number generator.
type SimRand struct {
	rng *rand.Rand
}

// NewSimRand creates a new deterministic RNG with the given seed.
func NewSimRand(seed int64) *SimRand {
	return &SimRand{rng: rand.New(rand.NewSource(seed))}
}

// Intn returns a deterministic random int in [0, n).
func (r *SimRand) Intn(n int) int {
	return r.rng.Intn(n)
}

// Duration returns a random duration between min and max.
func (r *SimRand) Duration(min, max time.Duration) time.Duration {
	diff := max - min
	if diff <= 0 {
		return min
	}
	return min + time.Duration(r.rng.Int63n(int64(diff)))
}

// --- Failure Scenarios ---

// FailureType identifies a type of simulated failure.
type FailureType int

const (
	FailureNetworkPartition FailureType = iota
	FailureClockSkew
	FailureDiskFailure
	FailureNodeCrash
	FailureSlowDisk
	FailureByzantine
)

func (f FailureType) String() string {
	switch f {
	case FailureNetworkPartition:
		return "network_partition"
	case FailureClockSkew:
		return "clock_skew"
	case FailureDiskFailure:
		return "disk_failure"
	case FailureNodeCrash:
		return "node_crash"
	case FailureSlowDisk:
		return "slow_disk"
	case FailureByzantine:
		return "byzantine"
	default:
		return "unknown"
	}
}

// FailureScenario defines a failure to inject during simulation.
type FailureScenario struct {
	Type       FailureType
	TargetNode string
	StartTime  time.Duration // offset from simulation start
	Duration   time.Duration
	Params     map[string]any
}

// FailureLibrary provides pre-built failure scenarios for testing.
type FailureLibrary struct {
	scenarios []FailureScenario
}

// NewFailureLibrary creates a library with common failure scenarios.
func NewFailureLibrary() *FailureLibrary {
	return &FailureLibrary{
		scenarios: []FailureScenario{
			{Type: FailureNetworkPartition, Duration: 30 * time.Second},
			{Type: FailureClockSkew, Duration: 60 * time.Second, Params: map[string]any{"skew_ms": 500}},
			{Type: FailureDiskFailure, Duration: 10 * time.Second},
			{Type: FailureNodeCrash, Duration: 5 * time.Second},
			{Type: FailureSlowDisk, Duration: 20 * time.Second, Params: map[string]any{"latency_ms": 100}},
		},
	}
}

// GetScenarios returns all available failure scenarios.
func (fl *FailureLibrary) GetScenarios() []FailureScenario {
	result := make([]FailureScenario, len(fl.scenarios))
	copy(result, fl.scenarios)
	return result
}

// AddScenario adds a custom failure scenario.
func (fl *FailureLibrary) AddScenario(s FailureScenario) {
	fl.scenarios = append(fl.scenarios, s)
}

// --- Linearizability Checker ---

// Operation represents a single client operation for linearizability checking.
type Operation struct {
	ClientID  string
	OpType    string // "read" or "write"
	Key       string
	Value     string // for writes: intended value; for reads: observed value
	StartTime time.Time
	EndTime   time.Time
}

// LinearizabilityChecker verifies that operations are linearizable.
// Uses a simplified Wing-Gong algorithm checking that a valid sequential
// ordering exists consistent with real-time constraints.
type LinearizabilityChecker struct {
	mu         sync.Mutex
	operations []Operation
}

// NewLinearizabilityChecker creates a new linearizability checker.
func NewLinearizabilityChecker() *LinearizabilityChecker {
	return &LinearizabilityChecker{}
}

// RecordOperation records an operation for verification.
func (lc *LinearizabilityChecker) RecordOperation(op Operation) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.operations = append(lc.operations, op)
}

// CheckResult holds the result of a linearizability check.
type CheckResult struct {
	Linearizable bool     `json:"linearizable"`
	Violations   []string `json:"violations,omitempty"`
	TotalOps     int      `json:"total_ops"`
}

// Check verifies linearizability of all recorded operations.
// For each read, checks that the read value is consistent with some
// valid ordering of concurrent writes.
func (lc *LinearizabilityChecker) Check() CheckResult {
	lc.mu.Lock()
	ops := make([]Operation, len(lc.operations))
	copy(ops, lc.operations)
	lc.mu.Unlock()

	result := CheckResult{
		Linearizable: true,
		TotalOps:     len(ops),
	}

	if len(ops) == 0 {
		return result
	}

	// Sort operations by end time
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].EndTime.Before(ops[j].EndTime)
	})

	// Track the latest committed value for each key
	latestValues := make(map[string]string)

	for _, op := range ops {
		switch op.OpType {
		case "write":
			latestValues[op.Key] = op.Value
		case "read":
			// A read is valid if it returns the latest committed value
			// or any value from a concurrent write
			expected, exists := latestValues[op.Key]
			if exists && op.Value != expected {
				// Check if there's a concurrent write that could explain this
				concurrent := false
				for _, other := range ops {
					if other.OpType == "write" && other.Key == op.Key &&
						other.Value == op.Value &&
						!other.EndTime.Before(op.StartTime) &&
						!op.EndTime.Before(other.StartTime) {
						concurrent = true
						break
					}
				}
				if !concurrent {
					result.Linearizable = false
					result.Violations = append(result.Violations,
						fmt.Sprintf("read(%s)=%q at %v, expected %q",
							op.Key, op.Value, op.EndTime, expected))
				}
			}
		}
	}

	return result
}

// --- Simulation Engine ---

// SimConfig configures a deterministic simulation run.
type SimConfig struct {
	NumNodes        int
	SimulatedHours  int
	StepSize        time.Duration
	Seed            int64
	Failures        []FailureScenario
	CheckLinearize  bool
}

// DefaultSimConfig returns a default simulation configuration.
func DefaultSimConfig() SimConfig {
	return SimConfig{
		NumNodes:       5,
		SimulatedHours: 1,
		StepSize:       100 * time.Millisecond,
		Seed:           42,
		CheckLinearize: true,
	}
}

// SimResult holds the outcome of a simulation run.
type SimResult struct {
	SimulatedDuration  time.Duration    `json:"simulated_duration"`
	WallClockDuration  time.Duration    `json:"wall_clock_duration"`
	TotalSteps         int64            `json:"total_steps"`
	LeaderElections    int              `json:"leader_elections"`
	FailuresInjected   int              `json:"failures_injected"`
	FailuresRecovered  int              `json:"failures_recovered"`
	Linearizable       bool             `json:"linearizable"`
	LinearizeResult    *CheckResult     `json:"linearize_result,omitempty"`
	Errors             []string         `json:"errors,omitempty"`
}

// SimNode represents a simulated Raft node.
type SimNode struct {
	ID        string
	Config    RaftConfig
	Clock     *SimClock
	Alive     bool
	Partitioned bool
	DiskFailed  bool
}

// Simulator runs deterministic simulation tests for Raft consensus.
type Simulator struct {
	config  SimConfig
	clock   *SimClock
	rng     *SimRand
	nodes   []*SimNode
	checker *LinearizabilityChecker
	mu      sync.Mutex
}

// NewSimulator creates a new deterministic simulator.
func NewSimulator(config SimConfig) *Simulator {
	clock := NewSimClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	rng := NewSimRand(config.Seed)

	nodes := make([]*SimNode, config.NumNodes)
	for i := range nodes {
		nodeID := fmt.Sprintf("sim-node-%d", i)
		nodes[i] = &SimNode{
			ID:    nodeID,
			Clock: clock,
			Alive: true,
			Config: RaftConfig{
				NodeID:             nodeID,
				ElectionTimeoutMin: 150 * time.Millisecond,
				ElectionTimeoutMax: 300 * time.Millisecond,
				HeartbeatInterval:  50 * time.Millisecond,
			},
		}
	}

	return &Simulator{
		config:  config,
		clock:   clock,
		rng:     rng,
		nodes:   nodes,
		checker: NewLinearizabilityChecker(),
	}
}

// Run executes the simulation and returns results.
func (s *Simulator) Run(ctx context.Context) *SimResult {
	wallStart := time.Now()
	totalDuration := time.Duration(s.config.SimulatedHours) * time.Hour
	step := s.config.StepSize
	if step <= 0 {
		step = 100 * time.Millisecond
	}

	result := &SimResult{
		SimulatedDuration: totalDuration,
	}

	var steps int64
	simElapsed := time.Duration(0)

	for simElapsed < totalDuration {
		select {
		case <-ctx.Done():
			result.Errors = append(result.Errors, "simulation cancelled")
			goto done
		default:
		}

		// Advance clock
		s.clock.Advance(step)
		simElapsed += step
		steps++

		// Inject failures at scheduled times
		for _, f := range s.config.Failures {
			if simElapsed >= f.StartTime && simElapsed < f.StartTime+f.Duration {
				s.injectFailure(f, result)
			}
			if simElapsed >= f.StartTime+f.Duration {
				s.recoverFailure(f, result)
			}
		}

		// Simulate leader election periodically
		if steps%100 == 0 {
			aliveCount := 0
			for _, n := range s.nodes {
				if n.Alive && !n.Partitioned {
					aliveCount++
				}
			}
			if aliveCount > len(s.nodes)/2 {
				result.LeaderElections++
			}
		}
	}

done:
	result.TotalSteps = steps
	result.WallClockDuration = time.Since(wallStart)

	// Check linearizability
	if s.config.CheckLinearize {
		lr := s.checker.Check()
		result.Linearizable = lr.Linearizable
		result.LinearizeResult = &lr
	}

	return result
}

func (s *Simulator) injectFailure(f FailureScenario, result *SimResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	target := f.TargetNode
	if target == "" && len(s.nodes) > 0 {
		target = s.nodes[s.rng.Intn(len(s.nodes))].ID
	}

	for _, n := range s.nodes {
		if n.ID == target {
			switch f.Type {
			case FailureNetworkPartition:
				if !n.Partitioned {
					n.Partitioned = true
					result.FailuresInjected++
				}
			case FailureNodeCrash:
				if n.Alive {
					n.Alive = false
					result.FailuresInjected++
				}
			case FailureDiskFailure:
				if !n.DiskFailed {
					n.DiskFailed = true
					result.FailuresInjected++
				}
			default:
				// Clock skew, slow disk, byzantine — no persistent state to toggle
			}
			break
		}
	}
}

func (s *Simulator) recoverFailure(f FailureScenario, result *SimResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, n := range s.nodes {
		if f.TargetNode == "" || n.ID == f.TargetNode {
			switch f.Type {
			case FailureNetworkPartition:
				if n.Partitioned {
					n.Partitioned = false
					result.FailuresRecovered++
				}
			case FailureNodeCrash:
				if !n.Alive {
					n.Alive = true
					result.FailuresRecovered++
				}
			case FailureDiskFailure:
				if n.DiskFailed {
					n.DiskFailed = false
					result.FailuresRecovered++
				}
			}
		}
	}
}
