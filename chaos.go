// Bridge: chaos.go
//
// Re-exports types from internal/chaos/ into the public chronicle package.
// Pattern: internal/chaos/ (implementation) → chaos.go (public API)

package chronicle

import "github.com/chronicle-db/chronicle/internal/chaos"

// ChaosEventType classifies events emitted during chaos testing.
type ChaosEventType = chaos.ChaosEventType

// FaultType identifies the category of injected fault.
type FaultType = chaos.FaultType

// ChaosConfig holds configuration for the chaos testing framework.
type ChaosConfig = chaos.ChaosConfig

// FaultConfig configures a specific fault injection.
type FaultConfig = chaos.FaultConfig

// ActiveFault represents a currently active injected fault.
type ActiveFault = chaos.ActiveFault

// FaultInterceptor intercepts I/O operations to inject faults.
type FaultInterceptor = chaos.FaultInterceptor

// ChaosEvent is an event emitted during chaos testing (fault injected, removed, etc.).
type ChaosEvent = chaos.ChaosEvent

// FaultInjector manages fault injection and removal.
type FaultInjector = chaos.FaultInjector

// NetworkFaultSimulator simulates network faults such as partitions and delays.
type NetworkFaultSimulator = chaos.NetworkFaultSimulator

// DiskFaultSimulator simulates disk I/O faults.
type DiskFaultSimulator = chaos.DiskFaultSimulator

// ClockSimulator simulates clock skew for time-sensitive testing.
type ClockSimulator = chaos.ClockSimulator

// ChaosStep is a single step in a chaos test scenario.
type ChaosStep = chaos.ChaosStep

// ChaosInvariant is a condition that must hold true during chaos testing.
type ChaosInvariant = chaos.ChaosInvariant

// ChaosStepResult records the outcome of executing a single chaos step.
type ChaosStepResult = chaos.ChaosStepResult

// ChaosInvariantResult records the outcome of checking an invariant.
type ChaosInvariantResult = chaos.ChaosInvariantResult

// ChaosScenarioResult contains the full results of a chaos scenario run.
type ChaosScenarioResult = chaos.ChaosScenarioResult

// ChaosScenario defines a complete chaos testing scenario with steps and invariants.
type ChaosScenario = chaos.ChaosScenario

// ChaosReport is a summary report of chaos testing results.
type ChaosReport = chaos.ChaosReport

// ChaosFaultInjector is an extended fault injector with additional capabilities.
type ChaosFaultInjector = chaos.ChaosFaultInjector

// RandomFaultInjector injects random faults from a configured set.
type RandomFaultInjector = chaos.RandomFaultInjector

// SlowIOFault simulates slow disk I/O operations.
type SlowIOFault = chaos.SlowIOFault

// CorruptWriteFault simulates data corruption during writes.
type CorruptWriteFault = chaos.CorruptWriteFault

// DiskFullFault simulates a full disk condition.
type DiskFullFault = chaos.DiskFullFault

// ClockSkewFault simulates clock drift between nodes.
type ClockSkewFault = chaos.ClockSkewFault

// OOMFault simulates out-of-memory conditions.
type OOMFault = chaos.OOMFault

// Constants from internal/chaos.
const (
ChaosEventFaultInjected  = chaos.ChaosEventFaultInjected
ChaosEventFaultRemoved   = chaos.ChaosEventFaultRemoved
ChaosEventInvariantCheck = chaos.ChaosEventInvariantCheck
ChaosEventStepExecuted   = chaos.ChaosEventStepExecuted

FaultNetworkPartition = chaos.FaultNetworkPartition
FaultNetworkDelay     = chaos.FaultNetworkDelay
FaultDiskSlow         = chaos.FaultDiskSlow
FaultDiskFail         = chaos.FaultDiskFail
FaultMemoryPressure   = chaos.FaultMemoryPressure
FaultClockSkew        = chaos.FaultClockSkew
FaultProcessPause     = chaos.FaultProcessPause
FaultCorruptData      = chaos.FaultCorruptData
)

// Constructor wrappers from internal/chaos.
var (
DefaultChaosConfig       = chaos.DefaultChaosConfig
NewFaultInjector         = chaos.NewFaultInjector
NewNetworkFaultSimulator = chaos.NewNetworkFaultSimulator
NewDiskFaultSimulator    = chaos.NewDiskFaultSimulator
NewClockSimulator        = chaos.NewClockSimulator
NewChaosScenario         = chaos.NewChaosScenario
GenerateChaosReport      = chaos.GenerateChaosReport
NewRandomFaultInjector   = chaos.NewRandomFaultInjector
NewSlowIOFault           = chaos.NewSlowIOFault
NewCorruptWriteFault     = chaos.NewCorruptWriteFault
NewClockSkewFault        = chaos.NewClockSkewFault

// Scenario constructors
NetworkPartitionScenario = chaos.NetworkPartitionScenario
SplitBrainScenario       = chaos.SplitBrainScenario
DiskFailureScenario      = chaos.DiskFailureScenario
RollingRestartScenario   = chaos.RollingRestartScenario
)
