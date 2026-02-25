// Bridge: chaos.go
//
// Re-exports types from internal/chaos/ into the public chronicle package.
// Pattern: internal/chaos/ (implementation) → chaos.go (public API)

package chronicle

import "github.com/chronicle-db/chronicle/internal/chaos"

// Type aliases from internal/chaos.
type ChaosEventType = chaos.ChaosEventType
type FaultType = chaos.FaultType
type ChaosConfig = chaos.ChaosConfig
type FaultConfig = chaos.FaultConfig
type ActiveFault = chaos.ActiveFault
type FaultInterceptor = chaos.FaultInterceptor
type ChaosEvent = chaos.ChaosEvent
type FaultInjector = chaos.FaultInjector
type NetworkFaultSimulator = chaos.NetworkFaultSimulator
type DiskFaultSimulator = chaos.DiskFaultSimulator
type ClockSimulator = chaos.ClockSimulator
type ChaosStep = chaos.ChaosStep
type ChaosInvariant = chaos.ChaosInvariant
type ChaosStepResult = chaos.ChaosStepResult
type ChaosInvariantResult = chaos.ChaosInvariantResult
type ChaosScenarioResult = chaos.ChaosScenarioResult
type ChaosScenario = chaos.ChaosScenario
type ChaosReport = chaos.ChaosReport
type ChaosFaultInjector = chaos.ChaosFaultInjector
type RandomFaultInjector = chaos.RandomFaultInjector
type SlowIOFault = chaos.SlowIOFault
type CorruptWriteFault = chaos.CorruptWriteFault
type DiskFullFault = chaos.DiskFullFault
type ClockSkewFault = chaos.ClockSkewFault
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
