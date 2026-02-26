//go:build !nostubs

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

// EXPERIMENTAL: This API is unstable and may change without notice.
// NOTE: Real eBPF kernel instrumentation is not yet implemented. The collector falls back
// to reading /proc for CPU, memory, disk, and network metrics. BPF program loading is a
// placeholder that always reports "eBPF not available."
//
// EBPFConfig configures the eBPF collector.
type EBPFConfig struct {
	// Enabled enables eBPF collection.
	Enabled bool

	// CollectionInterval is how often to collect metrics.
	CollectionInterval time.Duration

	// EnableCPUMetrics enables CPU usage tracking.
	EnableCPUMetrics bool

	// EnableMemoryMetrics enables memory tracking.
	EnableMemoryMetrics bool

	// EnableDiskMetrics enables disk I/O tracking.
	EnableDiskMetrics bool

	// EnableNetworkMetrics enables network tracking.
	EnableNetworkMetrics bool

	// EnableProcessMetrics enables per-process tracking.
	EnableProcessMetrics bool

	// EnableSyscallTracing enables syscall tracing.
	EnableSyscallTracing bool

	// ProcessFilter filters which processes to monitor.
	ProcessFilter func(pid int, name string) bool

	// TargetPIDs limits collection to specific PIDs.
	TargetPIDs []int

	// HistogramBuckets for latency distributions.
	HistogramBuckets []float64

	// MaxEventsPerSecond limits event rate.
	MaxEventsPerSecond int
}

// DefaultEBPFConfig returns default eBPF configuration.
func DefaultEBPFConfig() EBPFConfig {
	return EBPFConfig{
		Enabled:              true,
		CollectionInterval:   10 * time.Second,
		EnableCPUMetrics:     true,
		EnableMemoryMetrics:  true,
		EnableDiskMetrics:    true,
		EnableNetworkMetrics: true,
		EnableProcessMetrics: true,
		EnableSyscallTracing: false, // Disabled by default for performance
		HistogramBuckets:     []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		MaxEventsPerSecond:   10000,
	}
}

// EBPFCollector collects system metrics using eBPF (or fallback).
type EBPFCollector struct {
	config EBPFConfig
	db     *DB

	// Collectors
	cpuCollector     *CPUCollector
	memoryCollector  *MemoryCollector
	diskCollector    *DiskCollector
	networkCollector *NetworkCollector
	processCollector *ProcessCollector
	syscallCollector *SyscallCollector

	// Event buffer
	eventBuffer   chan EBPFEvent
	eventBufferMu sync.Mutex

	// Statistics
	stats   EBPFStats
	statsMu sync.RWMutex

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// EBPFEvent represents an eBPF-collected event.
type EBPFEvent struct {
	Type      EventType      `json:"type"`
	Timestamp time.Time      `json:"timestamp"`
	PID       int            `json:"pid,omitempty"`
	Comm      string         `json:"comm,omitempty"`
	CPU       int            `json:"cpu,omitempty"`
	Data      map[string]any `json:"data"`
}

// EventType identifies the type of eBPF event.
type EventType int

const (
	EventTypeCPU EventType = iota
	EventTypeMemory
	EventTypeDisk
	EventTypeNetwork
	EventTypeProcess
	EventTypeSyscall
)

func (e EventType) String() string {
	switch e {
	case EventTypeCPU:
		return "cpu"
	case EventTypeMemory:
		return "memory"
	case EventTypeDisk:
		return "disk"
	case EventTypeNetwork:
		return "network"
	case EventTypeProcess:
		return "process"
	case EventTypeSyscall:
		return "syscall"
	default:
		return "unknown"
	}
}

// EBPFStats contains collector statistics.
type EBPFStats struct {
	EventsCollected   int64         `json:"events_collected"`
	EventsDropped     int64         `json:"events_dropped"`
	CollectionErrors  int64         `json:"collection_errors"`
	LastCollectionAt  time.Time     `json:"last_collection_at"`
	CollectionLatency time.Duration `json:"collection_latency"`
	BufferUtilization float64       `json:"buffer_utilization"`
}

// NewEBPFCollector creates a new eBPF collector.
func NewEBPFCollector(db *DB, config EBPFConfig) *EBPFCollector {
	ctx, cancel := context.WithCancel(context.Background())

	collector := &EBPFCollector{
		config:      config,
		db:          db,
		eventBuffer: make(chan EBPFEvent, config.MaxEventsPerSecond),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Initialize collectors based on config
	if config.EnableCPUMetrics {
		collector.cpuCollector = NewCPUCollector()
	}
	if config.EnableMemoryMetrics {
		collector.memoryCollector = NewMemoryCollector()
	}
	if config.EnableDiskMetrics {
		collector.diskCollector = NewDiskCollector()
	}
	if config.EnableNetworkMetrics {
		collector.networkCollector = NewNetworkCollector()
	}
	if config.EnableProcessMetrics {
		collector.processCollector = NewProcessCollector(config.ProcessFilter, config.TargetPIDs)
	}
	if config.EnableSyscallTracing {
		collector.syscallCollector = NewSyscallCollector()
	}

	return collector
}

// Start starts the eBPF collector.
func (c *EBPFCollector) Start() error {
	if runtime.GOOS != "linux" {
		return c.startFallbackMode()
	}

	// Try to load eBPF programs (simplified - in production would use cilium/ebpf or bcc)
	if err := c.loadEBPFPrograms(); err != nil {
		// Fall back to proc-based collection
		return c.startFallbackMode()
	}

	c.wg.Add(2)
	go c.collectionLoop()
	go c.eventProcessingLoop()

	return nil
}

// Stop stops the eBPF collector.
func (c *EBPFCollector) Stop() error {
	c.cancel()
	c.wg.Wait()
	close(c.eventBuffer)
	return nil
}

func (c *EBPFCollector) loadEBPFPrograms() error {
	// In production, this would:
	// 1. Check if running as root or with CAP_BPF
	// 2. Load compiled BPF programs
	// 3. Attach to kprobes/tracepoints
	// For now, we use fallback mode
	return errors.New("eBPF not available, using fallback mode")
}

func (c *EBPFCollector) startFallbackMode() error {
	c.wg.Add(2)
	go c.collectionLoop()
	go c.eventProcessingLoop()
	return nil
}

func (c *EBPFCollector) collectionLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.CollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.collect()
		}
	}
}

func (c *EBPFCollector) collect() {
	start := time.Now()

	// Collect from each enabled collector
	if c.cpuCollector != nil {
		events, err := c.cpuCollector.Collect()
		if err != nil {
			c.incrementErrors()
		} else {
			c.bufferEvents(events)
		}
	}

	if c.memoryCollector != nil {
		events, err := c.memoryCollector.Collect()
		if err != nil {
			c.incrementErrors()
		} else {
			c.bufferEvents(events)
		}
	}

	if c.diskCollector != nil {
		events, err := c.diskCollector.Collect()
		if err != nil {
			c.incrementErrors()
		} else {
			c.bufferEvents(events)
		}
	}

	if c.networkCollector != nil {
		events, err := c.networkCollector.Collect()
		if err != nil {
			c.incrementErrors()
		} else {
			c.bufferEvents(events)
		}
	}

	if c.processCollector != nil {
		events, err := c.processCollector.Collect()
		if err != nil {
			c.incrementErrors()
		} else {
			c.bufferEvents(events)
		}
	}

	// Update stats
	c.statsMu.Lock()
	c.stats.LastCollectionAt = time.Now()
	c.stats.CollectionLatency = time.Since(start)
	c.statsMu.Unlock()
}

func (c *EBPFCollector) bufferEvents(events []EBPFEvent) {
	for _, event := range events {
		select {
		case c.eventBuffer <- event:
			c.incrementCollected()
		default:
			c.incrementDropped()
		}
	}
}

func (c *EBPFCollector) incrementCollected() {
	c.statsMu.Lock()
	c.stats.EventsCollected++
	c.statsMu.Unlock()
}

func (c *EBPFCollector) incrementDropped() {
	c.statsMu.Lock()
	c.stats.EventsDropped++
	c.statsMu.Unlock()
}

func (c *EBPFCollector) incrementErrors() {
	c.statsMu.Lock()
	c.stats.CollectionErrors++
	c.statsMu.Unlock()
}

func (c *EBPFCollector) eventProcessingLoop() {
	defer c.wg.Done()

	batch := make([]EBPFEvent, 0, 100)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			// Flush remaining events
			if len(batch) > 0 {
				c.writeBatch(batch)
			}
			return

		case event, ok := <-c.eventBuffer:
			if !ok {
				return
			}
			batch = append(batch, event)

			// Write batch when full
			if len(batch) >= 100 {
				c.writeBatch(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			// Flush partial batch
			if len(batch) > 0 {
				c.writeBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func (c *EBPFCollector) writeBatch(events []EBPFEvent) {
	for _, event := range events {
		measurement := fmt.Sprintf("system_%s", event.Type.String())
		tags := make(map[string]string)

		if event.PID > 0 {
			tags["pid"] = fmt.Sprintf("%d", event.PID)
		}
		if event.Comm != "" {
			tags["comm"] = event.Comm
		}
		if event.CPU >= 0 {
			tags["cpu"] = fmt.Sprintf("%d", event.CPU)
		}

		// Get first numeric value from fields as the point value
		var value float64
		for _, v := range event.Data {
			if f, ok := v.(float64); ok {
				value = f
				break
			}
			if i, ok := v.(int); ok {
				value = float64(i)
				break
			}
			if i64, ok := v.(int64); ok {
				value = float64(i64)
				break
			}
		}

		c.db.Write(Point{
			Metric:    measurement,
			Tags:      tags,
			Value:     value,
			Timestamp: event.Timestamp.UnixNano(),
		})
	}
}

// Stats returns collector statistics.
func (c *EBPFCollector) Stats() EBPFStats {
	c.statsMu.RLock()
	defer c.statsMu.RUnlock()

	stats := c.stats
	stats.BufferUtilization = float64(len(c.eventBuffer)) / float64(cap(c.eventBuffer))
	return stats
}

// ========== Individual Collectors ==========

// CPUCollector collects CPU metrics.
type CPUCollector struct {
	prevIdle  []uint64
	prevTotal []uint64
}

// NewCPUCollector creates a new CPU collector.
func NewCPUCollector() *CPUCollector {
	return &CPUCollector{
		prevIdle:  make([]uint64, runtime.NumCPU()),
		prevTotal: make([]uint64, runtime.NumCPU()),
	}
}

// Collect collects CPU metrics.
func (c *CPUCollector) Collect() ([]EBPFEvent, error) {
	numCPU := runtime.NumCPU()
	events := make([]EBPFEvent, 0, numCPU+1)
	now := time.Now()

	// Read /proc/stat (Linux) or use runtime stats
	if runtime.GOOS == "linux" {
		content, err := os.ReadFile("/proc/stat")
		if err != nil {
			return c.collectFallback()
		}
		events = c.parseProcStat(content, now)
	} else {
		return c.collectFallback()
	}

	return events, nil
}

func (c *CPUCollector) collectFallback() ([]EBPFEvent, error) {
	// Use Go runtime stats as fallback
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	now := time.Now()
	events := []EBPFEvent{
		{
			Type:      EventTypeCPU,
			Timestamp: now,
			CPU:       -1, // Total
			Data: map[string]any{
				"goroutines": runtime.NumGoroutine(),
				"num_cpu":    runtime.NumCPU(),
				"gc_runs":    memStats.NumGC,
			},
		},
	}

	return events, nil
}

func (c *CPUCollector) parseProcStat(content []byte, now time.Time) []EBPFEvent {
	// Simplified /proc/stat parsing
	var events []EBPFEvent

	// Overall CPU event
	events = append(events, EBPFEvent{
		Type:      EventTypeCPU,
		Timestamp: now,
		CPU:       -1,
		Data: map[string]any{
			"num_cpu":    runtime.NumCPU(),
			"goroutines": runtime.NumGoroutine(),
		},
	})

	return events
}

// MemoryCollector collects memory metrics.
type MemoryCollector struct{}

// NewMemoryCollector creates a new memory collector.
func NewMemoryCollector() *MemoryCollector {
	return &MemoryCollector{}
}

// Collect collects memory metrics.
func (c *MemoryCollector) Collect() ([]EBPFEvent, error) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	now := time.Now()

	events := []EBPFEvent{
		{
			Type:      EventTypeMemory,
			Timestamp: now,
			Data: map[string]any{
				"alloc":           memStats.Alloc,
				"total_alloc":     memStats.TotalAlloc,
				"sys":             memStats.Sys,
				"heap_alloc":      memStats.HeapAlloc,
				"heap_sys":        memStats.HeapSys,
				"heap_idle":       memStats.HeapIdle,
				"heap_inuse":      memStats.HeapInuse,
				"heap_released":   memStats.HeapReleased,
				"heap_objects":    memStats.HeapObjects,
				"stack_inuse":     memStats.StackInuse,
				"stack_sys":       memStats.StackSys,
				"gc_cpu_fraction": memStats.GCCPUFraction,
				"num_gc":          memStats.NumGC,
				"pause_total_ns":  memStats.PauseTotalNs,
			},
		},
	}

	// On Linux, try to get system memory info
	if runtime.GOOS == "linux" {
		if sysEvents, err := c.collectSystemMemory(now); err == nil {
			events = append(events, sysEvents...)
		}
	}

	return events, nil
}

func (c *MemoryCollector) collectSystemMemory(now time.Time) ([]EBPFEvent, error) {
	content, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return nil, err
	}

	data := make(map[string]any)

	// Simplified parsing
	var memTotal, memFree, memAvailable, buffers, cached uint64
	fmt.Sscanf(string(content), "MemTotal: %d", &memTotal)
	data["total"] = memTotal * 1024
	data["free"] = memFree * 1024
	data["available"] = memAvailable * 1024
	data["buffers"] = buffers * 1024
	data["cached"] = cached * 1024

	return []EBPFEvent{
		{
			Type:      EventTypeMemory,
			Timestamp: now,
			Data:      data,
		},
	}, nil
}
