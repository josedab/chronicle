package chronicle

import (
	"fmt"
	"os"
	"runtime"
	"time"
)

// Disk, network, process, and syscall collectors, system metrics, and monitor DB for eBPF.

// DiskCollector collects disk I/O metrics.
type DiskCollector struct {
	prevReads  map[string]uint64
	prevWrites map[string]uint64
}

// NewDiskCollector creates a new disk collector.
func NewDiskCollector() *DiskCollector {
	return &DiskCollector{
		prevReads:  make(map[string]uint64),
		prevWrites: make(map[string]uint64),
	}
}

// Collect collects disk metrics.
func (c *DiskCollector) Collect() ([]EBPFEvent, error) {
	now := time.Now()
	events := []EBPFEvent{}

	if runtime.GOOS == "linux" {
		// Read /proc/diskstats
		content, err := os.ReadFile("/proc/diskstats")
		if err != nil {
			return events, err
		}

		events = c.parseDiskStats(content, now)
	}

	return events, nil
}

func (c *DiskCollector) parseDiskStats(content []byte, now time.Time) []EBPFEvent {
	// Simplified diskstats parsing
	events := []EBPFEvent{
		{
			Type:      EventTypeDisk,
			Timestamp: now,
			Data: map[string]any{
				"available": true,
			},
		},
	}
	return events
}

// NetworkCollector collects network metrics.
type NetworkCollector struct {
	prevRxBytes map[string]uint64
	prevTxBytes map[string]uint64
}

// NewNetworkCollector creates a new network collector.
func NewNetworkCollector() *NetworkCollector {
	return &NetworkCollector{
		prevRxBytes: make(map[string]uint64),
		prevTxBytes: make(map[string]uint64),
	}
}

// Collect collects network metrics.
func (c *NetworkCollector) Collect() ([]EBPFEvent, error) {
	now := time.Now()
	events := []EBPFEvent{}

	if runtime.GOOS == "linux" {
		// Read /proc/net/dev
		content, err := os.ReadFile("/proc/net/dev")
		if err != nil {
			return events, err
		}

		events = c.parseNetDev(content, now)
	}

	return events, nil
}

func (c *NetworkCollector) parseNetDev(content []byte, now time.Time) []EBPFEvent {
	// Simplified net/dev parsing
	events := []EBPFEvent{
		{
			Type:      EventTypeNetwork,
			Timestamp: now,
			Data: map[string]any{
				"available": true,
			},
		},
	}
	return events
}

// ProcessCollector collects per-process metrics.
type ProcessCollector struct {
	filter     func(pid int, name string) bool
	targetPIDs []int
}

// NewProcessCollector creates a new process collector.
func NewProcessCollector(filter func(pid int, name string) bool, targetPIDs []int) *ProcessCollector {
	return &ProcessCollector{
		filter:     filter,
		targetPIDs: targetPIDs,
	}
}

// Collect collects process metrics.
func (c *ProcessCollector) Collect() ([]EBPFEvent, error) {
	now := time.Now()
	events := []EBPFEvent{}

	// Collect for current process
	pid := os.Getpid()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	events = append(events, EBPFEvent{
		Type:      EventTypeProcess,
		Timestamp: now,
		PID:       pid,
		Comm:      os.Args[0],
		Data: map[string]any{
			"memory_alloc": memStats.Alloc,
			"goroutines":   runtime.NumGoroutine(),
			"gc_cpu_frac":  memStats.GCCPUFraction,
		},
	})

	// On Linux, collect for target PIDs
	if runtime.GOOS == "linux" && len(c.targetPIDs) > 0 {
		for _, targetPID := range c.targetPIDs {
			if event, err := c.collectProcess(targetPID, now); err == nil {
				events = append(events, event)
			}
		}
	}

	return events, nil
}

func (c *ProcessCollector) collectProcess(pid int, now time.Time) (EBPFEvent, error) {
	// Read /proc/<pid>/stat
	statPath := fmt.Sprintf("/proc/%d/stat", pid)
	content, err := os.ReadFile(statPath)
	if err != nil {
		return EBPFEvent{}, err
	}

	// Simplified parsing
	var parsedPID int
	var comm string
	fmt.Sscanf(string(content), "%d (%s", &parsedPID, &comm)

	return EBPFEvent{
		Type:      EventTypeProcess,
		Timestamp: now,
		PID:       pid,
		Comm:      comm,
		Data: map[string]any{
			"collected": true,
		},
	}, nil
}

// SyscallCollector collects syscall traces.
type SyscallCollector struct {
	enabled bool
}

// NewSyscallCollector creates a new syscall collector.
func NewSyscallCollector() *SyscallCollector {
	return &SyscallCollector{
		enabled: false, // Requires eBPF
	}
}

// Collect collects syscall metrics (requires eBPF).
func (c *SyscallCollector) Collect() ([]EBPFEvent, error) {
	// Syscall tracing requires actual eBPF programs
	// This is a placeholder for when eBPF is available
	return nil, nil
}

// ========== High-Level API ==========

// SystemMetrics provides high-level system metrics access.
type SystemMetrics struct {
	collector *EBPFCollector
	db        *DB
}

// NewSystemMetrics creates a new system metrics collector.
func NewSystemMetrics(db *DB, config EBPFConfig) *SystemMetrics {
	return &SystemMetrics{
		collector: NewEBPFCollector(db, config),
		db:        db,
	}
}

// Start starts collecting system metrics.
func (m *SystemMetrics) Start() error {
	return m.collector.Start()
}

// Stop stops collecting system metrics.
func (m *SystemMetrics) Stop() error {
	return m.collector.Stop()
}

// GetCPUUsage returns recent CPU usage.
func (m *SystemMetrics) GetCPUUsage(duration time.Duration) ([]CPUUsage, error) {
	// Create query for CPU metrics
	endTime := time.Now()
	startTime := endTime.Add(-duration)

	query := &Query{
		Metric: "system_cpu",
		Start:  startTime.UnixNano(),
		End:    endTime.UnixNano(),
	}

	result, err := m.db.Execute(query)
	if err != nil {
		return nil, err
	}

	var usage []CPUUsage
	for _, p := range result.Points {
		u := CPUUsage{Timestamp: time.Unix(0, p.Timestamp)}
		usage = append(usage, u)
	}

	return usage, nil
}

// CPUUsage represents CPU usage at a point in time.
type CPUUsage struct {
	Timestamp time.Time `json:"timestamp"`
	CPU       int       `json:"cpu"`
	UserPct   float64   `json:"user_pct"`
	SystemPct float64   `json:"system_pct"`
	IdlePct   float64   `json:"idle_pct"`
	IOWaitPct float64   `json:"iowait_pct"`
	StealPct  float64   `json:"steal_pct"`
}

// GetMemoryUsage returns recent memory usage.
func (m *SystemMetrics) GetMemoryUsage(duration time.Duration) ([]MemoryUsage, error) {
	endTime := time.Now()
	startTime := endTime.Add(-duration)

	query := &Query{
		Metric: "system_memory",
		Start:  startTime.UnixNano(),
		End:    endTime.UnixNano(),
	}

	result, err := m.db.Execute(query)
	if err != nil {
		return nil, err
	}

	var usage []MemoryUsage
	for _, p := range result.Points {
		u := MemoryUsage{Timestamp: time.Unix(0, p.Timestamp)}
		usage = append(usage, u)
	}

	return usage, nil
}

// MemoryUsage represents memory usage at a point in time.
type MemoryUsage struct {
	Timestamp time.Time `json:"timestamp"`
	Total     uint64    `json:"total"`
	Used      uint64    `json:"used"`
	Free      uint64    `json:"free"`
	Buffers   uint64    `json:"buffers"`
	Cached    uint64    `json:"cached"`
	SwapTotal uint64    `json:"swap_total"`
	SwapUsed  uint64    `json:"swap_used"`
}

// GetDiskIO returns recent disk I/O.
func (m *SystemMetrics) GetDiskIO(duration time.Duration) ([]DiskIO, error) {
	endTime := time.Now()
	startTime := endTime.Add(-duration)

	query := &Query{
		Metric: "system_disk",
		Start:  startTime.UnixNano(),
		End:    endTime.UnixNano(),
	}

	result, err := m.db.Execute(query)
	if err != nil {
		return nil, err
	}

	var io []DiskIO
	for _, p := range result.Points {
		d := DiskIO{Timestamp: time.Unix(0, p.Timestamp)}
		io = append(io, d)
	}

	return io, nil
}

// DiskIO represents disk I/O at a point in time.
type DiskIO struct {
	Timestamp     time.Time `json:"timestamp"`
	Device        string    `json:"device"`
	ReadsPerSec   float64   `json:"reads_per_sec"`
	WritesPerSec  float64   `json:"writes_per_sec"`
	ReadBytesPS   float64   `json:"read_bytes_ps"`
	WriteBytesPS  float64   `json:"write_bytes_ps"`
	AvgQueueLen   float64   `json:"avg_queue_len"`
	AvgWaitTimeMs float64   `json:"avg_wait_time_ms"`
}

// GetNetworkIO returns recent network I/O.
func (m *SystemMetrics) GetNetworkIO(duration time.Duration) ([]NetworkIO, error) {
	endTime := time.Now()
	startTime := endTime.Add(-duration)

	query := &Query{
		Metric: "system_network",
		Start:  startTime.UnixNano(),
		End:    endTime.UnixNano(),
	}

	result, err := m.db.Execute(query)
	if err != nil {
		return nil, err
	}

	var io []NetworkIO
	for _, p := range result.Points {
		n := NetworkIO{Timestamp: time.Unix(0, p.Timestamp)}
		io = append(io, n)
	}

	return io, nil
}

// NetworkIO represents network I/O at a point in time.
type NetworkIO struct {
	Timestamp   time.Time `json:"timestamp"`
	Interface   string    `json:"interface"`
	RxBytesPS   float64   `json:"rx_bytes_ps"`
	TxBytesPS   float64   `json:"tx_bytes_ps"`
	RxPacketsPS float64   `json:"rx_packets_ps"`
	TxPacketsPS float64   `json:"tx_packets_ps"`
	RxErrors    uint64    `json:"rx_errors"`
	TxErrors    uint64    `json:"tx_errors"`
	RxDropped   uint64    `json:"rx_dropped"`
	TxDropped   uint64    `json:"tx_dropped"`
}

// Stats returns collector statistics.
func (m *SystemMetrics) Stats() EBPFStats {
	return m.collector.Stats()
}

// ========== eBPF Program Templates ==========

// EBPFProgram represents an eBPF program template.
type EBPFProgram struct {
	Name        string
	Type        string // kprobe, tracepoint, xdp, etc.
	AttachPoint string
	Source      string
}

// GetAvailablePrograms returns available eBPF programs.
func GetAvailablePrograms() []EBPFProgram {
	return []EBPFProgram{
		{
			Name:        "cpu_distribution",
			Type:        "kprobe",
			AttachPoint: "finish_task_switch",
			Source:      cpuDistributionBPF,
		},
		{
			Name:        "syscall_latency",
			Type:        "tracepoint",
			AttachPoint: "raw_syscalls/sys_enter",
			Source:      syscallLatencyBPF,
		},
		{
			Name:        "block_io",
			Type:        "kprobe",
			AttachPoint: "blk_mq_start_request",
			Source:      blockIOBPF,
		},
		{
			Name:        "tcp_connect",
			Type:        "kprobe",
			AttachPoint: "tcp_v4_connect",
			Source:      tcpConnectBPF,
		},
	}
}

// BPF program source templates (simplified examples)
const cpuDistributionBPF = `
// CPU distribution tracking
BPF_HISTOGRAM(cpu_time, int);

int kprobe__finish_task_switch(struct pt_regs *ctx, struct task_struct *prev) {
    u64 ts = bpf_ktime_get_ns();
    // Record CPU time distribution
    cpu_time.increment(bpf_log2l(ts));
    return 0;
}
`

const syscallLatencyBPF = `
// Syscall latency tracking
BPF_HASH(start_time, u32, u64);
BPF_HISTOGRAM(syscall_latency, int);

TRACEPOINT_PROBE(raw_syscalls, sys_enter) {
    u64 ts = bpf_ktime_get_ns();
    u32 tid = bpf_get_current_pid_tgid();
    start_time.update(&tid, &ts);
    return 0;
}

TRACEPOINT_PROBE(raw_syscalls, sys_exit) {
    u64 *tsp, delta;
    u32 tid = bpf_get_current_pid_tgid();
    
    tsp = start_time.lookup(&tid);
    if (tsp != 0) {
        delta = bpf_ktime_get_ns() - *tsp;
        syscall_latency.increment(bpf_log2l(delta / 1000));
        start_time.delete(&tid);
    }
    return 0;
}
`

const blockIOBPF = `
// Block I/O tracking
BPF_HASH(request_start, struct request *, u64);
BPF_HISTOGRAM(io_latency, int);

int kprobe__blk_mq_start_request(struct pt_regs *ctx, struct request *req) {
    u64 ts = bpf_ktime_get_ns();
    request_start.update(&req, &ts);
    return 0;
}

int kprobe__blk_account_io_done(struct pt_regs *ctx, struct request *req) {
    u64 *tsp, delta;
    
    tsp = request_start.lookup(&req);
    if (tsp != 0) {
        delta = bpf_ktime_get_ns() - *tsp;
        io_latency.increment(bpf_log2l(delta / 1000));
        request_start.delete(&req);
    }
    return 0;
}
`

const tcpConnectBPF = `
// TCP connection tracking
BPF_HASH(connect_start, struct sock *, u64);

int kprobe__tcp_v4_connect(struct pt_regs *ctx, struct sock *sk) {
    u64 ts = bpf_ktime_get_ns();
    connect_start.update(&sk, &ts);
    return 0;
}

int kretprobe__tcp_v4_connect(struct pt_regs *ctx) {
    // Connection established
    return 0;
}
`

// EBPFMonitorDB provides a database wrapper with eBPF monitoring.
type EBPFMonitorDB struct {
	*DB
	metrics *SystemMetrics
}

// NewEBPFMonitorDB creates a database with eBPF monitoring.
func NewEBPFMonitorDB(db *DB, config EBPFConfig) *EBPFMonitorDB {
	return &EBPFMonitorDB{
		DB:      db,
		metrics: NewSystemMetrics(db, config),
	}
}

// Metrics returns the system metrics collector.
func (db *EBPFMonitorDB) Metrics() *SystemMetrics {
	return db.metrics
}

// Start starts the monitoring.
func (db *EBPFMonitorDB) Start() error {
	return db.metrics.Start()
}

// Stop stops the monitoring.
func (db *EBPFMonitorDB) Stop() error {
	return db.metrics.Stop()
}
