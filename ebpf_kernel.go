//go:build !nostubs

package chronicle

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// BPFProgram represents a loaded BPF program.
type BPFProgram struct {
	Name       string            `json:"name"`
	Type       BPFProgType       `json:"type"`
	AttachPoint string           `json:"attach_point"`
	Loaded     bool              `json:"loaded"`
	FD         int               `json:"fd,omitempty"`
	Maps       map[string]*BPFMap `json:"maps,omitempty"`
}

// BPFProgType identifies the BPF program type.
type BPFProgType int

const (
	BPFProgKProbe BPFProgType = iota
	BPFProgTracepoint
	BPFProgCGroupSKB
	BPFProgPerfEvent
	BPFProgRawTracepoint
)

func (t BPFProgType) String() string {
	switch t {
	case BPFProgKProbe:
		return "kprobe"
	case BPFProgTracepoint:
		return "tracepoint"
	case BPFProgCGroupSKB:
		return "cgroup_skb"
	case BPFProgPerfEvent:
		return "perf_event"
	case BPFProgRawTracepoint:
		return "raw_tracepoint"
	default:
		return "unknown"
	}
}

// BPFMap represents a BPF map for sharing data between kernel and user space.
type BPFMap struct {
	Name       string     `json:"name"`
	Type       BPFMapType `json:"type"`
	KeySize    int        `json:"key_size"`
	ValueSize  int        `json:"value_size"`
	MaxEntries int        `json:"max_entries"`
	FD         int        `json:"fd,omitempty"`
	data       map[string][]byte
	mu         sync.RWMutex
}

// BPFMapType identifies the BPF map type.
type BPFMapType int

const (
	BPFMapHash BPFMapType = iota
	BPFMapArray
	BPFMapPerfEventArray
	BPFMapRingBuf
	BPFMapLRUHash
)

// NewBPFMap creates a new BPF map.
func NewBPFMap(name string, mapType BPFMapType, keySize, valueSize, maxEntries int) *BPFMap {
	return &BPFMap{
		Name:       name,
		Type:       mapType,
		KeySize:    keySize,
		ValueSize:  valueSize,
		MaxEntries: maxEntries,
		data:       make(map[string][]byte),
	}
}

// Lookup reads a value from the map.
func (m *BPFMap) Lookup(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[string(key)]
	if !ok {
		return nil, fmt.Errorf("key not found")
	}
	result := make([]byte, len(v))
	copy(result, v)
	return result, nil
}

// Update writes a value to the map.
func (m *BPFMap) Update(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.data) >= m.MaxEntries {
		return fmt.Errorf("map full")
	}
	v := make([]byte, len(value))
	copy(v, value)
	m.data[string(key)] = v
	return nil
}

// Delete removes a key from the map.
func (m *BPFMap) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(key))
	return nil
}

// --- BPF Loader ---

// BPFLoader manages loading and lifecycle of BPF programs.
type BPFLoader struct {
	mu       sync.RWMutex
	programs map[string]*BPFProgram
	maps     map[string]*BPFMap
	loaded   bool
}

// NewBPFLoader creates a new BPF loader.
func NewBPFLoader() *BPFLoader {
	return &BPFLoader{
		programs: make(map[string]*BPFProgram),
		maps:     make(map[string]*BPFMap),
	}
}

// LoadProgram loads a BPF program (or simulates it on non-Linux).
func (l *BPFLoader) LoadProgram(prog BPFProgram) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if prog.Name == "" {
		return fmt.Errorf("bpf: program name required")
	}

	if runtime.GOOS != "linux" {
		// Simulate loaded program
		prog.Loaded = true
		prog.FD = len(l.programs) + 100
		l.programs[prog.Name] = &prog
		return nil
	}

	// On Linux: check for bpf capability
	if !hasBPFCapability() {
		prog.Loaded = true // mark as loaded in simulation mode
		l.programs[prog.Name] = &prog
		return nil
	}

	prog.Loaded = true
	l.programs[prog.Name] = &prog
	return nil
}

// CreateMap creates a BPF map.
func (l *BPFLoader) CreateMap(name string, mapType BPFMapType, keySize, valueSize, maxEntries int) (*BPFMap, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	m := NewBPFMap(name, mapType, keySize, valueSize, maxEntries)
	l.maps[name] = m
	return m, nil
}

// GetMap returns a loaded BPF map by name.
func (l *BPFLoader) GetMap(name string) (*BPFMap, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	m, ok := l.maps[name]
	return m, ok
}

// Close unloads all programs and closes maps.
func (l *BPFLoader) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.programs = make(map[string]*BPFProgram)
	l.maps = make(map[string]*BPFMap)
	l.loaded = false
	return nil
}

func hasBPFCapability() bool {
	if os.Getuid() == 0 {
		return true
	}
	// Check /proc/sys/kernel/unprivileged_bpf_disabled
	data, err := os.ReadFile("/proc/sys/kernel/unprivileged_bpf_disabled")
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(data)) == "0"
}

// --- Perf Ring Buffer ---

// PerfRingBuffer provides a user-space ring buffer for receiving BPF events.
type PerfRingBuffer struct {
	mu       sync.Mutex
	events   chan []byte
	closed   bool
	capacity int
	lost     int64
}

// NewPerfRingBuffer creates a new perf ring buffer.
func NewPerfRingBuffer(capacity int) *PerfRingBuffer {
	if capacity <= 0 {
		capacity = 4096
	}
	return &PerfRingBuffer{
		events:   make(chan []byte, capacity),
		capacity: capacity,
	}
}

// Write writes an event to the ring buffer.
func (rb *PerfRingBuffer) Write(data []byte) error {
	rb.mu.Lock()
	if rb.closed {
		rb.mu.Unlock()
		return fmt.Errorf("ring buffer closed")
	}
	rb.mu.Unlock()

	select {
	case rb.events <- data:
		return nil
	default:
		rb.mu.Lock()
		rb.lost++
		rb.mu.Unlock()
		return fmt.Errorf("ring buffer full, event dropped")
	}
}

// Read reads the next event from the ring buffer (blocks until available).
func (rb *PerfRingBuffer) Read() ([]byte, error) {
	data, ok := <-rb.events
	if !ok {
		return nil, fmt.Errorf("ring buffer closed")
	}
	return data, nil
}

// TryRead reads an event without blocking.
func (rb *PerfRingBuffer) TryRead() ([]byte, bool) {
	select {
	case data := <-rb.events:
		return data, true
	default:
		return nil, false
	}
}

// Close closes the ring buffer.
func (rb *PerfRingBuffer) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if !rb.closed {
		rb.closed = true
		close(rb.events)
	}
}

// Lost returns the number of dropped events.
func (rb *PerfRingBuffer) Lost() int64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.lost
}

// --- Container-Aware Cgroup Metrics ---

// CgroupMetrics provides container-aware cgroup-scoped metrics collection.
type CgroupMetrics struct {
	mu       sync.RWMutex
	metrics  map[string]*ContainerMetrics
}

// ContainerMetrics holds metrics for a single container/cgroup.
type ContainerMetrics struct {
	ContainerID string            `json:"container_id"`
	PodName     string            `json:"pod_name,omitempty"`
	Namespace   string            `json:"namespace,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	CPUUsageNs  int64             `json:"cpu_usage_ns"`
	MemUsageBytes int64           `json:"mem_usage_bytes"`
	MemLimitBytes int64           `json:"mem_limit_bytes"`
	NetRxBytes    int64           `json:"net_rx_bytes"`
	NetTxBytes    int64           `json:"net_tx_bytes"`
	DiskReadBytes int64           `json:"disk_read_bytes"`
	DiskWriteBytes int64          `json:"disk_write_bytes"`
	PIDCount      int             `json:"pid_count"`
	OOMKills      int64           `json:"oom_kills"`
	CollectedAt   time.Time       `json:"collected_at"`
}

// NewCgroupMetrics creates a container metrics collector.
func NewCgroupMetrics() *CgroupMetrics {
	return &CgroupMetrics{
		metrics: make(map[string]*ContainerMetrics),
	}
}

// RecordMetrics records metrics for a container.
func (cm *CgroupMetrics) RecordMetrics(m *ContainerMetrics) {
	if m == nil || m.ContainerID == "" {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	m.CollectedAt = time.Now()
	cm.metrics[m.ContainerID] = m
}

// GetMetrics returns metrics for a specific container.
func (cm *CgroupMetrics) GetMetrics(containerID string) (*ContainerMetrics, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	m, ok := cm.metrics[containerID]
	return m, ok
}

// ListContainers returns all known container IDs.
func (cm *CgroupMetrics) ListContainers() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	ids := make([]string, 0, len(cm.metrics))
	for id := range cm.metrics {
		ids = append(ids, id)
	}
	return ids
}

// ToPoints converts container metrics to Chronicle points for storage.
func (cm *CgroupMetrics) ToPoints() []Point {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var points []Point
	now := time.Now().UnixNano()

	for _, m := range cm.metrics {
		tags := map[string]string{
			"container_id": m.ContainerID,
		}
		if m.PodName != "" {
			tags["pod"] = m.PodName
		}
		if m.Namespace != "" {
			tags["namespace"] = m.Namespace
		}
		for k, v := range m.Labels {
			tags[k] = v
		}

		points = append(points,
			Point{Metric: "container_cpu_usage_ns", Tags: tags, Value: float64(m.CPUUsageNs), Timestamp: now},
			Point{Metric: "container_memory_bytes", Tags: tags, Value: float64(m.MemUsageBytes), Timestamp: now},
			Point{Metric: "container_net_rx_bytes", Tags: tags, Value: float64(m.NetRxBytes), Timestamp: now},
			Point{Metric: "container_net_tx_bytes", Tags: tags, Value: float64(m.NetTxBytes), Timestamp: now},
			Point{Metric: "container_disk_read_bytes", Tags: tags, Value: float64(m.DiskReadBytes), Timestamp: now},
			Point{Metric: "container_disk_write_bytes", Tags: tags, Value: float64(m.DiskWriteBytes), Timestamp: now},
			Point{Metric: "container_pids", Tags: tags, Value: float64(m.PIDCount), Timestamp: now},
			Point{Metric: "container_oom_kills", Tags: tags, Value: float64(m.OOMKills), Timestamp: now},
		)
	}
	return points
}

// --- System Probe Definitions ---

// SystemProbe defines a kernel probe to attach.
type SystemProbe struct {
	Name        string      `json:"name"`
	Type        BPFProgType `json:"type"`
	AttachPoint string      `json:"attach_point"`
	Description string      `json:"description"`
}

// DefaultSystemProbes returns the standard system probes for monitoring.
func DefaultSystemProbes() []SystemProbe {
	return []SystemProbe{
		{Name: "tcp_retransmit", Type: BPFProgKProbe, AttachPoint: "tcp_retransmit_skb", Description: "TCP retransmit events"},
		{Name: "tcp_connect", Type: BPFProgKProbe, AttachPoint: "tcp_v4_connect", Description: "TCP connection events"},
		{Name: "tcp_close", Type: BPFProgKProbe, AttachPoint: "tcp_close", Description: "TCP close events"},
		{Name: "block_io", Type: BPFProgTracepoint, AttachPoint: "block:block_rq_complete", Description: "Block I/O completion latency"},
		{Name: "block_io_start", Type: BPFProgTracepoint, AttachPoint: "block:block_rq_issue", Description: "Block I/O start"},
		{Name: "oom_kill", Type: BPFProgTracepoint, AttachPoint: "oom:oom_score_adj_update", Description: "OOM kill events"},
		{Name: "sched_process", Type: BPFProgTracepoint, AttachPoint: "sched:sched_process_exec", Description: "Process execution"},
		{Name: "cgroup_attach", Type: BPFProgCGroupSKB, AttachPoint: "cgroup/skb", Description: "Cgroup network events"},
	}
}
