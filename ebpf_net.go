package chronicle

import (
	"sync"
	"time"
)

// EBPFNetEvent represents a network event captured via eBPF.
type EBPFNetEvent struct {
	Timestamp  int64  `json:"timestamp"`
	Type       string `json:"type"` // "tcp_connect", "tcp_close", "dns_query", "dns_response"
	PID        int    `json:"pid"`
	ProcessName string `json:"process_name,omitempty"`
	SourceIP   string `json:"source_ip"`
	SourcePort int    `json:"source_port"`
	DestIP     string `json:"dest_ip"`
	DestPort   int    `json:"dest_port"`
	BytesSent  int64  `json:"bytes_sent"`
	BytesRecv  int64  `json:"bytes_recv"`
	LatencyNs  int64  `json:"latency_ns,omitempty"`
	DNSQuery   string `json:"dns_query,omitempty"`
	DNSRcode   int    `json:"dns_rcode,omitempty"`
}

// EBPFNetCollector collects network metrics via eBPF or /proc fallback.
type EBPFNetCollector struct {
	events  []EBPFNetEvent
	stats   EBPFNetStats
	mu      sync.RWMutex
}

// EBPFNetStats provides aggregated network statistics.
type EBPFNetStats struct {
	TotalConnections int64            `json:"total_connections"`
	ActiveConnections int64           `json:"active_connections"`
	TotalBytesSent   int64            `json:"total_bytes_sent"`
	TotalBytesRecv   int64            `json:"total_bytes_recv"`
	DNSQueries       int64            `json:"dns_queries"`
	ConnectionsByPort map[int]int64   `json:"connections_by_port"`
	TopDestinations  map[string]int64 `json:"top_destinations"`
}

// NewEBPFNetCollector creates a new network collector.
func NewEBPFNetCollector() *EBPFNetCollector {
	return &EBPFNetCollector{
		events: make([]EBPFNetEvent, 0),
		stats: EBPFNetStats{
			ConnectionsByPort: make(map[int]int64),
			TopDestinations:   make(map[string]int64),
		},
	}
}

// RecordEvent records a network event.
func (c *EBPFNetCollector) RecordEvent(event EBPFNetEvent) {
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixNano()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.events) >= 10000 {
		c.events = c.events[1:]
	}
	c.events = append(c.events, event)

	// Update stats
	switch event.Type {
	case "tcp_connect":
		c.stats.TotalConnections++
		c.stats.ActiveConnections++
		c.stats.ConnectionsByPort[event.DestPort]++
		c.stats.TopDestinations[event.DestIP]++
	case "tcp_close":
		c.stats.ActiveConnections--
		if c.stats.ActiveConnections < 0 {
			c.stats.ActiveConnections = 0
		}
	case "dns_query":
		c.stats.DNSQueries++
	}

	c.stats.TotalBytesSent += event.BytesSent
	c.stats.TotalBytesRecv += event.BytesRecv
}

// GetStats returns network statistics.
func (c *EBPFNetCollector) GetStats() EBPFNetStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

// ToPoints converts network stats to time-series points.
func (c *EBPFNetCollector) ToPoints() []Point {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now().UnixNano()
	return []Point{
		{Metric: "ebpf_net_connections_total", Value: float64(c.stats.TotalConnections), Timestamp: now},
		{Metric: "ebpf_net_active_connections", Value: float64(c.stats.ActiveConnections), Timestamp: now},
		{Metric: "ebpf_net_bytes_sent_total", Value: float64(c.stats.TotalBytesSent), Timestamp: now},
		{Metric: "ebpf_net_bytes_recv_total", Value: float64(c.stats.TotalBytesRecv), Timestamp: now},
		{Metric: "ebpf_net_dns_queries_total", Value: float64(c.stats.DNSQueries), Timestamp: now},
	}
}
