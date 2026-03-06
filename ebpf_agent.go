package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// --- TCP Connection Tracking (kprobe stubs) ---

// TCPConnectionEvent represents a tracked TCP connection event.
type TCPConnectionEvent struct {
	Type      string    `json:"type"` // connect, accept, close
	SrcAddr   string    `json:"src_addr"`
	SrcPort   uint16    `json:"src_port"`
	DstAddr   string    `json:"dst_addr"`
	DstPort   uint16    `json:"dst_port"`
	PID       int       `json:"pid"`
	Comm      string    `json:"comm"`
	Duration  time.Duration `json:"duration,omitempty"`
	BytesSent uint64    `json:"bytes_sent,omitempty"`
	BytesRecv uint64    `json:"bytes_recv,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// TCPTracker tracks TCP connections using kprobe-based instrumentation.
type TCPTracker struct {
	connections map[string]*TCPConnectionEvent
	mu          sync.RWMutex
	stats       TCPTrackerStats
}

// TCPTrackerStats contains TCP tracking statistics.
type TCPTrackerStats struct {
	ActiveConnections int64 `json:"active_connections"`
	TotalConnects     int64 `json:"total_connects"`
	TotalAccepts      int64 `json:"total_accepts"`
	TotalCloses       int64 `json:"total_closes"`
}

// NewTCPTracker creates a new TCP connection tracker.
func NewTCPTracker() *TCPTracker {
	return &TCPTracker{
		connections: make(map[string]*TCPConnectionEvent),
	}
}

// RecordConnect records a TCP connect event (kprobe: tcp_v4_connect).
func (t *TCPTracker) RecordConnect(event TCPConnectionEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()

	event.Type = "connect"
	event.Timestamp = time.Now()
	key := fmt.Sprintf("%s:%d->%s:%d@%d", event.SrcAddr, event.SrcPort, event.DstAddr, event.DstPort, event.PID)
	t.connections[key] = &event
	t.stats.TotalConnects++
	t.stats.ActiveConnections++
}

// RecordAccept records a TCP accept event (kprobe: inet_csk_accept).
func (t *TCPTracker) RecordAccept(event TCPConnectionEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()

	event.Type = "accept"
	event.Timestamp = time.Now()
	key := fmt.Sprintf("%s:%d<-%s:%d@%d", event.DstAddr, event.DstPort, event.SrcAddr, event.SrcPort, event.PID)
	t.connections[key] = &event
	t.stats.TotalAccepts++
	t.stats.ActiveConnections++
}

// RecordClose records a TCP close event (kprobe: tcp_close).
func (t *TCPTracker) RecordClose(srcAddr string, srcPort uint16, dstAddr string, dstPort uint16, pid int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := fmt.Sprintf("%s:%d->%s:%d@%d", srcAddr, srcPort, dstAddr, dstPort, pid)
	if conn, ok := t.connections[key]; ok {
		conn.Duration = time.Since(conn.Timestamp)
		delete(t.connections, key)
	}
	t.stats.TotalCloses++
	if t.stats.ActiveConnections > 0 {
		t.stats.ActiveConnections--
	}
}

// ActiveConnections returns currently active connections.
func (t *TCPTracker) ActiveConnections() []*TCPConnectionEvent {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]*TCPConnectionEvent, 0, len(t.connections))
	for _, conn := range t.connections {
		result = append(result, conn)
	}
	return result
}

// Stats returns TCP tracking statistics.
func (t *TCPTracker) Stats() TCPTrackerStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.stats
}

// --- HTTP Request Tracing (uprobe stubs) ---

// HTTPRequestTrace represents a traced HTTP request.
type HTTPRequestTrace struct {
	Method     string            `json:"method"`
	URL        string            `json:"url"`
	StatusCode int               `json:"status_code"`
	Duration   time.Duration     `json:"duration"`
	PID        int               `json:"pid"`
	Comm       string            `json:"comm"`
	Headers    map[string]string `json:"headers,omitempty"`
	Timestamp  time.Time         `json:"timestamp"`
}

// HTTPTracer traces HTTP requests using uprobe-based instrumentation.
type HTTPTracer struct {
	traces []HTTPRequestTrace
	mu     sync.RWMutex
	stats  HTTPTracerStats
}

// HTTPTracerStats contains HTTP tracing statistics.
type HTTPTracerStats struct {
	TotalRequests   int64   `json:"total_requests"`
	TotalErrors     int64   `json:"total_errors"`
	AvgDurationMs   float64 `json:"avg_duration_ms"`
	P99DurationMs   float64 `json:"p99_duration_ms"`
}

// NewHTTPTracer creates a new HTTP request tracer.
func NewHTTPTracer() *HTTPTracer {
	return &HTTPTracer{
		traces: make([]HTTPRequestTrace, 0, 1000),
	}
}

// RecordTrace records an HTTP request trace.
func (t *HTTPTracer) RecordTrace(trace HTTPRequestTrace) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if trace.Timestamp.IsZero() {
		trace.Timestamp = time.Now()
	}

	t.traces = append(t.traces, trace)
	t.stats.TotalRequests++
	if trace.StatusCode >= 400 {
		t.stats.TotalErrors++
	}

	// Update average duration
	t.stats.AvgDurationMs = (t.stats.AvgDurationMs*float64(t.stats.TotalRequests-1) +
		float64(trace.Duration.Milliseconds())) / float64(t.stats.TotalRequests)

	// Trim old traces
	if len(t.traces) > 10000 {
		t.traces = t.traces[len(t.traces)-10000:]
	}
}

// Stats returns HTTP tracing statistics.
func (t *HTTPTracer) Stats() HTTPTracerStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.stats
}

// --- DNS Resolution Tracking ---

// DNSQueryEvent represents a tracked DNS query.
type DNSQueryEvent struct {
	Domain   string        `json:"domain"`
	Type     string        `json:"type"` // A, AAAA, CNAME, etc.
	Result   []string      `json:"result,omitempty"`
	Duration time.Duration `json:"duration"`
	PID      int           `json:"pid"`
	Comm     string        `json:"comm"`
	Error    string        `json:"error,omitempty"`
	Timestamp time.Time    `json:"timestamp"`
}

// DNSTracker tracks DNS resolution events.
type DNSTracker struct {
	queries []DNSQueryEvent
	cache   map[string]*DNSQueryEvent
	mu      sync.RWMutex
	stats   DNSTrackerStats
}

// DNSTrackerStats contains DNS tracking statistics.
type DNSTrackerStats struct {
	TotalQueries  int64   `json:"total_queries"`
	TotalErrors   int64   `json:"total_errors"`
	AvgLatencyMs  float64 `json:"avg_latency_ms"`
	CacheHitRate  float64 `json:"cache_hit_rate"`
	UniqueDomains int     `json:"unique_domains"`
}

// NewDNSTracker creates a new DNS tracker.
func NewDNSTracker() *DNSTracker {
	return &DNSTracker{
		queries: make([]DNSQueryEvent, 0, 1000),
		cache:   make(map[string]*DNSQueryEvent),
	}
}

// RecordQuery records a DNS query event.
func (t *DNSTracker) RecordQuery(event DNSQueryEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	t.queries = append(t.queries, event)
	t.cache[event.Domain] = &event
	t.stats.TotalQueries++
	if event.Error != "" {
		t.stats.TotalErrors++
	}
	t.stats.UniqueDomains = len(t.cache)
	t.stats.AvgLatencyMs = (t.stats.AvgLatencyMs*float64(t.stats.TotalQueries-1) +
		float64(event.Duration.Milliseconds())) / float64(t.stats.TotalQueries)

	if len(t.queries) > 10000 {
		t.queries = t.queries[len(t.queries)-10000:]
	}
}

// Stats returns DNS tracking statistics.
func (t *DNSTracker) Stats() DNSTrackerStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.stats
}

// --- Container-Aware Process Discovery ---

// EBPFContainerInfo describes a container discovered via cgroup.
type EBPFContainerInfo struct {
	ContainerID string            `json:"container_id"`
	Runtime     string            `json:"runtime"` // docker, containerd, cri-o
	PodName     string            `json:"pod_name,omitempty"`
	Namespace   string            `json:"namespace,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	PIDs        []int             `json:"pids"`
}

// ProcessInfo describes a discovered process.
type ProcessInfo struct {
	PID         int            `json:"pid"`
	PPID        int            `json:"ppid"`
	Comm        string         `json:"comm"`
	Cmdline     string         `json:"cmdline"`
	Container   *EBPFContainerInfo `json:"container,omitempty"`
	ServiceName string         `json:"service_name,omitempty"`
	StartTime   time.Time      `json:"start_time"`
}

// ProcessDiscovery discovers and tracks processes with container awareness.
type ProcessDiscovery struct {
	processes  map[int]*ProcessInfo
	containers map[string]*EBPFContainerInfo
	mu         sync.RWMutex
}

// NewProcessDiscovery creates a new process discovery engine.
func NewProcessDiscovery() *ProcessDiscovery {
	return &ProcessDiscovery{
		processes:  make(map[int]*ProcessInfo),
		containers: make(map[string]*EBPFContainerInfo),
	}
}

// RegisterProcess registers a discovered process.
func (pd *ProcessDiscovery) RegisterProcess(info ProcessInfo) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	if info.StartTime.IsZero() {
		info.StartTime = time.Now()
	}
	pd.processes[info.PID] = &info

	if info.Container != nil && info.Container.ContainerID != "" {
		existing, ok := pd.containers[info.Container.ContainerID]
		if ok {
			existing.PIDs = append(existing.PIDs, info.PID)
		} else {
			info.Container.PIDs = []int{info.PID}
			pd.containers[info.Container.ContainerID] = info.Container
		}
	}
}

// GetProcess returns process info by PID.
func (pd *ProcessDiscovery) GetProcess(pid int) (*ProcessInfo, bool) {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	p, ok := pd.processes[pid]
	return p, ok
}

// ListProcesses returns all discovered processes.
func (pd *ProcessDiscovery) ListProcesses() []*ProcessInfo {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	result := make([]*ProcessInfo, 0, len(pd.processes))
	for _, p := range pd.processes {
		result = append(result, p)
	}
	return result
}

// ListContainers returns all discovered containers.
func (pd *ProcessDiscovery) ListContainers() []*EBPFContainerInfo {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	result := make([]*EBPFContainerInfo, 0, len(pd.containers))
	for _, c := range pd.containers {
		result = append(result, c)
	}
	return result
}

// --- Service Dependency Graph ---

// EBPFServiceEdge represents a connection between two services.
type EBPFServiceEdge struct {
	Source      string    `json:"source"`
	Target      string    `json:"target"`
	Protocol    string    `json:"protocol"` // tcp, http, grpc, dns
	RequestCount int64    `json:"request_count"`
	ErrorCount  int64     `json:"error_count"`
	AvgLatency  time.Duration `json:"avg_latency"`
	LastSeen    time.Time `json:"last_seen"`
}

// ServiceDependencyGraph builds a service map from observed connections.
type ServiceDependencyGraph struct {
	edges map[string]*EBPFServiceEdge // keyed by "source->target"
	mu    sync.RWMutex
}

// NewServiceDependencyGraph creates a new service dependency graph.
func NewServiceDependencyGraph() *ServiceDependencyGraph {
	return &ServiceDependencyGraph{
		edges: make(map[string]*EBPFServiceEdge),
	}
}

// RecordConnection records an observed connection between services.
func (g *ServiceDependencyGraph) RecordConnection(source, target, protocol string, latency time.Duration, isError bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	key := source + "->" + target
	edge, ok := g.edges[key]
	if !ok {
		edge = &EBPFServiceEdge{
			Source:   source,
			Target:   target,
			Protocol: protocol,
		}
		g.edges[key] = edge
	}

	edge.RequestCount++
	if isError {
		edge.ErrorCount++
	}
	edge.AvgLatency = time.Duration(
		(int64(edge.AvgLatency)*int64(edge.RequestCount-1) + int64(latency)) / int64(edge.RequestCount))
	edge.LastSeen = time.Now()
}

// GetEdges returns all service dependency edges.
func (g *ServiceDependencyGraph) GetEdges() []*EBPFServiceEdge {
	g.mu.RLock()
	defer g.mu.RUnlock()
	result := make([]*EBPFServiceEdge, 0, len(g.edges))
	for _, e := range g.edges {
		result = append(result, e)
	}
	return result
}

// GetServiceMap returns the full service map as a structure for visualization.
func (g *ServiceDependencyGraph) GetServiceMap() map[string]interface{} {
	g.mu.RLock()
	defer g.mu.RUnlock()

	services := make(map[string]bool)
	edgeList := make([]map[string]interface{}, 0, len(g.edges))

	for _, e := range g.edges {
		services[e.Source] = true
		services[e.Target] = true
		edgeList = append(edgeList, map[string]interface{}{
			"source":        e.Source,
			"target":        e.Target,
			"protocol":      e.Protocol,
			"request_count": e.RequestCount,
			"error_count":   e.ErrorCount,
			"avg_latency_ms": float64(e.AvgLatency.Milliseconds()),
			"error_rate":    float64(e.ErrorCount) / float64(e.RequestCount),
		})
	}

	nodes := make([]string, 0, len(services))
	for s := range services {
		nodes = append(nodes, s)
	}

	return map[string]interface{}{
		"nodes": nodes,
		"edges": edgeList,
	}
}

// --- eBPF Auto-Instrumentation Agent ---

// AutoInstrumentationAgent is the main eBPF agent that combines all trackers.
type AutoInstrumentationAgent struct {
	config           EBPFConfig
	db               *DB
	tcpTracker       *TCPTracker
	httpTracer       *HTTPTracer
	dnsTracker       *DNSTracker
	processDiscovery *ProcessDiscovery
	serviceGraph     *ServiceDependencyGraph
	collector        *EBPFCollector
	ingestCancel     func()
	ingestWg         sync.WaitGroup
	mu               sync.RWMutex
}

// NewAutoInstrumentationAgent creates a new auto-instrumentation agent.
func NewAutoInstrumentationAgent(db *DB, config EBPFConfig) *AutoInstrumentationAgent {
	return &AutoInstrumentationAgent{
		config:           config,
		db:               db,
		tcpTracker:       NewTCPTracker(),
		httpTracer:       NewHTTPTracer(),
		dnsTracker:       NewDNSTracker(),
		processDiscovery: NewProcessDiscovery(),
		serviceGraph:     NewServiceDependencyGraph(),
		collector:        NewEBPFCollector(db, config),
	}
}

// Start starts the auto-instrumentation agent and periodic ingestion.
func (a *AutoInstrumentationAgent) Start() error {
	if err := a.collector.Start(); err != nil {
		return err
	}
	// Start periodic ingestion if we have a DB
	if a.db != nil && a.config.CollectionInterval > 0 {
		a.startPeriodicIngestion(a.config.CollectionInterval)
	}
	return nil
}

// Stop stops the agent and ingestion loop.
func (a *AutoInstrumentationAgent) Stop() error {
	if a.ingestCancel != nil {
		a.ingestCancel()
		a.ingestWg.Wait()
	}
	return a.collector.Stop()
}

func (a *AutoInstrumentationAgent) startPeriodicIngestion(interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	a.ingestCancel = cancel

	a.ingestWg.Add(1)
	go func() {
		defer a.ingestWg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.IngestTrackerData(a.db)
			}
		}
	}()
}

// TCPTracker returns the TCP connection tracker.
func (a *AutoInstrumentationAgent) TCPTracker() *TCPTracker { return a.tcpTracker }

// HTTPTracer returns the HTTP request tracer.
func (a *AutoInstrumentationAgent) HTTPTracer() *HTTPTracer { return a.httpTracer }

// DNSTracker returns the DNS tracker.
func (a *AutoInstrumentationAgent) DNSTracker() *DNSTracker { return a.dnsTracker }

// ProcessDiscovery returns the process discovery engine.
func (a *AutoInstrumentationAgent) ProcessDiscovery() *ProcessDiscovery { return a.processDiscovery }

// ServiceGraph returns the service dependency graph.
func (a *AutoInstrumentationAgent) ServiceGraph() *ServiceDependencyGraph { return a.serviceGraph }

// Stats returns aggregated agent statistics.
func (a *AutoInstrumentationAgent) Stats() map[string]interface{} {
	return map[string]interface{}{
		"collector":  a.collector.Stats(),
		"tcp":        a.tcpTracker.Stats(),
		"http":       a.httpTracer.Stats(),
		"dns":        a.dnsTracker.Stats(),
		"processes":  len(a.processDiscovery.ListProcesses()),
		"containers": len(a.processDiscovery.ListContainers()),
		"services":   a.serviceGraph.GetServiceMap(),
	}
}

// IngestTrackerData converts tracker data into Chronicle Points for ingestion.
func (a *AutoInstrumentationAgent) IngestTrackerData(db *DB) error {
	if db == nil {
		return fmt.Errorf("ebpf_agent: nil database")
	}

	var points []Point
	now := time.Now().UnixNano()

	// Ingest TCP tracker stats
	tcpStats := a.tcpTracker.Stats()
	points = append(points,
		Point{Metric: "ebpf_tcp_active_connections", Value: float64(tcpStats.ActiveConnections), Timestamp: now, Tags: map[string]string{"source": "ebpf"}},
		Point{Metric: "ebpf_tcp_total_connects", Value: float64(tcpStats.TotalConnects), Timestamp: now, Tags: map[string]string{"source": "ebpf"}},
		Point{Metric: "ebpf_tcp_total_accepts", Value: float64(tcpStats.TotalAccepts), Timestamp: now, Tags: map[string]string{"source": "ebpf"}},
		Point{Metric: "ebpf_tcp_total_closes", Value: float64(tcpStats.TotalCloses), Timestamp: now, Tags: map[string]string{"source": "ebpf"}},
	)

	// Ingest HTTP tracer stats
	httpStats := a.httpTracer.Stats()
	points = append(points,
		Point{Metric: "ebpf_http_total_requests", Value: float64(httpStats.TotalRequests), Timestamp: now, Tags: map[string]string{"source": "ebpf"}},
		Point{Metric: "ebpf_http_total_errors", Value: float64(httpStats.TotalErrors), Timestamp: now, Tags: map[string]string{"source": "ebpf"}},
		Point{Metric: "ebpf_http_avg_duration_ms", Value: httpStats.AvgDurationMs, Timestamp: now, Tags: map[string]string{"source": "ebpf"}},
	)

	// Ingest DNS tracker stats
	dnsStats := a.dnsTracker.Stats()
	points = append(points,
		Point{Metric: "ebpf_dns_total_queries", Value: float64(dnsStats.TotalQueries), Timestamp: now, Tags: map[string]string{"source": "ebpf"}},
		Point{Metric: "ebpf_dns_total_errors", Value: float64(dnsStats.TotalErrors), Timestamp: now, Tags: map[string]string{"source": "ebpf"}},
		Point{Metric: "ebpf_dns_unique_domains", Value: float64(dnsStats.UniqueDomains), Timestamp: now, Tags: map[string]string{"source": "ebpf"}},
	)

	// Ingest service graph edge metrics
	for _, edge := range a.serviceGraph.GetEdges() {
		tags := map[string]string{"source": edge.Source, "target": edge.Target, "protocol": edge.Protocol}
		points = append(points,
			Point{Metric: "ebpf_service_requests", Value: float64(edge.RequestCount), Timestamp: now, Tags: tags},
			Point{Metric: "ebpf_service_errors", Value: float64(edge.ErrorCount), Timestamp: now, Tags: tags},
			Point{Metric: "ebpf_service_latency_ms", Value: float64(edge.AvgLatency.Milliseconds()), Timestamp: now, Tags: tags},
		)
	}

	return db.WriteBatch(points)
}

// RegisterHTTPHandlers registers HTTP endpoints for the eBPF agent.
func (a *AutoInstrumentationAgent) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/ebpf/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a.Stats())
	})
	mux.HandleFunc("/api/v1/ebpf/service-map", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a.serviceGraph.GetServiceMap())
	})
	mux.HandleFunc("/api/v1/ebpf/connections", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a.tcpTracker.ActiveConnections())
	})
	mux.HandleFunc("/api/v1/ebpf/processes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a.processDiscovery.ListProcesses())
	})
	mux.HandleFunc("/api/v1/ebpf/containers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a.processDiscovery.ListContainers())
	})
}
