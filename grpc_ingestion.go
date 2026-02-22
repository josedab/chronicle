package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// GRPCIngestionConfig configures the gRPC ingestion server.
type GRPCIngestionConfig struct {
	Enabled         bool
	ListenAddr      string
	MaxMessageSize  int
	MaxConcurrent   int
	WriteTimeout    time.Duration
	QueryTimeout    time.Duration
	EnableReflection bool
	TLSCertFile     string
	TLSKeyFile      string
}

// DefaultGRPCIngestionConfig returns sensible defaults.
func DefaultGRPCIngestionConfig() GRPCIngestionConfig {
	return GRPCIngestionConfig{
		Enabled:         true,
		ListenAddr:      ":9095",
		MaxMessageSize:  16 * 1024 * 1024, // 16MB
		MaxConcurrent:   100,
		WriteTimeout:    10 * time.Second,
		QueryTimeout:    30 * time.Second,
		EnableReflection: true,
	}
}

// WriteRequest represents a protobuf-style write request.
type WriteRequest struct {
	TimeSeries []ProtoTimeSeries `json:"time_series"`
}

// ProtoTimeSeries represents a time series in the proto format.
type ProtoTimeSeries struct {
	Labels  []ProtoLabel  `json:"labels"`
	Samples []ProtoSample `json:"samples"`
}

// ProtoLabel represents a metric label.
type ProtoLabel struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ProtoSample represents a single data point.
type ProtoSample struct {
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
}

// QueryRequest represents a protobuf-style query request.
type QueryRequest struct {
	Metric    string            `json:"metric"`
	Labels    map[string]string `json:"labels"`
	StartTime int64            `json:"start_time"`
	EndTime   int64            `json:"end_time"`
	Aggregate string           `json:"aggregate"`
	GroupBy   []string          `json:"group_by"`
	Limit     int              `json:"limit"`
}

// QueryResponse represents a protobuf-style query response.
type QueryResponse struct {
	Series     []ResultSeries `json:"series"`
	QueryTimeMs int64         `json:"query_time_ms"`
	PointCount  int           `json:"point_count"`
}

// ResultSeries represents a single result series.
type ResultSeries struct {
	Metric  string            `json:"metric"`
	Labels  map[string]string `json:"labels"`
	Samples []ProtoSample     `json:"samples"`
}

// GRPCServiceInfo contains metadata about a registered gRPC service.
type GRPCServiceInfo struct {
	Name    string   `json:"name"`
	Methods []string `json:"methods"`
}

// GRPCIngestionEngine provides a gRPC-compatible ingestion and query server.
// While full gRPC requires protobuf codegen, this engine provides a
// high-performance binary-compatible JSON API following the same patterns
// and can serve as the foundation for full gRPC when proto dependencies are added.
type GRPCIngestionEngine struct {
	db     *DB
	config GRPCIngestionConfig

	mu       sync.RWMutex
	listener net.Listener
	running  bool
	stopCh   chan struct{}

	// Stats
	stats GRPCIngestionStats

	// Registered services for reflection
	services []GRPCServiceInfo

	// Interceptors (middleware chain)
	unaryInterceptors []UnaryInterceptor
}

// GRPCIngestionStats tracks server metrics.
type GRPCIngestionStats struct {
	TotalWriteRequests int64         `json:"total_write_requests"`
	TotalQueryRequests int64         `json:"total_query_requests"`
	TotalPointsWritten int64         `json:"total_points_written"`
	TotalErrors        int64         `json:"total_errors"`
	AvgWriteLatency    time.Duration `json:"avg_write_latency"`
	AvgQueryLatency    time.Duration `json:"avg_query_latency"`
	ActiveConnections  int64         `json:"active_connections"`
	StartedAt          time.Time     `json:"started_at"`
}

// UnaryInterceptor is a function that intercepts unary RPC calls.
type UnaryInterceptor func(ctx context.Context, method string, req interface{}) (interface{}, error)

// NewGRPCIngestionEngine creates a new gRPC ingestion engine.
func NewGRPCIngestionEngine(db *DB, cfg GRPCIngestionConfig) *GRPCIngestionEngine {
	g := &GRPCIngestionEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
		services: []GRPCServiceInfo{
			{
				Name: "chronicle.WriteService",
				Methods: []string{
					"Write",
					"WriteBatch",
					"WriteStream",
				},
			},
			{
				Name: "chronicle.QueryService",
				Methods: []string{
					"Query",
					"QueryRange",
					"QueryStream",
				},
			},
			{
				Name: "chronicle.AdminService",
				Methods: []string{
					"ListMetrics",
					"GetStats",
					"HealthCheck",
				},
			},
		},
	}
	return g
}

// Start starts the gRPC ingestion server.
func (g *GRPCIngestionEngine) Start() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.running {
		return nil
	}

	ln, err := net.Listen("tcp", g.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	g.listener = ln
	g.running = true
	g.stats.StartedAt = time.Now()

	go g.serve()
	return nil
}

// Stop stops the gRPC ingestion server.
func (g *GRPCIngestionEngine) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.running {
		return
	}
	g.running = false
	close(g.stopCh)
	if g.listener != nil {
		g.listener.Close()
	}
}

// AddInterceptor adds a unary interceptor to the chain.
func (g *GRPCIngestionEngine) AddInterceptor(interceptor UnaryInterceptor) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.unaryInterceptors = append(g.unaryInterceptors, interceptor)
}

// HandleWrite processes a write request.
func (g *GRPCIngestionEngine) HandleWrite(ctx context.Context, req *WriteRequest) error {
	if req == nil {
		return fmt.Errorf("nil write request")
	}

	start := time.Now()
	var pointCount int64

	for _, ts := range req.TimeSeries {
		metric := ""
		tags := make(map[string]string)
		for _, l := range ts.Labels {
			if l.Name == "__name__" {
				metric = l.Value
			} else {
				tags[l.Name] = l.Value
			}
		}
		if metric == "" {
			continue
		}

		for _, s := range ts.Samples {
			p := Point{
				Metric:    metric,
				Tags:      tags,
				Value:     s.Value,
				Timestamp: s.Timestamp,
			}
			if err := g.db.WriteContext(ctx, p); err != nil {
				g.mu.Lock()
				g.stats.TotalErrors++
				g.mu.Unlock()
				return fmt.Errorf("write point: %w", err)
			}
			pointCount++
		}
	}

	g.mu.Lock()
	g.stats.TotalWriteRequests++
	g.stats.TotalPointsWritten += pointCount
	elapsed := time.Since(start)
	if g.stats.AvgWriteLatency == 0 {
		g.stats.AvgWriteLatency = elapsed
	} else {
		g.stats.AvgWriteLatency = (g.stats.AvgWriteLatency + elapsed) / 2
	}
	g.mu.Unlock()

	return nil
}

// HandleQuery processes a query request.
func (g *GRPCIngestionEngine) HandleQuery(ctx context.Context, req *QueryRequest) (*QueryResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("nil query request")
	}

	start := time.Now()

	q := Query{
		Metric: req.Metric,
		Start:  req.StartTime,
		End:    req.EndTime,
		Tags:   req.Labels,
	}

	if req.Limit > 0 {
		q.Limit = req.Limit
	}

	if req.Aggregate != "" {
		agg := &Aggregation{}
		switch req.Aggregate {
		case "sum":
			agg.Function = AggSum
		case "avg", "mean":
			agg.Function = AggMean
		case "min":
			agg.Function = AggMin
		case "max":
			agg.Function = AggMax
		case "count":
			agg.Function = AggCount
		case "rate":
			agg.Function = AggRate
		}
		q.Aggregation = agg
	}

	result, err := g.db.ExecuteContext(ctx, &q)
	if err != nil {
		g.mu.Lock()
		g.stats.TotalErrors++
		g.mu.Unlock()
		return nil, fmt.Errorf("execute query: %w", err)
	}

	resp := &QueryResponse{
		Series: make([]ResultSeries, 0),
	}

	if result != nil {
		series := ResultSeries{
			Metric:  req.Metric,
			Labels:  req.Labels,
			Samples: make([]ProtoSample, 0, len(result.Points)),
		}
		for _, p := range result.Points {
			series.Samples = append(series.Samples, ProtoSample{
				Value:     p.Value,
				Timestamp: p.Timestamp,
			})
			resp.PointCount++
		}
		resp.Series = append(resp.Series, series)
	}

	elapsed := time.Since(start)
	resp.QueryTimeMs = elapsed.Milliseconds()

	g.mu.Lock()
	g.stats.TotalQueryRequests++
	if g.stats.AvgQueryLatency == 0 {
		g.stats.AvgQueryLatency = elapsed
	} else {
		g.stats.AvgQueryLatency = (g.stats.AvgQueryLatency + elapsed) / 2
	}
	g.mu.Unlock()

	return resp, nil
}

// Stats returns current server stats.
func (g *GRPCIngestionEngine) Stats() GRPCIngestionStats {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.stats
}

// Services returns the registered service info (for reflection).
func (g *GRPCIngestionEngine) Services() []GRPCServiceInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	result := make([]GRPCServiceInfo, len(g.services))
	copy(result, g.services)
	return result
}

func (g *GRPCIngestionEngine) serve() {
	for {
		select {
		case <-g.stopCh:
			return
		default:
		}

		conn, err := g.listener.Accept()
		if err != nil {
			select {
			case <-g.stopCh:
				return
			default:
				continue
			}
		}

		g.mu.Lock()
		g.stats.ActiveConnections++
		g.mu.Unlock()

		go g.handleConnection(conn)
	}
}

func (g *GRPCIngestionEngine) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		g.mu.Lock()
		g.stats.ActiveConnections--
		g.mu.Unlock()
	}()

	// Read framed messages from the connection
	buf := make([]byte, g.config.MaxMessageSize)
	for {
		select {
		case <-g.stopCh:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(g.config.WriteTimeout))
		n, err := conn.Read(buf)
		if err != nil {
			return
		}

		var envelope struct {
			Method string          `json:"method"`
			Body   json.RawMessage `json:"body"`
		}
		if err := json.Unmarshal(buf[:n], &envelope); err != nil {
			continue
		}

		ctx := context.Background()
		var resp interface{}
		var handleErr error

		switch envelope.Method {
		case "Write":
			var req WriteRequest
			if err := json.Unmarshal(envelope.Body, &req); err != nil {
				handleErr = err
			} else {
				handleErr = g.HandleWrite(ctx, &req)
				resp = map[string]string{"status": "ok"}
			}
		case "Query":
			var req QueryRequest
			if err := json.Unmarshal(envelope.Body, &req); err != nil {
				handleErr = err
			} else {
				resp, handleErr = g.HandleQuery(ctx, &req)
			}
		default:
			handleErr = fmt.Errorf("unknown method: %s", envelope.Method)
		}

		var respBytes []byte
		if handleErr != nil {
			respBytes, _ = json.Marshal(map[string]string{"error": handleErr.Error()})
		} else {
			respBytes, _ = json.Marshal(resp)
		}
		conn.Write(respBytes)
	}
}

// RegisterHTTPHandlers registers HTTP endpoints for the gRPC ingestion engine.
func (g *GRPCIngestionEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/grpc/write", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req WriteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := g.HandleWrite(r.Context(), &req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/api/v1/grpc/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := g.HandleQuery(r.Context(), &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/api/v1/grpc/services", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(g.Services())
	})

	mux.HandleFunc("/api/v1/grpc/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(g.Stats())
	})
}
