package chronicle

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"time"
)

// TCP server, connection handling, protobuf frame processing, and HTTP handlers for gRPC ingestion.

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

		data := buf[:n]
		ctx := context.Background()
		var resp interface{}
		var handleErr error

		// Detect protobuf vs JSON: protobuf frames start with 0x00 or 0x01 byte
		if g.config.EnableProtobuf && n >= 5 && (data[0] == 0x00 || data[0] == 0x01) {
			handleErr = g.handleProtobufFrame(ctx, data)
			if handleErr == nil {
				resp = map[string]string{"status": "ok"}
			}
		} else {
			// JSON envelope handling
			var envelope struct {
				Method string          `json:"method"`
				Body   json.RawMessage `json:"body"`
			}
			if err := json.Unmarshal(data, &envelope); err != nil {
				continue
			}

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
		}

		var respBytes []byte
		if handleErr != nil {
			respBytes, err = json.Marshal(map[string]string{"error": handleErr.Error()})
		} else {
			respBytes, err = json.Marshal(resp)
		}
		if err != nil {
			respBytes = []byte(`{"error":"internal marshal error"}`)
		}
		conn.Write(respBytes)
	}
}

// handleProtobufFrame decodes a gRPC-framed protobuf write request.
func (g *GRPCIngestionEngine) handleProtobufFrame(ctx context.Context, data []byte) error {
	if len(data) < 5 {
		return fmt.Errorf("invalid grpc frame: too short")
	}
	// Parse gRPC frame header
	// compressed := data[0] == 1
	length := binary.BigEndian.Uint32(data[1:5])
	if int(length)+5 > len(data) {
		return fmt.Errorf("incomplete grpc frame")
	}
	payload := data[5 : 5+length]

	req, err := DecodeWriteRequest(payload)
	if err != nil {
		return fmt.Errorf("decode protobuf: %w", err)
	}
	return g.HandleWrite(ctx, req)
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
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := g.HandleWrite(r.Context(), &req); err != nil {
			internalError(w, err, "internal error")
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
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		resp, err := g.HandleQuery(r.Context(), &req)
		if err != nil {
			internalError(w, err, "internal error")
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

	// OTLP MetricsService/Export RPC via HTTP/2 gRPC-compatible endpoint
	mux.HandleFunc("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		g.handleOTLPExport(w, r)
	})

	// Also serve at the OTLP HTTP path for gRPC-gateway compatibility
	mux.HandleFunc("/api/v1/grpc/otlp-metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		g.handleOTLPHTTPMetrics(w, r)
	})
}

// handleOTLPExport handles the OTLP gRPC Export RPC for metrics.
func (g *GRPCIngestionEngine) handleOTLPExport(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(g.config.MaxMessageSize)))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	batch, err := g.decodeOTLPMetricsRequest(body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	engine := NewOTLPProtoEngine(g.db, DefaultOTLPProtoConfig())
	result, err := engine.IngestBatch(batch)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	g.mu.Lock()
	g.stats.TotalWriteRequests++
	g.stats.TotalPointsWritten += int64(result.PointsWritten)
	g.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// handleOTLPHTTPMetrics handles OTLP HTTP/JSON metrics ingestion.
func (g *GRPCIngestionEngine) handleOTLPHTTPMetrics(w http.ResponseWriter, r *http.Request) {
	var batch OTLPMetricBatch
	if err := json.NewDecoder(io.LimitReader(r.Body, int64(g.config.MaxMessageSize))).Decode(&batch); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	engine := NewOTLPProtoEngine(g.db, DefaultOTLPProtoConfig())
	result, err := engine.IngestBatch(&batch)
	if err != nil {
		internalError(w, err, "internal error")
		return
	}

	g.mu.Lock()
	g.stats.TotalWriteRequests++
	g.stats.TotalPointsWritten += int64(result.PointsWritten)
	g.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// decodeOTLPMetricsRequest decodes a protobuf-encoded OTLP ExportMetricsServiceRequest.
// The proto wire format is decoded without external proto dependencies.
func (g *GRPCIngestionEngine) decodeOTLPMetricsRequest(data []byte) (*OTLPMetricBatch, error) {
	batch := &OTLPMetricBatch{}

	// If the data looks like a gRPC frame (compressed flag + length prefix), strip the header
	if len(data) >= 5 && (data[0] == 0x00 || data[0] == 0x01) {
		length := binary.BigEndian.Uint32(data[1:5])
		if int(length)+5 <= len(data) {
			data = data[5 : 5+length]
		}
	}

	// Try JSON first for HTTP/JSON path
	if len(data) > 0 && data[0] == '{' {
		if err := json.Unmarshal(data, batch); err == nil {
			return batch, nil
		}
	}

	// Protobuf wire format: ExportMetricsServiceRequest
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			break
		}
		if field.Number == 1 && field.WireType == protoWireBytes {
			// resource_metrics (repeated)
			metric, err := grpcDecodeOTLPResourceMetrics(field.Bytes)
			if err == nil {
				batch.Resource = metric.Resource
				batch.Scope = metric.Scope
				batch.Metrics = append(batch.Metrics, metric.Metrics...)
			}
		}
	}

	return batch, nil
}

// decodeOTLPResourceMetrics decodes a single ResourceMetrics proto message.
func grpcDecodeOTLPResourceMetrics(data []byte) (*OTLPMetricBatch, error) {
	batch := &OTLPMetricBatch{}
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			break
		}
		switch field.Number {
		case 1: // resource
			if field.WireType == protoWireBytes {
				resDec := newProtoDecoder(field.Bytes)
				for resDec.hasMore() {
					rf, err := resDec.readField()
					if err != nil {
						break
					}
					if rf.Number == 1 && rf.WireType == protoWireBytes {
						// attributes (repeated KeyValue)
						kv := grpcDecodeOTLPKeyValue(rf.Bytes)
						batch.Resource.Attributes = append(batch.Resource.Attributes, kv)
					}
				}
			}
		case 2: // scope_metrics (repeated)
			if field.WireType == protoWireBytes {
				scopeDec := newProtoDecoder(field.Bytes)
				for scopeDec.hasMore() {
					sf, err := scopeDec.readField()
					if err != nil {
						break
					}
					switch sf.Number {
					case 1: // scope
						if sf.WireType == protoWireBytes {
							innerDec := newProtoDecoder(sf.Bytes)
							for innerDec.hasMore() {
								inf, err := innerDec.readField()
								if err != nil {
									break
								}
								if inf.Number == 1 && inf.WireType == protoWireBytes {
									batch.Scope.Name = string(inf.Bytes)
								}
								if inf.Number == 2 && inf.WireType == protoWireBytes {
									batch.Scope.Version = string(inf.Bytes)
								}
							}
						}
					case 2: // metrics (repeated)
						if sf.WireType == protoWireBytes {
							metric := grpcDecodeOTLPMetric(sf.Bytes)
							batch.Metrics = append(batch.Metrics, metric)
						}
					}
				}
			}
		}
	}
	return batch, nil
}

// decodeOTLPMetric decodes a single Metric proto message.
func grpcDecodeOTLPMetric(data []byte) OTLPMetricData {
	m := OTLPMetricData{}
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			break
		}
		switch field.Number {
		case 1: // name
			if field.WireType == protoWireBytes {
				m.Name = string(field.Bytes)
			}
		case 2: // description
			if field.WireType == protoWireBytes {
				m.Description = string(field.Bytes)
			}
		case 3: // unit
			if field.WireType == protoWireBytes {
				m.Unit = string(field.Bytes)
			}
		case 5: // gauge
			m.Type = OTLPMetricGauge
			m.DataPoints = grpcDecodeOTLPDataPoints(field.Bytes)
		case 7: // sum
			m.Type = OTLPMetricSum
			m.DataPoints = grpcDecodeOTLPSumDataPoints(field.Bytes)
		case 9: // histogram
			m.Type = OTLPMetricHistogram
		case 11: // exponential_histogram
			m.Type = OTLPMetricExponentialHistogram
		case 10: // summary
			m.Type = OTLPMetricSummary
		}
	}
	return m
}

// decodeOTLPDataPoints decodes repeated NumberDataPoint from a Gauge message.
func grpcDecodeOTLPDataPoints(data []byte) []ProtoMetricPoint {
	var points []ProtoMetricPoint
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			break
		}
		if field.Number == 1 && field.WireType == protoWireBytes {
			p := grpcDecodeOTLPNumberDataPoint(field.Bytes)
			points = append(points, p)
		}
	}
	return points
}

// decodeOTLPSumDataPoints decodes repeated NumberDataPoint from a Sum message.
func grpcDecodeOTLPSumDataPoints(data []byte) []ProtoMetricPoint {
	var points []ProtoMetricPoint
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			break
		}
		if field.Number == 1 && field.WireType == protoWireBytes {
			p := grpcDecodeOTLPNumberDataPoint(field.Bytes)
			points = append(points, p)
		}
	}
	return points
}

// decodeOTLPNumberDataPoint decodes a single NumberDataPoint proto message.
func grpcDecodeOTLPNumberDataPoint(data []byte) ProtoMetricPoint {
	p := ProtoMetricPoint{Labels: make(map[string]string)}
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			break
		}
		switch field.Number {
		case 1: // attributes (repeated KeyValue)
			if field.WireType == protoWireBytes {
				kv := grpcDecodeOTLPKeyValue(field.Bytes)
				if kv.Value.StringValue != nil {
					p.Labels[kv.Key] = *kv.Value.StringValue
				}
			}
		case 3: // time_unix_nano
			p.Timestamp = int64(field.Varint)
		case 4: // as_double
			p.Value = math.Float64frombits(field.Fixed64)
		case 6: // as_int
			p.Value = float64(int64(field.Varint))
		}
	}
	return p
}

// decodeOTLPKeyValue decodes a single KeyValue proto message.
func grpcDecodeOTLPKeyValue(data []byte) OTLPKeyValue {
	kv := OTLPKeyValue{}
	dec := newProtoDecoder(data)
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			break
		}
		switch field.Number {
		case 1: // key
			if field.WireType == protoWireBytes {
				kv.Key = string(field.Bytes)
			}
		case 2: // value (AnyValue)
			if field.WireType == protoWireBytes {
				valDec := newProtoDecoder(field.Bytes)
				for valDec.hasMore() {
					vf, err := valDec.readField()
					if err != nil {
						break
					}
					switch vf.Number {
					case 1: // string_value
						s := string(vf.Bytes)
						kv.Value.StringValue = &s
					case 2: // bool_value
						b := vf.Varint != 0
						kv.Value.BoolValue = &b
					case 3: // int_value
						i := int64(vf.Varint)
						kv.Value.IntValue = &i
					case 4: // double_value
						d := math.Float64frombits(vf.Fixed64)
						kv.Value.DoubleValue = &d
					}
				}
			}
		}
	}
	return kv
}
