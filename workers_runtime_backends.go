package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// R2 object storage, KV store, cache, HTTP handler, and helper functions for the Workers runtime.

// --- R2 Backend (Cloudflare Object Storage) ---

// R2Backend provides storage using Cloudflare R2.
type R2Backend struct {
	bucketName string
	accountID  string
	apiToken   string
	client     *http.Client
	readCount  int64
	writeCount int64
	mu         sync.Mutex
}

// WritePoint writes a single point to R2.
func (r *R2Backend) WritePoint(ctx context.Context, p Point) error {
	r.mu.Lock()
	r.writeCount++
	r.mu.Unlock()

	key := r.generateKey(p)
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}

	return r.putObject(ctx, key, data)
}

// WriteBatch writes multiple points as a batch file.
func (r *R2Backend) WriteBatch(ctx context.Context, points []Point) error {
	r.mu.Lock()
	r.writeCount++
	r.mu.Unlock()

	if len(points) == 0 {
		return nil
	}

	// Group by metric and time bucket
	key := fmt.Sprintf("data/%s/%d/batch.json",
		points[0].Metric, points[0].Timestamp/int64(time.Hour))

	data, err := json.Marshal(points)
	if err != nil {
		return err
	}

	return r.putObject(ctx, key, data)
}

// Query queries points from R2.
func (r *R2Backend) Query(ctx context.Context, q *Query) (*WorkersQueryResult, error) {
	r.mu.Lock()
	r.readCount++
	r.mu.Unlock()

	// List objects in the time range
	prefix := fmt.Sprintf("data/%s/", q.Metric)
	objects, err := r.listObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}

	var allPoints []Point
	for _, obj := range objects {
		data, err := r.getObject(ctx, obj.Key)
		if err != nil {
			continue
		}

		// Try to parse as batch
		var batchPoints []Point
		if err := json.Unmarshal(data, &batchPoints); err != nil {
			// Try as single point
			var p Point
			if err := json.Unmarshal(data, &p); err != nil {
				continue
			}
			batchPoints = []Point{p}
		}

		// Filter points
		for _, p := range batchPoints {
			if p.Timestamp >= q.Start && p.Timestamp <= q.End {
				if workersMatchesTags(p.Tags, q.Tags) {
					allPoints = append(allPoints, p)
				}
			}
		}
	}

	// Sort by timestamp
	sort.Slice(allPoints, func(i, j int) bool {
		return allPoints[i].Timestamp < allPoints[j].Timestamp
	})

	// Apply limit
	if q.Limit > 0 && len(allPoints) > q.Limit {
		allPoints = allPoints[:q.Limit]
	}

	return &WorkersQueryResult{
		Points: allPoints,
		Count:  len(allPoints),
	}, nil
}

func (r *R2Backend) generateKey(p Point) string {
	return fmt.Sprintf("data/%s/%d/%d.json",
		p.Metric,
		p.Timestamp/int64(time.Hour),
		p.Timestamp)
}

func (r *R2Backend) putObject(ctx context.Context, key string, data []byte) error {
	url := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/r2/buckets/%s/objects/%s",
		r.accountID, r.bucketName, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+r.apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("R2 put failed: %s", string(body))
	}

	return nil
}

func (r *R2Backend) getObject(ctx context.Context, key string) ([]byte, error) {
	url := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/r2/buckets/%s/objects/%s",
		r.accountID, r.bucketName, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+r.apiToken)

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.New("object not found")
	}

	return io.ReadAll(resp.Body)
}

// R2Object represents an R2 object listing entry.
type R2Object struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
}

func (r *R2Backend) listObjects(ctx context.Context, prefix string) ([]R2Object, error) {
	url := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/r2/buckets/%s/objects?prefix=%s",
		r.accountID, r.bucketName, prefix)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+r.apiToken)

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Success bool `json:"success"`
		Result  struct {
			Objects []R2Object `json:"objects"`
		} `json:"result"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Result.Objects, nil
}

func workersMatchesTags(pointTags, queryTags map[string]string) bool {
	for k, v := range queryTags {
		if pointTags[k] != v {
			return false
		}
	}
	return true
}

// --- KV Backend (Cloudflare KV) ---

// KVBackend provides key-value storage using Cloudflare KV.
type KVBackend struct {
	namespace  string
	accountID  string
	apiToken   string
	client     *http.Client
	readCount  int64
	writeCount int64
	mu         sync.Mutex
}

// Get retrieves a value from KV.
func (kv *KVBackend) Get(ctx context.Context, key string) ([]byte, error) {
	kv.mu.Lock()
	kv.readCount++
	kv.mu.Unlock()

	url := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/storage/kv/namespaces/%s/values/%s",
		kv.accountID, kv.namespace, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+kv.apiToken)

	resp, err := kv.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	return io.ReadAll(resp.Body)
}

// Set stores a value in KV.
func (kv *KVBackend) Set(ctx context.Context, key string, value []byte, ttlSeconds int) error {
	kv.mu.Lock()
	kv.writeCount++
	kv.mu.Unlock()

	url := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/storage/kv/namespaces/%s/values/%s",
		kv.accountID, kv.namespace, key)

	if ttlSeconds > 0 {
		url += fmt.Sprintf("?expiration_ttl=%d", ttlSeconds)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(value))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+kv.apiToken)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := kv.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("KV write failed: %s", string(body))
	}

	return nil
}

// Delete removes a key from KV.
func (kv *KVBackend) Delete(ctx context.Context, key string) error {
	url := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/storage/kv/namespaces/%s/values/%s",
		kv.accountID, kv.namespace, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+kv.apiToken)

	resp, err := kv.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

// --- Workers Cache ---

// WorkersCache provides caching using KV.
type WorkersCache struct {
	kv     *KVBackend
	ttl    time.Duration
	prefix string
}

// Get retrieves a cached query result.
func (c *WorkersCache) Get(ctx context.Context, key string) (*WorkersQueryResult, bool) {
	data, err := c.kv.Get(ctx, c.prefix+key)
	if err != nil || data == nil {
		return nil, false
	}

	var result WorkersQueryResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, false
	}

	return &result, true
}

// Set caches a query result.
func (c *WorkersCache) Set(ctx context.Context, key string, result *WorkersQueryResult) {
	data, err := json.Marshal(result)
	if err != nil {
		return
	}

	c.kv.Set(ctx, c.prefix+key, data, int(c.ttl.Seconds()))
}

// --- Workers HTTP Handler ---

// WorkersHandler provides HTTP handling for Workers runtime.
type WorkersHandler struct {
	runtime *WorkersRuntime
}

// NewWorkersHandler creates a new Workers HTTP handler.
func NewWorkersHandler(runtime *WorkersRuntime) *WorkersHandler {
	return &WorkersHandler{runtime: runtime}
}

// ServeHTTP handles HTTP requests in Workers environment.
func (h *WorkersHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	switch r.URL.Path {
	case "/write":
		h.handleWrite(ctx, w, r)
	case "/query":
		h.handleQuery(ctx, w, r)
	case "/metrics":
		h.handleMetrics(w, r)
	case "/health":
		h.handleHealth(w, r)
	default:
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

func (h *WorkersHandler) handleWrite(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Points []Point `json:"points"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if len(req.Points) == 0 {
		http.Error(w, "No points provided", http.StatusBadRequest)
		return
	}

	if err := h.runtime.WriteBatch(ctx, req.Points); err != nil {
		internalError(w, err, "internal error")
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"written": len(req.Points),
	})
}

func (h *WorkersHandler) handleQuery(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var q Query

	if r.Method == http.MethodGet {
		// Parse from query params
		q.Metric = r.URL.Query().Get("metric")
		// Add more query param parsing as needed
	} else if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	result, err := h.runtime.Query(ctx, &q)
	if err != nil {
		internalError(w, err, "internal error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (h *WorkersHandler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := h.runtime.Metrics()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func (h *WorkersHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
	})
}

// --- Helper Functions ---

func aggregatePoints(points []Point, agg *Aggregation, groupBy []string) []Point {
	if len(points) == 0 {
		return points
	}

	// Group points by time window and group-by tags
	groups := make(map[string][]Point)
	windowNs := int64(agg.Window)

	for _, p := range points {
		windowStart := (p.Timestamp / windowNs) * windowNs
		key := fmt.Sprintf("%d", windowStart)

		// Add group-by tags to key
		for _, tag := range groupBy {
			if v, ok := p.Tags[tag]; ok {
				key += ":" + tag + "=" + v
			}
		}

		groups[key] = append(groups[key], p)
	}

	// Aggregate each group
	var result []Point
	for key, groupPoints := range groups {
		if len(groupPoints) == 0 {
			continue
		}

		aggValue := computeAggregation(groupPoints, agg.Function)

		// Parse window start from key
		var windowStart int64
		fmt.Sscanf(key, "%d", &windowStart)

		// Build result point
		p := Point{
			Metric:    groupPoints[0].Metric,
			Tags:      make(map[string]string),
			Value:     aggValue,
			Timestamp: windowStart,
		}

		// Copy group-by tags
		for _, tag := range groupBy {
			if v, ok := groupPoints[0].Tags[tag]; ok {
				p.Tags[tag] = v
			}
		}

		result = append(result, p)
	}

	// Sort by timestamp
	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result
}

func computeAggregation(points []Point, fn AggFunc) float64 {
	if len(points) == 0 {
		return 0
	}

	switch fn {
	case AggCount:
		return float64(len(points))
	case AggSum:
		var sum float64
		for _, p := range points {
			sum += p.Value
		}
		return sum
	case AggMean:
		var sum float64
		for _, p := range points {
			sum += p.Value
		}
		return sum / float64(len(points))
	case AggMin:
		min := points[0].Value
		for _, p := range points[1:] {
			if p.Value < min {
				min = p.Value
			}
		}
		return min
	case AggMax:
		max := points[0].Value
		for _, p := range points[1:] {
			if p.Value > max {
				max = p.Value
			}
		}
		return max
	case AggFirst:
		return points[0].Value
	case AggLast:
		return points[len(points)-1].Value
	default:
		return points[0].Value
	}
}
