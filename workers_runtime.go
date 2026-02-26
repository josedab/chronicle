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

// EXPERIMENTAL: This API is unstable and may change without notice.
// NOTE: This is a placeholder integration. Cloudflare Workers API calls (D1, R2, KV)
// are stubbed and require a real Cloudflare account with valid API credentials.
//
// WorkersRuntimeConfig configures Chronicle for Cloudflare Workers environment.
type WorkersRuntimeConfig struct {
	// D1DatabaseID is the Cloudflare D1 database binding ID.
	D1DatabaseID string `json:"d1_database_id"`

	// R2BucketName is the R2 bucket for large data storage.
	R2BucketName string `json:"r2_bucket_name"`

	// KVNamespace is the KV namespace for metadata.
	KVNamespace string `json:"kv_namespace"`

	// AccountID is the Cloudflare account ID.
	AccountID string `json:"account_id"`

	// APIToken is the Cloudflare API token.
	APIToken string `json:"api_token"`

	// MaxMemoryMB limits memory usage for edge constraints.
	MaxMemoryMB int `json:"max_memory_mb"`

	// EnableStreaming enables streaming query execution.
	EnableStreaming bool `json:"enable_streaming"`

	// CacheEnabled enables caching in KV.
	CacheEnabled bool `json:"cache_enabled"`

	// CacheTTL is the cache time-to-live.
	CacheTTL time.Duration `json:"cache_ttl"`

	// BatchSize for D1 operations.
	BatchSize int `json:"batch_size"`

	// Region preference for edge routing.
	Region string `json:"region"`
}

// DefaultWorkersRuntimeConfig returns default configuration for Workers.
func DefaultWorkersRuntimeConfig() WorkersRuntimeConfig {
	return WorkersRuntimeConfig{
		MaxMemoryMB:     128, // Workers memory limit
		EnableStreaming: true,
		CacheEnabled:    true,
		CacheTTL:        5 * time.Minute,
		BatchSize:       100, // D1 batch limit
	}
}

// WorkersQueryResult represents query results from Workers storage.
type WorkersQueryResult struct {
	Points   []Point `json:"points"`
	Series   string  `json:"series"`
	Count    int     `json:"count"`
	Duration int64   `json:"duration_ms"`
}

// WorkersRuntime provides Chronicle functionality for Cloudflare Workers.
type WorkersRuntime struct {
	config  WorkersRuntimeConfig
	d1      *D1Backend
	r2      *R2Backend
	kv      *KVBackend
	cache   *WorkersCache
	client  *http.Client
	mu      sync.RWMutex
	metrics *WorkersMetrics
}

// WorkersMetrics tracks runtime metrics.
type WorkersMetrics struct {
	D1Queries      int64 `json:"d1_queries"`
	D1Writes       int64 `json:"d1_writes"`
	R2Reads        int64 `json:"r2_reads"`
	R2Writes       int64 `json:"r2_writes"`
	KVReads        int64 `json:"kv_reads"`
	KVWrites       int64 `json:"kv_writes"`
	CacheHits      int64 `json:"cache_hits"`
	CacheMisses    int64 `json:"cache_misses"`
	TotalLatencyMs int64 `json:"total_latency_ms"`
	RequestCount   int64 `json:"request_count"`
}

// NewWorkersRuntime creates a new Workers runtime instance.
func NewWorkersRuntime(config WorkersRuntimeConfig) (*WorkersRuntime, error) {
	if config.D1DatabaseID == "" && config.R2BucketName == "" {
		return nil, errors.New("at least one of D1DatabaseID or R2BucketName required")
	}

	wr := &WorkersRuntime{
		config: config,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		metrics: &WorkersMetrics{},
	}

	// Initialize D1 backend if configured
	if config.D1DatabaseID != "" {
		wr.d1 = &D1Backend{
			databaseID: config.D1DatabaseID,
			accountID:  config.AccountID,
			apiToken:   config.APIToken,
			client:     wr.client,
			batchSize:  config.BatchSize,
		}
	}

	// Initialize R2 backend if configured
	if config.R2BucketName != "" {
		wr.r2 = &R2Backend{
			bucketName: config.R2BucketName,
			accountID:  config.AccountID,
			apiToken:   config.APIToken,
			client:     wr.client,
		}
	}

	// Initialize KV backend if configured
	if config.KVNamespace != "" {
		wr.kv = &KVBackend{
			namespace: config.KVNamespace,
			accountID: config.AccountID,
			apiToken:  config.APIToken,
			client:    wr.client,
		}
	}

	// Initialize cache
	if config.CacheEnabled && wr.kv != nil {
		wr.cache = &WorkersCache{
			kv:     wr.kv,
			ttl:    config.CacheTTL,
			prefix: "cache:",
		}
	}

	return wr, nil
}

// Write writes a point to Workers storage.
func (wr *WorkersRuntime) Write(ctx context.Context, p Point) error {
	start := time.Now()
	defer func() {
		wr.mu.Lock()
		wr.metrics.TotalLatencyMs += time.Since(start).Milliseconds()
		wr.metrics.RequestCount++
		wr.mu.Unlock()
	}()

	// Prefer D1 for structured data
	if wr.d1 != nil {
		return wr.d1.Write(ctx, p)
	}

	// Fallback to R2 for blob storage
	if wr.r2 != nil {
		return wr.r2.WritePoint(ctx, p)
	}

	return errors.New("no storage backend available")
}

// WriteBatch writes multiple points efficiently.
func (wr *WorkersRuntime) WriteBatch(ctx context.Context, points []Point) error {
	start := time.Now()
	defer func() {
		wr.mu.Lock()
		wr.metrics.TotalLatencyMs += time.Since(start).Milliseconds()
		wr.metrics.RequestCount++
		wr.mu.Unlock()
	}()

	if wr.d1 != nil {
		return wr.d1.WriteBatch(ctx, points)
	}

	if wr.r2 != nil {
		return wr.r2.WriteBatch(ctx, points)
	}

	return errors.New("no storage backend available")
}

// Query executes a query against Workers storage.
func (wr *WorkersRuntime) Query(ctx context.Context, q *Query) (*WorkersQueryResult, error) {
	start := time.Now()
	defer func() {
		wr.mu.Lock()
		wr.metrics.TotalLatencyMs += time.Since(start).Milliseconds()
		wr.metrics.RequestCount++
		wr.mu.Unlock()
	}()

	// Check cache first
	if wr.cache != nil {
		cacheKey := wr.generateCacheKey(q)
		if cached, ok := wr.cache.Get(ctx, cacheKey); ok {
			wr.mu.Lock()
			wr.metrics.CacheHits++
			wr.mu.Unlock()
			return cached, nil
		}
		wr.mu.Lock()
		wr.metrics.CacheMisses++
		wr.mu.Unlock()
	}

	var result *WorkersQueryResult
	var err error

	// Query from D1
	if wr.d1 != nil {
		result, err = wr.d1.Query(ctx, q)
	} else if wr.r2 != nil {
		result, err = wr.r2.Query(ctx, q)
	} else {
		return nil, errors.New("no storage backend available")
	}

	if err != nil {
		return nil, err
	}

	// Cache result
	if wr.cache != nil && result != nil {
		cacheKey := wr.generateCacheKey(q)
		wr.cache.Set(ctx, cacheKey, result)
	}

	return result, nil
}

// Metrics returns current runtime metrics.
func (wr *WorkersRuntime) Metrics() WorkersMetrics {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	metrics := *wr.metrics
	if wr.d1 != nil {
		metrics.D1Queries = wr.d1.queryCount
		metrics.D1Writes = wr.d1.writeCount
	}
	if wr.r2 != nil {
		metrics.R2Reads = wr.r2.readCount
		metrics.R2Writes = wr.r2.writeCount
	}
	if wr.kv != nil {
		metrics.KVReads = wr.kv.readCount
		metrics.KVWrites = wr.kv.writeCount
	}
	return metrics
}

func (wr *WorkersRuntime) generateCacheKey(q *Query) string {
	return fmt.Sprintf("%s:%d:%d:%s", q.Metric, q.Start, q.End, sortedTags(q.Tags))
}

func sortedTags(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var result string
	for _, k := range keys {
		result += k + "=" + tags[k] + ","
	}
	return result
}

// --- D1 Backend (Cloudflare SQLite) ---

// D1Backend provides storage using Cloudflare D1.
type D1Backend struct {
	databaseID string
	accountID  string
	apiToken   string
	client     *http.Client
	batchSize  int
	queryCount int64
	writeCount int64
	mu         sync.Mutex
}

// D1Response represents a D1 API response.
type D1Response struct {
	Success  bool            `json:"success"`
	Errors   []D1Error       `json:"errors"`
	Messages []string        `json:"messages"`
	Result   json.RawMessage `json:"result"`
}

// D1Error represents a D1 API error.
type D1Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// D1QueryResult represents D1 query results.
type D1QueryResult struct {
	Results []map[string]any `json:"results"`
	Success bool             `json:"success"`
	Meta    D1Meta           `json:"meta"`
}

// D1Meta contains query metadata.
type D1Meta struct {
	Duration       float64 `json:"duration"`
	ChangedDB      bool    `json:"changed_db"`
	Changes        int     `json:"changes"`
	LastRowID      int64   `json:"last_row_id"`
	RowsRead       int     `json:"rows_read"`
	RowsWritten    int     `json:"rows_written"`
	ServedByLeader bool    `json:"served_by_leader"`
}

// Initialize creates the required tables in D1.
func (d *D1Backend) Initialize(ctx context.Context) error {
	schema := `
		CREATE TABLE IF NOT EXISTS points (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			metric TEXT NOT NULL,
			tags TEXT,
			value REAL NOT NULL,
			timestamp INTEGER NOT NULL,
			created_at INTEGER DEFAULT (strftime('%s', 'now'))
		);
		CREATE INDEX IF NOT EXISTS idx_points_metric_time ON points(metric, timestamp);
		CREATE INDEX IF NOT EXISTS idx_points_timestamp ON points(timestamp);
	`
	return d.execute(ctx, schema)
}

// Write writes a single point to D1.
func (d *D1Backend) Write(ctx context.Context, p Point) error {
	d.mu.Lock()
	d.writeCount++
	d.mu.Unlock()

	tagsJSON, _ := json.Marshal(p.Tags)
	sql := fmt.Sprintf(
		"INSERT INTO points (metric, tags, value, timestamp) VALUES ('%s', '%s', %f, %d)",
		escapeSQL(p.Metric), escapeSQL(string(tagsJSON)), p.Value, p.Timestamp)

	return d.execute(ctx, sql)
}

// WriteBatch writes multiple points in a batch.
func (d *D1Backend) WriteBatch(ctx context.Context, points []Point) error {
	d.mu.Lock()
	d.writeCount += int64(len(points))
	d.mu.Unlock()

	// D1 has limits on batch size, so we chunk
	for i := 0; i < len(points); i += d.batchSize {
		end := i + d.batchSize
		if end > len(points) {
			end = len(points)
		}

		batch := points[i:end]
		if err := d.writeBatchChunk(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

func (d *D1Backend) writeBatchChunk(ctx context.Context, points []Point) error {
	if len(points) == 0 {
		return nil
	}

	var values string
	for i, p := range points {
		tagsJSON, _ := json.Marshal(p.Tags)
		if i > 0 {
			values += ", "
		}
		values += fmt.Sprintf("('%s', '%s', %f, %d)",
			escapeSQL(p.Metric), escapeSQL(string(tagsJSON)), p.Value, p.Timestamp)
	}

	sql := "INSERT INTO points (metric, tags, value, timestamp) VALUES " + values
	return d.execute(ctx, sql)
}

// Query executes a query against D1.
func (d *D1Backend) Query(ctx context.Context, q *Query) (*WorkersQueryResult, error) {
	d.mu.Lock()
	d.queryCount++
	d.mu.Unlock()

	// Build SQL query
	sql := fmt.Sprintf(
		"SELECT metric, tags, value, timestamp FROM points WHERE metric = '%s'",
		escapeSQL(q.Metric))

	if q.Start > 0 {
		sql += fmt.Sprintf(" AND timestamp >= %d", q.Start)
	}
	if q.End > 0 {
		sql += fmt.Sprintf(" AND timestamp <= %d", q.End)
	}

	// Add tag filters
	for k, v := range q.Tags {
		sql += fmt.Sprintf(" AND json_extract(tags, '$.%s') = '%s'", sanitizeIdentifier(k), escapeSQL(v))
	}

	sql += " ORDER BY timestamp ASC"

	if q.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", q.Limit)
	}

	// Execute query
	result, err := d.query(ctx, sql)
	if err != nil {
		return nil, err
	}

	// Convert to QueryResult
	points := make([]Point, 0, len(result.Results))
	for _, row := range result.Results {
		var tags map[string]string
		if tagsStr, ok := row["tags"].(string); ok && tagsStr != "" {
			json.Unmarshal([]byte(tagsStr), &tags)
		}

		p := Point{
			Metric:    row["metric"].(string),
			Tags:      tags,
			Value:     row["value"].(float64),
			Timestamp: int64(row["timestamp"].(float64)),
		}
		points = append(points, p)
	}

	// Apply aggregation if specified
	if q.Aggregation != nil {
		points = aggregatePoints(points, q.Aggregation, q.GroupBy)
	}

	return &WorkersQueryResult{
		Points: points,
		Count:  len(points),
	}, nil
}

func (d *D1Backend) execute(ctx context.Context, sql string) error {
	url := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/d1/database/%s/query",
		d.accountID, d.databaseID)

	body := map[string]string{"sql": sql}
	jsonBody, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+d.apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var d1Resp D1Response
	if err := json.NewDecoder(resp.Body).Decode(&d1Resp); err != nil {
		return err
	}

	if !d1Resp.Success {
		if len(d1Resp.Errors) > 0 {
			return fmt.Errorf("D1 error: %s", d1Resp.Errors[0].Message)
		}
		return errors.New("D1 operation failed")
	}

	return nil
}

func (d *D1Backend) query(ctx context.Context, sql string) (*D1QueryResult, error) {
	url := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/d1/database/%s/query",
		d.accountID, d.databaseID)

	body := map[string]string{"sql": sql}
	jsonBody, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+d.apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var d1Resp D1Response
	respBody, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(respBody, &d1Resp); err != nil {
		return nil, err
	}

	if !d1Resp.Success {
		if len(d1Resp.Errors) > 0 {
			return nil, fmt.Errorf("D1 error: %s", d1Resp.Errors[0].Message)
		}
		return nil, errors.New("D1 query failed")
	}

	// Parse the result
	var results []D1QueryResult
	if err := json.Unmarshal(d1Resp.Result, &results); err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return &D1QueryResult{Results: []map[string]any{}}, nil
	}

	return &results[0], nil
}
