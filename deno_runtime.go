package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// EXPERIMENTAL: This API is unstable and may change without notice.
// NOTE: This is a placeholder integration. Deno Deploy API calls are stubbed and
// require a real Deno Deploy project and authentication token to function.
//
// DenoRuntimeConfig configures Chronicle for Deno Deploy environment.
type DenoRuntimeConfig struct {
	// ProjectID is the Deno Deploy project ID.
	ProjectID string `json:"project_id"`

	// KVDatabaseID is the Deno KV database identifier.
	KVDatabaseID string `json:"kv_database_id"`

	// Region preference for Deno Deploy edge routing.
	Region string `json:"region"`

	// MaxMemoryMB limits memory usage for edge constraints.
	MaxMemoryMB int `json:"max_memory_mb"`

	// EnableStreaming enables streaming query execution.
	EnableStreaming bool `json:"enable_streaming"`

	// CacheEnabled enables in-memory caching.
	CacheEnabled bool `json:"cache_enabled"`

	// CacheTTL is the cache time-to-live.
	CacheTTL time.Duration `json:"cache_ttl"`

	// BatchSize for KV operations.
	BatchSize int `json:"batch_size"`

	// ConsistencyLevel for KV reads ("strong" or "eventual").
	ConsistencyLevel string `json:"consistency_level"`

	// APIToken for Deno Deploy API access.
	APIToken string `json:"api_token"`
}

// DefaultDenoRuntimeConfig returns default configuration for Deno Deploy.
func DefaultDenoRuntimeConfig() DenoRuntimeConfig {
	return DenoRuntimeConfig{
		MaxMemoryMB:      512,
		EnableStreaming:  true,
		CacheEnabled:     true,
		CacheTTL:         5 * time.Minute,
		BatchSize:        500,
		ConsistencyLevel: "strong",
	}
}

// DenoRuntime provides Chronicle functionality on Deno Deploy.
type DenoRuntime struct {
	config  DenoRuntimeConfig
	kv      *DenoKVBackend
	cache   *denoCache
	client  *http.Client
	metrics *DenoMetrics
	mu      sync.RWMutex
}

// DenoMetrics tracks runtime metrics for Deno Deploy.
type DenoMetrics struct {
	KVReads        int64 `json:"kv_reads"`
	KVWrites       int64 `json:"kv_writes"`
	KVDeletes      int64 `json:"kv_deletes"`
	CacheHits      int64 `json:"cache_hits"`
	CacheMisses    int64 `json:"cache_misses"`
	TotalLatencyMs int64 `json:"total_latency_ms"`
	RequestCount   int64 `json:"request_count"`
}

// DenoKVBackend wraps Deno KV operations via the HTTP API.
type DenoKVBackend struct {
	databaseID  string
	apiToken    string
	baseURL     string
	client      *http.Client
	batchSize   int
	consistency string
}

// NewDenoRuntime creates a new Deno Deploy runtime instance.
func NewDenoRuntime(config DenoRuntimeConfig) (*DenoRuntime, error) {
	if config.KVDatabaseID == "" && config.ProjectID == "" {
		return nil, errors.New("deno_runtime: at least one of KVDatabaseID or ProjectID required")
	}

	dr := &DenoRuntime{
		config:  config,
		client:  &http.Client{Timeout: 30 * time.Second},
		metrics: &DenoMetrics{},
	}

	if config.KVDatabaseID != "" {
		dr.kv = &DenoKVBackend{
			databaseID:  config.KVDatabaseID,
			apiToken:    config.APIToken,
			baseURL:     "https://api.deno.com/databases/" + config.KVDatabaseID,
			client:      dr.client,
			batchSize:   config.BatchSize,
			consistency: config.ConsistencyLevel,
		}
	}

	if config.CacheEnabled {
		dr.cache = newDenoCache(config.CacheTTL)
	}

	return dr, nil
}

// Write writes a point to Deno KV storage.
func (dr *DenoRuntime) Write(ctx context.Context, p Point) error {
	start := time.Now()
	defer dr.recordLatency(start)

	if dr.kv == nil {
		return errors.New("deno_runtime: no KV backend")
	}

	return dr.kv.WritePoint(ctx, p)
}

// WriteBatch writes multiple points to Deno KV.
func (dr *DenoRuntime) WriteBatch(ctx context.Context, points []Point) error {
	start := time.Now()
	defer dr.recordLatency(start)

	if dr.kv == nil {
		return errors.New("deno_runtime: no KV backend")
	}

	// Process in batches
	for i := 0; i < len(points); i += dr.config.BatchSize {
		end := i + dr.config.BatchSize
		if end > len(points) {
			end = len(points)
		}
		if err := dr.kv.WriteBatch(ctx, points[i:end]); err != nil {
			return fmt.Errorf("deno_runtime: batch write at offset %d: %w", i, err)
		}
	}
	return nil
}

// Query executes a query against Deno KV storage.
func (dr *DenoRuntime) Query(ctx context.Context, q *Query) (*DenoQueryResult, error) {
	start := time.Now()
	defer dr.recordLatency(start)

	if q == nil {
		return nil, errors.New("deno_runtime: nil query")
	}

	// Check cache
	if dr.cache != nil {
		cacheKey := fmt.Sprintf("%s|%d|%d", q.Metric, q.Start, q.End)
		if cached := dr.cache.get(cacheKey); cached != nil {
			dr.mu.Lock()
			dr.metrics.CacheHits++
			dr.mu.Unlock()
			return cached, nil
		}
		dr.mu.Lock()
		dr.metrics.CacheMisses++
		dr.mu.Unlock()
	}

	if dr.kv == nil {
		return nil, errors.New("deno_runtime: no KV backend")
	}

	result, err := dr.kv.Query(ctx, q)
	if err != nil {
		return nil, err
	}

	// Cache result
	if dr.cache != nil {
		cacheKey := fmt.Sprintf("%s|%d|%d", q.Metric, q.Start, q.End)
		dr.cache.set(cacheKey, result)
	}

	return result, nil
}

// Metrics returns runtime metrics.
func (dr *DenoRuntime) Metrics() DenoMetrics {
	dr.mu.RLock()
	defer dr.mu.RUnlock()
	return *dr.metrics
}

func (dr *DenoRuntime) recordLatency(start time.Time) {
	dr.mu.Lock()
	dr.metrics.TotalLatencyMs += time.Since(start).Milliseconds()
	dr.metrics.RequestCount++
	dr.mu.Unlock()
}

// DenoQueryResult represents query results from Deno KV.
type DenoQueryResult struct {
	Points   []Point `json:"points"`
	Series   string  `json:"series"`
	Count    int     `json:"count"`
	Duration int64   `json:"duration_ms"`
}

// --- Deno KV Backend Operations ---

func (kv *DenoKVBackend) WritePoint(ctx context.Context, p Point) error {
	key := fmt.Sprintf("ts:%s:%d", p.Metric, p.Timestamp)
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}

	return kv.kvSet(ctx, key, data)
}

func (kv *DenoKVBackend) WriteBatch(ctx context.Context, points []Point) error {
	ops := make([]denoKVOp, 0, len(points))
	for _, p := range points {
		key := fmt.Sprintf("ts:%s:%d", p.Metric, p.Timestamp)
		data, err := json.Marshal(p)
		if err != nil {
			return err
		}
		ops = append(ops, denoKVOp{
			Type:  "set",
			Key:   []string{key},
			Value: data,
		})
	}

	return kv.kvAtomic(ctx, ops)
}

func (kv *DenoKVBackend) Query(ctx context.Context, q *Query) (*DenoQueryResult, error) {
	prefix := fmt.Sprintf("ts:%s:", q.Metric)

	start := time.Now()
	entries, err := kv.kvList(ctx, prefix, kv.batchSize)
	if err != nil {
		return nil, err
	}

	var points []Point
	for _, entry := range entries {
		var p Point
		if err := json.Unmarshal(entry.Value, &p); err != nil {
			continue
		}

		// Apply time range filter
		if q.Start > 0 && p.Timestamp < q.Start {
			continue
		}
		if q.End > 0 && p.Timestamp > q.End {
			continue
		}

		// Apply tag filter
		if denoMatchesTags(p.Tags, q.Tags) {
			points = append(points, p)
		}
	}

	return &DenoQueryResult{
		Points:   points,
		Series:   q.Metric,
		Count:    len(points),
		Duration: time.Since(start).Milliseconds(),
	}, nil
}

func denoMatchesTags(pointTags, queryTags map[string]string) bool {
	for k, v := range queryTags {
		if pv, ok := pointTags[k]; !ok || pv != v {
			return false
		}
	}
	return true
}

func (kv *DenoKVBackend) kvSet(ctx context.Context, key string, value []byte) error {
	body := map[string]any{
		"key":   []string{key},
		"value": value,
	}
	data, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, kv.baseURL+"/set", bytes.NewReader(data))
	if err != nil {
		return err
	}
	kv.setHeaders(req)

	resp, err := kv.client.Do(req)
	if err != nil {
		return fmt.Errorf("deno_kv: set failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("deno_kv: set error %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (kv *DenoKVBackend) kvAtomic(ctx context.Context, ops []denoKVOp) error {
	body := map[string]any{
		"operations": ops,
	}
	data, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, kv.baseURL+"/atomic", bytes.NewReader(data))
	if err != nil {
		return err
	}
	kv.setHeaders(req)

	resp, err := kv.client.Do(req)
	if err != nil {
		return fmt.Errorf("deno_kv: atomic failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("deno_kv: atomic error %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (kv *DenoKVBackend) kvList(ctx context.Context, prefix string, limit int) ([]denoKVEntry, error) {
	url := fmt.Sprintf("%s/list?prefix=%s&limit=%d&consistency=%s",
		kv.baseURL, prefix, limit, kv.consistency)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	kv.setHeaders(req)

	resp, err := kv.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("deno_kv: list failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("deno_kv: list error %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Entries []denoKVEntry `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Entries, nil
}

func (kv *DenoKVBackend) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	if kv.apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+kv.apiToken)
	}
}

type denoKVOp struct {
	Type  string          `json:"type"`
	Key   []string        `json:"key"`
	Value json.RawMessage `json:"value,omitempty"`
}

type denoKVEntry struct {
	Key   []string        `json:"key"`
	Value json.RawMessage `json:"value"`
}

// --- Deno Cache ---

type denoCache struct {
	mu      sync.RWMutex
	entries map[string]*denoCacheEntry
	ttl     time.Duration
}

type denoCacheEntry struct {
	result    *DenoQueryResult
	expiresAt time.Time
}

func newDenoCache(ttl time.Duration) *denoCache {
	return &denoCache{
		entries: make(map[string]*denoCacheEntry),
		ttl:     ttl,
	}
}

func (c *denoCache) get(key string) *DenoQueryResult {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[key]
	if !ok || time.Now().After(entry.expiresAt) {
		return nil
	}
	return entry.result
}

func (c *denoCache) set(key string, result *DenoQueryResult) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = &denoCacheEntry{
		result:    result,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// --- Edge Platform Manager ---

// EdgePlatform identifies a serverless edge platform.
type EdgePlatform string

const (
	EdgePlatformWorkers EdgePlatform = "cloudflare_workers"
	EdgePlatformDeno    EdgePlatform = "deno_deploy"
)

// EdgeDeployConfig provides unified deployment configuration.
type EdgeDeployConfig struct {
	Platform     EdgePlatform          `json:"platform"`
	Workers      *WorkersRuntimeConfig `json:"workers,omitempty"`
	Deno         *DenoRuntimeConfig    `json:"deno,omitempty"`
	FeatureFlags map[string]bool       `json:"feature_flags,omitempty"`
}

// EdgePlatformManager manages deployments across edge platforms.
type EdgePlatformManager struct {
	workersRuntime *WorkersRuntime
	denoRuntime    *DenoRuntime
	mu             sync.RWMutex
}

// NewEdgePlatformManager creates a manager for edge platform runtimes.
func NewEdgePlatformManager() *EdgePlatformManager {
	return &EdgePlatformManager{}
}

// InitWorkers initializes the Cloudflare Workers runtime.
func (epm *EdgePlatformManager) InitWorkers(config WorkersRuntimeConfig) error {
	wr, err := NewWorkersRuntime(config)
	if err != nil {
		return err
	}
	epm.mu.Lock()
	epm.workersRuntime = wr
	epm.mu.Unlock()
	return nil
}

// InitDeno initializes the Deno Deploy runtime.
func (epm *EdgePlatformManager) InitDeno(config DenoRuntimeConfig) error {
	dr, err := NewDenoRuntime(config)
	if err != nil {
		return err
	}
	epm.mu.Lock()
	epm.denoRuntime = dr
	epm.mu.Unlock()
	return nil
}

// Workers returns the Cloudflare Workers runtime if initialized.
func (epm *EdgePlatformManager) Workers() *WorkersRuntime {
	epm.mu.RLock()
	defer epm.mu.RUnlock()
	return epm.workersRuntime
}

// Deno returns the Deno Deploy runtime if initialized.
func (epm *EdgePlatformManager) Deno() *DenoRuntime {
	epm.mu.RLock()
	defer epm.mu.RUnlock()
	return epm.denoRuntime
}
