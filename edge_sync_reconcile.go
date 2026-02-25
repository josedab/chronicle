// edge_sync_reconcile.go contains extended edge sync functionality.
package chronicle

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

func (m *EdgeSyncManager) recordError(err error) {
	m.statsMu.Lock()
	m.stats.SyncFailures++
	m.stats.LastSyncError = err.Error()
	m.statsMu.Unlock()
}

// syncBatch represents a batch of points for syncing.
type syncBatch struct {
	Version   int     `json:"version"`
	Timestamp int64   `json:"timestamp"`
	Points    []Point `json:"points"`
	Checksum  string  `json:"checksum"`
}

// syncQueue is a persistent queue for points awaiting sync.
type syncQueue struct {
	path      string
	maxSize   int64
	mu        sync.Mutex
	items     []Point
	sizeBytes int64
}

func newSyncQueue(path string, maxSize int64) (*syncQueue, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	q := &syncQueue{
		path:    path,
		maxSize: maxSize,
	}

	// Load existing queue if present
	if err := q.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return q, nil
}

func (q *syncQueue) Enqueue(points []Point) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Estimate size
	addSize := int64(len(points) * 100) // Rough estimate

	// Check capacity
	if q.sizeBytes+addSize > q.maxSize {
		// Drop oldest points to make room
		dropCount := (q.sizeBytes + addSize - q.maxSize) / 100
		if dropCount > int64(len(q.items)) {
			dropCount = int64(len(q.items))
		}
		q.items = q.items[dropCount:]
		q.sizeBytes -= dropCount * 100
	}

	q.items = append(q.items, points...)
	q.sizeBytes += addSize

	return q.persist()
}

func (q *syncQueue) Dequeue(count int) ([]Point, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return nil, nil
	}

	if count > len(q.items) {
		count = len(q.items)
	}

	batch := make([]Point, count)
	copy(batch, q.items[:count])
	q.items = q.items[count:]
	q.sizeBytes -= int64(count * 100)

	if err := q.persist(); err != nil {
		return nil, err
	}

	return batch, nil
}

func (q *syncQueue) Stats() (int64, int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int64(len(q.items)), q.sizeBytes
}

func (q *syncQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.persist()
}

func (q *syncQueue) persist() error {
	data, err := json.Marshal(q.items)
	if err != nil {
		return err
	}
	return os.WriteFile(q.path, data, 0o644)
}

func (q *syncQueue) load() error {
	data, err := os.ReadFile(q.path)
	if err != nil {
		return err
	}

	var items []Point
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}

	q.items = items
	q.sizeBytes = int64(len(items) * 100)
	return nil
}

// CloudAdapter is the interface for cloud provider adapters.
type CloudAdapter interface {
	Send(ctx context.Context, payload []byte) error
}

// newCloudAdapter creates the appropriate cloud adapter.
func newCloudAdapter(config EdgeSyncConfig) (CloudAdapter, error) {
	switch config.Provider {
	case EdgeSyncProviderGenericHTTP, EdgeSyncProviderChronicle:
		return newHTTPAdapter(config)
	case EdgeSyncProviderInfluxDB:
		return newInfluxDBAdapter(config)
	case EdgeSyncProviderS3:
		return newS3Adapter(config)
	default:
		return newHTTPAdapter(config)
	}
}

// httpAdapter sends data to a generic HTTP endpoint.
type httpAdapter struct {
	endpoint   string
	auth       *EdgeSyncAuth
	compressed bool
	client     *http.Client
}

func newHTTPAdapter(config EdgeSyncConfig) (*httpAdapter, error) {
	return &httpAdapter{
		endpoint:   config.Endpoint,
		auth:       config.Auth,
		compressed: config.CompressionEnabled,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (a *httpAdapter) Send(ctx context.Context, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", a.endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if a.compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}

	// Add auth
	if a.auth != nil {
		switch a.auth.Type {
		case "api_key":
			req.Header.Set("X-API-Key", a.auth.APIKey)
		case "bearer":
			req.Header.Set("Authorization", "Bearer "+a.auth.BearerToken)
		case "basic":
			req.SetBasicAuth(a.auth.Username, a.auth.Password)
		}
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// influxDBAdapter sends data to InfluxDB Cloud.
type influxDBAdapter struct {
	endpoint string
	org      string
	bucket   string
	token    string
	client   *http.Client
}

func newInfluxDBAdapter(config EdgeSyncConfig) (*influxDBAdapter, error) {
	token := ""
	if config.Auth != nil {
		token = config.Auth.BearerToken
		if token == "" {
			token = config.Auth.APIKey
		}
	}

	return &influxDBAdapter{
		endpoint: config.Endpoint,
		token:    token,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (a *influxDBAdapter) Send(ctx context.Context, payload []byte) error {
	// Decompress if needed
	var batch syncBatch
	data := payload
	if len(payload) > 2 && payload[0] == 0x1f && payload[1] == 0x8b {
		// Gzip compressed
		gr, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return err
		}
		data, err = io.ReadAll(gr)
		gr.Close()
		if err != nil {
			return err
		}
	}

	if err := json.Unmarshal(data, &batch); err != nil {
		return err
	}

	// Convert to InfluxDB line protocol
	var lines []byte
	for _, p := range batch.Points {
		line := pointToLineProtocol(p)
		lines = append(lines, []byte(line+"\n")...)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", a.endpoint+"/api/v2/write", bytes.NewReader(lines))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Authorization", "Token "+a.token)

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("InfluxDB error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// pointToLineProtocol converts a Point to InfluxDB line protocol.
func pointToLineProtocol(p Point) string {
	// measurement,tags field=value timestamp
	var b bytes.Buffer
	b.WriteString(p.Metric)

	// Sort tags for consistent output
	var keys []string
	for k := range p.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		b.WriteByte(',')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(p.Tags[k])
	}

	b.WriteString(" value=")
	b.WriteString(fmt.Sprintf("%f", p.Value))

	b.WriteByte(' ')
	b.WriteString(fmt.Sprintf("%d", p.Timestamp))

	return b.String()
}

// s3Adapter uploads data to S3.
type s3Adapter struct {
	bucket   string
	prefix   string
	endpoint string
	auth     *EdgeSyncAuth
	client   *http.Client
}

func newS3Adapter(config EdgeSyncConfig) (*s3Adapter, error) {
	return &s3Adapter{
		endpoint: config.Endpoint,
		auth:     config.Auth,
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
	}, nil
}

func (a *s3Adapter) Send(ctx context.Context, payload []byte) error {
	// Generate unique object key
	key := fmt.Sprintf("chronicle/%d/%s.json.gz",
		time.Now().Unix(),
		hex.EncodeToString(sha256.New().Sum(payload)[:8]))

	req, err := http.NewRequestWithContext(ctx, "PUT", a.endpoint+"/"+key, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if len(payload) > 2 && payload[0] == 0x1f && payload[1] == 0x8b {
		req.Header.Set("Content-Encoding", "gzip")
	}

	// Add auth (simplified - real implementation would use AWS SDK)
	if a.auth != nil && a.auth.APIKey != "" {
		req.Header.Set("X-API-Key", a.auth.APIKey)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("S3 error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// bandwidthLimiter provides bandwidth limiting for sync operations.
type bandwidthLimiter struct {
	bytesPerSec int64
	tokens      int64
	lastRefill  time.Time
	mu          sync.Mutex
}

func newBandwidthLimiter(bytesPerSec int64) *bandwidthLimiter {
	return &bandwidthLimiter{
		bytesPerSec: bytesPerSec,
		tokens:      bytesPerSec,
		lastRefill:  time.Now(),
	}
}

func (r *bandwidthLimiter) Wait(ctx context.Context, bytes int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Refill tokens
	now := time.Now()
	elapsed := now.Sub(r.lastRefill)
	refill := int64(elapsed.Seconds() * float64(r.bytesPerSec))
	r.tokens += refill
	if r.tokens > r.bytesPerSec {
		r.tokens = r.bytesPerSec
	}
	r.lastRefill = now

	// Wait if needed
	if bytes > r.tokens {
		waitTime := time.Duration(float64(bytes-r.tokens) / float64(r.bytesPerSec) * float64(time.Second))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			r.tokens = 0
		}
	} else {
		r.tokens -= bytes
	}

	return nil
}

// EdgeSyncStatus represents the current sync status.
type EdgeSyncStatus struct {
	Running      bool      `json:"running"`
	LastSync     time.Time `json:"last_sync"`
	QueuedPoints int64     `json:"queued_points"`
	QueuedBytes  int64     `json:"queued_bytes"`
	TotalSynced  int64     `json:"total_synced"`
	Errors       int64     `json:"errors"`
	LastError    string    `json:"last_error,omitempty"`
}

// Status returns the current sync status.
func (m *EdgeSyncManager) Status() EdgeSyncStatus {
	stats := m.Stats()
	return EdgeSyncStatus{
		Running:      m.running.Load(),
		LastSync:     stats.LastSyncTime,
		QueuedPoints: stats.QueuedPoints,
		QueuedBytes:  stats.QueuedBytes,
		TotalSynced:  stats.PointsSynced,
		Errors:       stats.SyncFailures,
		LastError:    stats.LastSyncError,
	}
}
