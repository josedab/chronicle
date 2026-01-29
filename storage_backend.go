package chronicle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// StorageBackend defines the interface for partition storage.
// This allows Chronicle to store data in different backends like
// local filesystem, S3, GCS, or Azure Blob Storage.
type StorageBackend interface {
	// Read reads a partition from storage.
	Read(ctx context.Context, key string) ([]byte, error)

	// Write writes a partition to storage.
	Write(ctx context.Context, key string, data []byte) error

	// Delete removes a partition from storage.
	Delete(ctx context.Context, key string) error

	// List returns all partition keys matching a prefix.
	List(ctx context.Context, prefix string) ([]string, error)

	// Exists checks if a partition exists.
	Exists(ctx context.Context, key string) (bool, error)

	// Close releases any resources.
	Close() error
}

// FileBackend implements StorageBackend using the local filesystem.
type FileBackend struct {
	baseDir string
}

// NewFileBackend creates a new file-based storage backend.
func NewFileBackend(baseDir string) (*FileBackend, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}
	// Store the cleaned absolute path for consistent path traversal checks
	absDir, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve base directory: %w", err)
	}
	return &FileBackend{baseDir: filepath.Clean(absDir)}, nil
}

// safePath validates and returns a safe path within the base directory.
// It prevents path traversal attacks by ensuring the resolved path stays within baseDir.
func (f *FileBackend) safePath(key string) (string, error) {
	// Clean the key first to normalize path separators and remove redundant elements
	cleanKey := filepath.Clean(key)
	
	// Join with base directory
	joined := filepath.Join(f.baseDir, cleanKey)
	
	// Clean the full path to resolve any remaining .. elements
	resolved := filepath.Clean(joined)
	
	// Verify the resolved path is still within baseDir
	// Must either equal baseDir or be a child path (has baseDir/ as prefix)
	if resolved != f.baseDir && !strings.HasPrefix(resolved, f.baseDir+string(os.PathSeparator)) {
		return "", errors.New("invalid key: path traversal attempt detected")
	}
	
	return resolved, nil
}

func (f *FileBackend) Read(ctx context.Context, key string) ([]byte, error) {
	path, err := f.safePath(key)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

func (f *FileBackend) Write(ctx context.Context, key string, data []byte) error {
	path, err := f.safePath(key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (f *FileBackend) Delete(ctx context.Context, key string) error {
	path, err := f.safePath(key)
	if err != nil {
		return err
	}
	return os.Remove(path)
}

func (f *FileBackend) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	searchPath, err := f.safePath(prefix)
	if err != nil {
		return nil, err
	}

	err = filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Ignore errors
		}
		if !info.IsDir() {
			rel, _ := filepath.Rel(f.baseDir, path)
			keys = append(keys, rel)
		}
		return nil
	})

	return keys, err
}

func (f *FileBackend) Exists(ctx context.Context, key string) (bool, error) {
	path, err := f.safePath(key)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

func (f *FileBackend) Close() error {
	return nil
}

// S3BackendConfig configures the S3 storage backend.
type S3BackendConfig struct {
	Bucket   string
	Region   string
	Endpoint string // For S3-compatible services (MinIO, etc.)
	// AccessKeyID for authentication. Prefer using IAM roles, instance profiles,
	// or environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) instead
	// of setting these directly. DO NOT commit credentials to source control.
	AccessKeyID     string
	SecretAccessKey string
	Prefix          string // Key prefix for all objects
	UsePathStyle    bool   // Use path-style addressing
	CacheSize       int    // Number of partitions to cache (default: 100)

	// Retry configuration
	MaxRetries int // Max retry attempts for S3 operations (default: 3)
}

// S3Backend implements StorageBackend using S3 or S3-compatible storage.
type S3Backend struct {
	client  *s3.Client
	config  S3BackendConfig
	cache   *LRUCache
	mu      sync.RWMutex
	retryer *Retryer
}

// LRUCache is a simple LRU cache for partition data.
type LRUCache struct {
	capacity int
	items    map[string]*cacheItem
	order    []string
	mu       sync.Mutex
}

type cacheItem struct {
	data      []byte
	timestamp time.Time
}

// NewLRUCache creates a new LRU cache.
func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		items:    make(map[string]*cacheItem),
	}
}

// Get retrieves an item from the cache.
func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.items[key]
	if !ok {
		return nil, false
	}

	// Move to end (most recently used)
	c.moveToEnd(key)
	return item.data, true
}

// Put adds an item to the cache.
func (c *LRUCache) Put(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[key]; ok {
		c.items[key].data = data
		c.items[key].timestamp = time.Now()
		c.moveToEnd(key)
		return
	}

	// Evict if at capacity
	for len(c.items) >= c.capacity && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.items, oldest)
	}

	c.items[key] = &cacheItem{data: data, timestamp: time.Now()}
	c.order = append(c.order, key)
}

// Delete removes an item from the cache.
func (c *LRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
}

func (c *LRUCache) moveToEnd(key string) {
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			c.order = append(c.order, key)
			break
		}
	}
}

// NewS3Backend creates a new S3 storage backend.
func NewS3Backend(cfg S3BackendConfig) (*S3Backend, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.CacheSize <= 0 {
		cfg.CacheSize = 100
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}

	// Build AWS config options
	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(cfg.Region))

	// Use static credentials if provided
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Build S3 client options
	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = cfg.UsePathStyle
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &S3Backend{
		client: client,
		config: cfg,
		cache:  NewLRUCache(cfg.CacheSize),
		retryer: NewRetryer(RetryConfig{
			MaxAttempts:       cfg.MaxRetries,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        10 * time.Second,
			BackoffMultiplier: 2.0,
			Jitter:            0.1,
			RetryIf:           IsRetryable,
		}),
	}, nil
}

func (s *S3Backend) Read(ctx context.Context, key string) ([]byte, error) {
	fullKey := s.config.Prefix + key

	// Check cache first
	if data, ok := s.cache.Get(fullKey); ok {
		return data, nil
	}

	val, result := s.retryer.DoWithResult(ctx, func() (interface{}, error) {
		resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.config.Bucket),
			Key:    aws.String(fullKey),
		})
		if err != nil {
			return nil, fmt.Errorf("S3 get object failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		d, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("S3 read body failed: %w", err)
		}
		return d, nil
	})

	if result.LastErr != nil {
		return nil, result.LastErr
	}

	data := val.([]byte)
	s.cache.Put(fullKey, data)
	return data, nil
}

func (s *S3Backend) Write(ctx context.Context, key string, data []byte) error {
	fullKey := s.config.Prefix + key

	result := s.retryer.Do(ctx, func() error {
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.config.Bucket),
			Key:    aws.String(fullKey),
			Body:   bytes.NewReader(data),
		})
		if err != nil {
			return fmt.Errorf("S3 put object failed: %w", err)
		}
		return nil
	})

	if result.LastErr != nil {
		return result.LastErr
	}

	s.cache.Put(fullKey, data)
	return nil
}

func (s *S3Backend) Delete(ctx context.Context, key string) error {
	fullKey := s.config.Prefix + key

	result := s.retryer.Do(ctx, func() error {
		_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.config.Bucket),
			Key:    aws.String(fullKey),
		})
		if err != nil {
			return fmt.Errorf("S3 delete object failed: %w", err)
		}
		return nil
	})

	if result.LastErr != nil {
		return result.LastErr
	}

	s.cache.Delete(fullKey)
	return nil
}

func (s *S3Backend) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := s.config.Prefix + prefix

	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("S3 list objects failed: %w", err)
		}
		for _, obj := range page.Contents {
			// Remove the prefix to return relative keys
			key := strings.TrimPrefix(*obj.Key, s.config.Prefix)
			keys = append(keys, key)
		}
	}

	return keys, nil
}

func (s *S3Backend) Exists(ctx context.Context, key string) (bool, error) {
	fullKey := s.config.Prefix + key

	// Check cache first
	if _, ok := s.cache.Get(fullKey); ok {
		return true, nil
	}

	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		// Check if it's a "not found" error
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return false, nil
		}
		// For other errors, check if it contains "NotFound" or "404"
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, fmt.Errorf("S3 head object failed: %w", err)
	}

	return true, nil
}

func (s *S3Backend) Close() error {
	return nil
}

// MemoryBackend implements StorageBackend using in-memory storage.
// Useful for testing and WASM environments.
type MemoryBackend struct {
	data map[string][]byte
	mu   sync.RWMutex
}

// NewMemoryBackend creates a new in-memory storage backend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		data: make(map[string][]byte),
	}
}

func (m *MemoryBackend) Read(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.data[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return data, nil
}

func (m *MemoryBackend) Write(ctx context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = append([]byte(nil), data...)
	return nil
}

func (m *MemoryBackend) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

func (m *MemoryBackend) List(ctx context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for k := range m.data {
		if len(prefix) == 0 || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *MemoryBackend) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.data[key]
	return ok, nil
}

func (m *MemoryBackend) Close() error {
	return nil
}

// Size returns the number of items in memory.
func (m *MemoryBackend) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

// TieredBackend implements a two-tier storage system with hot and cold storage.
type TieredBackend struct {
	hot  StorageBackend // Fast local storage for recent data
	cold StorageBackend // Slow remote storage for older data
	age  time.Duration  // Data older than this moves to cold storage
}

// NewTieredBackend creates a tiered storage backend.
func NewTieredBackend(hot, cold StorageBackend, age time.Duration) *TieredBackend {
	return &TieredBackend{
		hot:  hot,
		cold: cold,
		age:  age,
	}
}

func (t *TieredBackend) Read(ctx context.Context, key string) ([]byte, error) {
	// Try hot storage first
	data, err := t.hot.Read(ctx, key)
	if err == nil {
		return data, nil
	}

	// Fall back to cold storage
	data, err = t.cold.Read(ctx, key)
	if err != nil {
		return nil, err
	}

	// Promote to hot storage
	_ = t.hot.Write(ctx, key, data)
	return data, nil
}

func (t *TieredBackend) Write(ctx context.Context, key string, data []byte) error {
	// Always write to hot storage
	return t.hot.Write(ctx, key, data)
}

func (t *TieredBackend) Delete(ctx context.Context, key string) error {
	errHot := t.hot.Delete(ctx, key)
	errCold := t.cold.Delete(ctx, key)
	if errHot != nil && errCold != nil {
		return errHot
	}
	return nil
}

func (t *TieredBackend) List(ctx context.Context, prefix string) ([]string, error) {
	hotKeys, err := t.hot.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	coldKeys, err := t.cold.List(ctx, prefix)
	if err != nil {
		return hotKeys, nil // Return hot keys even if cold fails
	}

	// Merge and deduplicate
	seen := make(map[string]bool)
	for _, k := range hotKeys {
		seen[k] = true
	}
	for _, k := range coldKeys {
		if !seen[k] {
			hotKeys = append(hotKeys, k)
		}
	}
	return hotKeys, nil
}

func (t *TieredBackend) Exists(ctx context.Context, key string) (bool, error) {
	exists, err := t.hot.Exists(ctx, key)
	if err == nil && exists {
		return true, nil
	}
	return t.cold.Exists(ctx, key)
}

func (t *TieredBackend) Close() error {
	errHot := t.hot.Close()
	errCold := t.cold.Close()
	if errHot != nil {
		return errHot
	}
	return errCold
}

// BufferedWriter wraps a StorageBackend with write buffering.
type BufferedWriter struct {
	backend StorageBackend
	buffer  *bytes.Buffer
	maxSize int
}

// Ensure interfaces are implemented
var (
	_ StorageBackend = (*FileBackend)(nil)
	_ StorageBackend = (*S3Backend)(nil)
	_ StorageBackend = (*MemoryBackend)(nil)
	_ StorageBackend = (*TieredBackend)(nil)
)

// StorageBackendFromReader creates a simple ReadCloser-based reader.
func StorageBackendFromReader(r io.ReadCloser) ([]byte, error) {
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}
