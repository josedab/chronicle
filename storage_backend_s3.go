package chronicle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

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

	val, result := s.retryer.DoWithResult(ctx, func() (any, error) {
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
