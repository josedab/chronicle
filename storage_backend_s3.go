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
	CacheTTL        time.Duration // TTL for cached items (default: 0 = no expiry)

	// Retry configuration
	MaxRetries int // Max retry attempts for S3 operations (default: 3)

	// MultipartThreshold is the size above which uploads use multipart.
	// Default: 5MB. Set to 0 to disable multipart.
	MultipartThreshold int64

	// MultipartPartSize is the size of each multipart upload part.
	// Default: 5MB. Must be at least 5MB per S3 spec.
	MultipartPartSize int64
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
	ttl      time.Duration
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

// NewLRUCacheWithTTL creates a new LRU cache with time-based expiry.
func NewLRUCacheWithTTL(capacity int, ttl time.Duration) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		ttl:      ttl,
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

	// TTL eviction
	if c.ttl > 0 && time.Since(item.timestamp) > c.ttl {
		delete(c.items, key)
		for i, k := range c.order {
			if k == key {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
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
	if cfg.MultipartThreshold <= 0 {
		cfg.MultipartThreshold = 5 * 1024 * 1024 // 5MB
	}
	if cfg.MultipartPartSize <= 0 {
		cfg.MultipartPartSize = 5 * 1024 * 1024 // 5MB minimum per S3 spec
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
		cache:  NewLRUCacheWithTTL(cfg.CacheSize, cfg.CacheTTL),
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

	data, ok := val.([]byte)
	if !ok {
		return nil, fmt.Errorf("unexpected type from S3 read for key %s", fullKey)
	}
	s.cache.Put(fullKey, data)
	return data, nil
}

func (s *S3Backend) Write(ctx context.Context, key string, data []byte) error {
	fullKey := s.config.Prefix + key

	// Use multipart upload for large objects
	if int64(len(data)) > s.config.MultipartThreshold {
		if err := s.writeMultipart(ctx, fullKey, data); err != nil {
			return err
		}
		s.cache.Put(fullKey, data)
		return nil
	}

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

func (s *S3Backend) writeMultipart(ctx context.Context, fullKey string, data []byte) error {
	createResp, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return fmt.Errorf("S3 create multipart upload failed: %w", err)
	}

	uploadID := createResp.UploadId
	partSize := s.config.MultipartPartSize
	var completedParts []s3types.CompletedPart

	for partNum := int32(1); int64((partNum-1))*partSize < int64(len(data)); partNum++ {
		start := int64(partNum-1) * partSize
		end := start + partSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}

		uploadResult, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(s.config.Bucket),
			Key:        aws.String(fullKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(partNum),
			Body:       bytes.NewReader(data[start:end]),
		})
		if err != nil {
			// Abort on failure
			_, _ = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(s.config.Bucket),
				Key:      aws.String(fullKey),
				UploadId: uploadID,
			})
			return fmt.Errorf("S3 upload part %d failed: %w", partNum, err)
		}

		completedParts = append(completedParts, s3types.CompletedPart{
			ETag:       uploadResult.ETag,
			PartNumber: aws.Int32(partNum),
		})
	}

	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.config.Bucket),
		Key:      aws.String(fullKey),
		UploadId: uploadID,
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return fmt.Errorf("S3 complete multipart upload failed: %w", err)
	}

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

// --- Multipart Upload Resumption ---

// MultipartUploadState tracks the state of an in-progress multipart upload.
type MultipartUploadState struct {
	UploadID       string                     `json:"upload_id"`
	Key            string                     `json:"key"`
	CompletedParts []s3types.CompletedPart     `json:"completed_parts"`
	NextPartNumber int32                       `json:"next_part_number"`
	TotalSize      int64                       `json:"total_size"`
	StartedAt      time.Time                   `json:"started_at"`
}

// WriteMultipartResumable performs a multipart upload that can be resumed after failure.
// It returns the upload state for tracking and potential resumption.
func (s *S3Backend) WriteMultipartResumable(ctx context.Context, key string, data []byte, state *MultipartUploadState) (*MultipartUploadState, error) {
	fullKey := s.config.Prefix + key

	if state == nil {
		// Start a new multipart upload
		createResp, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(s.config.Bucket),
			Key:    aws.String(fullKey),
		})
		if err != nil {
			return nil, fmt.Errorf("create multipart upload: %w", err)
		}
		state = &MultipartUploadState{
			UploadID:       *createResp.UploadId,
			Key:            fullKey,
			NextPartNumber: 1,
			TotalSize:      int64(len(data)),
			StartedAt:      time.Now(),
		}
	}

	partSize := s.config.MultipartPartSize
	for partNum := state.NextPartNumber; int64((partNum-1))*partSize < int64(len(data)); partNum++ {
		start := int64(partNum-1) * partSize
		end := start + partSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}

		var uploadResult *s3.UploadPartOutput
		result := s.retryer.Do(ctx, func() error {
			var err error
			uploadResult, err = s.client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(s.config.Bucket),
				Key:        aws.String(fullKey),
				UploadId:   aws.String(state.UploadID),
				PartNumber: aws.Int32(partNum),
				Body:       bytes.NewReader(data[start:end]),
			})
			return err
		})
		if result.LastErr != nil {
			state.NextPartNumber = partNum
			return state, fmt.Errorf("upload part %d: %w", partNum, result.LastErr)
		}

		state.CompletedParts = append(state.CompletedParts, s3types.CompletedPart{
			ETag:       uploadResult.ETag,
			PartNumber: aws.Int32(partNum),
		})
		state.NextPartNumber = partNum + 1
	}

	// Complete the upload
	_, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.config.Bucket),
		Key:      aws.String(fullKey),
		UploadId: aws.String(state.UploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: state.CompletedParts,
		},
	})
	if err != nil {
		return state, fmt.Errorf("complete multipart upload: %w", err)
	}

	s.cache.Put(fullKey, data)
	return state, nil
}

// AbortMultipartUpload aborts an in-progress multipart upload.
func (s *S3Backend) AbortMultipartUpload(ctx context.Context, state *MultipartUploadState) error {
	if state == nil || state.UploadID == "" {
		return nil
	}
	_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.config.Bucket),
		Key:      aws.String(state.Key),
		UploadId: aws.String(state.UploadID),
	})
	return err
}

// --- Cost-Optimized Tiering Policies ---

// S3StorageClass represents an S3 storage class for cost optimization.
type S3StorageClass string

const (
	S3ClassStandard           S3StorageClass = "STANDARD"
	S3ClassInfrequentAccess   S3StorageClass = "STANDARD_IA"
	S3ClassOneZoneIA          S3StorageClass = "ONEZONE_IA"
	S3ClassIntelligentTiering S3StorageClass = "INTELLIGENT_TIERING"
	S3ClassGlacier            S3StorageClass = "GLACIER"
	S3ClassGlacierInstant     S3StorageClass = "GLACIER_IR"
	S3ClassDeepArchive        S3StorageClass = "DEEP_ARCHIVE"
)

// S3TieringPolicy defines when to transition objects between storage classes.
type S3TieringPolicy struct {
	Name             string         `json:"name"`
	FromClass        S3StorageClass `json:"from_class"`
	ToClass          S3StorageClass `json:"to_class"`
	AfterDays        int            `json:"after_days"`
	MinSizeBytes     int64          `json:"min_size_bytes"`
	MaxSizeBytes     int64          `json:"max_size_bytes,omitempty"`
	KeyPrefixPattern string         `json:"key_prefix_pattern,omitempty"`
}

// S3TieringConfig holds the tiering configuration.
type S3TieringConfig struct {
	Enabled  bool               `json:"enabled"`
	Policies []S3TieringPolicy  `json:"policies"`
}

// DefaultS3TieringConfig returns cost-optimized tiering policies.
func DefaultS3TieringConfig() S3TieringConfig {
	return S3TieringConfig{
		Enabled: true,
		Policies: []S3TieringPolicy{
			{
				Name:         "hot-to-warm",
				FromClass:    S3ClassStandard,
				ToClass:      S3ClassInfrequentAccess,
				AfterDays:    30,
				MinSizeBytes: 128 * 1024, // 128KB minimum for IA
			},
			{
				Name:         "warm-to-cold",
				FromClass:    S3ClassInfrequentAccess,
				ToClass:      S3ClassGlacierInstant,
				AfterDays:    90,
				MinSizeBytes: 128 * 1024,
			},
			{
				Name:         "cold-to-archive",
				FromClass:    S3ClassGlacierInstant,
				ToClass:      S3ClassDeepArchive,
				AfterDays:    365,
				MinSizeBytes: 128 * 1024,
			},
		},
	}
}

// ShouldTransition checks if an object should be transitioned based on its age
// and the provided policy.
func (p *S3TieringPolicy) ShouldTransition(objectAge time.Duration, objectSize int64) bool {
	if objectAge < time.Duration(p.AfterDays)*24*time.Hour {
		return false
	}
	if objectSize < p.MinSizeBytes {
		return false
	}
	if p.MaxSizeBytes > 0 && objectSize > p.MaxSizeBytes {
		return false
	}
	return true
}

// WriteWithStorageClass writes an object with a specific storage class.
func (s *S3Backend) WriteWithStorageClass(ctx context.Context, key string, data []byte, class S3StorageClass) error {
	fullKey := s.config.Prefix + key

	result := s.retryer.Do(ctx, func() error {
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:       aws.String(s.config.Bucket),
			Key:          aws.String(fullKey),
			Body:         bytes.NewReader(data),
			StorageClass: s3types.StorageClass(class),
		})
		return err
	})
	if result.LastErr != nil {
		return fmt.Errorf("write with storage class %s: %w", class, result.LastErr)
	}

	s.cache.Put(fullKey, data)
	return nil
}

// --- Eventual Consistency Handling ---

// ConsistentRead reads an object with eventual consistency retry.
// Retries reads that return NoSuchKey immediately after a write.
func (s *S3Backend) ConsistentRead(ctx context.Context, key string, maxWait time.Duration) ([]byte, error) {
	if maxWait <= 0 {
		maxWait = 5 * time.Second
	}

	// Check cache first
	if data, ok := s.cache.Get(s.config.Prefix + key); ok {
		return data, nil
	}

	deadline := time.Now().Add(maxWait)
	backoff := 100 * time.Millisecond

	for {
		data, err := s.Read(ctx, key)
		if err == nil {
			return data, nil
		}

		// If it's a "not found" error and we haven't exceeded the deadline,
		// retry to handle S3 eventual consistency
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("consistent read timeout for %s: %w", key, err)
		}

		if !isS3NotFoundError(err) {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}

		backoff = time.Duration(float64(backoff) * 1.5)
		if backoff > time.Second {
			backoff = time.Second
		}
	}
}

// ConsistentWrite performs a write followed by a read-back verification.
func (s *S3Backend) ConsistentWrite(ctx context.Context, key string, data []byte) error {
	if err := s.Write(ctx, key, data); err != nil {
		return err
	}

	// Verify the write is consistent by reading back
	_, err := s.ConsistentRead(ctx, key, 3*time.Second)
	return err
}

func isS3NotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "NoSuchKey") ||
		strings.Contains(errStr, "NotFound") ||
		strings.Contains(errStr, "not found")
}

// --- S3 Backend Status for Promotion ---

// S3BackendStatus reports the health and readiness of the S3 backend.
type S3BackendStatus struct {
	Healthy          bool          `json:"healthy"`
	MaturityLevel    string        `json:"maturity_level"`
	BucketAccessible bool          `json:"bucket_accessible"`
	MultipartEnabled bool          `json:"multipart_enabled"`
	RetryConfigured  bool          `json:"retry_configured"`
	CacheEnabled     bool          `json:"cache_enabled"`
	TieringEnabled   bool          `json:"tiering_enabled"`
	Latency          time.Duration `json:"latency"`
}

// Status returns the current health status of the S3 backend.
func (s *S3Backend) Status(ctx context.Context) *S3BackendStatus {
	status := &S3BackendStatus{
		MaturityLevel:    "production",
		MultipartEnabled: s.config.MultipartThreshold > 0,
		RetryConfigured:  s.config.MaxRetries > 0,
		CacheEnabled:     s.config.CacheSize > 0,
	}

	start := time.Now()
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.config.Bucket),
	})
	status.Latency = time.Since(start)
	status.BucketAccessible = err == nil
	status.Healthy = err == nil

	return status
}
