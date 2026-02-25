package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MultiModelConfig configures the multi-model storage engine.
type MultiModelConfig struct {
	// Enabled enables multi-model storage
	Enabled bool `json:"enabled"`

	// KeyValueTTL default TTL for key-value entries
	KeyValueTTL time.Duration `json:"kv_ttl"`

	// MaxDocumentSize maximum size for documents in bytes
	MaxDocumentSize int `json:"max_document_size"`

	// MaxLogRetention maximum age for log entries
	MaxLogRetention time.Duration `json:"max_log_retention"`

	// IndexPaths JSON paths to index in documents
	IndexPaths []string `json:"index_paths"`

	// CompactionInterval for background cleanup
	CompactionInterval time.Duration `json:"compaction_interval"`
}

// DefaultMultiModelConfig returns default configuration.
func DefaultMultiModelConfig() MultiModelConfig {
	return MultiModelConfig{
		Enabled:            true,
		KeyValueTTL:        24 * time.Hour,
		MaxDocumentSize:    1024 * 1024, // 1MB
		MaxLogRetention:    7 * 24 * time.Hour,
		IndexPaths:         []string{"$.id", "$.type", "$.name"},
		CompactionInterval: time.Hour,
	}
}

// MultiModelStore provides key-value, document, and log storage alongside time-series.
type MultiModelStore struct {
	db     *DB
	config MultiModelConfig
	mu     sync.RWMutex

	// Key-Value store
	kvStore map[string]*KVEntry

	// Document store with collections
	docStore map[string]map[string]*Document

	// Log store
	logStore map[string]*LogStream

	// Secondary indexes
	indexes map[string]*SecondaryIndex

	// Background context
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// KVEntry represents a key-value entry.
type KVEntry struct {
	Key       string        `json:"key"`
	Value     any           `json:"value"`
	Version   int64         `json:"version"`
	Created   time.Time     `json:"created"`
	Updated   time.Time     `json:"updated"`
	ExpiresAt time.Time     `json:"expires_at,omitempty"`
	TTL       time.Duration `json:"ttl,omitempty"`
}

// Document represents a stored document.
type Document struct {
	ID         string            `json:"id"`
	Collection string            `json:"collection"`
	Data       map[string]any    `json:"data"`
	Version    int64             `json:"version"`
	Created    time.Time         `json:"created"`
	Updated    time.Time         `json:"updated"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// MultiModelLogEntry represents a log entry in multi-model storage.
type MultiModelLogEntry struct {
	ID        string             `json:"id"`
	Stream    string             `json:"stream"`
	Timestamp int64              `json:"timestamp"`
	Level     MultiModelLogLevel `json:"level"`
	Message   string             `json:"message"`
	Fields    map[string]any     `json:"fields,omitempty"`
	Tags      map[string]string  `json:"tags,omitempty"`
}

// MultiModelLogLevel represents log severity.
type MultiModelLogLevel int

const (
	MultiModelLogLevelDebug MultiModelLogLevel = iota
	MultiModelLogLevelInfo
	MultiModelLogLevelWarn
	MultiModelLogLevelError
	MultiModelLogLevelFatal
)

func (l MultiModelLogLevel) String() string {
	switch l {
	case MultiModelLogLevelDebug:
		return "DEBUG"
	case MultiModelLogLevelInfo:
		return "INFO"
	case MultiModelLogLevelWarn:
		return "WARN"
	case MultiModelLogLevelError:
		return "ERROR"
	case MultiModelLogLevelFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// LogStream contains log entries for a stream.
type LogStream struct {
	Name      string                `json:"name"`
	Entries   []*MultiModelLogEntry `json:"entries"`
	Created   time.Time             `json:"created"`
	LastWrite time.Time             `json:"last_write"`
	mu        sync.RWMutex
}

// SecondaryIndex for document lookups.
type SecondaryIndex struct {
	Name   string                       `json:"name"`
	Path   string                       `json:"path"`
	Values map[string]map[string]string // value -> collection -> docID
	mu     sync.RWMutex
}

// NewMultiModelStore creates a new multi-model store.
func NewMultiModelStore(db *DB, config MultiModelConfig) *MultiModelStore {
	ctx, cancel := context.WithCancel(context.Background())

	store := &MultiModelStore{
		db:       db,
		config:   config,
		kvStore:  make(map[string]*KVEntry),
		docStore: make(map[string]map[string]*Document),
		logStore: make(map[string]*LogStream),
		indexes:  make(map[string]*SecondaryIndex),
		ctx:      ctx,
		cancel:   cancel,
	}

	// Initialize indexes
	for _, path := range config.IndexPaths {
		store.indexes[path] = &SecondaryIndex{
			Name:   path,
			Path:   path,
			Values: make(map[string]map[string]string),
		}
	}

	return store
}

// Start starts background tasks.
func (m *MultiModelStore) Start() {
	m.wg.Add(1)
	go m.compactionLoop()
}

// Stop stops the multi-model store.
func (m *MultiModelStore) Stop() {
	m.cancel()
	m.wg.Wait()
}

func (m *MultiModelStore) compactionLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.runCompaction()
		}
	}
}

func (m *MultiModelStore) runCompaction() {
	now := time.Now()

	// Clean expired KV entries
	m.mu.Lock()
	for key, entry := range m.kvStore {
		if !entry.ExpiresAt.IsZero() && now.After(entry.ExpiresAt) {
			delete(m.kvStore, key)
		}
	}
	m.mu.Unlock()

	// Clean old log entries
	cutoff := now.Add(-m.config.MaxLogRetention)
	m.mu.Lock()
	for _, stream := range m.logStore {
		stream.mu.Lock()
		retained := make([]*MultiModelLogEntry, 0, len(stream.Entries))
		for _, entry := range stream.Entries {
			if time.Unix(0, entry.Timestamp).After(cutoff) {
				retained = append(retained, entry)
			}
		}
		stream.Entries = retained
		stream.mu.Unlock()
	}
	m.mu.Unlock()
}

// --- Key-Value Operations ---

// Set stores a key-value pair.
func (m *MultiModelStore) Set(key string, value any, opts ...KVOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	entry := &KVEntry{
		Key:     key,
		Value:   value,
		Created: now,
		Updated: now,
		Version: 1,
	}

	// Apply options
	for _, opt := range opts {
		opt(entry)
	}

	// Check for existing entry
	if existing, ok := m.kvStore[key]; ok {
		entry.Version = existing.Version + 1
		entry.Created = existing.Created
	}

	// Set expiry if TTL specified
	if entry.TTL > 0 {
		entry.ExpiresAt = now.Add(entry.TTL)
	} else if m.config.KeyValueTTL > 0 {
		entry.ExpiresAt = now.Add(m.config.KeyValueTTL)
	}

	m.kvStore[key] = entry
	return nil
}

// Get retrieves a value by key.
func (m *MultiModelStore) Get(key string) (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.kvStore[key]
	if !ok {
		return nil, false
	}

	// Check expiry
	if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
		return nil, false
	}

	return entry.Value, true
}

// GetEntry retrieves a full KV entry.
func (m *MultiModelStore) GetEntry(key string) (*KVEntry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.kvStore[key]
	if !ok {
		return nil, false
	}

	if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
		return nil, false
	}

	return entry, true
}

// Delete removes a key.
func (m *MultiModelStore) Delete(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.kvStore[key]; ok {
		delete(m.kvStore, key)
		return true
	}
	return false
}

// Keys returns all keys matching a pattern.
func (m *MultiModelStore) Keys(pattern string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	regex := patternToRegex(pattern)

	for key, entry := range m.kvStore {
		// Skip expired
		if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
			continue
		}

		if regex.MatchString(key) {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)
	return keys
}

// Incr increments a numeric value.
func (m *MultiModelStore) Incr(key string, delta int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.kvStore[key]
	if !ok {
		entry = &KVEntry{
			Key:     key,
			Value:   int64(0),
			Created: time.Now(),
			Version: 1,
		}
		m.kvStore[key] = entry
	}

	// Convert to int64
	var current int64
	switch v := entry.Value.(type) {
	case int64:
		current = v
	case int:
		current = int64(v)
	case float64:
		current = int64(v)
	default:
		return 0, errors.New("value is not numeric")
	}

	newVal := current + delta
	entry.Value = newVal
	entry.Updated = time.Now()
	entry.Version++

	return newVal, nil
}

// KVOption configures KV operations.
type KVOption func(*KVEntry)

// WithTTL sets TTL for KV entry.
func WithTTL(ttl time.Duration) KVOption {
	return func(e *KVEntry) {
		e.TTL = ttl
	}
}

// WithExpiry sets explicit expiry time.
func WithExpiry(t time.Time) KVOption {
	return func(e *KVEntry) {
		e.ExpiresAt = t
	}
}

// --- Document Operations ---

// InsertDocument inserts a document into a collection.
func (m *MultiModelStore) InsertDocument(collection string, doc map[string]any) (*Document, error) {
	// Validate size
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}
	if len(data) > m.config.MaxDocumentSize {
		return nil, fmt.Errorf("document exceeds max size (%d > %d)", len(data), m.config.MaxDocumentSize)
	}

	// Extract or generate ID
	id, ok := doc["id"].(string)
	if !ok || id == "" {
		id = fmt.Sprintf("doc-%d", time.Now().UnixNano())
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Ensure collection exists
	if m.docStore[collection] == nil {
		m.docStore[collection] = make(map[string]*Document)
	}

	// Check for duplicate
	if _, exists := m.docStore[collection][id]; exists {
		return nil, fmt.Errorf("document %s already exists", id)
	}

	now := time.Now()
	document := &Document{
		ID:         id,
		Collection: collection,
		Data:       doc,
		Version:    1,
		Created:    now,
		Updated:    now,
	}

	m.docStore[collection][id] = document

	// Update indexes
	m.updateIndexes(collection, document)

	return document, nil
}

// GetDocument retrieves a document by ID.
func (m *MultiModelStore) GetDocument(collection, id string) (*Document, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	coll, ok := m.docStore[collection]
	if !ok {
		return nil, false
	}

	doc, ok := coll[id]
	return doc, ok
}

// UpdateDocument updates a document.
func (m *MultiModelStore) UpdateDocument(collection, id string, updates map[string]any) (*Document, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	coll, ok := m.docStore[collection]
	if !ok {
		return nil, errors.New("collection not found")
	}

	doc, ok := coll[id]
	if !ok {
		return nil, errors.New("document not found")
	}

	// Apply updates
	for k, v := range updates {
		doc.Data[k] = v
	}
	doc.Version++
	doc.Updated = time.Now()

	// Update indexes
	m.updateIndexes(collection, doc)

	return doc, nil
}

// DeleteDocument removes a document.
func (m *MultiModelStore) DeleteDocument(collection, id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	coll, ok := m.docStore[collection]
	if !ok {
		return false
	}

	doc, ok := coll[id]
	if !ok {
		return false
	}

	// Remove from indexes
	m.removeFromIndexes(collection, doc)

	delete(coll, id)
	return true
}

// FindDocuments queries documents in a collection.
func (m *MultiModelStore) FindDocuments(collection string, query DocumentQuery) ([]*Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	coll, ok := m.docStore[collection]
	if !ok {
		return nil, nil
	}

	var results []*Document

	for _, doc := range coll {
		if m.matchesDocumentQuery(doc, query) {
			results = append(results, doc)
		}
	}

	// Sort if specified
	if query.SortField != "" {
		sort.Slice(results, func(i, j int) bool {
			vi := getNestedValue(results[i].Data, query.SortField)
			vj := getNestedValue(results[j].Data, query.SortField)
			cmp := compareValues(vi, vj)
			if query.SortDesc {
				return cmp > 0
			}
			return cmp < 0
		})
	}

	// Apply limit
	if query.Limit > 0 && len(results) > query.Limit {
		results = results[:query.Limit]
	}

	return results, nil
}
