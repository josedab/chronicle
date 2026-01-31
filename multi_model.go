package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
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
	Key       string      `json:"key"`
	Value     interface{} `json:"value"`
	Version   int64       `json:"version"`
	Created   time.Time   `json:"created"`
	Updated   time.Time   `json:"updated"`
	ExpiresAt time.Time   `json:"expires_at,omitempty"`
	TTL       time.Duration `json:"ttl,omitempty"`
}

// Document represents a stored document.
type Document struct {
	ID          string                 `json:"id"`
	Collection  string                 `json:"collection"`
	Data        map[string]interface{} `json:"data"`
	Version     int64                  `json:"version"`
	Created     time.Time              `json:"created"`
	Updated     time.Time              `json:"updated"`
	Metadata    map[string]string      `json:"metadata,omitempty"`
}

// MultiModelLogEntry represents a log entry in multi-model storage.
type MultiModelLogEntry struct {
	ID        string                 `json:"id"`
	Stream    string                 `json:"stream"`
	Timestamp int64                  `json:"timestamp"`
	Level     MultiModelLogLevel     `json:"level"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
	Tags      map[string]string      `json:"tags,omitempty"`
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
	Name      string                 `json:"name"`
	Entries   []*MultiModelLogEntry  `json:"entries"`
	Created   time.Time              `json:"created"`
	LastWrite time.Time              `json:"last_write"`
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
func (m *MultiModelStore) Set(key string, value interface{}, opts ...KVOption) error {
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
func (m *MultiModelStore) Get(key string) (interface{}, bool) {
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
func (m *MultiModelStore) InsertDocument(collection string, doc map[string]interface{}) (*Document, error) {
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
func (m *MultiModelStore) UpdateDocument(collection, id string, updates map[string]interface{}) (*Document, error) {
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

// DocumentQuery represents a document query.
type DocumentQuery struct {
	Filters   map[string]interface{} `json:"filters"`
	SortField string                 `json:"sort_field"`
	SortDesc  bool                   `json:"sort_desc"`
	Limit     int                    `json:"limit"`
	Skip      int                    `json:"skip"`
}

func (m *MultiModelStore) matchesDocumentQuery(doc *Document, query DocumentQuery) bool {
	for field, value := range query.Filters {
		docValue := getNestedValue(doc.Data, field)
		if !matchesFilter(docValue, value) {
			return false
		}
	}
	return true
}

func matchesFilter(docValue, filterValue interface{}) bool {
	if filterValue == nil {
		return docValue == nil
	}

	// Handle filter operators
	if fmap, ok := filterValue.(map[string]interface{}); ok {
		for op, val := range fmap {
			switch op {
			case "$eq":
				return compareValues(docValue, val) == 0
			case "$ne":
				return compareValues(docValue, val) != 0
			case "$gt":
				return compareValues(docValue, val) > 0
			case "$gte":
				return compareValues(docValue, val) >= 0
			case "$lt":
				return compareValues(docValue, val) < 0
			case "$lte":
				return compareValues(docValue, val) <= 0
			case "$in":
				if arr, ok := val.([]interface{}); ok {
					for _, v := range arr {
						if compareValues(docValue, v) == 0 {
							return true
						}
					}
				}
				return false
			case "$regex":
				if pattern, ok := val.(string); ok {
					if s, ok := docValue.(string); ok {
						matched, _ := regexp.MatchString(pattern, s)
						return matched
					}
				}
				return false
			}
		}
	}

	// Direct comparison
	return compareValues(docValue, filterValue) == 0
}

func getNestedValue(data map[string]interface{}, path string) interface{} {
	parts := strings.Split(path, ".")
	var current interface{} = data

	for _, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			current = m[part]
		} else {
			return nil
		}
	}

	return current
}

func compareValues(a, b interface{}) int {
	// Convert to comparable types
	switch va := a.(type) {
	case float64:
		if vb, ok := toFloat64(b); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int64:
		if vb, ok := toFloat64(b); ok {
			vaf := float64(va)
			if vaf < vb {
				return -1
			} else if vaf > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			return strings.Compare(va, vb)
		}
	}

	// Fallback to string comparison
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	return strings.Compare(sa, sb)
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	default:
		return 0, false
	}
}

func (m *MultiModelStore) updateIndexes(collection string, doc *Document) {
	for _, idx := range m.indexes {
		idx.mu.Lock()

		// Extract value at path
		value := extractJSONPath(doc.Data, idx.Path)
		if value != "" {
			if idx.Values[value] == nil {
				idx.Values[value] = make(map[string]string)
			}
			idx.Values[value][collection] = doc.ID
		}

		idx.mu.Unlock()
	}
}

func (m *MultiModelStore) removeFromIndexes(collection string, doc *Document) {
	for _, idx := range m.indexes {
		idx.mu.Lock()

		value := extractJSONPath(doc.Data, idx.Path)
		if value != "" {
			if collMap := idx.Values[value]; collMap != nil {
				delete(collMap, collection)
			}
		}

		idx.mu.Unlock()
	}
}

func extractJSONPath(data map[string]interface{}, path string) string {
	// Simple JSON path extraction ($.field or field)
	path = strings.TrimPrefix(path, "$.")
	parts := strings.Split(path, ".")

	var current interface{} = data
	for _, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			current = m[part]
		} else {
			return ""
		}
	}

	return fmt.Sprintf("%v", current)
}

// FindByIndex finds documents using a secondary index.
func (m *MultiModelStore) FindByIndex(indexPath string, value string) []*Document {
	m.mu.RLock()
	idx, ok := m.indexes[indexPath]
	if !ok {
		m.mu.RUnlock()
		return nil
	}

	idx.mu.RLock()
	collMap := idx.Values[value]
	idx.mu.RUnlock()

	var docs []*Document
	for collection, docID := range collMap {
		if coll := m.docStore[collection]; coll != nil {
			if doc := coll[docID]; doc != nil {
				docs = append(docs, doc)
			}
		}
	}
	m.mu.RUnlock()

	return docs
}

// ListCollections returns all collection names.
func (m *MultiModelStore) ListCollections() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collections := make([]string, 0, len(m.docStore))
	for name := range m.docStore {
		collections = append(collections, name)
	}
	sort.Strings(collections)
	return collections
}

// CountDocuments returns document count in a collection.
func (m *MultiModelStore) CountDocuments(collection string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if coll := m.docStore[collection]; coll != nil {
		return len(coll)
	}
	return 0
}

// --- Log Operations ---

// AppendLog appends a log entry to a stream.
func (m *MultiModelStore) AppendLog(streamName string, entry *MultiModelLogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.logStore[streamName]
	if !ok {
		stream = &LogStream{
			Name:    streamName,
			Entries: make([]*MultiModelLogEntry, 0),
			Created: time.Now(),
		}
		m.logStore[streamName] = stream
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()

	// Set defaults
	if entry.ID == "" {
		entry.ID = fmt.Sprintf("log-%d", time.Now().UnixNano())
	}
	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixNano()
	}
	entry.Stream = streamName

	stream.Entries = append(stream.Entries, entry)
	stream.LastWrite = time.Now()

	return nil
}

// QueryLogs queries log entries.
func (m *MultiModelStore) QueryLogs(query LogQuery) ([]*MultiModelLogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []*MultiModelLogEntry

	// If stream specified, only query that stream
	streams := make([]*LogStream, 0)
	if query.Stream != "" {
		if stream, ok := m.logStore[query.Stream]; ok {
			streams = append(streams, stream)
		}
	} else {
		for _, stream := range m.logStore {
			streams = append(streams, stream)
		}
	}

	for _, stream := range streams {
		stream.mu.RLock()
		for _, entry := range stream.Entries {
			if m.matchesLogQuery(entry, query) {
				results = append(results, entry)
			}
		}
		stream.mu.RUnlock()
	}

	// Sort by timestamp
	sort.Slice(results, func(i, j int) bool {
		if query.Desc {
			return results[i].Timestamp > results[j].Timestamp
		}
		return results[i].Timestamp < results[j].Timestamp
	})

	// Apply limit
	if query.Limit > 0 && len(results) > query.Limit {
		results = results[:query.Limit]
	}

	return results, nil
}

// LogQuery represents a log query.
type LogQuery struct {
	Stream    string                  `json:"stream"`
	Level     *MultiModelLogLevel     `json:"level,omitempty"`
	MinLevel  *MultiModelLogLevel     `json:"min_level,omitempty"`
	StartTime int64                   `json:"start_time"`
	EndTime   int64                   `json:"end_time"`
	Contains  string                  `json:"contains"`
	Tags      map[string]string       `json:"tags"`
	Limit     int                     `json:"limit"`
	Desc      bool                    `json:"desc"`
}

func (m *MultiModelStore) matchesLogQuery(entry *MultiModelLogEntry, query LogQuery) bool {
	// Time range
	if query.StartTime > 0 && entry.Timestamp < query.StartTime {
		return false
	}
	if query.EndTime > 0 && entry.Timestamp > query.EndTime {
		return false
	}

	// Level filter
	if query.Level != nil && entry.Level != *query.Level {
		return false
	}
	if query.MinLevel != nil && entry.Level < *query.MinLevel {
		return false
	}

	// Text search
	if query.Contains != "" && !strings.Contains(entry.Message, query.Contains) {
		return false
	}

	// Tag filter
	for k, v := range query.Tags {
		if entry.Tags[k] != v {
			return false
		}
	}

	return true
}

// ListStreams returns all log stream names.
func (m *MultiModelStore) ListStreams() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	streams := make([]string, 0, len(m.logStore))
	for name := range m.logStore {
		streams = append(streams, name)
	}
	sort.Strings(streams)
	return streams
}

// GetStreamStats returns statistics for a log stream.
func (m *MultiModelStore) GetStreamStats(streamName string) (*LogStreamStats, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stream, ok := m.logStore[streamName]
	if !ok {
		return nil, false
	}

	stream.mu.RLock()
	defer stream.mu.RUnlock()

	stats := &LogStreamStats{
		Name:      stream.Name,
		Count:     len(stream.Entries),
		Created:   stream.Created,
		LastWrite: stream.LastWrite,
	}

	if len(stream.Entries) > 0 {
		stats.OldestEntry = stream.Entries[0].Timestamp
		stats.NewestEntry = stream.Entries[len(stream.Entries)-1].Timestamp

		// Count by level
		stats.LevelCounts = make(map[MultiModelLogLevel]int)
		for _, entry := range stream.Entries {
			stats.LevelCounts[entry.Level]++
		}
	}

	return stats, true
}

// LogStreamStats contains log stream statistics.
type LogStreamStats struct {
	Name        string                      `json:"name"`
	Count       int                         `json:"count"`
	Created     time.Time                   `json:"created"`
	LastWrite   time.Time                   `json:"last_write"`
	OldestEntry int64                       `json:"oldest_entry"`
	NewestEntry int64                       `json:"newest_entry"`
	LevelCounts map[MultiModelLogLevel]int  `json:"level_counts"`
}

// --- Utility Functions ---

func patternToRegex(pattern string) *regexp.Regexp {
	// Convert glob pattern to regex
	escaped := regexp.QuoteMeta(pattern)
	escaped = strings.ReplaceAll(escaped, `\*`, `.*`)
	escaped = strings.ReplaceAll(escaped, `\?`, `.`)
	regex, _ := regexp.Compile("^" + escaped + "$")
	return regex
}

// GetStats returns multi-model store statistics.
func (m *MultiModelStore) GetStats() MultiModelStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := MultiModelStats{
		KVCount:         len(m.kvStore),
		CollectionCount: len(m.docStore),
		StreamCount:     len(m.logStore),
		IndexCount:      len(m.indexes),
		DocumentCounts:  make(map[string]int),
		StreamSizes:     make(map[string]int),
	}

	for name, coll := range m.docStore {
		stats.DocumentCounts[name] = len(coll)
		stats.TotalDocuments += len(coll)
	}

	for name, stream := range m.logStore {
		stream.mu.RLock()
		stats.StreamSizes[name] = len(stream.Entries)
		stats.TotalLogEntries += len(stream.Entries)
		stream.mu.RUnlock()
	}

	return stats
}

// MultiModelStats contains store statistics.
type MultiModelStats struct {
	KVCount         int            `json:"kv_count"`
	CollectionCount int            `json:"collection_count"`
	TotalDocuments  int            `json:"total_documents"`
	StreamCount     int            `json:"stream_count"`
	TotalLogEntries int            `json:"total_log_entries"`
	IndexCount      int            `json:"index_count"`
	DocumentCounts  map[string]int `json:"document_counts"`
	StreamSizes     map[string]int `json:"stream_sizes"`
}
