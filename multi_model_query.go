package chronicle

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Document query matching and nested value helpers for the multi-model store.

// DocumentQuery represents a document query.
type DocumentQuery struct {
	Filters   map[string]any `json:"filters"`
	SortField string         `json:"sort_field"`
	SortDesc  bool           `json:"sort_desc"`
	Limit     int            `json:"limit"`
	Skip      int            `json:"skip"`
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

func matchesFilter(docValue, filterValue any) bool {
	if filterValue == nil {
		return docValue == nil
	}

	// Handle filter operators
	if fmap, ok := filterValue.(map[string]any); ok {
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
				if arr, ok := val.([]any); ok {
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

func getNestedValue(data map[string]any, path string) any {
	parts := strings.Split(path, ".")
	var current any = data

	for _, part := range parts {
		if m, ok := current.(map[string]any); ok {
			current = m[part]
		} else {
			return nil
		}
	}

	return current
}

func compareValues(a, b any) int {
	// Convert to comparable types
	switch va := a.(type) {
	case float64:
		if vb, ok := multiModelToFloat64(b); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int64:
		if vb, ok := multiModelToFloat64(b); ok {
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

func multiModelToFloat64(v any) (float64, bool) {
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

func extractJSONPath(data map[string]any, path string) string {
	// Simple JSON path extraction ($.field or field)
	path = strings.TrimPrefix(path, "$.")
	parts := strings.Split(path, ".")

	var current any = data
	for _, part := range parts {
		if m, ok := current.(map[string]any); ok {
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
func (m *MultiModelStore) QueryLogs(query MultiModelLogQuery) ([]*MultiModelLogEntry, error) {
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

// MultiModelLogQuery represents a log query.
type MultiModelLogQuery struct {
	Stream    string              `json:"stream"`
	Level     *MultiModelLogLevel `json:"level,omitempty"`
	MinLevel  *MultiModelLogLevel `json:"min_level,omitempty"`
	StartTime int64               `json:"start_time"`
	EndTime   int64               `json:"end_time"`
	Contains  string              `json:"contains"`
	Tags      map[string]string   `json:"tags"`
	Limit     int                 `json:"limit"`
	Desc      bool                `json:"desc"`
}

func (m *MultiModelStore) matchesLogQuery(entry *MultiModelLogEntry, query MultiModelLogQuery) bool {
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
	Name        string                     `json:"name"`
	Count       int                        `json:"count"`
	Created     time.Time                  `json:"created"`
	LastWrite   time.Time                  `json:"last_write"`
	OldestEntry int64                      `json:"oldest_entry"`
	NewestEntry int64                      `json:"newest_entry"`
	LevelCounts map[MultiModelLogLevel]int `json:"level_counts"`
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
