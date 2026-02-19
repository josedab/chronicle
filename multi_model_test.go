package chronicle

import (
	"testing"
	"time"
)

func TestMultiModelStore_KeyValue(t *testing.T) {
	db := &DB{}
	store := NewMultiModelStore(db, DefaultMultiModelConfig())
	store.Start()
	defer store.Stop()

	// Test Set and Get
	err := store.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Set error: %v", err)
	}

	value, ok := store.Get("key1")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if value != "value1" {
		t.Errorf("expected value1, got %v", value)
	}

	// Test GetEntry
	entry, ok := store.GetEntry("key1")
	if !ok {
		t.Fatal("expected entry to exist")
	}
	if entry.Version != 1 {
		t.Errorf("expected version 1, got %d", entry.Version)
	}

	// Test update increments version
	err = store.Set("key1", "value2")
	if err != nil {
		t.Fatalf("Set error: %v", err)
	}
	entry, _ = store.GetEntry("key1")
	if entry.Version != 2 {
		t.Errorf("expected version 2, got %d", entry.Version)
	}

	// Test Delete
	deleted := store.Delete("key1")
	if !deleted {
		t.Error("expected delete to return true")
	}
	_, ok = store.Get("key1")
	if ok {
		t.Error("expected key to be deleted")
	}
}

func TestMultiModelStore_KeyValueTTL(t *testing.T) {
	config := DefaultMultiModelConfig()
	config.KeyValueTTL = 0 // Disable default TTL

	db := &DB{}
	store := NewMultiModelStore(db, config)

	// Set with short TTL
	err := store.Set("expiring", "value", WithTTL(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Set error: %v", err)
	}

	// Should exist initially
	_, ok := store.Get("expiring")
	if !ok {
		t.Error("expected key to exist before expiry")
	}

	// Wait for expiry
	time.Sleep(60 * time.Millisecond)

	// Should be expired
	_, ok = store.Get("expiring")
	if ok {
		t.Error("expected key to be expired")
	}
}

func TestMultiModelStore_KeyValueIncr(t *testing.T) {
	db := &DB{}
	store := NewMultiModelStore(db, DefaultMultiModelConfig())

	// Test Incr on non-existent key
	val, err := store.Incr("counter", 1)
	if err != nil {
		t.Fatalf("Incr error: %v", err)
	}
	if val != 1 {
		t.Errorf("expected 1, got %d", val)
	}

	// Test increment
	val, err = store.Incr("counter", 5)
	if err != nil {
		t.Fatalf("Incr error: %v", err)
	}
	if val != 6 {
		t.Errorf("expected 6, got %d", val)
	}

	// Test decrement
	val, err = store.Incr("counter", -2)
	if err != nil {
		t.Fatalf("Incr error: %v", err)
	}
	if val != 4 {
		t.Errorf("expected 4, got %d", val)
	}
}

func TestMultiModelStore_Keys(t *testing.T) {
	db := &DB{}
	store := NewMultiModelStore(db, DefaultMultiModelConfig())

	store.Set("user:1", "alice")
	store.Set("user:2", "bob")
	store.Set("user:3", "charlie")
	store.Set("session:1", "data")

	// Test pattern matching
	keys := store.Keys("user:*")
	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}

	keys = store.Keys("*:1")
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}

	keys = store.Keys("*")
	if len(keys) != 4 {
		t.Errorf("expected 4 keys, got %d", len(keys))
	}
}

func TestMultiModelStore_Documents(t *testing.T) {
	db := &DB{}
	store := NewMultiModelStore(db, DefaultMultiModelConfig())
	store.Start()
	defer store.Stop()

	// Insert document
	doc, err := store.InsertDocument("users", map[string]any{
		"id":    "user1",
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   30,
	})
	if err != nil {
		t.Fatalf("InsertDocument error: %v", err)
	}
	if doc.ID != "user1" {
		t.Errorf("expected id user1, got %s", doc.ID)
	}

	// Get document
	retrieved, ok := store.GetDocument("users", "user1")
	if !ok {
		t.Fatal("expected document to exist")
	}
	if retrieved.Data["name"] != "Alice" {
		t.Errorf("expected name Alice, got %v", retrieved.Data["name"])
	}

	// Update document
	updated, err := store.UpdateDocument("users", "user1", map[string]any{
		"age": 31,
	})
	if err != nil {
		t.Fatalf("UpdateDocument error: %v", err)
	}
	if updated.Version != 2 {
		t.Errorf("expected version 2, got %d", updated.Version)
	}

	// Duplicate insert should fail
	_, err = store.InsertDocument("users", map[string]any{
		"id":   "user1",
		"name": "Duplicate",
	})
	if err == nil {
		t.Error("expected duplicate error")
	}

	// Delete document
	deleted := store.DeleteDocument("users", "user1")
	if !deleted {
		t.Error("expected delete to succeed")
	}
}

func TestMultiModelStore_FindDocuments(t *testing.T) {
	db := &DB{}
	store := NewMultiModelStore(db, DefaultMultiModelConfig())

	// Insert test documents
	store.InsertDocument("products", map[string]any{
		"id":       "p1",
		"name":     "Widget",
		"price":    9.99,
		"category": "electronics",
	})
	store.InsertDocument("products", map[string]any{
		"id":       "p2",
		"name":     "Gadget",
		"price":    19.99,
		"category": "electronics",
	})
	store.InsertDocument("products", map[string]any{
		"id":       "p3",
		"name":     "Thing",
		"price":    5.99,
		"category": "misc",
	})

	// Find by category
	results, err := store.FindDocuments("products", DocumentQuery{
		Filters: map[string]any{
			"category": "electronics",
		},
	})
	if err != nil {
		t.Fatalf("FindDocuments error: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// Find with $gt operator
	results, err = store.FindDocuments("products", DocumentQuery{
		Filters: map[string]any{
			"price": map[string]any{"$gt": 10.0},
		},
	})
	if err != nil {
		t.Fatalf("FindDocuments error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}

	// Test sorting
	results, err = store.FindDocuments("products", DocumentQuery{
		SortField: "price",
		SortDesc:  true,
	})
	if err != nil {
		t.Fatalf("FindDocuments error: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestMultiModelStore_Logs(t *testing.T) {
	db := &DB{}
	store := NewMultiModelStore(db, DefaultMultiModelConfig())
	store.Start()
	defer store.Stop()

	// Append logs
	err := store.AppendLog("app", &MultiModelLogEntry{
		Level:   MultiModelLogLevelInfo,
		Message: "Application started",
		Tags:    map[string]string{"service": "api"},
	})
	if err != nil {
		t.Fatalf("AppendLog error: %v", err)
	}

	err = store.AppendLog("app", &MultiModelLogEntry{
		Level:   MultiModelLogLevelError,
		Message: "Connection failed",
		Tags:    map[string]string{"service": "db"},
	})
	if err != nil {
		t.Fatalf("AppendLog error: %v", err)
	}

	err = store.AppendLog("app", &MultiModelLogEntry{
		Level:   MultiModelLogLevelInfo,
		Message: "Request processed",
		Tags:    map[string]string{"service": "api"},
	})
	if err != nil {
		t.Fatalf("AppendLog error: %v", err)
	}

	// Query all logs
	logs, err := store.QueryLogs(MultiModelLogQuery{Stream: "app"})
	if err != nil {
		t.Fatalf("QueryLogs error: %v", err)
	}
	if len(logs) != 3 {
		t.Errorf("expected 3 logs, got %d", len(logs))
	}

	// Query by level
	errorLevel := MultiModelLogLevelError
	logs, err = store.QueryLogs(MultiModelLogQuery{
		Stream: "app",
		Level:  &errorLevel,
	})
	if err != nil {
		t.Fatalf("QueryLogs error: %v", err)
	}
	if len(logs) != 1 {
		t.Errorf("expected 1 error log, got %d", len(logs))
	}

	// Query by tag
	logs, err = store.QueryLogs(MultiModelLogQuery{
		Stream: "app",
		Tags:   map[string]string{"service": "api"},
	})
	if err != nil {
		t.Fatalf("QueryLogs error: %v", err)
	}
	if len(logs) != 2 {
		t.Errorf("expected 2 api logs, got %d", len(logs))
	}

	// Query by text
	logs, err = store.QueryLogs(MultiModelLogQuery{
		Stream:   "app",
		Contains: "started",
	})
	if err != nil {
		t.Fatalf("QueryLogs error: %v", err)
	}
	if len(logs) != 1 {
		t.Errorf("expected 1 log containing 'started', got %d", len(logs))
	}
}

func TestMultiModelStore_StreamStats(t *testing.T) {
	db := &DB{}
	store := NewMultiModelStore(db, DefaultMultiModelConfig())

	store.AppendLog("metrics", &MultiModelLogEntry{Level: MultiModelLogLevelInfo, Message: "test1"})
	store.AppendLog("metrics", &MultiModelLogEntry{Level: MultiModelLogLevelError, Message: "test2"})
	store.AppendLog("metrics", &MultiModelLogEntry{Level: MultiModelLogLevelInfo, Message: "test3"})

	stats, ok := store.GetStreamStats("metrics")
	if !ok {
		t.Fatal("expected stream stats")
	}

	if stats.Count != 3 {
		t.Errorf("expected count 3, got %d", stats.Count)
	}
	if stats.LevelCounts[MultiModelLogLevelInfo] != 2 {
		t.Errorf("expected 2 info logs, got %d", stats.LevelCounts[MultiModelLogLevelInfo])
	}
	if stats.LevelCounts[MultiModelLogLevelError] != 1 {
		t.Errorf("expected 1 error log, got %d", stats.LevelCounts[MultiModelLogLevelError])
	}
}

func TestMultiModelStore_Collections(t *testing.T) {
	db := &DB{}
	store := NewMultiModelStore(db, DefaultMultiModelConfig())

	store.InsertDocument("users", map[string]any{"id": "1"})
	store.InsertDocument("products", map[string]any{"id": "1"})
	store.InsertDocument("orders", map[string]any{"id": "1"})

	collections := store.ListCollections()
	if len(collections) != 3 {
		t.Errorf("expected 3 collections, got %d", len(collections))
	}

	count := store.CountDocuments("users")
	if count != 1 {
		t.Errorf("expected 1 document, got %d", count)
	}
}

func TestMultiModelStore_GetStats(t *testing.T) {
	db := &DB{}
	store := NewMultiModelStore(db, DefaultMultiModelConfig())

	store.Set("key1", "value1")
	store.Set("key2", "value2")
	store.InsertDocument("users", map[string]any{"id": "1"})
	store.AppendLog("app", &MultiModelLogEntry{Message: "test"})

	stats := store.GetStats()

	if stats.KVCount != 2 {
		t.Errorf("expected 2 KV entries, got %d", stats.KVCount)
	}
	if stats.CollectionCount != 1 {
		t.Errorf("expected 1 collection, got %d", stats.CollectionCount)
	}
	if stats.TotalDocuments != 1 {
		t.Errorf("expected 1 document, got %d", stats.TotalDocuments)
	}
	if stats.StreamCount != 1 {
		t.Errorf("expected 1 stream, got %d", stats.StreamCount)
	}
	if stats.TotalLogEntries != 1 {
		t.Errorf("expected 1 log entry, got %d", stats.TotalLogEntries)
	}
}

func TestMultiModelLogLevel_String(t *testing.T) {
	tests := []struct {
		level MultiModelLogLevel
		want  string
	}{
		{MultiModelLogLevelDebug, "DEBUG"},
		{MultiModelLogLevelInfo, "INFO"},
		{MultiModelLogLevelWarn, "WARN"},
		{MultiModelLogLevelError, "ERROR"},
		{MultiModelLogLevelFatal, "FATAL"},
	}

	for _, tt := range tests {
		if got := tt.level.String(); got != tt.want {
			t.Errorf("LogLevel.String() = %v, want %v", got, tt.want)
		}
	}
}

func TestDefaultMultiModelConfig(t *testing.T) {
	config := DefaultMultiModelConfig()

	if !config.Enabled {
		t.Error("expected Enabled to be true")
	}
	if config.MaxDocumentSize != 1024*1024 {
		t.Errorf("expected MaxDocumentSize 1MB, got %d", config.MaxDocumentSize)
	}
}
