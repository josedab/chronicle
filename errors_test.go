package chronicle

import (
	"context"
	"errors"
	"testing"
)

func TestQueryError(t *testing.T) {
	query := &Query{Metric: "test"}
	cause := errors.New("underlying cause")

	// Test timeout error
	err := newQueryError(QueryErrorTypeTimeout, "query timeout", query, cause)
	if err.Type != QueryErrorTypeTimeout {
		t.Errorf("expected timeout type, got %v", err.Type)
	}
	if err.Query != query {
		t.Error("expected query to be preserved")
	}
	if !errors.Is(err, ErrQueryTimeout) {
		t.Error("expected error to match ErrQueryTimeout")
	}
	if !errors.Is(err, cause) {
		t.Error("expected error to unwrap to cause")
	}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}

	// Test memory error
	memErr := newQueryError(QueryErrorTypeMemory, "memory exceeded", query, nil)
	if !errors.Is(memErr, ErrMemoryBudgetExceeded) {
		t.Error("expected error to match ErrMemoryBudgetExceeded")
	}

	// Test invalid error
	invalidErr := newQueryError(QueryErrorTypeInvalid, "invalid query", nil, nil)
	if !errors.Is(invalidErr, ErrInvalidQuery) {
		t.Error("expected error to match ErrInvalidQuery")
	}

	// Test unknown type doesn't match specific errors
	unknownErr := newQueryError(QueryErrorTypeUnknown, "unknown", nil, nil)
	if errors.Is(unknownErr, ErrQueryTimeout) {
		t.Error("unknown error should not match timeout")
	}
}

func TestStorageError(t *testing.T) {
	cause := errors.New("disk full")

	// Test corruption error
	err := newStorageError(StorageErrorTypeCorruption, "checksum mismatch", "/data/file", cause)
	if err.Type != StorageErrorTypeCorruption {
		t.Errorf("expected corruption type, got %v", err.Type)
	}
	if err.Path != "/data/file" {
		t.Error("expected path to be preserved")
	}
	if !errors.Is(err, ErrStorageCorruption) {
		t.Error("expected error to match ErrStorageCorruption")
	}
	if !errors.Is(err, cause) {
		t.Error("expected error to unwrap to cause")
	}

	// Test sync error
	syncErr := newStorageError(StorageErrorTypeSync, "sync failed", "", nil)
	if !errors.Is(syncErr, ErrWALSync) {
		t.Error("expected error to match ErrWALSync")
	}

	// Test error message with path
	errWithPath := newStorageError(StorageErrorTypeWrite, "write failed", "/path/to/file", nil)
	if errWithPath.Error() == "" {
		t.Error("expected non-empty error message")
	}

	// Test error message without path
	errWithoutPath := newStorageError(StorageErrorTypeRead, "read failed", "", nil)
	if errWithoutPath.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestWALSyncError(t *testing.T) {
	flushErr := errors.New("flush error")
	syncErr := errors.New("sync error")

	// Test with both errors
	err := &WALSyncError{FlushErr: flushErr, SyncErr: syncErr}
	if !errors.Is(err, ErrWALSync) {
		t.Error("expected error to match ErrWALSync")
	}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
	unwrapped := err.Unwrap()
	if unwrapped != flushErr {
		t.Error("expected to unwrap to flush error first")
	}

	// Test with only flush error
	err2 := &WALSyncError{FlushErr: flushErr}
	if err2.Error() == "" {
		t.Error("expected non-empty error message")
	}
	if err2.Unwrap() != flushErr {
		t.Error("expected to unwrap to flush error")
	}

	// Test with only sync error
	err3 := &WALSyncError{SyncErr: syncErr}
	if err3.Error() == "" {
		t.Error("expected non-empty error message")
	}
	if err3.Unwrap() != syncErr {
		t.Error("expected to unwrap to sync error")
	}

	// Test with no errors
	err4 := &WALSyncError{}
	if err4.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestQueryExecuteContext(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", DefaultConfig(dir+"/test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// Write some test data
	points := make([]Point, 10)
	for i := 0; i < 10; i++ {
		points[i] = Point{
			Metric:    "test",
			Value:     float64(i),
			Timestamp: int64(i * 1000),
		}
	}
	if err := db.WriteBatch(points); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	// Test normal execution
	result, err := db.ExecuteContext(context.Background(), &Query{Metric: "test"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(result.Points) == 0 {
		t.Error("expected some points")
	}

	// Test with nil query
	result, err = db.ExecuteContext(context.Background(), nil)
	if err != nil {
		t.Fatalf("execute nil query: %v", err)
	}
	if len(result.Points) != 0 {
		t.Error("expected empty result for nil query")
	}
}

func TestWriteError(t *testing.T) {
	cause := errors.New("disk full")
	err := NewWriteError("cpu.usage", cause)

	if !errors.Is(err, cause) {
		t.Error("WriteError should unwrap to its cause")
	}

	msg := err.Error()
	if msg != `write failed for metric "cpu.usage": disk full` {
		t.Errorf("unexpected message: %s", msg)
	}
}

func TestConfigError(t *testing.T) {
	err := NewConfigError("Storage.BufferSize", "must be positive")
	msg := err.Error()
	if msg != "invalid config Storage.BufferSize: must be positive" {
		t.Errorf("unexpected message: %s", msg)
	}
}
