package chronicle

import (
	"errors"
	"fmt"
)

// Common sentinel errors for the chronicle package.
var (
	// ErrClosed is returned when operations are attempted on a closed database.
	ErrClosed = errors.New("database is closed")

	// ErrQueryTimeout is returned when a query exceeds the configured timeout.
	ErrQueryTimeout = errors.New("query timeout")

	// ErrMemoryBudgetExceeded is returned when a query exceeds memory limits.
	ErrMemoryBudgetExceeded = errors.New("query memory budget exceeded")

	// ErrInvalidQuery is returned for malformed queries.
	ErrInvalidQuery = errors.New("invalid query")

	// ErrSchemaValidation is returned when a point fails schema validation.
	ErrSchemaValidation = errors.New("schema validation failed")

	// ErrCardinalityLimit is returned when cardinality limits are exceeded.
	ErrCardinalityLimit = errors.New("cardinality limit exceeded")

	// ErrStorageCorruption is returned when data corruption is detected.
	ErrStorageCorruption = errors.New("storage corruption detected")

	// ErrWALSync is returned when WAL sync operations fail.
	ErrWALSync = errors.New("WAL sync failed")
)

// QueryErrorType categorizes query errors.
type QueryErrorType int

const (
	// QueryErrorTypeUnknown is an unclassified error.
	QueryErrorTypeUnknown QueryErrorType = iota
	// QueryErrorTypeTimeout indicates the query exceeded time limits.
	QueryErrorTypeTimeout
	// QueryErrorTypeMemory indicates the query exceeded memory limits.
	QueryErrorTypeMemory
	// QueryErrorTypeInvalid indicates the query is malformed.
	QueryErrorTypeInvalid
	// QueryErrorTypeCanceled indicates the query was canceled via context.
	QueryErrorTypeCanceled
)

// QueryError provides detailed information about query execution failures.
type QueryError struct {
	Type    QueryErrorType
	Message string
	Query   *Query
	Cause   error
}

func (e *QueryError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *QueryError) Unwrap() error {
	return e.Cause
}

// Is implements error matching for QueryError.
func (e *QueryError) Is(target error) bool {
	switch e.Type {
	case QueryErrorTypeTimeout:
		return target == ErrQueryTimeout
	case QueryErrorTypeMemory:
		return target == ErrMemoryBudgetExceeded
	case QueryErrorTypeInvalid:
		return target == ErrInvalidQuery
	}
	return false
}

// newQueryError creates a new QueryError.
func newQueryError(errType QueryErrorType, message string, query *Query, cause error) *QueryError {
	return &QueryError{
		Type:    errType,
		Message: message,
		Query:   query,
		Cause:   cause,
	}
}

// StorageErrorType categorizes storage errors.
type StorageErrorType int

const (
	// StorageErrorTypeUnknown is an unclassified storage error.
	StorageErrorTypeUnknown StorageErrorType = iota
	// StorageErrorTypeRead indicates a read failure.
	StorageErrorTypeRead
	// StorageErrorTypeWrite indicates a write failure.
	StorageErrorTypeWrite
	// StorageErrorTypeCorruption indicates data corruption.
	StorageErrorTypeCorruption
	// StorageErrorTypeSync indicates a sync/flush failure.
	StorageErrorTypeSync
)

// StorageError provides detailed information about storage failures.
type StorageError struct {
	Type    StorageErrorType
	Message string
	Path    string
	Cause   error
}

func (e *StorageError) Error() string {
	if e.Path != "" {
		if e.Cause != nil {
			return fmt.Sprintf("%s [%s]: %v", e.Message, e.Path, e.Cause)
		}
		return fmt.Sprintf("%s [%s]", e.Message, e.Path)
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *StorageError) Unwrap() error {
	return e.Cause
}

// Is implements error matching for StorageError.
func (e *StorageError) Is(target error) bool {
	switch e.Type {
	case StorageErrorTypeCorruption:
		return target == ErrStorageCorruption
	case StorageErrorTypeSync:
		return target == ErrWALSync
	}
	return false
}

// newStorageError creates a new StorageError.
func newStorageError(errType StorageErrorType, message, path string, cause error) *StorageError {
	return &StorageError{
		Type:    errType,
		Message: message,
		Path:    path,
		Cause:   cause,
	}
}

// WALSyncError accumulates WAL sync errors for monitoring.
type WALSyncError struct {
	FlushErr error
	SyncErr  error
}

func (e *WALSyncError) Error() string {
	if e.FlushErr != nil && e.SyncErr != nil {
		return fmt.Sprintf("WAL sync failed: flush=%v, sync=%v", e.FlushErr, e.SyncErr)
	}
	if e.FlushErr != nil {
		return fmt.Sprintf("WAL flush failed: %v", e.FlushErr)
	}
	if e.SyncErr != nil {
		return fmt.Sprintf("WAL sync failed: %v", e.SyncErr)
	}
	return "WAL sync failed"
}

func (e *WALSyncError) Unwrap() error {
	if e.FlushErr != nil {
		return e.FlushErr
	}
	return e.SyncErr
}

// Is implements error matching for WALSyncError.
func (e *WALSyncError) Is(target error) bool {
	return target == ErrWALSync
}
