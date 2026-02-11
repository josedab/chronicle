package chronicle

/*
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

// ChronicleDB opaque handle
typedef uintptr_t chronicle_db_t;

// Configuration for opening a database
typedef struct {
    const char* path;
    int64_t retention_hours;
    int64_t partition_duration_hours;
    int32_t max_series;
    bool enable_compression;
    bool enable_wal;
} chronicle_config_t;

// Time-series point
typedef struct {
    const char* metric;
    double value;
    int64_t timestamp;
    const char* tags_json;  // JSON-encoded tags
} chronicle_point_t;

// Query parameters
typedef struct {
    const char* metric;
    int64_t start;
    int64_t end;
    int32_t limit;
    const char* aggregation;  // "avg", "sum", "min", "max", "count"
    int64_t step;
} chronicle_query_t;

// Query result
typedef struct {
    chronicle_point_t* points;
    int32_t count;
    char* error;
} chronicle_result_t;

// Error codes
typedef enum {
    CHRONICLE_OK = 0,
    CHRONICLE_ERR_INVALID_ARG = 1,
    CHRONICLE_ERR_NOT_FOUND = 2,
    CHRONICLE_ERR_IO = 3,
    CHRONICLE_ERR_QUERY = 4,
    CHRONICLE_ERR_CLOSED = 5,
    CHRONICLE_ERR_INTERNAL = 100
} chronicle_error_t;

*/
import "C"

import (
	"encoding/json"
	"runtime/cgo"
	"sync"
	"time"
	"unsafe"
)

// FFI handle registry
var (
	ffiHandles   = make(map[cgo.Handle]*DB)
	ffiHandlesMu sync.RWMutex
	ffiNextID    cgo.Handle = 1
)

// registerHandle registers a DB with FFI and returns a handle.
func registerHandle(db *DB) cgo.Handle {
	ffiHandlesMu.Lock()
	defer ffiHandlesMu.Unlock()

	handle := ffiNextID
	ffiNextID++
	ffiHandles[handle] = db
	return handle
}

// getHandle retrieves a DB from a handle.
func getHandle(handle cgo.Handle) (*DB, bool) {
	ffiHandlesMu.RLock()
	defer ffiHandlesMu.RUnlock()

	db, ok := ffiHandles[handle]
	return db, ok
}

// removeHandle removes a handle from the registry.
func removeHandle(handle cgo.Handle) {
	ffiHandlesMu.Lock()
	defer ffiHandlesMu.Unlock()

	delete(ffiHandles, handle)
}

//export chronicle_version
func chronicle_version() *C.char {
	return C.CString("0.5.0")
}

//export chronicle_open
func chronicle_open(config *C.chronicle_config_t) C.chronicle_db_t {
	if config == nil {
		return 0
	}

	path := C.GoString(config.path)
	if path == "" {
		return 0
	}

	cfg := DefaultConfig(path)

	if config.retention_hours > 0 {
		cfg.Retention.RetentionDuration = time.Duration(config.retention_hours) * time.Hour
	}
	if config.partition_duration_hours > 0 {
		cfg.Storage.PartitionDuration = time.Duration(config.partition_duration_hours) * time.Hour
	}

	db, err := Open(path, cfg)
	if err != nil {
		return 0
	}

	handle := registerHandle(db)
	return C.chronicle_db_t(handle)
}

//export chronicle_close
func chronicle_close(db C.chronicle_db_t) C.chronicle_error_t {
	if db == 0 {
		return C.CHRONICLE_ERR_INVALID_ARG
	}

	handle := cgo.Handle(db)
	dbPtr, ok := getHandle(handle)
	if !ok {
		return C.CHRONICLE_ERR_NOT_FOUND
	}

	removeHandle(handle)
	if err := dbPtr.Close(); err != nil {
		return C.CHRONICLE_ERR_IO
	}

	return C.CHRONICLE_OK
}

//export chronicle_write
func chronicle_write(db C.chronicle_db_t, point *C.chronicle_point_t) C.chronicle_error_t {
	if db == 0 || point == nil {
		return C.CHRONICLE_ERR_INVALID_ARG
	}

	handle := cgo.Handle(db)
	dbPtr, ok := getHandle(handle)
	if !ok {
		return C.CHRONICLE_ERR_NOT_FOUND
	}

	p := Point{
		Metric:    C.GoString(point.metric),
		Value:     float64(point.value),
		Timestamp: int64(point.timestamp),
	}

	if point.tags_json != nil {
		tagsJSON := C.GoString(point.tags_json)
		if tagsJSON != "" {
			if err := json.Unmarshal([]byte(tagsJSON), &p.Tags); err != nil {
				return C.CHRONICLE_ERR_INVALID_ARG
			}
		}
	}

	if err := dbPtr.Write(p); err != nil {
		return C.CHRONICLE_ERR_IO
	}

	return C.CHRONICLE_OK
}

//export chronicle_write_batch
func chronicle_write_batch(db C.chronicle_db_t, points *C.chronicle_point_t, count C.int32_t) C.chronicle_error_t {
	if db == 0 || points == nil || count <= 0 {
		return C.CHRONICLE_ERR_INVALID_ARG
	}

	handle := cgo.Handle(db)
	dbPtr, ok := getHandle(handle)
	if !ok {
		return C.CHRONICLE_ERR_NOT_FOUND
	}

	// Convert C array to Go slice
	pointsSlice := unsafe.Slice(points, int(count))

	for _, cp := range pointsSlice {
		p := Point{
			Metric:    C.GoString(cp.metric),
			Value:     float64(cp.value),
			Timestamp: int64(cp.timestamp),
		}

		if cp.tags_json != nil {
			tagsJSON := C.GoString(cp.tags_json)
			if tagsJSON != "" {
				if err := json.Unmarshal([]byte(tagsJSON), &p.Tags); err != nil {
					continue // Skip invalid points
				}
			}
		}

		if err := dbPtr.Write(p); err != nil {
			return C.CHRONICLE_ERR_IO
		}
	}

	return C.CHRONICLE_OK
}

//export chronicle_query
func chronicle_query(db C.chronicle_db_t, query *C.chronicle_query_t) *C.chronicle_result_t {
	result := (*C.chronicle_result_t)(C.malloc(C.size_t(unsafe.Sizeof(C.chronicle_result_t{}))))
	result.points = nil
	result.count = 0
	result.error = nil

	if db == 0 || query == nil {
		result.error = C.CString("invalid arguments")
		return result
	}

	handle := cgo.Handle(db)
	dbPtr, ok := getHandle(handle)
	if !ok {
		result.error = C.CString("database not found")
		return result
	}

	q := &Query{
		Metric: C.GoString(query.metric),
		Start:  int64(query.start),
		End:    int64(query.end),
	}

	if query.limit > 0 {
		q.Limit = int(query.limit)
	}

	if query.aggregation != nil {
		agg := C.GoString(query.aggregation)
		if agg != "" {
			aggFn := stringToAggFunc(agg)
			if aggFn != AggNone {
				window := time.Duration(query.step) * time.Nanosecond
				if window <= 0 {
					window = time.Minute
				}
				q.Aggregation = &Aggregation{
					Function: aggFn,
					Window:   window,
				}
			}
		}
	}

	queryResult, err := dbPtr.Execute(q)
	if err != nil {
		result.error = C.CString(err.Error())
		return result
	}

	if len(queryResult.Points) == 0 {
		return result
	}

	// Allocate C array for points
	result.count = C.int32_t(len(queryResult.Points))
	result.points = (*C.chronicle_point_t)(C.malloc(C.size_t(len(queryResult.Points)) * C.size_t(unsafe.Sizeof(C.chronicle_point_t{}))))

	pointsSlice := unsafe.Slice(result.points, len(queryResult.Points))

	for i, p := range queryResult.Points {
		pointsSlice[i].metric = C.CString(p.Metric)
		pointsSlice[i].value = C.double(p.Value)
		pointsSlice[i].timestamp = C.int64_t(p.Timestamp)

		if len(p.Tags) > 0 {
			tagsJSON, _ := json.Marshal(p.Tags)
			pointsSlice[i].tags_json = C.CString(string(tagsJSON))
		} else {
			pointsSlice[i].tags_json = nil
		}
	}

	return result
}

//export chronicle_result_free
func chronicle_result_free(result *C.chronicle_result_t) {
	if result == nil {
		return
	}

	if result.points != nil {
		pointsSlice := unsafe.Slice(result.points, int(result.count))
		for _, p := range pointsSlice {
			if p.metric != nil {
				C.free(unsafe.Pointer(p.metric))
			}
			if p.tags_json != nil {
				C.free(unsafe.Pointer(p.tags_json))
			}
		}
		C.free(unsafe.Pointer(result.points))
	}

	if result.error != nil {
		C.free(unsafe.Pointer(result.error))
	}

	C.free(unsafe.Pointer(result))
}

//export chronicle_flush
func chronicle_flush(db C.chronicle_db_t) C.chronicle_error_t {
	if db == 0 {
		return C.CHRONICLE_ERR_INVALID_ARG
	}

	handle := cgo.Handle(db)
	dbPtr, ok := getHandle(handle)
	if !ok {
		return C.CHRONICLE_ERR_NOT_FOUND
	}

	if err := dbPtr.Flush(); err != nil {
		return C.CHRONICLE_ERR_IO
	}

	return C.CHRONICLE_OK
}

//export chronicle_metrics_count
func chronicle_metrics_count(db C.chronicle_db_t) C.int32_t {
	if db == 0 {
		return -1
	}

	handle := cgo.Handle(db)
	dbPtr, ok := getHandle(handle)
	if !ok {
		return -1
	}

	return C.int32_t(len(dbPtr.Metrics()))
}

//export chronicle_metrics_list
func chronicle_metrics_list(db C.chronicle_db_t, out **C.char, maxCount C.int32_t) C.int32_t {
	if db == 0 || out == nil || maxCount <= 0 {
		return -1
	}

	handle := cgo.Handle(db)
	dbPtr, ok := getHandle(handle)
	if !ok {
		return -1
	}

	metrics := dbPtr.Metrics()
	count := len(metrics)
	if count > int(maxCount) {
		count = int(maxCount)
	}

	outSlice := unsafe.Slice(out, count)
	for i := 0; i < count; i++ {
		outSlice[i] = C.CString(metrics[i])
	}

	return C.int32_t(count)
}

//export chronicle_string_free
func chronicle_string_free(s *C.char) {
	if s != nil {
		C.free(unsafe.Pointer(s))
	}
}

//export chronicle_stats
func chronicle_stats(db C.chronicle_db_t) *C.char {
	if db == 0 {
		return nil
	}

	handle := cgo.Handle(db)
	dbPtr, ok := getHandle(handle)
	if !ok {
		return nil
	}

	// Build stats from available methods
	metrics := dbPtr.Metrics()
	stats := map[string]interface{}{
		"metric_count": len(metrics),
	}
	statsJSON, err := json.Marshal(stats)
	if err != nil {
		return nil
	}

	return C.CString(string(statsJSON))
}

//export chronicle_execute_sql
func chronicle_execute_sql(db C.chronicle_db_t, sqlStr *C.char) *C.char {
	if db == 0 || sqlStr == nil {
		return C.CString(`{"error": "invalid arguments"}`)
	}

	handle := cgo.Handle(db)
	dbPtr, ok := getHandle(handle)
	if !ok {
		return C.CString(`{"error": "database not found"}`)
	}

	sqlQuery := C.GoString(sqlStr)

	parser := &QueryParser{}
	query, err := parser.Parse(sqlQuery)
	if err != nil {
		errJSON, _ := json.Marshal(map[string]string{"error": err.Error()})
		return C.CString(string(errJSON))
	}

	result, err := dbPtr.Execute(query)
	if err != nil {
		errJSON, _ := json.Marshal(map[string]string{"error": err.Error()})
		return C.CString(string(errJSON))
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		errJSON, _ := json.Marshal(map[string]string{"error": err.Error()})
		return C.CString(string(errJSON))
	}

	return C.CString(string(resultJSON))
}

//export chronicle_delete_metric
func chronicle_delete_metric(db C.chronicle_db_t, metric *C.char) C.chronicle_error_t {
	// Note: DeleteMetric not implemented in current Chronicle version
	return C.CHRONICLE_ERR_INTERNAL
}

//export chronicle_compact
func chronicle_compact(db C.chronicle_db_t) C.chronicle_error_t {
	// Note: Compact is an internal method in current Chronicle version
	return C.CHRONICLE_ERR_INTERNAL
}

// stringToAggFunc converts a string aggregation name to AggFunc.
func stringToAggFunc(s string) AggFunc {
	switch s {
	case "avg", "mean":
		return AggMean
	case "sum":
		return AggSum
	case "min":
		return AggMin
	case "max":
		return AggMax
	case "count":
		return AggCount
	case "first":
		return AggFirst
	case "last":
		return AggLast
	case "stddev":
		return AggStddev
	case "rate":
		return AggRate
	default:
		return AggNone
	}
}

// GenerateCHeader generates the C header file content for the FFI.
func GenerateCHeader() string {
	return `/*
 * Chronicle Time-Series Database C/FFI Interface
 * Auto-generated header file
 * 
 * Usage:
 *   1. Build Chronicle as a shared library: go build -buildmode=c-shared -o libchronicle.so
 *   2. Include this header in your C/C++/Rust code
 *   3. Link against libchronicle.so
 */

#ifndef CHRONICLE_H
#define CHRONICLE_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque database handle */
typedef uintptr_t chronicle_db_t;

/* Configuration for opening a database */
typedef struct {
    const char* path;
    int64_t retention_hours;
    int64_t partition_duration_hours;
    int32_t max_series;
    bool enable_compression;
    bool enable_wal;
} chronicle_config_t;

/* Time-series data point */
typedef struct {
    const char* metric;
    double value;
    int64_t timestamp;
    const char* tags_json;  /* JSON-encoded tags, e.g., {"host":"server1"} */
} chronicle_point_t;

/* Query parameters */
typedef struct {
    const char* metric;
    int64_t start;
    int64_t end;
    int32_t limit;
    const char* aggregation;  /* "avg", "sum", "min", "max", "count" */
    int64_t step;            /* Aggregation step in nanoseconds */
} chronicle_query_t;

/* Query result */
typedef struct {
    chronicle_point_t* points;
    int32_t count;
    char* error;
} chronicle_result_t;

/* Error codes */
typedef enum {
    CHRONICLE_OK = 0,
    CHRONICLE_ERR_INVALID_ARG = 1,
    CHRONICLE_ERR_NOT_FOUND = 2,
    CHRONICLE_ERR_IO = 3,
    CHRONICLE_ERR_QUERY = 4,
    CHRONICLE_ERR_CLOSED = 5,
    CHRONICLE_ERR_INTERNAL = 100
} chronicle_error_t;

/*
 * Get the Chronicle library version.
 * Returns: Version string (caller must free with chronicle_string_free)
 */
char* chronicle_version(void);

/*
 * Open a Chronicle database.
 * 
 * @param config Database configuration
 * @return Database handle, or NULL on error
 */
chronicle_db_t chronicle_open(chronicle_config_t* config);

/*
 * Close a Chronicle database.
 * 
 * @param db Database handle
 * @return CHRONICLE_OK on success
 */
chronicle_error_t chronicle_close(chronicle_db_t db);

/*
 * Write a single data point.
 * 
 * @param db Database handle
 * @param point Data point to write
 * @return CHRONICLE_OK on success
 */
chronicle_error_t chronicle_write(chronicle_db_t db, chronicle_point_t* point);

/*
 * Write a batch of data points.
 * 
 * @param db Database handle
 * @param points Array of data points
 * @param count Number of points
 * @return CHRONICLE_OK on success
 */
chronicle_error_t chronicle_write_batch(chronicle_db_t db, chronicle_point_t* points, int32_t count);

/*
 * Execute a query.
 * 
 * @param db Database handle
 * @param query Query parameters
 * @return Query result (caller must free with chronicle_result_free)
 */
chronicle_result_t* chronicle_query(chronicle_db_t db, chronicle_query_t* query);

/*
 * Free a query result.
 * 
 * @param result Result to free
 */
void chronicle_result_free(chronicle_result_t* result);

/*
 * Flush all pending writes to disk.
 * 
 * @param db Database handle
 * @return CHRONICLE_OK on success
 */
chronicle_error_t chronicle_flush(chronicle_db_t db);

/*
 * Get the number of metrics in the database.
 * 
 * @param db Database handle
 * @return Number of metrics, or -1 on error
 */
int32_t chronicle_metrics_count(chronicle_db_t db);

/*
 * List metric names.
 * 
 * @param db Database handle
 * @param out Output array (caller provides, must free each string)
 * @param max_count Maximum number of metrics to return
 * @return Number of metrics returned, or -1 on error
 */
int32_t chronicle_metrics_list(chronicle_db_t db, char** out, int32_t max_count);

/*
 * Free a string returned by Chronicle.
 * 
 * @param s String to free
 */
void chronicle_string_free(char* s);

/*
 * Get database statistics as JSON.
 * 
 * @param db Database handle
 * @return JSON string (caller must free with chronicle_string_free)
 */
char* chronicle_stats(chronicle_db_t db);

/*
 * Execute a SQL query.
 * 
 * @param db Database handle
 * @param sql SQL query string
 * @return JSON result (caller must free with chronicle_string_free)
 */
char* chronicle_execute_sql(chronicle_db_t db, const char* sql);

/*
 * Delete a metric and all its data.
 * 
 * @param db Database handle
 * @param metric Metric name
 * @return CHRONICLE_OK on success
 */
chronicle_error_t chronicle_delete_metric(chronicle_db_t db, const char* metric);

/*
 * Compact the database to reclaim space.
 * 
 * @param db Database handle
 * @return CHRONICLE_OK on success
 */
chronicle_error_t chronicle_compact(chronicle_db_t db);

#ifdef __cplusplus
}
#endif

#endif /* CHRONICLE_H */
`
}

// FFIConfig contains configuration for FFI operations.
type FFIConfig struct {
	// EnableThreadSafety enables mutex protection for all operations
	EnableThreadSafety bool

	// MaxHandles limits the number of concurrent database handles
	MaxHandles int
}

// DefaultFFIConfig returns default FFI configuration.
func DefaultFFIConfig() FFIConfig {
	return FFIConfig{
		EnableThreadSafety: true,
		MaxHandles:         100,
	}
}
