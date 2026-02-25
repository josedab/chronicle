package chronicle

/*
#include "chronicle_cffi.h"
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
