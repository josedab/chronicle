#ifndef CHRONICLE_CFFI_H
#define CHRONICLE_CFFI_H

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

#endif /* CHRONICLE_CFFI_H */
