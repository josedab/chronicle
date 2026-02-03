/*
 * Chronicle Time-Series Database C/FFI Interface
 *
 * This header provides the C-compatible interface for embedding Chronicle
 * in applications written in C, Python, Node.js, Rust, Java, and other languages.
 *
 * Usage:
 *   1. Build the shared library: go build -buildmode=c-shared -o libchronicle.so
 *   2. Include this header in your C/C++ code
 *   3. Link against the generated shared library
 *
 * Example (C):
 *   #include "chronicle.h"
 *
 *   int main() {
 *       chronicle_config_t config = {
 *           .path = "test.db",
 *           .retention_hours = 24 * 7,
 *           .partition_duration_hours = 1,
 *           .max_series = 10000,
 *           .enable_compression = true,
 *           .enable_wal = true
 *       };
 *
 *       chronicle_db_t db = chronicle_open(&config);
 *       if (db == NULL) {
 *           printf("Failed to open database\n");
 *           return 1;
 *       }
 *
 *       chronicle_point_t point = {
 *           .metric = "temperature",
 *           .value = 22.5,
 *           .timestamp = time(NULL) * 1000000000LL,
 *           .tags_json = "{\"room\": \"living\"}"
 *       };
 *
 *       chronicle_write(db, &point);
 *       chronicle_close(db);
 *       return 0;
 *   }
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
typedef void* chronicle_db_t;

/* Configuration for opening a database */
typedef struct {
    const char* path;                    /* Database file path (required) */
    int64_t retention_hours;             /* Data retention in hours (0 = keep forever) */
    int64_t partition_duration_hours;    /* Partition duration in hours (default: 1) */
    int32_t max_series;                  /* Maximum number of series (0 = unlimited) */
    bool enable_compression;             /* Enable data compression */
    bool enable_wal;                     /* Enable write-ahead logging */
} chronicle_config_t;

/* Time-series data point */
typedef struct {
    const char* metric;      /* Metric name (required) */
    double value;            /* Numeric value */
    int64_t timestamp;       /* Unix timestamp in nanoseconds (0 = use current time) */
    const char* tags_json;   /* JSON-encoded tags, e.g., {"host":"server1"} (optional) */
} chronicle_point_t;

/* Query parameters */
typedef struct {
    const char* metric;      /* Metric name to query */
    int64_t start;           /* Start time in nanoseconds (0 = no lower bound) */
    int64_t end;             /* End time in nanoseconds (0 = no upper bound) */
    int32_t limit;           /* Maximum results (0 = unlimited) */
    const char* aggregation; /* Aggregation function: "avg", "sum", "min", "max", "count", "first", "last", "stddev", "rate" */
    int64_t step;            /* Aggregation window in nanoseconds */
} chronicle_query_t;

/* Query result */
typedef struct {
    chronicle_point_t* points;  /* Array of result points */
    int32_t count;              /* Number of points */
    char* error;                /* Error message (NULL on success) */
} chronicle_result_t;

/* Error codes */
typedef enum {
    CHRONICLE_OK = 0,               /* Success */
    CHRONICLE_ERR_INVALID_ARG = 1,  /* Invalid argument */
    CHRONICLE_ERR_NOT_FOUND = 2,    /* Database or metric not found */
    CHRONICLE_ERR_IO = 3,           /* I/O error */
    CHRONICLE_ERR_QUERY = 4,        /* Query error */
    CHRONICLE_ERR_CLOSED = 5,       /* Database is closed */
    CHRONICLE_ERR_INTERNAL = 100    /* Internal error */
} chronicle_error_t;

/* Aggregation function constants (for use with chronicle_query_agg) */
#define CHRONICLE_AGG_NONE 0
#define CHRONICLE_AGG_COUNT 1
#define CHRONICLE_AGG_SUM 2
#define CHRONICLE_AGG_MEAN 3
#define CHRONICLE_AGG_MIN 4
#define CHRONICLE_AGG_MAX 5
#define CHRONICLE_AGG_STDDEV 6
#define CHRONICLE_AGG_PERCENTILE 7
#define CHRONICLE_AGG_RATE 8
#define CHRONICLE_AGG_FIRST 9
#define CHRONICLE_AGG_LAST 10

/*
 * Get the Chronicle library version.
 *
 * @return Version string (caller must free with chronicle_string_free)
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
 * @param out Output array (caller provides, must free each string with chronicle_string_free)
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
 * Execute a SQL-like query.
 *
 * Supported syntax:
 *   SELECT <aggregation>(<field>) FROM <metric>
 *   WHERE <tag> = '<value>'
 *   GROUP BY time(<window>), <tag>
 *   LIMIT <n>
 *
 * @param db Database handle
 * @param sql SQL query string
 * @return JSON result (caller must free with chronicle_string_free)
 */
char* chronicle_execute_sql(chronicle_db_t db, const char* sql);

/*
 * Delete a metric and all its data.
 * Note: Not yet implemented.
 *
 * @param db Database handle
 * @param metric Metric name
 * @return CHRONICLE_OK on success
 */
chronicle_error_t chronicle_delete_metric(chronicle_db_t db, const char* metric);

/*
 * Compact the database to reclaim space.
 * Note: Compaction runs automatically; this forces immediate compaction.
 *
 * @param db Database handle
 * @return CHRONICLE_OK on success
 */
chronicle_error_t chronicle_compact(chronicle_db_t db);

/* Time duration constants (in nanoseconds) for convenience */
#define CHRONICLE_NANOSECOND  1LL
#define CHRONICLE_MICROSECOND 1000LL
#define CHRONICLE_MILLISECOND 1000000LL
#define CHRONICLE_SECOND      1000000000LL
#define CHRONICLE_MINUTE      (60LL * CHRONICLE_SECOND)
#define CHRONICLE_HOUR        (60LL * CHRONICLE_MINUTE)
#define CHRONICLE_DAY         (24LL * CHRONICLE_HOUR)

#ifdef __cplusplus
}
#endif

#endif /* CHRONICLE_H */
