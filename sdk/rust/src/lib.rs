//! Chronicle Time-Series Database Rust FFI Bindings.
//!
//! Provides safe Rust wrappers around the Chronicle C FFI.
//!
//! # Example
//! ```no_run
//! use chronicle::ChronicleDB;
//!
//! let db = ChronicleDB::open("test.db", Default::default()).unwrap();
//! db.write("cpu_usage", 42.5, &[("host", "server1")]).unwrap();
//! let results = db.query("cpu_usage", 0, 0, None).unwrap();
//! db.close().unwrap();
//! ```

#![allow(non_camel_case_types)]

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_double, c_int, c_void};
use std::ptr;

// --- Raw FFI bindings (generated from chronicle.h) ---

pub type chronicle_db_t = *mut c_void;

#[repr(C)]
pub struct chronicle_config_t {
    pub path: *const c_char,
    pub retention_hours: i64,
    pub partition_duration_hours: i64,
    pub max_series: i32,
    pub enable_compression: bool,
    pub enable_wal: bool,
}

#[repr(C)]
pub struct chronicle_point_t {
    pub metric: *const c_char,
    pub value: c_double,
    pub timestamp: i64,
    pub tags_json: *const c_char,
}

#[repr(C)]
pub struct chronicle_query_t {
    pub metric: *const c_char,
    pub start: i64,
    pub end: i64,
    pub limit: i32,
    pub aggregation: *const c_char,
    pub step: i64,
}

#[repr(C)]
pub struct chronicle_result_t {
    pub points: *mut chronicle_point_t,
    pub count: i32,
    pub error: *mut c_char,
}

pub const CHRONICLE_OK: c_int = 0;
pub const CHRONICLE_ERR_INVALID_ARG: c_int = 1;
pub const CHRONICLE_ERR_NOT_FOUND: c_int = 2;
pub const CHRONICLE_ERR_IO: c_int = 3;
pub const CHRONICLE_ERR_QUERY: c_int = 4;
pub const CHRONICLE_ERR_CLOSED: c_int = 5;
pub const CHRONICLE_ERR_INTERNAL: c_int = 100;

extern "C" {
    pub fn chronicle_version() -> *mut c_char;
    pub fn chronicle_open(config: *const chronicle_config_t) -> chronicle_db_t;
    pub fn chronicle_close(db: chronicle_db_t) -> c_int;
    pub fn chronicle_write(db: chronicle_db_t, point: *const chronicle_point_t) -> c_int;
    pub fn chronicle_write_batch(
        db: chronicle_db_t,
        points: *const chronicle_point_t,
        count: i32,
    ) -> c_int;
    pub fn chronicle_query(
        db: chronicle_db_t,
        query: *const chronicle_query_t,
    ) -> *mut chronicle_result_t;
    pub fn chronicle_result_free(result: *mut chronicle_result_t);
    pub fn chronicle_flush(db: chronicle_db_t) -> c_int;
    pub fn chronicle_metrics_count(db: chronicle_db_t) -> i32;
    pub fn chronicle_string_free(s: *mut c_char);
    pub fn chronicle_stats(db: chronicle_db_t) -> *mut c_char;
    pub fn chronicle_execute_sql(db: chronicle_db_t, sql: *const c_char) -> *mut c_char;
}

// --- Safe Rust wrapper types ---

/// Error type for Chronicle operations.
#[derive(Debug, Clone)]
pub struct ChronicleError {
    pub code: i32,
    pub message: String,
}

impl std::fmt::Display for ChronicleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chronicle error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for ChronicleError {}

impl ChronicleError {
    fn from_code(code: c_int) -> Self {
        let message = match code {
            CHRONICLE_ERR_INVALID_ARG => "invalid argument",
            CHRONICLE_ERR_NOT_FOUND => "not found",
            CHRONICLE_ERR_IO => "I/O error",
            CHRONICLE_ERR_QUERY => "query error",
            CHRONICLE_ERR_CLOSED => "database closed",
            CHRONICLE_ERR_INTERNAL => "internal error",
            _ => "unknown error",
        };
        ChronicleError {
            code,
            message: message.to_string(),
        }
    }
}

pub type Result<T> = std::result::Result<T, ChronicleError>;

/// A time-series data point.
#[derive(Debug, Clone)]
pub struct Point {
    pub metric: String,
    pub value: f64,
    pub timestamp: i64,
    pub tags: HashMap<String, String>,
}

/// Configuration for opening a Chronicle database.
#[derive(Debug, Clone)]
pub struct Config {
    pub retention_hours: i64,
    pub partition_duration_hours: i64,
    pub max_series: i32,
    pub enable_compression: bool,
    pub enable_wal: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            retention_hours: 168,
            partition_duration_hours: 1,
            max_series: 0,
            enable_compression: true,
            enable_wal: true,
        }
    }
}

/// Safe wrapper around a Chronicle database handle.
pub struct ChronicleDB {
    handle: chronicle_db_t,
    closed: bool,
}

// ChronicleDB is Send but not Sync (FFI handles are thread-safe via Go mutexes)
unsafe impl Send for ChronicleDB {}

impl ChronicleDB {
    /// Open a Chronicle database.
    pub fn open(path: &str, config: Config) -> Result<Self> {
        let c_path = CString::new(path).map_err(|_| ChronicleError {
            code: CHRONICLE_ERR_INVALID_ARG,
            message: "invalid path".to_string(),
        })?;

        let c_config = chronicle_config_t {
            path: c_path.as_ptr(),
            retention_hours: config.retention_hours,
            partition_duration_hours: config.partition_duration_hours,
            max_series: config.max_series,
            enable_compression: config.enable_compression,
            enable_wal: config.enable_wal,
        };

        let handle = unsafe { chronicle_open(&c_config) };
        if handle.is_null() {
            return Err(ChronicleError {
                code: CHRONICLE_ERR_IO,
                message: format!("failed to open database at {}", path),
            });
        }

        Ok(ChronicleDB {
            handle,
            closed: false,
        })
    }

    /// Close the database.
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        let rc = unsafe { chronicle_close(self.handle) };
        self.closed = true;
        if rc != CHRONICLE_OK {
            return Err(ChronicleError::from_code(rc));
        }
        Ok(())
    }

    /// Write a single data point.
    pub fn write(&self, metric: &str, value: f64, tags: &[(&str, &str)]) -> Result<()> {
        let c_metric = CString::new(metric).map_err(|_| ChronicleError {
            code: CHRONICLE_ERR_INVALID_ARG,
            message: "invalid metric name".to_string(),
        })?;

        let tags_map: HashMap<&str, &str> = tags.iter().cloned().collect();
        let tags_json = serde_json_minimal(&tags_map);
        let c_tags = CString::new(tags_json).unwrap();

        let point = chronicle_point_t {
            metric: c_metric.as_ptr(),
            value,
            timestamp: 0,
            tags_json: c_tags.as_ptr(),
        };

        let rc = unsafe { chronicle_write(self.handle, &point) };
        if rc != CHRONICLE_OK {
            return Err(ChronicleError::from_code(rc));
        }
        Ok(())
    }

    /// Write a batch of data points.
    pub fn write_batch(&self, points: &[Point]) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }

        let mut c_metrics: Vec<CString> = Vec::with_capacity(points.len());
        let mut c_tags: Vec<CString> = Vec::with_capacity(points.len());
        let mut c_points: Vec<chronicle_point_t> = Vec::with_capacity(points.len());

        for p in points {
            let m = CString::new(p.metric.as_str()).unwrap();
            let t = CString::new(serde_json_from_hashmap(&p.tags)).unwrap();
            c_metrics.push(m);
            c_tags.push(t);
        }

        for (i, p) in points.iter().enumerate() {
            c_points.push(chronicle_point_t {
                metric: c_metrics[i].as_ptr(),
                value: p.value,
                timestamp: p.timestamp,
                tags_json: c_tags[i].as_ptr(),
            });
        }

        let rc =
            unsafe { chronicle_write_batch(self.handle, c_points.as_ptr(), points.len() as i32) };
        if rc != CHRONICLE_OK {
            return Err(ChronicleError::from_code(rc));
        }
        Ok(())
    }

    /// Execute a query.
    pub fn query(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        aggregation: Option<&str>,
    ) -> Result<Vec<Point>> {
        let c_metric = CString::new(metric).unwrap();
        let c_agg = aggregation.map(|a| CString::new(a).unwrap());

        let query = chronicle_query_t {
            metric: c_metric.as_ptr(),
            start,
            end,
            limit: 0,
            aggregation: c_agg.as_ref().map_or(ptr::null(), |a| a.as_ptr()),
            step: 0,
        };

        let result = unsafe { chronicle_query(self.handle, &query) };
        if result.is_null() {
            return Err(ChronicleError {
                code: CHRONICLE_ERR_INTERNAL,
                message: "query returned null".to_string(),
            });
        }

        let res = unsafe { &*result };

        if !res.error.is_null() {
            let err_msg = unsafe { CStr::from_ptr(res.error) }
                .to_string_lossy()
                .to_string();
            unsafe { chronicle_result_free(result) };
            return Err(ChronicleError {
                code: CHRONICLE_ERR_QUERY,
                message: err_msg,
            });
        }

        let mut points = Vec::with_capacity(res.count as usize);
        for i in 0..res.count as isize {
            let cp = unsafe { &*res.points.offset(i) };
            let metric_str = if cp.metric.is_null() {
                metric.to_string()
            } else {
                unsafe { CStr::from_ptr(cp.metric) }
                    .to_string_lossy()
                    .to_string()
            };
            let tags = if cp.tags_json.is_null() {
                HashMap::new()
            } else {
                let json_str = unsafe { CStr::from_ptr(cp.tags_json) }
                    .to_string_lossy()
                    .to_string();
                parse_json_tags(&json_str)
            };
            points.push(Point {
                metric: metric_str,
                value: cp.value,
                timestamp: cp.timestamp,
                tags,
            });
        }

        unsafe { chronicle_result_free(result) };
        Ok(points)
    }

    /// Flush pending writes.
    pub fn flush(&self) -> Result<()> {
        let rc = unsafe { chronicle_flush(self.handle) };
        if rc != CHRONICLE_OK {
            return Err(ChronicleError::from_code(rc));
        }
        Ok(())
    }

    /// Get number of metrics.
    pub fn metrics_count(&self) -> Result<i32> {
        let count = unsafe { chronicle_metrics_count(self.handle) };
        if count < 0 {
            return Err(ChronicleError::from_code(CHRONICLE_ERR_INTERNAL));
        }
        Ok(count)
    }
}

impl Drop for ChronicleDB {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.close();
        }
    }
}

// Minimal JSON serialization without serde dependency
fn serde_json_minimal(tags: &HashMap<&str, &str>) -> String {
    if tags.is_empty() {
        return "{}".to_string();
    }
    let entries: Vec<String> = tags
        .iter()
        .map(|(k, v)| format!("\"{}\":\"{}\"", k, v))
        .collect();
    format!("{{{}}}", entries.join(","))
}

fn serde_json_from_hashmap(tags: &HashMap<String, String>) -> String {
    if tags.is_empty() {
        return "{}".to_string();
    }
    let entries: Vec<String> = tags
        .iter()
        .map(|(k, v)| format!("\"{}\":\"{}\"", k, v))
        .collect();
    format!("{{{}}}", entries.join(","))
}

fn parse_json_tags(json: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let trimmed = json.trim().trim_start_matches('{').trim_end_matches('}');
    if trimmed.is_empty() {
        return map;
    }
    for pair in trimmed.split(',') {
        let parts: Vec<&str> = pair.splitn(2, ':').collect();
        if parts.len() == 2 {
            let key = parts[0].trim().trim_matches('"').to_string();
            let val = parts[1].trim().trim_matches('"').to_string();
            map.insert(key, val);
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_minimal() {
        let mut tags = HashMap::new();
        tags.insert("host", "server1");
        let json = serde_json_minimal(&tags);
        assert!(json.contains("host"));
        assert!(json.contains("server1"));
    }

    #[test]
    fn test_parse_json_tags() {
        let tags = parse_json_tags(r#"{"host":"server1","region":"us"}"#);
        assert_eq!(tags.get("host").unwrap(), "server1");
        assert_eq!(tags.get("region").unwrap(), "us");
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.retention_hours, 168);
        assert!(config.enable_wal);
    }
}
