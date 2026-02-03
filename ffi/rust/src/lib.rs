//! Chronicle Time-Series Database - Rust Bindings
//!
//! This crate provides Rust bindings for the Chronicle embedded time-series database.
//!
//! # Example
//!
//! ```no_run
//! use chronicle::{Chronicle, Point, Config};
//! use std::collections::HashMap;
//!
//! fn main() -> Result<(), chronicle::Error> {
//!     // Open database with default config
//!     let db = Chronicle::open("sensors.db")?;
//!
//!     // Write data
//!     let mut tags = HashMap::new();
//!     tags.insert("room".to_string(), "living".to_string());
//!
//!     db.write(Point {
//!         metric: "temperature".to_string(),
//!         tags,
//!         value: 22.5,
//!         timestamp: 0, // Use current time
//!     })?;
//!
//!     // Query data
//!     let results = db.query("temperature", None, 0, 0, 0)?;
//!     for point in results {
//!         println!("{}: {}", point.metric, point.value);
//!     }
//!
//!     db.close()?;
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_double, c_int};
use std::path::Path;
use std::ptr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Error codes from the Chronicle library
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ErrorCode {
    Ok = 0,
    InvalidArg = 1,
    NotFound = 2,
    Io = 3,
    Query = 4,
    Closed = 5,
    Internal = 100,
}

impl From<c_int> for ErrorCode {
    fn from(code: c_int) -> Self {
        match code {
            0 => ErrorCode::Ok,
            1 => ErrorCode::InvalidArg,
            2 => ErrorCode::NotFound,
            3 => ErrorCode::Io,
            4 => ErrorCode::Query,
            5 => ErrorCode::Closed,
            _ => ErrorCode::Internal,
        }
    }
}

/// Chronicle error type
#[derive(Debug)]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Chronicle error {:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for Error {}

/// Result type for Chronicle operations
pub type Result<T> = std::result::Result<T, Error>;

/// A time-series data point
#[derive(Debug, Clone)]
pub struct Point {
    pub metric: String,
    pub tags: HashMap<String, String>,
    pub value: f64,
    pub timestamp: i64, // Nanoseconds since epoch; 0 = use current time
}

impl Point {
    /// Create a new point with the current timestamp
    pub fn now(metric: &str, value: f64, tags: HashMap<String, String>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        Self {
            metric: metric.to_string(),
            tags,
            value,
            timestamp,
        }
    }

    /// Create a point from a SystemTime
    pub fn from_time(metric: &str, value: f64, time: SystemTime, tags: HashMap<String, String>) -> Self {
        let timestamp = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        Self {
            metric: metric.to_string(),
            tags,
            value,
            timestamp,
        }
    }

    /// Convert timestamp to SystemTime
    pub fn to_time(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_nanos(self.timestamp as u64)
    }
}

/// Database configuration matching cffi.go structure
#[derive(Debug, Clone)]
pub struct Config {
    pub path: String,
    pub retention_hours: i64,
    pub partition_duration_hours: i64,
    pub max_series: i32,
    pub enable_compression: bool,
    pub enable_wal: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: String::new(),
            retention_hours: 0, // Keep forever
            partition_duration_hours: 1,
            max_series: 0, // Unlimited
            enable_compression: true,
            enable_wal: true,
        }
    }
}

impl Config {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            ..Default::default()
        }
    }

    pub fn with_retention_hours(mut self, hours: i64) -> Self {
        self.retention_hours = hours;
        self
    }

    pub fn with_partition_hours(mut self, hours: i64) -> Self {
        self.partition_duration_hours = hours;
        self
    }

    pub fn with_max_series(mut self, max: i32) -> Self {
        self.max_series = max;
        self
    }

    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.enable_compression = enabled;
        self
    }

    pub fn with_wal(mut self, enabled: bool) -> Self {
        self.enable_wal = enabled;
        self
    }
}

// FFI structures matching cffi.go
#[repr(C)]
struct CChronicleConfig {
    path: *const c_char,
    retention_hours: i64,
    partition_duration_hours: i64,
    max_series: i32,
    enable_compression: bool,
    enable_wal: bool,
}

#[repr(C)]
struct CChroniclePoint {
    metric: *const c_char,
    value: c_double,
    timestamp: i64,
    tags_json: *const c_char,
}

#[repr(C)]
struct CChronicleQuery {
    metric: *const c_char,
    start: i64,
    end: i64,
    limit: i32,
    aggregation: *const c_char,
    step: i64,
}

#[repr(C)]
struct CChronicleResult {
    points: *mut CChroniclePoint,
    count: i32,
    error: *mut c_char,
}

// FFI function declarations
#[link(name = "chronicle")]
extern "C" {
    fn chronicle_version() -> *mut c_char;
    fn chronicle_open(config: *const CChronicleConfig) -> *mut std::ffi::c_void;
    fn chronicle_close(db: *mut std::ffi::c_void) -> c_int;
    fn chronicle_write(db: *mut std::ffi::c_void, point: *const CChroniclePoint) -> c_int;
    fn chronicle_write_batch(db: *mut std::ffi::c_void, points: *const CChroniclePoint, count: i32) -> c_int;
    fn chronicle_query(db: *mut std::ffi::c_void, query: *const CChronicleQuery) -> *mut CChronicleResult;
    fn chronicle_result_free(result: *mut CChronicleResult);
    fn chronicle_flush(db: *mut std::ffi::c_void) -> c_int;
    fn chronicle_metrics_count(db: *mut std::ffi::c_void) -> i32;
    fn chronicle_metrics_list(db: *mut std::ffi::c_void, out: *mut *mut c_char, max_count: i32) -> i32;
    fn chronicle_string_free(s: *mut c_char);
    fn chronicle_stats(db: *mut std::ffi::c_void) -> *mut c_char;
    fn chronicle_execute_sql(db: *mut std::ffi::c_void, sql: *const c_char) -> *mut c_char;
}

fn check_error(code: c_int, operation: &str) -> Result<()> {
    if code == 0 {
        return Ok(());
    }

    let message = match ErrorCode::from(code) {
        ErrorCode::Ok => return Ok(()),
        ErrorCode::InvalidArg => "Invalid argument",
        ErrorCode::NotFound => "Not found",
        ErrorCode::Io => "I/O error",
        ErrorCode::Query => "Query error",
        ErrorCode::Closed => "Database is closed",
        ErrorCode::Internal => "Internal error",
    };

    Err(Error {
        code: ErrorCode::from(code),
        message: format!("{}: {}", operation, message),
    })
}

/// Chronicle database handle
pub struct Chronicle {
    handle: *mut std::ffi::c_void,
}

// Safety: Chronicle uses internal synchronization
unsafe impl Send for Chronicle {}
unsafe impl Sync for Chronicle {}

impl Chronicle {
    /// Open a database with default configuration
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config = Config::new(&path.as_ref().to_string_lossy());
        Self::open_with_config(config)
    }

    /// Open a database with custom configuration
    pub fn open_with_config(config: Config) -> Result<Self> {
        let c_path = CString::new(config.path.as_str()).map_err(|_| Error {
            code: ErrorCode::InvalidArg,
            message: "Invalid path".to_string(),
        })?;

        let c_config = CChronicleConfig {
            path: c_path.as_ptr(),
            retention_hours: config.retention_hours,
            partition_duration_hours: config.partition_duration_hours,
            max_series: config.max_series,
            enable_compression: config.enable_compression,
            enable_wal: config.enable_wal,
        };

        let handle = unsafe { chronicle_open(&c_config) };

        if handle.is_null() {
            return Err(Error {
                code: ErrorCode::Io,
                message: format!("Failed to open database at {}", config.path),
            });
        }

        Ok(Self { handle })
    }

    /// Close the database
    pub fn close(mut self) -> Result<()> {
        if !self.handle.is_null() {
            let code = unsafe { chronicle_close(self.handle) };
            self.handle = ptr::null_mut();
            check_error(code, "close")?;
        }
        Ok(())
    }

    fn check_open(&self) -> Result<()> {
        if self.handle.is_null() {
            return Err(Error {
                code: ErrorCode::Closed,
                message: "Database is closed".to_string(),
            });
        }
        Ok(())
    }

    /// Write a single point
    pub fn write(&self, point: Point) -> Result<()> {
        self.check_open()?;

        let c_metric = CString::new(point.metric.as_str()).map_err(|_| Error {
            code: ErrorCode::InvalidArg,
            message: "Invalid metric name".to_string(),
        })?;

        let tags_json = if point.tags.is_empty() {
            None
        } else {
            Some(CString::new(serde_json::to_string(&point.tags).unwrap_or_default()).unwrap())
        };

        let c_point = CChroniclePoint {
            metric: c_metric.as_ptr(),
            value: point.value,
            timestamp: point.timestamp,
            tags_json: tags_json.as_ref().map(|s| s.as_ptr()).unwrap_or(ptr::null()),
        };

        let code = unsafe { chronicle_write(self.handle, &c_point) };
        check_error(code, "write")
    }

    /// Write multiple points
    pub fn write_batch(&self, points: &[Point]) -> Result<()> {
        self.check_open()?;

        if points.is_empty() {
            return Ok(());
        }

        // Keep CStrings alive during the FFI call
        let mut c_strings: Vec<(CString, Option<CString>)> = Vec::with_capacity(points.len());
        let mut c_points: Vec<CChroniclePoint> = Vec::with_capacity(points.len());

        for point in points {
            let c_metric = CString::new(point.metric.as_str()).map_err(|_| Error {
                code: ErrorCode::InvalidArg,
                message: "Invalid metric name".to_string(),
            })?;

            let tags_json = if point.tags.is_empty() {
                None
            } else {
                Some(CString::new(serde_json::to_string(&point.tags).unwrap_or_default()).unwrap())
            };

            c_strings.push((c_metric, tags_json));
        }

        for (i, point) in points.iter().enumerate() {
            let (ref c_metric, ref tags_json) = c_strings[i];
            c_points.push(CChroniclePoint {
                metric: c_metric.as_ptr(),
                value: point.value,
                timestamp: point.timestamp,
                tags_json: tags_json.as_ref().map(|s| s.as_ptr()).unwrap_or(ptr::null()),
            });
        }

        let code = unsafe { chronicle_write_batch(self.handle, c_points.as_ptr(), points.len() as i32) };
        check_error(code, "write_batch")
    }

    /// Flush buffered writes to disk
    pub fn flush(&self) -> Result<()> {
        self.check_open()?;
        let code = unsafe { chronicle_flush(self.handle) };
        check_error(code, "flush")
    }

    /// Query data points
    pub fn query(
        &self,
        metric: &str,
        aggregation: Option<&str>,
        start_ns: i64,
        end_ns: i64,
        limit: i32,
    ) -> Result<Vec<Point>> {
        self.check_open()?;

        let c_metric = CString::new(metric).map_err(|_| Error {
            code: ErrorCode::InvalidArg,
            message: "Invalid metric name".to_string(),
        })?;

        let c_aggregation = aggregation.map(|a| CString::new(a).ok()).flatten();

        let c_query = CChronicleQuery {
            metric: c_metric.as_ptr(),
            start: start_ns,
            end: end_ns,
            limit,
            aggregation: c_aggregation.as_ref().map(|s| s.as_ptr()).unwrap_or(ptr::null()),
            step: 0,
        };

        let result_ptr = unsafe { chronicle_query(self.handle, &c_query) };

        if result_ptr.is_null() {
            return Ok(vec![]);
        }

        let result = unsafe { &*result_ptr };

        // Check for error
        if !result.error.is_null() {
            let error_msg = unsafe { CStr::from_ptr(result.error).to_string_lossy().to_string() };
            unsafe { chronicle_result_free(result_ptr) };
            return Err(Error {
                code: ErrorCode::Query,
                message: error_msg,
            });
        }

        let mut points = Vec::with_capacity(result.count as usize);

        if result.count > 0 && !result.points.is_null() {
            for i in 0..result.count {
                let cp = unsafe { &*result.points.offset(i as isize) };

                let metric = if !cp.metric.is_null() {
                    unsafe { CStr::from_ptr(cp.metric).to_string_lossy().to_string() }
                } else {
                    String::new()
                };

                let tags = if !cp.tags_json.is_null() {
                    let tags_str = unsafe { CStr::from_ptr(cp.tags_json).to_string_lossy() };
                    serde_json::from_str(&tags_str).unwrap_or_default()
                } else {
                    HashMap::new()
                };

                points.push(Point {
                    metric,
                    value: cp.value,
                    timestamp: cp.timestamp,
                    tags,
                });
            }
        }

        unsafe { chronicle_result_free(result_ptr) };

        Ok(points)
    }

    /// Execute SQL-like query
    pub fn query_sql(&self, sql: &str) -> Result<Vec<Point>> {
        self.check_open()?;

        let c_sql = CString::new(sql).map_err(|_| Error {
            code: ErrorCode::InvalidArg,
            message: "Invalid SQL".to_string(),
        })?;

        let result_ptr = unsafe { chronicle_execute_sql(self.handle, c_sql.as_ptr()) };

        if result_ptr.is_null() {
            return Ok(vec![]);
        }

        let result_str = unsafe {
            let s = CStr::from_ptr(result_ptr).to_string_lossy().to_string();
            chronicle_string_free(result_ptr);
            s
        };

        #[derive(serde::Deserialize)]
        struct JsonResult {
            #[serde(rename = "Points")]
            points: Option<Vec<JsonPoint>>,
            error: Option<String>,
        }

        #[derive(serde::Deserialize)]
        struct JsonPoint {
            #[serde(rename = "Metric")]
            metric: Option<String>,
            #[serde(rename = "Value")]
            value: Option<f64>,
            #[serde(rename = "Timestamp")]
            timestamp: Option<i64>,
            #[serde(rename = "Tags")]
            tags: Option<HashMap<String, String>>,
        }

        let json_result: JsonResult = serde_json::from_str(&result_str).map_err(|e| Error {
            code: ErrorCode::Internal,
            message: format!("Failed to parse result: {}", e),
        })?;

        if let Some(error) = json_result.error {
            return Err(Error {
                code: ErrorCode::Query,
                message: error,
            });
        }

        let points = json_result
            .points
            .unwrap_or_default()
            .into_iter()
            .map(|p| Point {
                metric: p.metric.unwrap_or_default(),
                value: p.value.unwrap_or(0.0),
                timestamp: p.timestamp.unwrap_or(0),
                tags: p.tags.unwrap_or_default(),
            })
            .collect();

        Ok(points)
    }

    /// Get all metric names
    pub fn metrics(&self) -> Result<Vec<String>> {
        self.check_open()?;

        let count = unsafe { chronicle_metrics_count(self.handle) };
        if count <= 0 {
            return Ok(vec![]);
        }

        let mut ptrs: Vec<*mut c_char> = vec![ptr::null_mut(); count as usize];
        let actual_count = unsafe {
            chronicle_metrics_list(self.handle, ptrs.as_mut_ptr(), count)
        };

        let mut result = Vec::with_capacity(actual_count as usize);
        for i in 0..actual_count as usize {
            if !ptrs[i].is_null() {
                let s = unsafe { CStr::from_ptr(ptrs[i]).to_string_lossy().to_string() };
                unsafe { chronicle_string_free(ptrs[i]) };
                result.push(s);
            }
        }

        Ok(result)
    }

    /// Get database statistics
    pub fn stats(&self) -> Result<serde_json::Value> {
        self.check_open()?;

        let stats_ptr = unsafe { chronicle_stats(self.handle) };
        if stats_ptr.is_null() {
            return Ok(serde_json::json!({}));
        }

        let stats_str = unsafe {
            let s = CStr::from_ptr(stats_ptr).to_string_lossy().to_string();
            chronicle_string_free(stats_ptr);
            s
        };

        serde_json::from_str(&stats_str).map_err(|e| Error {
            code: ErrorCode::Internal,
            message: format!("Failed to parse stats: {}", e),
        })
    }

    /// Get Chronicle library version
    pub fn version() -> String {
        let ptr = unsafe { chronicle_version() };
        if ptr.is_null() {
            return "unknown".to_string();
        }
        unsafe {
            let s = CStr::from_ptr(ptr).to_string_lossy().to_string();
            chronicle_string_free(ptr);
            s
        }
    }
}

impl Drop for Chronicle {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { chronicle_close(self.handle) };
            self.handle = ptr::null_mut();
        }
    }
}

// Time duration constants
pub mod time {
    pub const NANOSECOND: i64 = 1;
    pub const MICROSECOND: i64 = 1_000;
    pub const MILLISECOND: i64 = 1_000_000;
    pub const SECOND: i64 = 1_000_000_000;
    pub const MINUTE: i64 = 60 * SECOND;
    pub const HOUR: i64 = 60 * MINUTE;
    pub const DAY: i64 = 24 * HOUR;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_creation() {
        let mut tags = HashMap::new();
        tags.insert("room".to_string(), "living".to_string());

        let point = Point::now("temperature", 22.5, tags);
        assert_eq!(point.metric, "temperature");
        assert_eq!(point.value, 22.5);
        assert!(point.timestamp > 0);
    }

    #[test]
    fn test_config_builder() {
        let config = Config::new("/tmp/test.db")
            .with_retention_hours(168) // 1 week
            .with_partition_hours(1)
            .with_max_series(10000)
            .with_compression(true)
            .with_wal(true);

        assert_eq!(config.path, "/tmp/test.db");
        assert_eq!(config.retention_hours, 168);
        assert_eq!(config.partition_duration_hours, 1);
        assert_eq!(config.max_series, 10000);
        assert!(config.enable_compression);
        assert!(config.enable_wal);
    }

    #[test]
    fn test_error_codes() {
        assert_eq!(ErrorCode::from(0), ErrorCode::Ok);
        assert_eq!(ErrorCode::from(1), ErrorCode::InvalidArg);
        assert_eq!(ErrorCode::from(2), ErrorCode::NotFound);
        assert_eq!(ErrorCode::from(3), ErrorCode::Io);
        assert_eq!(ErrorCode::from(4), ErrorCode::Query);
        assert_eq!(ErrorCode::from(5), ErrorCode::Closed);
        assert_eq!(ErrorCode::from(100), ErrorCode::Internal);
    }
}
