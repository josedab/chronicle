/**
 * Chronicle Time-Series Database - Node.js Bindings
 *
 * This module provides Node.js bindings for the Chronicle embedded time-series database.
 * It uses ffi-napi to interface with the Chronicle shared library.
 *
 * Installation:
 *   npm install ffi-napi ref-napi ref-struct-napi
 *
 * Example usage:
 *   const { Chronicle, Point, Config } = require('./chronicle');
 *
 *   // Open database
 *   const db = new Chronicle('sensors.db');
 *
 *   // Write data
 *   db.write({
 *     metric: 'temperature',
 *     tags: { room: 'living' },
 *     value: 22.5
 *   });
 *
 *   // Query data
 *   const results = db.query('temperature');
 *   results.forEach(point => {
 *     console.log(`${point.metric}: ${point.value}`);
 *   });
 *
 *   // Close database
 *   db.close();
 */

const ffi = require('ffi-napi');
const ref = require('ref-napi');
const Struct = require('ref-struct-napi');
const path = require('path');
const fs = require('fs');

// Error codes
const CHRONICLE_OK = 0;
const CHRONICLE_ERR_INVALID_ARG = 1;
const CHRONICLE_ERR_NOT_FOUND = 2;
const CHRONICLE_ERR_IO = 3;
const CHRONICLE_ERR_QUERY = 4;
const CHRONICLE_ERR_CLOSED = 5;
const CHRONICLE_ERR_INTERNAL = 100;

// Time constants (nanoseconds)
const TIME = {
  NANOSECOND: 1n,
  MICROSECOND: 1000n,
  MILLISECOND: 1000000n,
  SECOND: 1000000000n,
  MINUTE: 60000000000n,
  HOUR: 3600000000000n,
  DAY: 86400000000000n,
};

// C struct definitions matching cffi.go
const ChronicleConfig = Struct({
  path: ref.types.CString,
  retention_hours: ref.types.int64,
  partition_duration_hours: ref.types.int64,
  max_series: ref.types.int32,
  enable_compression: ref.types.bool,
  enable_wal: ref.types.bool,
});

const ChroniclePoint = Struct({
  metric: ref.types.CString,
  value: ref.types.double,
  timestamp: ref.types.int64,
  tags_json: ref.types.CString,
});

const ChronicleQuery = Struct({
  metric: ref.types.CString,
  start: ref.types.int64,
  end: ref.types.int64,
  limit: ref.types.int32,
  aggregation: ref.types.CString,
  step: ref.types.int64,
});

const ChronicleResult = Struct({
  points: ref.refType(ChroniclePoint),
  count: ref.types.int32,
  error: ref.types.CString,
});

const ChronicleConfigPtr = ref.refType(ChronicleConfig);
const ChroniclePointPtr = ref.refType(ChroniclePoint);
const ChronicleQueryPtr = ref.refType(ChronicleQuery);
const ChronicleResultPtr = ref.refType(ChronicleResult);

/**
 * Find the Chronicle shared library
 */
function findLibrary() {
  let libName;
  switch (process.platform) {
    case 'darwin':
      libName = 'libchronicle.dylib';
      break;
    case 'win32':
      libName = 'chronicle.dll';
      break;
    default:
      libName = 'libchronicle.so';
  }

  const searchPaths = [
    path.join(__dirname, libName),
    path.join(__dirname, '..', libName),
    path.join(__dirname, '..', '..', libName),
    path.join(process.cwd(), libName),
    `/usr/local/lib/${libName}`,
    `/usr/lib/${libName}`,
  ];

  const ldPath = process.env.LD_LIBRARY_PATH || '';
  ldPath.split(':').forEach((p) => {
    if (p) searchPaths.push(path.join(p, libName));
  });

  for (const searchPath of searchPaths) {
    if (fs.existsSync(searchPath)) {
      return searchPath;
    }
  }

  throw new Error(
    `Could not find ${libName}. Build it with: go build -buildmode=c-shared -o ${libName}`
  );
}

// Load the library
let lib = null;

function loadLibrary(libPath) {
  if (lib) return lib;

  const libraryPath = libPath || findLibrary();

  lib = ffi.Library(libraryPath, {
    chronicle_version: [ref.types.CString, []],
    chronicle_open: [ref.types.void_ptr, [ChronicleConfigPtr]],
    chronicle_close: [ref.types.int, [ref.types.void_ptr]],
    chronicle_write: [ref.types.int, [ref.types.void_ptr, ChroniclePointPtr]],
    chronicle_write_batch: [
      ref.types.int,
      [ref.types.void_ptr, ChroniclePointPtr, ref.types.int32],
    ],
    chronicle_query: [ChronicleResultPtr, [ref.types.void_ptr, ChronicleQueryPtr]],
    chronicle_result_free: [ref.types.void, [ChronicleResultPtr]],
    chronicle_flush: [ref.types.int, [ref.types.void_ptr]],
    chronicle_metrics_count: [ref.types.int32, [ref.types.void_ptr]],
    chronicle_metrics_list: [
      ref.types.int32,
      [ref.types.void_ptr, ref.refType(ref.types.CString), ref.types.int32],
    ],
    chronicle_string_free: [ref.types.void, [ref.types.CString]],
    chronicle_stats: [ref.types.CString, [ref.types.void_ptr]],
    chronicle_execute_sql: [ref.types.CString, [ref.types.void_ptr, ref.types.CString]],
  });

  return lib;
}

/**
 * Chronicle database error
 */
class ChronicleError extends Error {
  constructor(code, message) {
    super(`Chronicle error ${code}: ${message}`);
    this.code = code;
    this.name = 'ChronicleError';
  }
}

/**
 * Point data class
 */
class Point {
  constructor({ metric, value, tags = {}, timestamp = 0 }) {
    this.metric = metric;
    this.value = value;
    this.tags = tags;
    this.timestamp = typeof timestamp === 'bigint' ? timestamp : BigInt(timestamp);
  }

  static now(metric, value, tags = {}) {
    const timestamp = BigInt(Date.now()) * TIME.MILLISECOND;
    return new Point({ metric, value, tags, timestamp });
  }

  static fromDate(metric, value, date, tags = {}) {
    const timestamp = BigInt(date.getTime()) * TIME.MILLISECOND;
    return new Point({ metric, value, tags, timestamp });
  }

  toDate() {
    return new Date(Number(this.timestamp / TIME.MILLISECOND));
  }
}

/**
 * Database configuration
 */
class Config {
  constructor(options = {}) {
    this.path = options.path || '';
    this.retentionHours = options.retentionHours || 0;
    this.partitionDurationHours = options.partitionDurationHours || 1;
    this.maxSeries = options.maxSeries || 0;
    this.enableCompression = options.enableCompression !== false;
    this.enableWal = options.enableWal !== false;
  }

  toC() {
    const cfg = new ChronicleConfig();
    cfg.path = this.path;
    cfg.retention_hours = this.retentionHours;
    cfg.partition_duration_hours = this.partitionDurationHours;
    cfg.max_series = this.maxSeries;
    cfg.enable_compression = this.enableCompression;
    cfg.enable_wal = this.enableWal;
    return cfg;
  }
}

/**
 * Chronicle database client
 */
class Chronicle {
  constructor(pathOrConfig, options = {}) {
    loadLibrary(options.libPath);
    this._handle = null;

    let config;
    if (typeof pathOrConfig === 'string') {
      config = new Config({ path: pathOrConfig });
    } else if (pathOrConfig instanceof Config) {
      config = pathOrConfig;
    } else {
      config = new Config(pathOrConfig);
    }

    const cfg = config.toC();
    this._handle = lib.chronicle_open(cfg.ref());

    if (!this._handle || this._handle.isNull()) {
      throw new ChronicleError(CHRONICLE_ERR_IO, `Failed to open database at ${config.path}`);
    }
  }

  _checkError(code, operation) {
    if (code !== CHRONICLE_OK) {
      const messages = {
        [CHRONICLE_ERR_INVALID_ARG]: 'Invalid argument',
        [CHRONICLE_ERR_NOT_FOUND]: 'Not found',
        [CHRONICLE_ERR_IO]: 'I/O error',
        [CHRONICLE_ERR_QUERY]: 'Query error',
        [CHRONICLE_ERR_CLOSED]: 'Database is closed',
        [CHRONICLE_ERR_INTERNAL]: 'Internal error',
      };
      throw new ChronicleError(code, `${operation}: ${messages[code] || 'Unknown error'}`);
    }
  }

  _checkOpen() {
    if (!this._handle) {
      throw new ChronicleError(CHRONICLE_ERR_CLOSED, 'Database is closed');
    }
  }

  close() {
    if (this._handle) {
      const code = lib.chronicle_close(this._handle);
      this._handle = null;
      this._checkError(code, 'close');
    }
  }

  write(point) {
    this._checkOpen();

    if (!(point instanceof Point)) {
      point = new Point(point);
    }

    const cPoint = new ChroniclePoint();
    cPoint.metric = point.metric;
    cPoint.value = point.value;
    cPoint.timestamp = Number(point.timestamp);
    cPoint.tags_json = Object.keys(point.tags).length > 0 ? JSON.stringify(point.tags) : null;

    const code = lib.chronicle_write(this._handle, cPoint.ref());
    this._checkError(code, 'write');
  }

  writeBatch(points) {
    this._checkOpen();

    if (!points || points.length === 0) return;

    const cPoints = Buffer.alloc(ChroniclePoint.size * points.length);

    points.forEach((p, i) => {
      if (!(p instanceof Point)) {
        p = new Point(p);
      }

      const cPoint = new ChroniclePoint();
      cPoint.metric = p.metric;
      cPoint.value = p.value;
      cPoint.timestamp = Number(p.timestamp);
      cPoint.tags_json = Object.keys(p.tags).length > 0 ? JSON.stringify(p.tags) : null;

      cPoint.ref().copy(cPoints, i * ChroniclePoint.size);
    });

    const code = lib.chronicle_write_batch(this._handle, cPoints, points.length);
    this._checkError(code, 'write_batch');
  }

  flush() {
    this._checkOpen();
    const code = lib.chronicle_flush(this._handle);
    this._checkError(code, 'flush');
  }

  query(metric, options = {}) {
    this._checkOpen();

    const { start = 0, end = 0, aggregation = null, step = 0, limit = 0 } = options;

    const startNs = this._toNanos(start);
    const endNs = this._toNanos(end);

    const cQuery = new ChronicleQuery();
    cQuery.metric = metric;
    cQuery.start = startNs;
    cQuery.end = endNs;
    cQuery.limit = limit;
    cQuery.aggregation = aggregation;
    cQuery.step = typeof step === 'bigint' ? Number(step) : step;

    const resultPtr = lib.chronicle_query(this._handle, cQuery.ref());
    if (!resultPtr || resultPtr.isNull()) {
      return [];
    }

    try {
      const result = resultPtr.deref();

      if (result.error) {
        throw new ChronicleError(CHRONICLE_ERR_QUERY, result.error);
      }

      const points = [];
      if (result.count > 0 && result.points) {
        for (let i = 0; i < result.count; i++) {
          const cp = result.points[i];
          let tags = {};
          if (cp.tags_json) {
            try {
              tags = JSON.parse(cp.tags_json);
            } catch (e) {
              // Ignore parse errors
            }
          }

          points.push(
            new Point({
              metric: cp.metric || '',
              value: cp.value,
              timestamp: BigInt(cp.timestamp),
              tags,
            })
          );
        }
      }

      return points;
    } finally {
      lib.chronicle_result_free(resultPtr);
    }
  }

  querySql(sql) {
    this._checkOpen();

    const resultJson = lib.chronicle_execute_sql(this._handle, sql);
    if (!resultJson) {
      return [];
    }

    try {
      const data = JSON.parse(resultJson);

      if (data.error) {
        throw new ChronicleError(CHRONICLE_ERR_QUERY, data.error);
      }

      if (data.Points) {
        return data.Points.map(
          (p) =>
            new Point({
              metric: p.Metric || '',
              value: p.Value || 0,
              timestamp: BigInt(p.Timestamp || 0),
              tags: p.Tags || {},
            })
        );
      }

      return [];
    } finally {
      lib.chronicle_string_free(resultJson);
    }
  }

  metrics() {
    this._checkOpen();

    const count = lib.chronicle_metrics_count(this._handle);
    if (count <= 0) {
      return [];
    }

    const metricsArray = Buffer.alloc(ref.sizeof.pointer * count);
    const actualCount = lib.chronicle_metrics_list(this._handle, metricsArray, count);

    const result = [];
    for (let i = 0; i < actualCount; i++) {
      const strPtr = metricsArray.readPointer(i * ref.sizeof.pointer);
      if (!strPtr.isNull()) {
        result.push(strPtr.readCString());
        lib.chronicle_string_free(strPtr);
      }
    }

    return result;
  }

  stats() {
    this._checkOpen();

    const statsJson = lib.chronicle_stats(this._handle);
    if (!statsJson) {
      return {};
    }

    try {
      return JSON.parse(statsJson);
    } finally {
      lib.chronicle_string_free(statsJson);
    }
  }

  _toNanos(t) {
    if (t instanceof Date) {
      return BigInt(t.getTime()) * TIME.MILLISECOND;
    }
    if (typeof t === 'bigint') {
      return Number(t);
    }
    return t;
  }

  static version() {
    loadLibrary();
    return lib.chronicle_version();
  }
}

module.exports = {
  Chronicle,
  Point,
  Config,
  ChronicleError,
  TIME,
  CHRONICLE_OK,
  CHRONICLE_ERR_INVALID_ARG,
  CHRONICLE_ERR_NOT_FOUND,
  CHRONICLE_ERR_IO,
  CHRONICLE_ERR_QUERY,
  CHRONICLE_ERR_CLOSED,
  CHRONICLE_ERR_INTERNAL,
};
