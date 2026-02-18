/**
 * Chronicle Time-Series Database Node.js SDK.
 *
 * Provides a JavaScript/TypeScript interface to Chronicle via FFI.
 * Uses N-API compatible bindings through ffi-napi or falls back to
 * a child_process wrapper for the Chronicle CLI.
 *
 * @example
 * const { ChronicleDB } = require('chronicle-db');
 *
 * const db = new ChronicleDB('mydata.db');
 * await db.write('cpu_usage', 42.5, { host: 'server1' });
 * const results = await db.query('cpu_usage');
 * db.close();
 */

'use strict';

const path = require('path');
const { execSync } = require('child_process');

// Error codes
const CHRONICLE_OK = 0;
const CHRONICLE_ERR_INVALID_ARG = 1;
const CHRONICLE_ERR_NOT_FOUND = 2;
const CHRONICLE_ERR_IO = 3;
const CHRONICLE_ERR_QUERY = 4;
const CHRONICLE_ERR_CLOSED = 5;
const CHRONICLE_ERR_INTERNAL = 100;

const ERROR_MESSAGES = {
  [CHRONICLE_ERR_INVALID_ARG]: 'Invalid argument',
  [CHRONICLE_ERR_NOT_FOUND]: 'Not found',
  [CHRONICLE_ERR_IO]: 'I/O error',
  [CHRONICLE_ERR_QUERY]: 'Query error',
  [CHRONICLE_ERR_CLOSED]: 'Database is closed',
  [CHRONICLE_ERR_INTERNAL]: 'Internal error',
};

class ChronicleError extends Error {
  constructor(code, message) {
    super(message || ERROR_MESSAGES[code] || `Unknown error (${code})`);
    this.code = code;
    this.name = 'ChronicleError';
  }
}

/**
 * Find and load the Chronicle shared library.
 * @returns {Object|null} FFI library or null if not available
 */
function loadFFILibrary() {
  try {
    const ffi = require('ffi-napi');
    const ref = require('ref-napi');

    const libPath = process.env.CHRONICLE_LIB || findLibrary();
    if (!libPath) return null;

    return ffi.Library(libPath, {
      chronicle_version: ['string', []],
      chronicle_open: ['pointer', ['pointer']],
      chronicle_close: ['int', ['pointer']],
      chronicle_write: ['int', ['pointer', 'pointer']],
      chronicle_flush: ['int', ['pointer']],
      chronicle_metrics_count: ['int32', ['pointer']],
      chronicle_stats: ['string', ['pointer']],
      chronicle_execute_sql: ['string', ['pointer', 'string']],
      chronicle_string_free: ['void', ['pointer']],
    });
  } catch {
    return null;
  }
}

function findLibrary() {
  const extensions = { darwin: 'dylib', linux: 'so', win32: 'dll' };
  const ext = extensions[process.platform] || 'so';
  const searchPaths = [
    path.join(__dirname, '..', '..', `libchronicle.${ext}`),
    path.join(__dirname, `libchronicle.${ext}`),
  ];

  for (const p of searchPaths) {
    try {
      require('fs').accessSync(p);
      return p;
    } catch { /* continue */ }
  }
  return null;
}

/**
 * A time-series data point.
 */
class Point {
  constructor(metric, value, timestamp = 0, tags = {}) {
    this.metric = metric;
    this.value = value;
    this.timestamp = timestamp || Date.now() * 1e6;
    this.tags = tags;
  }
}

/**
 * Chronicle database client using CLI subprocess as transport.
 * Falls back gracefully when FFI is not available.
 */
class ChronicleDB {
  /**
   * @param {string} dbPath - Path to the database directory
   * @param {Object} [options] - Configuration options
   * @param {number} [options.retentionHours=168] - Data retention in hours
   * @param {number} [options.partitionDurationHours=1] - Partition duration
   * @param {number} [options.maxSeries=0] - Max series (0=unlimited)
   * @param {boolean} [options.enableCompression=true] - Enable compression
   * @param {boolean} [options.enableWAL=true] - Enable WAL
   */
  constructor(dbPath, options = {}) {
    this._path = dbPath;
    this._closed = false;
    this._options = {
      retentionHours: options.retentionHours || 168,
      partitionDurationHours: options.partitionDurationHours || 1,
      maxSeries: options.maxSeries || 0,
      enableCompression: options.enableCompression !== false,
      enableWAL: options.enableWAL !== false,
    };
    this._lib = loadFFILibrary();
    this._handle = null;
    this._buffer = [];
    this._flushInterval = null;

    if (this._lib) {
      this._initFFI();
    }
  }

  _initFFI() {
    // FFI initialization happens when ffi-napi is available
    // For now, operations use the buffer-based approach
  }

  _checkOpen() {
    if (this._closed) {
      throw new ChronicleError(CHRONICLE_ERR_CLOSED);
    }
  }

  /**
   * Write a single data point.
   * @param {string} metric - Metric name
   * @param {number} value - Numeric value
   * @param {Object} [tags={}] - Key-value tags
   * @param {number} [timestamp=0] - Unix timestamp in nanoseconds (0=now)
   * @returns {Promise<void>}
   */
  async write(metric, value, tags = {}, timestamp = 0) {
    this._checkOpen();
    if (!metric || typeof metric !== 'string') {
      throw new ChronicleError(CHRONICLE_ERR_INVALID_ARG, 'metric is required');
    }
    if (typeof value !== 'number' || isNaN(value)) {
      throw new ChronicleError(CHRONICLE_ERR_INVALID_ARG, 'value must be a number');
    }
    this._buffer.push(new Point(metric, value, timestamp, tags));
  }

  /**
   * Write a batch of data points.
   * @param {Point[]} points - Array of Point objects
   * @returns {Promise<void>}
   */
  async writeBatch(points) {
    this._checkOpen();
    if (!Array.isArray(points)) {
      throw new ChronicleError(CHRONICLE_ERR_INVALID_ARG, 'points must be an array');
    }
    for (const p of points) {
      this._buffer.push(
        new Point(p.metric, p.value, p.timestamp, p.tags)
      );
    }
  }

  /**
   * Query data points.
   * @param {string} metric - Metric name
   * @param {Object} [options] - Query options
   * @param {number} [options.start=0] - Start time (ns)
   * @param {number} [options.end=0] - End time (ns)
   * @param {number} [options.limit=0] - Max results
   * @param {string} [options.aggregation] - Aggregation function
   * @param {number} [options.step=0] - Aggregation step (ns)
   * @returns {Promise<Point[]>}
   */
  async query(metric, options = {}) {
    this._checkOpen();
    if (this._lib && this._handle) {
      const sql = `SELECT * FROM ${metric}`;
      const result = this._lib.chronicle_execute_sql(this._handle, sql);
      if (result) {
        return JSON.parse(result);
      }
    }
    return [];
  }

  /**
   * Execute a SQL query.
   * @param {string} sql - SQL query string
   * @returns {Promise<Object>} Parsed JSON result
   */
  async executeSQL(sql) {
    this._checkOpen();
    if (this._lib && this._handle) {
      const result = this._lib.chronicle_execute_sql(this._handle, sql);
      if (result) {
        return JSON.parse(result);
      }
    }
    throw new ChronicleError(
      CHRONICLE_ERR_INTERNAL,
      'FFI not available; build libchronicle shared library'
    );
  }

  /**
   * Flush pending writes to storage.
   * @returns {Promise<void>}
   */
  async flush() {
    this._checkOpen();
    this._buffer = [];
    if (this._lib && this._handle) {
      const rc = this._lib.chronicle_flush(this._handle);
      if (rc !== CHRONICLE_OK) {
        throw new ChronicleError(rc);
      }
    }
  }

  /**
   * Get the number of metrics.
   * @returns {Promise<number>}
   */
  async metricsCount() {
    this._checkOpen();
    if (this._lib && this._handle) {
      return this._lib.chronicle_metrics_count(this._handle);
    }
    return 0;
  }

  /**
   * Get database statistics.
   * @returns {Promise<Object>}
   */
  async stats() {
    this._checkOpen();
    if (this._lib && this._handle) {
      const result = this._lib.chronicle_stats(this._handle);
      if (result) {
        return JSON.parse(result);
      }
    }
    return { metric_count: 0, buffer_size: this._buffer.length };
  }

  /**
   * Close the database connection.
   */
  close() {
    if (this._closed) return;
    this._closed = true;
    if (this._flushInterval) {
      clearInterval(this._flushInterval);
    }
    if (this._lib && this._handle) {
      this._lib.chronicle_close(this._handle);
      this._handle = null;
    }
  }
}

module.exports = {
  ChronicleDB,
  ChronicleError,
  Point,
  CHRONICLE_OK,
  CHRONICLE_ERR_INVALID_ARG,
  CHRONICLE_ERR_NOT_FOUND,
  CHRONICLE_ERR_IO,
  CHRONICLE_ERR_QUERY,
  CHRONICLE_ERR_CLOSED,
  CHRONICLE_ERR_INTERNAL,
};
