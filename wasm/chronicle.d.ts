/**
 * @chronicle-db/wasm - Chronicle Time-Series Database for WebAssembly
 *
 * Provides an in-browser time-series database with write, query,
 * and streaming capabilities.
 */

/** Configuration for opening a Chronicle database. */
export interface ChronicleConfig {
  /** Maximum memory budget in bytes (default: 64MB). */
  maxMemory?: number;
  /** Partition duration in nanoseconds (default: 1 hour). */
  partitionDuration?: number;
  /** Write buffer size in points (default: 10000). */
  bufferSize?: number;
}

/** A single time-series data point. */
export interface Point {
  /** Metric name (e.g., "cpu_usage"). */
  metric: string;
  /** Float64 value. */
  value: number;
  /** Tags as key-value pairs. */
  tags?: Record<string, string>;
  /** Unix timestamp in nanoseconds. If omitted, uses current time. */
  timestamp?: number;
}

/** Query definition for retrieving data. */
export interface Query {
  /** Metric name to query. */
  metric: string;
  /** Tag filters (exact match). */
  tags?: Record<string, string>;
  /** Start time as Unix nanoseconds. */
  start?: number;
  /** End time as Unix nanoseconds. */
  end?: number;
  /** Aggregation function: "sum" | "avg" | "min" | "max" | "count". */
  aggregation?: string;
  /** Aggregation window in nanoseconds. */
  window?: number;
  /** Maximum number of points to return. */
  limit?: number;
}

/** Result of a query execution. */
export interface QueryResult {
  /** Matched data points. */
  points: ResultPoint[];
}

/** A single point in query results. */
export interface ResultPoint {
  metric: string;
  value: number;
  timestamp: number;
  tags: Record<string, string>;
}

/** Database statistics. */
export interface Stats {
  pointsWritten: number;
  queriesExecuted: number;
  partitionCount: number;
  memoryUsedBytes: number;
}

/**
 * Chronicle WASM Database.
 *
 * @example
 * ```typescript
 * const db = await Chronicle.open("data.db");
 * await db.write({ metric: "temperature", value: 22.5, tags: { room: "kitchen" } });
 * const result = await db.query({ metric: "temperature", start: Date.now() * 1e6 - 3.6e12 });
 * db.close();
 * ```
 */
export declare class Chronicle {
  /** Open or create a database at the given path. */
  static open(path: string, config?: ChronicleConfig): Promise<Chronicle>;

  /** Write a single data point. */
  write(point: Point): Promise<void>;

  /** Write multiple data points in a batch. */
  writeBatch(points: Point[]): Promise<void>;

  /** Execute a query and return matching points. */
  query(query: Query): Promise<QueryResult>;

  /** Execute a SQL-like query string. */
  execute(sql: string): Promise<QueryResult>;

  /** Get database statistics. */
  stats(): Stats;

  /** Close the database and release resources. */
  close(): void;
}

export default Chronicle;
