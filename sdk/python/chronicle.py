"""
Chronicle Time-Series Database Python SDK.

Provides a Pythonic interface to the Chronicle database via ctypes FFI.
Supports pandas DataFrame integration for batch operations.

Usage:
    from chronicle import ChronicleDB

    db = ChronicleDB("mydata.db")
    db.write("cpu_usage", 42.5, tags={"host": "server1"})
    results = db.query("cpu_usage", start=0, end=0)
    db.close()
"""

import ctypes
import ctypes.util
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# Error codes matching chronicle_error_t
CHRONICLE_OK = 0
CHRONICLE_ERR_INVALID_ARG = 1
CHRONICLE_ERR_NOT_FOUND = 2
CHRONICLE_ERR_IO = 3
CHRONICLE_ERR_QUERY = 4
CHRONICLE_ERR_CLOSED = 5
CHRONICLE_ERR_INTERNAL = 100

_ERROR_MESSAGES = {
    CHRONICLE_ERR_INVALID_ARG: "Invalid argument",
    CHRONICLE_ERR_NOT_FOUND: "Not found",
    CHRONICLE_ERR_IO: "I/O error",
    CHRONICLE_ERR_QUERY: "Query error",
    CHRONICLE_ERR_CLOSED: "Database is closed",
    CHRONICLE_ERR_INTERNAL: "Internal error",
}


class ChronicleError(Exception):
    """Base exception for Chronicle errors."""

    def __init__(self, code: int, message: str = ""):
        self.code = code
        self.message = message or _ERROR_MESSAGES.get(code, f"Unknown error ({code})")
        super().__init__(self.message)


@dataclass
class Point:
    """A time-series data point."""

    metric: str
    value: float
    timestamp: int = 0
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class QueryResult:
    """Result of a query operation."""

    points: List[Point] = field(default_factory=list)
    error: Optional[str] = None


# C structure definitions
class CConfig(ctypes.Structure):
    _fields_ = [
        ("path", ctypes.c_char_p),
        ("retention_hours", ctypes.c_int64),
        ("partition_duration_hours", ctypes.c_int64),
        ("max_series", ctypes.c_int32),
        ("enable_compression", ctypes.c_bool),
        ("enable_wal", ctypes.c_bool),
    ]


class CPoint(ctypes.Structure):
    _fields_ = [
        ("metric", ctypes.c_char_p),
        ("value", ctypes.c_double),
        ("timestamp", ctypes.c_int64),
        ("tags_json", ctypes.c_char_p),
    ]


class CQuery(ctypes.Structure):
    _fields_ = [
        ("metric", ctypes.c_char_p),
        ("start", ctypes.c_int64),
        ("end", ctypes.c_int64),
        ("limit", ctypes.c_int32),
        ("aggregation", ctypes.c_char_p),
        ("step", ctypes.c_int64),
    ]


class CResult(ctypes.Structure):
    _fields_ = [
        ("points", ctypes.POINTER(CPoint)),
        ("count", ctypes.c_int32),
        ("error", ctypes.c_char_p),
    ]


# Callback type for result iteration
RESULT_CALLBACK = ctypes.CFUNCTYPE(
    ctypes.c_int,  # return 0 to continue, non-zero to stop
    ctypes.c_char_p,  # metric
    ctypes.c_double,  # value
    ctypes.c_int64,  # timestamp
    ctypes.c_char_p,  # tags_json
    ctypes.c_void_p,  # user_data
)


def _find_library() -> str:
    """Find the Chronicle shared library."""
    search_paths = [
        os.environ.get("CHRONICLE_LIB", ""),
        os.path.join(os.path.dirname(__file__), "..", "..", "libchronicle.so"),
        os.path.join(os.path.dirname(__file__), "..", "..", "libchronicle.dylib"),
        os.path.join(os.path.dirname(__file__), "..", "..", "libchronicle.dll"),
    ]

    for p in search_paths:
        if p and os.path.isfile(p):
            return p

    found = ctypes.util.find_library("chronicle")
    if found:
        return found

    raise OSError(
        "Cannot find libchronicle shared library. "
        "Set CHRONICLE_LIB environment variable or build with: "
        "go build -buildmode=c-shared -o libchronicle.so"
    )


def _load_library(path: Optional[str] = None) -> ctypes.CDLL:
    """Load and configure the Chronicle shared library."""
    lib_path = path or _find_library()
    lib = ctypes.CDLL(lib_path)

    # chronicle_open
    lib.chronicle_open.argtypes = [ctypes.POINTER(CConfig)]
    lib.chronicle_open.restype = ctypes.c_void_p

    # chronicle_close
    lib.chronicle_close.argtypes = [ctypes.c_void_p]
    lib.chronicle_close.restype = ctypes.c_int

    # chronicle_write
    lib.chronicle_write.argtypes = [ctypes.c_void_p, ctypes.POINTER(CPoint)]
    lib.chronicle_write.restype = ctypes.c_int

    # chronicle_write_batch
    lib.chronicle_write_batch.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(CPoint),
        ctypes.c_int32,
    ]
    lib.chronicle_write_batch.restype = ctypes.c_int

    # chronicle_query
    lib.chronicle_query.argtypes = [ctypes.c_void_p, ctypes.POINTER(CQuery)]
    lib.chronicle_query.restype = ctypes.POINTER(CResult)

    # chronicle_result_free
    lib.chronicle_result_free.argtypes = [ctypes.POINTER(CResult)]
    lib.chronicle_result_free.restype = None

    # chronicle_flush
    lib.chronicle_flush.argtypes = [ctypes.c_void_p]
    lib.chronicle_flush.restype = ctypes.c_int

    # chronicle_metrics_count
    lib.chronicle_metrics_count.argtypes = [ctypes.c_void_p]
    lib.chronicle_metrics_count.restype = ctypes.c_int32

    # chronicle_string_free
    lib.chronicle_string_free.argtypes = [ctypes.c_char_p]
    lib.chronicle_string_free.restype = None

    # chronicle_stats
    lib.chronicle_stats.argtypes = [ctypes.c_void_p]
    lib.chronicle_stats.restype = ctypes.c_char_p

    # chronicle_execute_sql
    lib.chronicle_execute_sql.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.chronicle_execute_sql.restype = ctypes.c_char_p

    # chronicle_version
    lib.chronicle_version.argtypes = []
    lib.chronicle_version.restype = ctypes.c_char_p

    return lib


class ChronicleDB:
    """Python client for Chronicle time-series database via FFI."""

    def __init__(
        self,
        path: str,
        retention_hours: int = 168,
        partition_duration_hours: int = 1,
        max_series: int = 0,
        enable_compression: bool = True,
        enable_wal: bool = True,
        lib_path: Optional[str] = None,
    ):
        self._lib = _load_library(lib_path)
        self._closed = False

        config = CConfig()
        config.path = path.encode("utf-8")
        config.retention_hours = retention_hours
        config.partition_duration_hours = partition_duration_hours
        config.max_series = max_series
        config.enable_compression = enable_compression
        config.enable_wal = enable_wal

        self._handle = self._lib.chronicle_open(ctypes.byref(config))
        if not self._handle:
            raise ChronicleError(CHRONICLE_ERR_IO, f"Failed to open database at {path}")

    def close(self) -> None:
        """Close the database connection."""
        if self._closed:
            return
        rc = self._lib.chronicle_close(self._handle)
        self._closed = True
        if rc != CHRONICLE_OK:
            raise ChronicleError(rc)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        if not self._closed:
            try:
                self.close()
            except Exception:
                pass

    def _check_open(self):
        if self._closed:
            raise ChronicleError(CHRONICLE_ERR_CLOSED)

    def write(
        self,
        metric: str,
        value: float,
        timestamp: int = 0,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """Write a single data point."""
        self._check_open()
        point = CPoint()
        point.metric = metric.encode("utf-8")
        point.value = value
        point.timestamp = timestamp or int(time.time() * 1e9)
        point.tags_json = json.dumps(tags or {}).encode("utf-8")

        rc = self._lib.chronicle_write(self._handle, ctypes.byref(point))
        if rc != CHRONICLE_OK:
            raise ChronicleError(rc)

    def write_batch(self, points: List[Point]) -> None:
        """Write a batch of data points."""
        self._check_open()
        if not points:
            return

        c_points = (CPoint * len(points))()
        # Keep references to prevent garbage collection of encoded strings
        _refs = []
        for i, p in enumerate(points):
            metric_b = p.metric.encode("utf-8")
            tags_b = json.dumps(p.tags).encode("utf-8")
            _refs.extend([metric_b, tags_b])
            c_points[i].metric = metric_b
            c_points[i].value = p.value
            c_points[i].timestamp = p.timestamp or int(time.time() * 1e9)
            c_points[i].tags_json = tags_b

        rc = self._lib.chronicle_write_batch(
            self._handle, c_points, ctypes.c_int32(len(points))
        )
        if rc != CHRONICLE_OK:
            raise ChronicleError(rc)

    def query(
        self,
        metric: str,
        start: int = 0,
        end: int = 0,
        limit: int = 0,
        aggregation: Optional[str] = None,
        step: int = 0,
    ) -> QueryResult:
        """Execute a query and return results."""
        self._check_open()
        q = CQuery()
        q.metric = metric.encode("utf-8")
        q.start = start
        q.end = end
        q.limit = limit
        q.aggregation = aggregation.encode("utf-8") if aggregation else None
        q.step = step

        result_ptr = self._lib.chronicle_query(self._handle, ctypes.byref(q))
        if not result_ptr:
            raise ChronicleError(CHRONICLE_ERR_INTERNAL, "Query returned null")

        try:
            result = result_ptr.contents
            if result.error:
                error_msg = result.error.decode("utf-8")
                raise ChronicleError(CHRONICLE_ERR_QUERY, error_msg)

            points = []
            for i in range(result.count):
                cp = result.points[i]
                tags = {}
                if cp.tags_json:
                    try:
                        tags = json.loads(cp.tags_json.decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass
                points.append(
                    Point(
                        metric=cp.metric.decode("utf-8") if cp.metric else metric,
                        value=cp.value,
                        timestamp=cp.timestamp,
                        tags=tags,
                    )
                )
            return QueryResult(points=points)
        finally:
            self._lib.chronicle_result_free(result_ptr)

    def execute_sql(self, sql: str) -> Any:
        """Execute a SQL query and return parsed JSON result."""
        self._check_open()
        result = self._lib.chronicle_execute_sql(
            self._handle, sql.encode("utf-8")
        )
        if not result:
            raise ChronicleError(CHRONICLE_ERR_INTERNAL)
        try:
            return json.loads(result.decode("utf-8"))
        finally:
            self._lib.chronicle_string_free(result)

    def flush(self) -> None:
        """Flush pending writes to storage."""
        self._check_open()
        rc = self._lib.chronicle_flush(self._handle)
        if rc != CHRONICLE_OK:
            raise ChronicleError(rc)

    def metrics_count(self) -> int:
        """Get the number of metrics in the database."""
        self._check_open()
        count = self._lib.chronicle_metrics_count(self._handle)
        if count < 0:
            raise ChronicleError(CHRONICLE_ERR_INTERNAL)
        return count

    def stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        self._check_open()
        result = self._lib.chronicle_stats(self._handle)
        if not result:
            return {}
        try:
            return json.loads(result.decode("utf-8"))
        finally:
            self._lib.chronicle_string_free(result)

    def version(self) -> str:
        """Get the Chronicle library version."""
        result = self._lib.chronicle_version()
        if result:
            v = result.decode("utf-8")
            self._lib.chronicle_string_free(result)
            return v
        return "unknown"

    def to_dataframe(self, result: QueryResult):
        """Convert a QueryResult to a pandas DataFrame.

        Requires pandas to be installed.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for DataFrame conversion: pip install pandas")

        records = []
        for p in result.points:
            row = {
                "metric": p.metric,
                "value": p.value,
                "timestamp": pd.Timestamp(p.timestamp, unit="ns"),
            }
            row.update(p.tags)
            records.append(row)

        return pd.DataFrame(records)

    def write_dataframe(self, df, metric_col: str = "metric", value_col: str = "value",
                        timestamp_col: str = "timestamp", tag_cols: Optional[List[str]] = None) -> None:
        """Write a pandas DataFrame as batch points.

        Args:
            df: pandas DataFrame
            metric_col: Column name for metric names
            value_col: Column name for values
            timestamp_col: Column name for timestamps (ns epoch or datetime)
            tag_cols: List of columns to use as tags (default: all other columns)
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required: pip install pandas")

        if tag_cols is None:
            tag_cols = [c for c in df.columns if c not in (metric_col, value_col, timestamp_col)]

        points = []
        for _, row in df.iterrows():
            ts = row.get(timestamp_col, 0)
            if isinstance(ts, pd.Timestamp):
                ts = int(ts.value)
            elif isinstance(ts, (int, float)):
                ts = int(ts)
            else:
                ts = 0

            tags = {col: str(row[col]) for col in tag_cols if col in row.index and pd.notna(row[col])}
            points.append(Point(
                metric=str(row[metric_col]),
                value=float(row[value_col]),
                timestamp=ts,
                tags=tags,
            ))

        self.write_batch(points)
