"""
Chronicle Time-Series Database - Python Bindings

This module provides Python bindings for the Chronicle embedded time-series database.
It uses ctypes to interface with the Chronicle shared library.

Example usage:
    from chronicle import Chronicle, Point, Config

    # Open database
    db = Chronicle("sensors.db")

    # Write data
    db.write(Point(
        metric="temperature",
        tags={"room": "living"},
        value=22.5
    ))

    # Query data
    results = db.query("temperature", tags={"room": "living"})
    for point in results:
        print(f"{point.metric}: {point.value} at {point.timestamp}")

    # SQL-like queries
    results = db.query_sql("SELECT mean(value) FROM temperature GROUP BY time(5m)")

    # Close database
    db.close()

Requirements:
    - libchronicle.so (or .dylib/.dll) must be built and accessible
    - Build with: go build -buildmode=c-shared -o libchronicle.so
"""

import ctypes
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

# Error codes
CHRONICLE_OK = 0
CHRONICLE_ERR_INVALID_ARG = 1
CHRONICLE_ERR_NOT_FOUND = 2
CHRONICLE_ERR_IO = 3
CHRONICLE_ERR_QUERY = 4
CHRONICLE_ERR_CLOSED = 5
CHRONICLE_ERR_INTERNAL = 100


class ChronicleError(Exception):
    """Exception raised for Chronicle database errors."""

    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(f"Chronicle error {code}: {message}")


@dataclass
class Point:
    """Represents a single time-series data point."""

    metric: str
    value: float
    tags: Dict[str, str] = field(default_factory=dict)
    timestamp: int = 0  # Nanoseconds since epoch; 0 = use current time

    @classmethod
    def from_datetime(
        cls,
        metric: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
        dt: Optional[datetime] = None,
    ) -> "Point":
        """Create a point with a datetime timestamp."""
        if dt is None:
            dt = datetime.now()
        ts = int(dt.timestamp() * 1_000_000_000)
        return cls(metric=metric, value=value, tags=tags or {}, timestamp=ts)

    def to_datetime(self) -> datetime:
        """Convert timestamp to datetime."""
        return datetime.fromtimestamp(self.timestamp / 1_000_000_000)


@dataclass
class Config:
    """Database configuration options."""

    path: str
    retention_hours: int = 0  # 0 = keep forever
    partition_duration_hours: int = 1
    max_series: int = 0  # 0 = unlimited
    enable_compression: bool = True
    enable_wal: bool = True


# C struct definitions matching cffi.go
class CChronicleConfig(ctypes.Structure):
    _fields_ = [
        ("path", ctypes.c_char_p),
        ("retention_hours", ctypes.c_int64),
        ("partition_duration_hours", ctypes.c_int64),
        ("max_series", ctypes.c_int32),
        ("enable_compression", ctypes.c_bool),
        ("enable_wal", ctypes.c_bool),
    ]


class CChroniclePoint(ctypes.Structure):
    _fields_ = [
        ("metric", ctypes.c_char_p),
        ("value", ctypes.c_double),
        ("timestamp", ctypes.c_int64),
        ("tags_json", ctypes.c_char_p),
    ]


class CChronicleQuery(ctypes.Structure):
    _fields_ = [
        ("metric", ctypes.c_char_p),
        ("start", ctypes.c_int64),
        ("end", ctypes.c_int64),
        ("limit", ctypes.c_int32),
        ("aggregation", ctypes.c_char_p),
        ("step", ctypes.c_int64),
    ]


class CChronicleResult(ctypes.Structure):
    _fields_ = [
        ("points", ctypes.POINTER(CChroniclePoint)),
        ("count", ctypes.c_int32),
        ("error", ctypes.c_char_p),
    ]


def _find_library() -> str:
    """Find the Chronicle shared library."""
    if sys.platform == "darwin":
        lib_name = "libchronicle.dylib"
    elif sys.platform == "win32":
        lib_name = "chronicle.dll"
    else:
        lib_name = "libchronicle.so"

    search_paths = [
        Path(__file__).parent / lib_name,
        Path(__file__).parent.parent / lib_name,
        Path(__file__).parent.parent.parent / lib_name,
        Path.cwd() / lib_name,
        Path("/usr/local/lib") / lib_name,
        Path("/usr/lib") / lib_name,
    ]

    ld_path = os.environ.get("LD_LIBRARY_PATH", "")
    for p in ld_path.split(":"):
        if p:
            search_paths.append(Path(p) / lib_name)

    for path in search_paths:
        if path.exists():
            return str(path)

    raise FileNotFoundError(
        f"Could not find {lib_name}. Build it with: "
        "go build -buildmode=c-shared -o libchronicle.so"
    )


class Chronicle:
    """Chronicle time-series database client."""

    _lib: ctypes.CDLL = None

    def __init__(
        self,
        path: str,
        config: Optional[Config] = None,
        lib_path: Optional[str] = None,
    ):
        """
        Open or create a Chronicle database.

        Args:
            path: Database file path
            config: Optional configuration (uses defaults if not provided)
            lib_path: Optional path to the shared library
        """
        self._load_library(lib_path)
        self._handle = None

        if config is None:
            config = Config(path=path)

        cfg = CChronicleConfig(
            path=config.path.encode("utf-8"),
            retention_hours=config.retention_hours,
            partition_duration_hours=config.partition_duration_hours,
            max_series=config.max_series,
            enable_compression=config.enable_compression,
            enable_wal=config.enable_wal,
        )

        self._handle = self._lib.chronicle_open(ctypes.byref(cfg))
        if not self._handle:
            raise ChronicleError(CHRONICLE_ERR_IO, f"Failed to open database at {path}")

    def _load_library(self, lib_path: Optional[str] = None):
        """Load the Chronicle shared library."""
        if Chronicle._lib is not None:
            return

        if lib_path is None:
            lib_path = _find_library()

        Chronicle._lib = ctypes.CDLL(lib_path)
        lib = Chronicle._lib

        # chronicle_version
        lib.chronicle_version.restype = ctypes.c_char_p
        lib.chronicle_version.argtypes = []

        # chronicle_open
        lib.chronicle_open.restype = ctypes.c_void_p
        lib.chronicle_open.argtypes = [ctypes.POINTER(CChronicleConfig)]

        # chronicle_close
        lib.chronicle_close.restype = ctypes.c_int
        lib.chronicle_close.argtypes = [ctypes.c_void_p]

        # chronicle_write
        lib.chronicle_write.restype = ctypes.c_int
        lib.chronicle_write.argtypes = [ctypes.c_void_p, ctypes.POINTER(CChroniclePoint)]

        # chronicle_write_batch
        lib.chronicle_write_batch.restype = ctypes.c_int
        lib.chronicle_write_batch.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(CChroniclePoint),
            ctypes.c_int32,
        ]

        # chronicle_query
        lib.chronicle_query.restype = ctypes.POINTER(CChronicleResult)
        lib.chronicle_query.argtypes = [ctypes.c_void_p, ctypes.POINTER(CChronicleQuery)]

        # chronicle_result_free
        lib.chronicle_result_free.restype = None
        lib.chronicle_result_free.argtypes = [ctypes.POINTER(CChronicleResult)]

        # chronicle_flush
        lib.chronicle_flush.restype = ctypes.c_int
        lib.chronicle_flush.argtypes = [ctypes.c_void_p]

        # chronicle_metrics_count
        lib.chronicle_metrics_count.restype = ctypes.c_int32
        lib.chronicle_metrics_count.argtypes = [ctypes.c_void_p]

        # chronicle_metrics_list
        lib.chronicle_metrics_list.restype = ctypes.c_int32
        lib.chronicle_metrics_list.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_int32,
        ]

        # chronicle_string_free
        lib.chronicle_string_free.restype = None
        lib.chronicle_string_free.argtypes = [ctypes.c_char_p]

        # chronicle_stats
        lib.chronicle_stats.restype = ctypes.c_char_p
        lib.chronicle_stats.argtypes = [ctypes.c_void_p]

        # chronicle_execute_sql
        lib.chronicle_execute_sql.restype = ctypes.c_char_p
        lib.chronicle_execute_sql.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    def _check_error(self, code: int, operation: str):
        """Check error code and raise exception if needed."""
        if code != CHRONICLE_OK:
            messages = {
                CHRONICLE_ERR_INVALID_ARG: "Invalid argument",
                CHRONICLE_ERR_NOT_FOUND: "Not found",
                CHRONICLE_ERR_IO: "I/O error",
                CHRONICLE_ERR_QUERY: "Query error",
                CHRONICLE_ERR_CLOSED: "Database is closed",
                CHRONICLE_ERR_INTERNAL: "Internal error",
            }
            raise ChronicleError(code, f"{operation}: {messages.get(code, 'Unknown error')}")

    def _check_open(self):
        """Ensure database is open."""
        if self._handle is None:
            raise ChronicleError(CHRONICLE_ERR_CLOSED, "Database is closed")

    def close(self):
        """Close the database."""
        if self._handle is not None:
            code = self._lib.chronicle_close(self._handle)
            self._handle = None
            self._check_error(code, "close")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def write(self, point: Point):
        """Write a single data point."""
        self._check_open()
        c_point = CChroniclePoint(
            metric=point.metric.encode("utf-8"),
            value=point.value,
            timestamp=point.timestamp,
            tags_json=json.dumps(point.tags).encode("utf-8") if point.tags else None,
        )
        code = self._lib.chronicle_write(self._handle, ctypes.byref(c_point))
        self._check_error(code, "write")

    def write_batch(self, points: List[Point]):
        """Write multiple data points efficiently."""
        self._check_open()
        if not points:
            return

        c_points = (CChroniclePoint * len(points))()
        for i, p in enumerate(points):
            c_points[i] = CChroniclePoint(
                metric=p.metric.encode("utf-8"),
                value=p.value,
                timestamp=p.timestamp,
                tags_json=json.dumps(p.tags).encode("utf-8") if p.tags else None,
            )

        code = self._lib.chronicle_write_batch(self._handle, c_points, len(points))
        self._check_error(code, "write_batch")

    def flush(self):
        """Flush buffered writes to disk."""
        self._check_open()
        code = self._lib.chronicle_flush(self._handle)
        self._check_error(code, "flush")

    def query(
        self,
        metric: str,
        tags: Optional[Dict[str, str]] = None,
        start: Optional[Union[int, datetime]] = None,
        end: Optional[Union[int, datetime]] = None,
        aggregation: Optional[str] = None,
        step_ns: int = 0,
        limit: int = 0,
    ) -> List[Point]:
        """
        Query data points.

        Args:
            metric: Metric name
            tags: Optional tag filters (not yet supported in query)
            start: Start time (nanoseconds or datetime)
            end: End time (nanoseconds or datetime)
            aggregation: Aggregation function ("avg", "sum", "min", "max", "count")
            step_ns: Aggregation window in nanoseconds
            limit: Maximum results (0 = unlimited)

        Returns:
            List of matching points
        """
        self._check_open()

        start_ns = self._to_nanos(start) if start else 0
        end_ns = self._to_nanos(end) if end else 0

        c_query = CChronicleQuery(
            metric=metric.encode("utf-8"),
            start=start_ns,
            end=end_ns,
            limit=limit,
            aggregation=aggregation.encode("utf-8") if aggregation else None,
            step=step_ns,
        )

        result_ptr = self._lib.chronicle_query(self._handle, ctypes.byref(c_query))
        if not result_ptr:
            return []

        try:
            result = result_ptr.contents

            if result.error:
                error_msg = result.error.decode("utf-8")
                raise ChronicleError(CHRONICLE_ERR_QUERY, error_msg)

            points = []
            if result.count > 0 and result.points:
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
                            metric=cp.metric.decode("utf-8") if cp.metric else "",
                            value=cp.value,
                            timestamp=cp.timestamp,
                            tags=tags,
                        )
                    )

            return points
        finally:
            self._lib.chronicle_result_free(result_ptr)

    def query_sql(self, sql: str) -> List[Point]:
        """
        Execute a SQL-like query.

        Args:
            sql: SQL query string

        Returns:
            List of result points
        """
        self._check_open()

        result_json = self._lib.chronicle_execute_sql(
            self._handle, sql.encode("utf-8")
        )

        if not result_json:
            return []

        try:
            result_str = result_json.decode("utf-8")
            data = json.loads(result_str)

            if "error" in data:
                raise ChronicleError(CHRONICLE_ERR_QUERY, data["error"])

            # Handle Result structure: {"Points": [...]}
            if "Points" in data:
                return [
                    Point(
                        metric=p.get("Metric", ""),
                        value=p.get("Value", 0.0),
                        timestamp=p.get("Timestamp", 0),
                        tags=p.get("Tags") or {},
                    )
                    for p in data["Points"]
                ]

            return []
        finally:
            self._lib.chronicle_string_free(result_json)

    def metrics(self) -> List[str]:
        """Get all metric names."""
        self._check_open()

        count = self._lib.chronicle_metrics_count(self._handle)
        if count <= 0:
            return []

        # Allocate array for metric names
        metrics_array = (ctypes.c_char_p * count)()
        actual_count = self._lib.chronicle_metrics_list(
            self._handle, metrics_array, count
        )

        result = []
        for i in range(actual_count):
            if metrics_array[i]:
                result.append(metrics_array[i].decode("utf-8"))
                self._lib.chronicle_string_free(metrics_array[i])

        return result

    def stats(self) -> dict:
        """Get database statistics."""
        self._check_open()

        stats_json = self._lib.chronicle_stats(self._handle)
        if not stats_json:
            return {}

        try:
            return json.loads(stats_json.decode("utf-8"))
        finally:
            self._lib.chronicle_string_free(stats_json)

    @staticmethod
    def _to_nanos(t: Union[int, datetime]) -> int:
        """Convert time to nanoseconds."""
        if isinstance(t, datetime):
            return int(t.timestamp() * 1_000_000_000)
        return t

    @staticmethod
    def version() -> str:
        """Get Chronicle library version."""
        if Chronicle._lib is None:
            Chronicle._lib = ctypes.CDLL(_find_library())
            Chronicle._lib.chronicle_version.restype = ctypes.c_char_p
        result = Chronicle._lib.chronicle_version()
        return result.decode("utf-8") if result else "unknown"


# Time duration constants (in nanoseconds)
NANOSECOND = 1
MICROSECOND = 1000
MILLISECOND = 1000 * MICROSECOND
SECOND = 1000 * MILLISECOND
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR


if __name__ == "__main__":
    import tempfile

    print("Chronicle Python Bindings")

    try:
        lib_path = _find_library()
        print(f"Library path: {lib_path}")
    except FileNotFoundError as e:
        print(f"Library not found: {e}")
        print("Build with: go build -buildmode=c-shared -o libchronicle.so")
        sys.exit(1)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        try:
            with Chronicle(db_path) as db:
                # Write some data
                for i in range(10):
                    db.write(
                        Point(
                            metric="temperature",
                            tags={"room": "living", "sensor": "1"},
                            value=20.0 + i * 0.5,
                        )
                    )

                db.flush()

                # Query data
                results = db.query("temperature")
                print(f"\nQuery results ({len(results)} points):")
                for p in results[:3]:
                    print(f"  {p.metric}: {p.value}")

                # Get metrics
                metrics = db.metrics()
                print(f"\nMetrics: {metrics}")

                # Get stats
                stats = db.stats()
                print(f"\nStats: {stats}")

            print("\nDone!")
        except ChronicleError as e:
            print(f"Chronicle error: {e}")
