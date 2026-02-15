"""
Chronicle Time-Series Database - Jupyter Integration

Provides Jupyter magic commands for interactive time-series exploration:
    %%chronicle - Execute Chronicle queries directly in notebook cells
    %chronicle_connect - Connect to a Chronicle database
    %chronicle_status - Show connection status

Auto-visualization for query results with pandas/matplotlib integration.

Example usage:
    %load_ext chronicle_jupyter

    %chronicle_connect sensors.db

    %%chronicle
    SELECT mean(value) FROM temperature
    WHERE time > now() - 1h
    GROUP BY time(5m), host

Requirements:
    - chronicle.py (Chronicle base bindings)
    - IPython (for magic commands)
    - pandas (optional, for DataFrame display)
    - matplotlib (optional, for auto-visualization)
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

try:
    from .chronicle import Chronicle, Point, Config, ChronicleError
except ImportError:
    try:
        from chronicle import Chronicle, Point, Config, ChronicleError
    except ImportError:
        Chronicle = None

try:
    from .chronicle_dataframe import ChronicleConnection, connect as df_connect
except ImportError:
    try:
        from chronicle_dataframe import ChronicleConnection, connect as df_connect
    except ImportError:
        ChronicleConnection = None
        df_connect = None


# --- Jupyter Magic Commands ---

_ACTIVE_CONNECTION: Optional[Any] = None
_CONNECTION_PATH: Optional[str] = None


class ChronicleQueryResult:
    """Wraps query results with auto-display and conversion methods."""

    def __init__(self, points: List[Point], query: str, duration_ms: float):
        self.points = points
        self.query = query
        self.duration_ms = duration_ms

    @property
    def count(self) -> int:
        return len(self.points)

    def to_pandas(self):
        """Convert results to a pandas DataFrame."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for to_pandas(). Install with: pip install pandas")

        records = []
        for p in self.points:
            record = {
                "metric": p.metric,
                "value": p.value,
                "timestamp": datetime.fromtimestamp(p.timestamp / 1_000_000_000)
                if p.timestamp > 0 else None,
            }
            record.update(p.tags)
            records.append(record)
        return pd.DataFrame(records)

    def to_polars(self):
        """Convert results to a polars DataFrame."""
        try:
            import polars as pl
        except ImportError:
            raise ImportError("polars is required for to_polars(). Install with: pip install polars")

        records = []
        for p in self.points:
            record = {
                "metric": p.metric,
                "value": p.value,
                "timestamp": p.timestamp,
            }
            record.update(p.tags)
            records.append(record)
        return pl.DataFrame(records)

    def plot(self, **kwargs):
        """Auto-visualize results as a time-series chart."""
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            raise ImportError("matplotlib is required for plot(). Install with: pip install matplotlib")

        df = self.to_pandas()
        if "timestamp" in df.columns and "value" in df.columns:
            fig, ax = plt.subplots(figsize=kwargs.get("figsize", (12, 4)))
            ax.plot(df["timestamp"], df["value"], **{k: v for k, v in kwargs.items() if k != "figsize"})
            ax.set_title(self.query[:80])
            ax.set_xlabel("Time")
            ax.set_ylabel("Value")
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            return fig
        return None

    def _repr_html_(self):
        """Rich display in Jupyter notebooks."""
        html = f'<div style="font-size:12px;color:#666">Query: <code>{self.query}</code> '
        html += f'({self.count} points, {self.duration_ms:.1f}ms)</div>'
        if self.count > 0:
            try:
                df = self.to_pandas()
                html += df.head(20).to_html(index=False, max_rows=20)
                if self.count > 20:
                    html += f'<div style="color:#999">... and {self.count - 20} more rows</div>'
            except ImportError:
                html += "<pre>"
                for p in self.points[:10]:
                    html += f"{p.metric}: {p.value}\n"
                html += "</pre>"
        return html

    def __repr__(self):
        return f"ChronicleQueryResult(count={self.count}, duration={self.duration_ms:.1f}ms)"

    def __len__(self):
        return self.count

    def __iter__(self):
        return iter(self.points)


def _get_connection():
    """Get the active Chronicle connection."""
    global _ACTIVE_CONNECTION
    if _ACTIVE_CONNECTION is None:
        raise RuntimeError(
            "No active Chronicle connection. Use %chronicle_connect <path> first."
        )
    return _ACTIVE_CONNECTION


def chronicle_connect(path: str, **kwargs) -> Optional[Any]:
    """
    Connect to a Chronicle database for use in Jupyter.

    Args:
        path: Database file path
        **kwargs: Additional configuration options

    Returns:
        The active connection
    """
    global _ACTIVE_CONNECTION, _CONNECTION_PATH

    if _ACTIVE_CONNECTION is not None:
        try:
            _ACTIVE_CONNECTION.close()
        except Exception:
            pass

    if Chronicle is not None:
        _ACTIVE_CONNECTION = Chronicle(path, **kwargs)
        _CONNECTION_PATH = path
        return _ACTIVE_CONNECTION

    raise RuntimeError("Chronicle library not available")


def chronicle_query(query_str: str) -> ChronicleQueryResult:
    """
    Execute a Chronicle query and return wrapped results.

    Args:
        query_str: The query string (metric name or SQL-like query)

    Returns:
        ChronicleQueryResult with auto-display capabilities
    """
    db = _get_connection()
    start = time.monotonic()

    query_str = query_str.strip()

    # Detect query type and execute
    points = []
    if query_str.upper().startswith("SELECT"):
        points = db.query_sql(query_str)
    else:
        # Simple metric query
        parts = query_str.split()
        metric = parts[0]
        points = db.query(metric)

    duration = (time.monotonic() - start) * 1000
    return ChronicleQueryResult(points=points, query=query_str, duration_ms=duration)


def chronicle_status() -> Dict[str, Any]:
    """Get the current connection status."""
    global _ACTIVE_CONNECTION, _CONNECTION_PATH

    status = {
        "connected": _ACTIVE_CONNECTION is not None,
        "path": _CONNECTION_PATH,
    }

    if _ACTIVE_CONNECTION is not None:
        try:
            stats = _ACTIVE_CONNECTION.stats()
            status["stats"] = stats
        except Exception:
            pass

    return status


# --- IPython Magic Registration ---

def load_ipython_extension(ipython):
    """Load the Chronicle magic extension into IPython/Jupyter."""
    try:
        from IPython.core.magic import register_line_magic, register_cell_magic
    except ImportError:
        print("IPython not available. Magic commands disabled.")
        return

    @register_line_magic
    def chronicle_connect_magic(line):
        """Connect to a Chronicle database: %chronicle_connect path/to/db"""
        path = line.strip()
        if not path:
            print("Usage: %chronicle_connect <path>")
            return
        try:
            conn = chronicle_connect(path)
            print(f"Connected to Chronicle database: {path}")
            return conn
        except Exception as e:
            print(f"Connection failed: {e}")

    @register_cell_magic
    def chronicle(line, cell):
        """Execute a Chronicle query: %%chronicle"""
        try:
            result = chronicle_query(cell)
            return result
        except Exception as e:
            print(f"Query error: {e}")

    @register_line_magic
    def chronicle_status_magic(line):
        """Show connection status: %chronicle_status"""
        status = chronicle_status()
        for k, v in status.items():
            print(f"  {k}: {v}")

    # Register with shorter names
    ipython.register_magic_function(chronicle_connect_magic, magic_kind="line", magic_name="chronicle_connect")
    ipython.register_magic_function(chronicle, magic_kind="cell", magic_name="chronicle")
    ipython.register_magic_function(chronicle_status_magic, magic_kind="line", magic_name="chronicle_status")

    print("Chronicle magic commands loaded. Use %chronicle_connect <path> to connect.")


# --- Interactive Query Builder Widget ---

class QueryBuilder:
    """
    Interactive query builder for Jupyter notebooks.

    Example:
        qb = QueryBuilder("temperature")
        qb.where(host="server1").time_range("1h").aggregate("mean", "5m")
        result = qb.execute(db)
    """

    def __init__(self, metric: str):
        self._metric = metric
        self._tags: Dict[str, str] = {}
        self._start: Optional[int] = None
        self._end: Optional[int] = None
        self._agg_func: Optional[str] = None
        self._agg_window: Optional[str] = None
        self._group_by: List[str] = []
        self._limit: int = 0

    def where(self, **tags) -> "QueryBuilder":
        """Add tag filters."""
        self._tags.update({k: str(v) for k, v in tags.items()})
        return self

    def time_range(self, lookback: str = "1h", end: Optional[datetime] = None) -> "QueryBuilder":
        """Set the time range with a lookback duration string."""
        units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        unit = lookback[-1]
        num = int(lookback[:-1])
        seconds = num * units.get(unit, 3600)

        now = end or datetime.now()
        self._end = int(now.timestamp() * 1_000_000_000)
        self._start = int((now - timedelta(seconds=seconds)).timestamp() * 1_000_000_000)
        return self

    def aggregate(self, func: str, window: str = "1m") -> "QueryBuilder":
        """Set aggregation function and window."""
        self._agg_func = func
        self._agg_window = window
        return self

    def group_by(self, *tags) -> "QueryBuilder":
        """Set group-by tags."""
        self._group_by.extend(tags)
        return self

    def limit(self, n: int) -> "QueryBuilder":
        """Limit result count."""
        self._limit = n
        return self

    def execute(self, db=None) -> ChronicleQueryResult:
        """Execute the built query."""
        if db is None:
            db = _get_connection()

        start_time = time.monotonic()
        points = db.query(
            self._metric,
            tags=self._tags if self._tags else None,
            start=self._start or 0,
            end=self._end or 0,
        )
        duration = (time.monotonic() - start_time) * 1000

        query_repr = f"{self._metric}"
        if self._tags:
            query_repr += "{" + ", ".join(f'{k}="{v}"' for k, v in self._tags.items()) + "}"
        if self._agg_func:
            query_repr = f"{self._agg_func}({query_repr}[{self._agg_window}])"

        return ChronicleQueryResult(points=points, query=query_repr, duration_ms=duration)

    def __repr__(self):
        parts = [f"QueryBuilder({self._metric!r})"]
        if self._tags:
            parts.append(f".where({self._tags})")
        if self._start:
            parts.append(".time_range(...)")
        if self._agg_func:
            parts.append(f".aggregate({self._agg_func!r}, {self._agg_window!r})")
        return "".join(parts)
