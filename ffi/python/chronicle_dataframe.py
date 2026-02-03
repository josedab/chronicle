"""
Chronicle Time-Series Database - Python DataFrame API

This module provides a pandas/polars-compatible DataFrame API for working with
Chronicle time-series data. It supports lazy evaluation, vectorized operations,
and seamless integration with the Python data science ecosystem.

Example usage:
    from chronicle_dataframe import ChronicleDataFrame, connect

    # Connect to database
    db = connect("sensors.db")

    # Query data as DataFrame
    df = db.query_df("cpu_usage").filter(
        col("host") == "server1"
    ).select([
        "timestamp", "value", "host"
    ]).sort("timestamp")

    # Convert to pandas
    pdf = df.to_pandas()

    # Aggregations
    result = df.group_by("host").agg([
        col("value").mean().alias("avg_cpu"),
        col("value").max().alias("max_cpu"),
        col("value").count().alias("samples")
    ])

    # Windowed operations
    windowed = df.with_columns([
        col("value").rolling_mean(window="5m").alias("rolling_avg"),
        col("value").diff().alias("change")
    ])

Requirements:
    - chronicle.py (Chronicle base bindings)
    - numpy (optional, for array operations)
    - pandas (optional, for to_pandas())
    - polars (optional, for to_polars())
"""

from __future__ import annotations

import json
import math
import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import (
    Any, Callable, Dict, Iterator, List,
    Optional, Sequence, Tuple, Union, TYPE_CHECKING
)

# Import base Chronicle client
try:
    from .chronicle import Chronicle, Point, Config, ChronicleError
except ImportError:
    from chronicle import Chronicle, Point, Config, ChronicleError


# Time constants
NANOSECOND = 1
MICROSECOND = 1000
MILLISECOND = 1_000_000
SECOND = 1_000_000_000
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR


def connect(path: str, **kwargs) -> "ChronicleConnection":
    """
    Connect to a Chronicle database.

    Args:
        path: Database file path
        **kwargs: Additional configuration options

    Returns:
        ChronicleConnection instance
    """
    return ChronicleConnection(path, **kwargs)


def col(name: str) -> "Expr":
    """
    Create a column expression.

    Args:
        name: Column name

    Returns:
        Column expression
    """
    return Expr(ExprType.COLUMN, name=name)


def lit(value: Any) -> "Expr":
    """
    Create a literal value expression.

    Args:
        value: Literal value

    Returns:
        Literal expression
    """
    return Expr(ExprType.LITERAL, value=value)


def when(condition: "Expr") -> "WhenBuilder":
    """
    Start a when/then/otherwise expression.

    Args:
        condition: Boolean condition

    Returns:
        WhenBuilder for chaining
    """
    return WhenBuilder(condition)


class ExprType(Enum):
    """Expression type identifiers."""
    COLUMN = auto()
    LITERAL = auto()
    BINARY_OP = auto()
    UNARY_OP = auto()
    FUNCTION = auto()
    AGGREGATION = auto()
    WINDOW = auto()
    CAST = auto()
    ALIAS = auto()
    CASE = auto()


class BinaryOp(Enum):
    """Binary operation types."""
    ADD = "+"
    SUB = "-"
    MUL = "*"
    DIV = "/"
    MOD = "%"
    EQ = "=="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    AND = "and"
    OR = "or"


class AggFunc(Enum):
    """Aggregation function types."""
    COUNT = "count"
    SUM = "sum"
    MEAN = "mean"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    STDDEV = "stddev"
    VAR = "var"
    MEDIAN = "median"
    PERCENTILE = "percentile"


class WindowFunc(Enum):
    """Window function types."""
    ROLLING_MEAN = "rolling_mean"
    ROLLING_SUM = "rolling_sum"
    ROLLING_MIN = "rolling_min"
    ROLLING_MAX = "rolling_max"
    ROLLING_COUNT = "rolling_count"
    ROLLING_STDDEV = "rolling_stddev"
    LAG = "lag"
    LEAD = "lead"
    DIFF = "diff"
    PCT_CHANGE = "pct_change"
    CUMSUM = "cumsum"
    CUMMIN = "cummin"
    CUMMAX = "cummax"
    RANK = "rank"
    ROW_NUMBER = "row_number"


@dataclass
class Expr:
    """
    Expression class for building query operations.

    Expressions support lazy evaluation and can be combined
    to build complex transformations.
    """
    type: ExprType
    name: Optional[str] = None
    value: Any = None
    left: Optional["Expr"] = None
    right: Optional["Expr"] = None
    op: Optional[Union[BinaryOp, str]] = None
    func: Optional[Union[AggFunc, WindowFunc, str]] = None
    args: List["Expr"] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)
    alias_name: Optional[str] = None

    # Arithmetic operations
    def __add__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.ADD, other)

    def __radd__(self, other: Any) -> "Expr":
        return lit(other) + self

    def __sub__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.SUB, other)

    def __rsub__(self, other: Any) -> "Expr":
        return lit(other) - self

    def __mul__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.MUL, other)

    def __rmul__(self, other: Any) -> "Expr":
        return lit(other) * self

    def __truediv__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.DIV, other)

    def __rtruediv__(self, other: Any) -> "Expr":
        return lit(other) / self

    def __mod__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.MOD, other)

    def __neg__(self) -> "Expr":
        return Expr(ExprType.UNARY_OP, op="-", left=self)

    # Comparison operations
    def __eq__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.EQ, other)

    def __ne__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.NE, other)

    def __lt__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.LT, other)

    def __le__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.LE, other)

    def __gt__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.GT, other)

    def __ge__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.GE, other)

    # Logical operations
    def __and__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.AND, other)

    def __or__(self, other: Union["Expr", Any]) -> "Expr":
        return self._binary_op(BinaryOp.OR, other)

    def __invert__(self) -> "Expr":
        return Expr(ExprType.UNARY_OP, op="not", left=self)

    def _binary_op(self, op: BinaryOp, other: Union["Expr", Any]) -> "Expr":
        if not isinstance(other, Expr):
            other = lit(other)
        return Expr(ExprType.BINARY_OP, op=op, left=self, right=other)

    # Alias
    def alias(self, name: str) -> "Expr":
        """Assign an alias to this expression."""
        result = copy.copy(self)
        result.alias_name = name
        return result

    # Type casting
    def cast(self, dtype: str) -> "Expr":
        """Cast to a different data type."""
        return Expr(ExprType.CAST, left=self, params={"dtype": dtype})

    # Null handling
    def is_null(self) -> "Expr":
        """Check if value is null."""
        return Expr(ExprType.FUNCTION, func="is_null", args=[self])

    def is_not_null(self) -> "Expr":
        """Check if value is not null."""
        return Expr(ExprType.FUNCTION, func="is_not_null", args=[self])

    def fill_null(self, value: Any) -> "Expr":
        """Fill null values."""
        return Expr(ExprType.FUNCTION, func="fill_null", args=[self, lit(value)])

    # String operations
    def str_contains(self, pattern: str) -> "Expr":
        """Check if string contains pattern."""
        return Expr(ExprType.FUNCTION, func="str_contains",
                    args=[self], params={"pattern": pattern})

    def str_starts_with(self, prefix: str) -> "Expr":
        """Check if string starts with prefix."""
        return Expr(ExprType.FUNCTION, func="str_starts_with",
                    args=[self], params={"prefix": prefix})

    def str_ends_with(self, suffix: str) -> "Expr":
        """Check if string ends with suffix."""
        return Expr(ExprType.FUNCTION, func="str_ends_with",
                    args=[self], params={"suffix": suffix})

    # Aggregation functions
    def count(self) -> "Expr":
        """Count non-null values."""
        return Expr(ExprType.AGGREGATION, func=AggFunc.COUNT, args=[self])

    def sum(self) -> "Expr":
        """Sum values."""
        return Expr(ExprType.AGGREGATION, func=AggFunc.SUM, args=[self])

    def mean(self) -> "Expr":
        """Calculate mean."""
        return Expr(ExprType.AGGREGATION, func=AggFunc.MEAN, args=[self])

    def min(self) -> "Expr":
        """Get minimum value."""
        return Expr(ExprType.AGGREGATION, func=AggFunc.MIN, args=[self])

    def max(self) -> "Expr":
        """Get maximum value."""
        return Expr(ExprType.AGGREGATION, func=AggFunc.MAX, args=[self])

    def first(self) -> "Expr":
        """Get first value."""
        return Expr(ExprType.AGGREGATION, func=AggFunc.FIRST, args=[self])

    def last(self) -> "Expr":
        """Get last value."""
        return Expr(ExprType.AGGREGATION, func=AggFunc.LAST, args=[self])

    def std(self) -> "Expr":
        """Calculate standard deviation."""
        return Expr(ExprType.AGGREGATION, func=AggFunc.STDDEV, args=[self])

    def var(self) -> "Expr":
        """Calculate variance."""
        return Expr(ExprType.AGGREGATION, func=AggFunc.VAR, args=[self])

    def median(self) -> "Expr":
        """Calculate median."""
        return Expr(ExprType.AGGREGATION, func=AggFunc.MEDIAN, args=[self])

    def percentile(self, q: float) -> "Expr":
        """Calculate percentile."""
        return Expr(ExprType.AGGREGATION, func=AggFunc.PERCENTILE,
                    args=[self], params={"q": q})

    # Window functions
    def rolling_mean(self, window: Union[int, str] = 3) -> "Expr":
        """Calculate rolling mean."""
        return Expr(ExprType.WINDOW, func=WindowFunc.ROLLING_MEAN,
                    args=[self], params={"window": _parse_window(window)})

    def rolling_sum(self, window: Union[int, str] = 3) -> "Expr":
        """Calculate rolling sum."""
        return Expr(ExprType.WINDOW, func=WindowFunc.ROLLING_SUM,
                    args=[self], params={"window": _parse_window(window)})

    def rolling_min(self, window: Union[int, str] = 3) -> "Expr":
        """Calculate rolling minimum."""
        return Expr(ExprType.WINDOW, func=WindowFunc.ROLLING_MIN,
                    args=[self], params={"window": _parse_window(window)})

    def rolling_max(self, window: Union[int, str] = 3) -> "Expr":
        """Calculate rolling maximum."""
        return Expr(ExprType.WINDOW, func=WindowFunc.ROLLING_MAX,
                    args=[self], params={"window": _parse_window(window)})

    def rolling_std(self, window: Union[int, str] = 3) -> "Expr":
        """Calculate rolling standard deviation."""
        return Expr(ExprType.WINDOW, func=WindowFunc.ROLLING_STDDEV,
                    args=[self], params={"window": _parse_window(window)})

    def lag(self, n: int = 1) -> "Expr":
        """Get value from n rows before."""
        return Expr(ExprType.WINDOW, func=WindowFunc.LAG,
                    args=[self], params={"n": n})

    def lead(self, n: int = 1) -> "Expr":
        """Get value from n rows after."""
        return Expr(ExprType.WINDOW, func=WindowFunc.LEAD,
                    args=[self], params={"n": n})

    def diff(self, n: int = 1) -> "Expr":
        """Calculate difference from n rows before."""
        return Expr(ExprType.WINDOW, func=WindowFunc.DIFF,
                    args=[self], params={"n": n})

    def pct_change(self, n: int = 1) -> "Expr":
        """Calculate percentage change from n rows before."""
        return Expr(ExprType.WINDOW, func=WindowFunc.PCT_CHANGE,
                    args=[self], params={"n": n})

    def cumsum(self) -> "Expr":
        """Calculate cumulative sum."""
        return Expr(ExprType.WINDOW, func=WindowFunc.CUMSUM, args=[self])

    def cummin(self) -> "Expr":
        """Calculate cumulative minimum."""
        return Expr(ExprType.WINDOW, func=WindowFunc.CUMMIN, args=[self])

    def cummax(self) -> "Expr":
        """Calculate cumulative maximum."""
        return Expr(ExprType.WINDOW, func=WindowFunc.CUMMAX, args=[self])

    # Math functions
    def abs(self) -> "Expr":
        """Absolute value."""
        return Expr(ExprType.FUNCTION, func="abs", args=[self])

    def sqrt(self) -> "Expr":
        """Square root."""
        return Expr(ExprType.FUNCTION, func="sqrt", args=[self])

    def pow(self, exponent: float) -> "Expr":
        """Power function."""
        return Expr(ExprType.FUNCTION, func="pow",
                    args=[self], params={"exponent": exponent})

    def log(self) -> "Expr":
        """Natural logarithm."""
        return Expr(ExprType.FUNCTION, func="log", args=[self])

    def exp(self) -> "Expr":
        """Exponential function."""
        return Expr(ExprType.FUNCTION, func="exp", args=[self])

    def floor(self) -> "Expr":
        """Floor function."""
        return Expr(ExprType.FUNCTION, func="floor", args=[self])

    def ceil(self) -> "Expr":
        """Ceiling function."""
        return Expr(ExprType.FUNCTION, func="ceil", args=[self])

    def round(self, decimals: int = 0) -> "Expr":
        """Round to specified decimals."""
        return Expr(ExprType.FUNCTION, func="round",
                    args=[self], params={"decimals": decimals})

    # Time functions
    def dt_year(self) -> "Expr":
        """Extract year from timestamp."""
        return Expr(ExprType.FUNCTION, func="dt_year", args=[self])

    def dt_month(self) -> "Expr":
        """Extract month from timestamp."""
        return Expr(ExprType.FUNCTION, func="dt_month", args=[self])

    def dt_day(self) -> "Expr":
        """Extract day from timestamp."""
        return Expr(ExprType.FUNCTION, func="dt_day", args=[self])

    def dt_hour(self) -> "Expr":
        """Extract hour from timestamp."""
        return Expr(ExprType.FUNCTION, func="dt_hour", args=[self])

    def dt_minute(self) -> "Expr":
        """Extract minute from timestamp."""
        return Expr(ExprType.FUNCTION, func="dt_minute", args=[self])

    def dt_second(self) -> "Expr":
        """Extract second from timestamp."""
        return Expr(ExprType.FUNCTION, func="dt_second", args=[self])

    def dt_weekday(self) -> "Expr":
        """Extract weekday (0=Monday)."""
        return Expr(ExprType.FUNCTION, func="dt_weekday", args=[self])

    def dt_truncate(self, unit: str) -> "Expr":
        """Truncate timestamp to unit (s, m, h, d)."""
        return Expr(ExprType.FUNCTION, func="dt_truncate",
                    args=[self], params={"unit": unit})

    # Membership
    def is_in(self, values: List[Any]) -> "Expr":
        """Check if value is in list."""
        return Expr(ExprType.FUNCTION, func="is_in",
                    args=[self], params={"values": values})

    def is_between(self, lower: Any, upper: Any) -> "Expr":
        """Check if value is between lower and upper."""
        return (self >= lower) & (self <= upper)


class WhenBuilder:
    """Builder for when/then/otherwise expressions."""

    def __init__(self, condition: Expr):
        self._conditions: List[Tuple[Expr, Expr]] = []
        self._current_condition = condition

    def then(self, value: Union[Expr, Any]) -> "WhenBuilder":
        """Specify the result when condition is true."""
        if not isinstance(value, Expr):
            value = lit(value)
        self._conditions.append((self._current_condition, value))
        return self

    def when(self, condition: Expr) -> "WhenBuilder":
        """Add another condition."""
        self._current_condition = condition
        return self

    def otherwise(self, value: Union[Expr, Any]) -> Expr:
        """Specify the default value."""
        if not isinstance(value, Expr):
            value = lit(value)
        return Expr(ExprType.CASE,
                    params={"conditions": self._conditions, "otherwise": value})


def _parse_window(window: Union[int, str]) -> int:
    """Parse window specification to number of points or nanoseconds."""
    if isinstance(window, int):
        return window

    # Parse time duration string (e.g., "5m", "1h")
    unit_map = {
        'ns': NANOSECOND,
        'us': MICROSECOND,
        'ms': MILLISECOND,
        's': SECOND,
        'm': MINUTE,
        'h': HOUR,
        'd': DAY,
    }

    for suffix, multiplier in unit_map.items():
        if window.endswith(suffix):
            return int(window[:-len(suffix)]) * multiplier

    return int(window)


class ChronicleConnection:
    """
    Connection to a Chronicle database with DataFrame support.
    """

    def __init__(self, path: str, **kwargs):
        """
        Create a connection to a Chronicle database.

        Args:
            path: Database file path
            **kwargs: Additional Chronicle configuration
        """
        config = Config(path=path, **{k: v for k, v in kwargs.items()
                                       if k in ['retention_hours', 'partition_duration_hours',
                                                'max_series', 'enable_compression', 'enable_wal']})
        self._db = Chronicle(path, config)
        self._path = path

    def query_df(
        self,
        metric: str,
        start: Optional[Union[int, datetime]] = None,
        end: Optional[Union[int, datetime]] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> "ChronicleDataFrame":
        """
        Query data and return a DataFrame.

        Args:
            metric: Metric name
            start: Optional start time
            end: Optional end time
            tags: Optional tag filters

        Returns:
            ChronicleDataFrame with lazy evaluation
        """
        return ChronicleDataFrame(
            connection=self,
            metric=metric,
            start=start,
            end=end,
            tags=tags,
        )

    def sql(self, query: str) -> "ChronicleDataFrame":
        """
        Execute SQL query and return DataFrame.

        Args:
            query: SQL query string

        Returns:
            ChronicleDataFrame with results
        """
        points = self._db.query_sql(query)
        return ChronicleDataFrame.from_points(points, connection=self)

    def write_df(
        self,
        df: "ChronicleDataFrame",
        metric: Optional[str] = None,
    ) -> int:
        """
        Write DataFrame to database.

        Args:
            df: DataFrame to write
            metric: Optional metric name (uses column if not specified)

        Returns:
            Number of points written
        """
        points = df.to_points(metric_column="metric" if metric is None else None)
        if metric:
            for p in points:
                p.metric = metric
        self._db.write_batch(points)
        return len(points)

    def metrics(self) -> List[str]:
        """Get all metric names."""
        return self._db.metrics()

    def stats(self) -> dict:
        """Get database statistics."""
        return self._db.stats()

    def flush(self):
        """Flush pending writes."""
        self._db.flush()

    def close(self):
        """Close the connection."""
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


@dataclass
class Schema:
    """DataFrame schema definition."""
    columns: Dict[str, str]  # column name -> type

    @classmethod
    def from_points(cls, points: List[Point]) -> "Schema":
        """Infer schema from points."""
        columns = {
            "timestamp": "int64",
            "metric": "string",
            "value": "float64",
        }
        # Add tag columns
        for point in points:
            for key in point.tags:
                columns[key] = "string"
        return cls(columns=columns)


class ChronicleDataFrame:
    """
    DataFrame interface for Chronicle time-series data.

    Supports lazy evaluation and chainable operations similar to
    pandas and polars DataFrames.
    """

    def __init__(
        self,
        connection: Optional[ChronicleConnection] = None,
        metric: Optional[str] = None,
        start: Optional[Union[int, datetime]] = None,
        end: Optional[Union[int, datetime]] = None,
        tags: Optional[Dict[str, str]] = None,
        data: Optional[List[Dict[str, Any]]] = None,
        schema: Optional[Schema] = None,
    ):
        """
        Create a ChronicleDataFrame.

        Args:
            connection: Database connection
            metric: Source metric
            start: Query start time
            end: Query end time
            tags: Tag filters
            data: Pre-loaded data (for in-memory operations)
            schema: DataFrame schema
        """
        self._connection = connection
        self._metric = metric
        self._start = start
        self._end = end
        self._tags = tags or {}
        self._data = data
        self._schema = schema

        # Lazy operations
        self._operations: List[Tuple[str, Any]] = []
        self._collected = False

    @classmethod
    def from_points(
        cls,
        points: List[Point],
        connection: Optional[ChronicleConnection] = None,
    ) -> "ChronicleDataFrame":
        """Create DataFrame from list of Points."""
        data = []
        for p in points:
            row = {
                "timestamp": p.timestamp,
                "metric": p.metric,
                "value": p.value,
            }
            row.update(p.tags)
            data.append(row)

        schema = Schema.from_points(points)
        return cls(connection=connection, data=data, schema=schema)

    @classmethod
    def from_dict(cls, data: Dict[str, List[Any]]) -> "ChronicleDataFrame":
        """Create DataFrame from dictionary of columns."""
        if not data:
            return cls(data=[])

        # Get column names and length
        columns = list(data.keys())
        length = len(data[columns[0]])

        # Convert to row-based format
        rows = []
        for i in range(length):
            row = {col: data[col][i] for col in columns}
            rows.append(row)

        return cls(data=rows)

    @classmethod
    def from_pandas(cls, df) -> "ChronicleDataFrame":
        """Create from pandas DataFrame."""
        data = df.to_dict('records')
        return cls(data=data)

    def _collect(self) -> List[Dict[str, Any]]:
        """Materialize the DataFrame by executing all operations."""
        if self._collected and self._data is not None:
            return self._data

        # Fetch from database if needed
        if self._data is None:
            if self._connection is None:
                raise ValueError("No data and no connection")

            points = self._connection._db.query(
                self._metric,
                tags=self._tags,
                start=self._start,
                end=self._end,
            )

            self._data = []
            for p in points:
                row = {
                    "timestamp": p.timestamp,
                    "metric": p.metric,
                    "value": p.value,
                }
                row.update(p.tags)
                self._data.append(row)

        # Apply operations
        data = self._data
        for op_name, op_args in self._operations:
            data = self._apply_operation(data, op_name, op_args)

        self._data = data
        self._operations = []
        self._collected = True
        return data

    def _apply_operation(
        self,
        data: List[Dict[str, Any]],
        op_name: str,
        op_args: Any
    ) -> List[Dict[str, Any]]:
        """Apply a single operation to the data."""
        if op_name == "filter":
            expr = op_args
            return [row for row in data if self._eval_expr(row, expr)]

        elif op_name == "select":
            columns = op_args
            return [{c: row.get(c) for c in columns} for row in data]

        elif op_name == "with_columns":
            exprs = op_args
            result = []
            for row in data:
                new_row = dict(row)
                for expr in exprs:
                    name = expr.alias_name or self._get_expr_name(expr)
                    new_row[name] = self._eval_expr(row, expr, data, result)
                result.append(new_row)
            return result

        elif op_name == "sort":
            columns, descending = op_args
            reverse = descending if isinstance(descending, bool) else descending[0] if descending else False
            return sorted(data, key=lambda r: tuple(r.get(c) for c in columns), reverse=reverse)

        elif op_name == "limit":
            n = op_args
            return data[:n]

        elif op_name == "head":
            n = op_args
            return data[:n]

        elif op_name == "tail":
            n = op_args
            return data[-n:]

        elif op_name == "unique":
            columns = op_args
            seen = set()
            result = []
            for row in data:
                key = tuple(row.get(c) for c in columns)
                if key not in seen:
                    seen.add(key)
                    result.append(row)
            return result

        elif op_name == "drop":
            columns = op_args
            return [{k: v for k, v in row.items() if k not in columns} for row in data]

        elif op_name == "rename":
            mapping = op_args
            return [{mapping.get(k, k): v for k, v in row.items()} for row in data]

        elif op_name == "group_by":
            columns, agg_exprs = op_args
            return self._apply_group_by(data, columns, agg_exprs)

        return data

    def _eval_expr(
        self,
        row: Dict[str, Any],
        expr: Expr,
        all_data: Optional[List[Dict[str, Any]]] = None,
        processed_data: Optional[List[Dict[str, Any]]] = None,
    ) -> Any:
        """Evaluate an expression for a single row."""
        if expr.type == ExprType.COLUMN:
            return row.get(expr.name)

        elif expr.type == ExprType.LITERAL:
            return expr.value

        elif expr.type == ExprType.BINARY_OP:
            left = self._eval_expr(row, expr.left, all_data, processed_data)
            right = self._eval_expr(row, expr.right, all_data, processed_data)

            if left is None or right is None:
                return None

            op = expr.op
            if op == BinaryOp.ADD: return left + right
            elif op == BinaryOp.SUB: return left - right
            elif op == BinaryOp.MUL: return left * right
            elif op == BinaryOp.DIV: return left / right if right != 0 else None
            elif op == BinaryOp.MOD: return left % right if right != 0 else None
            elif op == BinaryOp.EQ: return left == right
            elif op == BinaryOp.NE: return left != right
            elif op == BinaryOp.LT: return left < right
            elif op == BinaryOp.LE: return left <= right
            elif op == BinaryOp.GT: return left > right
            elif op == BinaryOp.GE: return left >= right
            elif op == BinaryOp.AND: return left and right
            elif op == BinaryOp.OR: return left or right

        elif expr.type == ExprType.UNARY_OP:
            val = self._eval_expr(row, expr.left, all_data, processed_data)
            if expr.op == "-": return -val if val is not None else None
            elif expr.op == "not": return not val

        elif expr.type == ExprType.FUNCTION:
            args = [self._eval_expr(row, a, all_data, processed_data) for a in expr.args]
            return self._eval_function(expr.func, args, expr.params)

        elif expr.type == ExprType.WINDOW:
            # Window functions need context
            return self._eval_window(row, expr, all_data, processed_data)

        return None

    def _eval_function(self, func: str, args: List[Any], params: Dict[str, Any]) -> Any:
        """Evaluate a scalar function."""
        val = args[0] if args else None

        if val is None:
            return None

        if func == "abs": return abs(val)
        elif func == "sqrt": return math.sqrt(val) if val >= 0 else None
        elif func == "log": return math.log(val) if val > 0 else None
        elif func == "exp": return math.exp(val)
        elif func == "floor": return math.floor(val)
        elif func == "ceil": return math.ceil(val)
        elif func == "round": return round(val, params.get("decimals", 0))
        elif func == "pow": return val ** params.get("exponent", 2)
        elif func == "is_null": return val is None
        elif func == "is_not_null": return val is not None
        elif func == "fill_null": return args[1] if val is None else val
        elif func == "is_in": return val in params.get("values", [])

        # Date functions (timestamp in nanoseconds)
        elif func == "dt_year":
            dt = datetime.fromtimestamp(val / SECOND)
            return dt.year
        elif func == "dt_month":
            dt = datetime.fromtimestamp(val / SECOND)
            return dt.month
        elif func == "dt_day":
            dt = datetime.fromtimestamp(val / SECOND)
            return dt.day
        elif func == "dt_hour":
            dt = datetime.fromtimestamp(val / SECOND)
            return dt.hour
        elif func == "dt_minute":
            dt = datetime.fromtimestamp(val / SECOND)
            return dt.minute
        elif func == "dt_second":
            dt = datetime.fromtimestamp(val / SECOND)
            return dt.second
        elif func == "dt_weekday":
            dt = datetime.fromtimestamp(val / SECOND)
            return dt.weekday()

        return val

    def _eval_window(
        self,
        row: Dict[str, Any],
        expr: Expr,
        all_data: Optional[List[Dict[str, Any]]],
        processed_data: Optional[List[Dict[str, Any]]],
    ) -> Any:
        """Evaluate a window function."""
        if all_data is None:
            return None

        # Find current row index
        idx = len(processed_data) if processed_data else 0

        # Get column values
        col_expr = expr.args[0] if expr.args else None
        if col_expr is None:
            return None

        col_name = col_expr.name if col_expr.type == ExprType.COLUMN else None
        if col_name is None:
            return None

        values = [r.get(col_name) for r in all_data]

        func = expr.func
        params = expr.params

        if func == WindowFunc.LAG:
            n = params.get("n", 1)
            return values[idx - n] if idx >= n else None

        elif func == WindowFunc.LEAD:
            n = params.get("n", 1)
            return values[idx + n] if idx + n < len(values) else None

        elif func == WindowFunc.DIFF:
            n = params.get("n", 1)
            if idx < n or values[idx] is None or values[idx - n] is None:
                return None
            return values[idx] - values[idx - n]

        elif func == WindowFunc.PCT_CHANGE:
            n = params.get("n", 1)
            if idx < n or values[idx] is None or values[idx - n] is None:
                return None
            prev = values[idx - n]
            if prev == 0:
                return None
            return (values[idx] - prev) / prev

        elif func == WindowFunc.CUMSUM:
            return sum(v for v in values[:idx + 1] if v is not None)

        elif func == WindowFunc.CUMMIN:
            valid = [v for v in values[:idx + 1] if v is not None]
            return min(valid) if valid else None

        elif func == WindowFunc.CUMMAX:
            valid = [v for v in values[:idx + 1] if v is not None]
            return max(valid) if valid else None

        # Rolling functions
        window = params.get("window", 3)
        if isinstance(window, int) and window > 0:
            start_idx = max(0, idx - window + 1)
            window_values = [v for v in values[start_idx:idx + 1] if v is not None]

            if not window_values:
                return None

            if func == WindowFunc.ROLLING_MEAN:
                return sum(window_values) / len(window_values)
            elif func == WindowFunc.ROLLING_SUM:
                return sum(window_values)
            elif func == WindowFunc.ROLLING_MIN:
                return min(window_values)
            elif func == WindowFunc.ROLLING_MAX:
                return max(window_values)
            elif func == WindowFunc.ROLLING_COUNT:
                return len(window_values)
            elif func == WindowFunc.ROLLING_STDDEV:
                if len(window_values) < 2:
                    return None
                mean = sum(window_values) / len(window_values)
                variance = sum((x - mean) ** 2 for x in window_values) / len(window_values)
                return math.sqrt(variance)

        return None

    def _apply_group_by(
        self,
        data: List[Dict[str, Any]],
        columns: List[str],
        agg_exprs: List[Expr],
    ) -> List[Dict[str, Any]]:
        """Apply group by with aggregations."""
        # Group data
        groups: Dict[tuple, List[Dict[str, Any]]] = {}
        for row in data:
            key = tuple(row.get(c) for c in columns)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)

        # Apply aggregations
        result = []
        for key, group_data in groups.items():
            row = {c: k for c, k in zip(columns, key)}

            for expr in agg_exprs:
                name = expr.alias_name or self._get_expr_name(expr)
                row[name] = self._eval_aggregation(group_data, expr)

            result.append(row)

        return result

    def _eval_aggregation(self, data: List[Dict[str, Any]], expr: Expr) -> Any:
        """Evaluate an aggregation expression on grouped data."""
        if expr.type != ExprType.AGGREGATION or not expr.args:
            return None

        col_expr = expr.args[0]
        if col_expr.type != ExprType.COLUMN:
            return None

        col_name = col_expr.name
        values = [r.get(col_name) for r in data if r.get(col_name) is not None]

        if not values:
            return None

        func = expr.func
        if func == AggFunc.COUNT: return len(values)
        elif func == AggFunc.SUM: return sum(values)
        elif func == AggFunc.MEAN: return sum(values) / len(values)
        elif func == AggFunc.MIN: return min(values)
        elif func == AggFunc.MAX: return max(values)
        elif func == AggFunc.FIRST: return values[0]
        elif func == AggFunc.LAST: return values[-1]
        elif func == AggFunc.STDDEV:
            if len(values) < 2:
                return None
            mean = sum(values) / len(values)
            variance = sum((x - mean) ** 2 for x in values) / len(values)
            return math.sqrt(variance)
        elif func == AggFunc.VAR:
            if len(values) < 2:
                return None
            mean = sum(values) / len(values)
            return sum((x - mean) ** 2 for x in values) / len(values)
        elif func == AggFunc.MEDIAN:
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            if n % 2 == 0:
                return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
            return sorted_vals[n // 2]
        elif func == AggFunc.PERCENTILE:
            q = expr.params.get("q", 0.5)
            sorted_vals = sorted(values)
            idx = int(len(sorted_vals) * q)
            return sorted_vals[min(idx, len(sorted_vals) - 1)]

        return None

    def _get_expr_name(self, expr: Expr) -> str:
        """Get a name for an expression."""
        if expr.alias_name:
            return expr.alias_name
        if expr.type == ExprType.COLUMN:
            return expr.name
        if expr.type == ExprType.AGGREGATION:
            func_name = expr.func.value if isinstance(expr.func, Enum) else str(expr.func)
            if expr.args and expr.args[0].type == ExprType.COLUMN:
                return f"{func_name}_{expr.args[0].name}"
            return func_name
        return "expr"

    # DataFrame operations
    def filter(self, expr: Expr) -> "ChronicleDataFrame":
        """Filter rows based on expression."""
        df = self._clone()
        df._operations.append(("filter", expr))
        return df

    def select(self, columns: Union[str, List[str], Expr, List[Expr]]) -> "ChronicleDataFrame":
        """Select specific columns."""
        if isinstance(columns, str):
            columns = [columns]
        elif isinstance(columns, Expr):
            columns = [columns.name] if columns.type == ExprType.COLUMN else ["value"]
        elif isinstance(columns, list) and columns and isinstance(columns[0], Expr):
            columns = [e.name if e.type == ExprType.COLUMN else "value" for e in columns]

        df = self._clone()
        df._operations.append(("select", columns))
        return df

    def with_columns(self, exprs: Union[Expr, List[Expr]]) -> "ChronicleDataFrame":
        """Add or replace columns."""
        if isinstance(exprs, Expr):
            exprs = [exprs]

        df = self._clone()
        df._operations.append(("with_columns", exprs))
        return df

    def sort(
        self,
        columns: Union[str, List[str]],
        descending: Union[bool, List[bool]] = False
    ) -> "ChronicleDataFrame":
        """Sort by columns."""
        if isinstance(columns, str):
            columns = [columns]

        df = self._clone()
        df._operations.append(("sort", (columns, descending)))
        return df

    def limit(self, n: int) -> "ChronicleDataFrame":
        """Limit to first n rows."""
        df = self._clone()
        df._operations.append(("limit", n))
        return df

    def head(self, n: int = 5) -> "ChronicleDataFrame":
        """Get first n rows."""
        df = self._clone()
        df._operations.append(("head", n))
        return df

    def tail(self, n: int = 5) -> "ChronicleDataFrame":
        """Get last n rows."""
        df = self._clone()
        df._operations.append(("tail", n))
        return df

    def unique(self, columns: Optional[List[str]] = None) -> "ChronicleDataFrame":
        """Get unique rows."""
        df = self._clone()
        cols = columns or list(self._schema.columns.keys()) if self._schema else ["value"]
        df._operations.append(("unique", cols))
        return df

    def drop(self, columns: Union[str, List[str]]) -> "ChronicleDataFrame":
        """Drop columns."""
        if isinstance(columns, str):
            columns = [columns]

        df = self._clone()
        df._operations.append(("drop", columns))
        return df

    def rename(self, mapping: Dict[str, str]) -> "ChronicleDataFrame":
        """Rename columns."""
        df = self._clone()
        df._operations.append(("rename", mapping))
        return df

    def group_by(self, columns: Union[str, List[str]]) -> "GroupBy":
        """Group by columns."""
        if isinstance(columns, str):
            columns = [columns]
        return GroupBy(self, columns)

    def join(
        self,
        other: "ChronicleDataFrame",
        on: Union[str, List[str]],
        how: str = "inner",
    ) -> "ChronicleDataFrame":
        """Join with another DataFrame."""
        if isinstance(on, str):
            on = [on]

        left_data = self._collect()
        right_data = other._collect()

        # Build index on right
        right_index: Dict[tuple, List[Dict]] = {}
        for row in right_data:
            key = tuple(row.get(c) for c in on)
            if key not in right_index:
                right_index[key] = []
            right_index[key].append(row)

        result = []
        for left_row in left_data:
            key = tuple(left_row.get(c) for c in on)
            matches = right_index.get(key, [])

            if matches:
                for right_row in matches:
                    merged = dict(left_row)
                    for k, v in right_row.items():
                        if k not in on:
                            merged[k] = v
                    result.append(merged)
            elif how == "left":
                result.append(dict(left_row))

        return ChronicleDataFrame(data=result, connection=self._connection)

    def _clone(self) -> "ChronicleDataFrame":
        """Create a copy of this DataFrame."""
        df = ChronicleDataFrame(
            connection=self._connection,
            metric=self._metric,
            start=self._start,
            end=self._end,
            tags=self._tags.copy() if self._tags else None,
            data=self._data,
            schema=self._schema,
        )
        df._operations = list(self._operations)
        df._collected = self._collected
        return df

    # Output methods
    def collect(self) -> "ChronicleDataFrame":
        """Force collection of lazy operations."""
        self._collect()
        return self

    def to_dicts(self) -> List[Dict[str, Any]]:
        """Convert to list of dictionaries."""
        return self._collect()

    def to_points(self, metric_column: Optional[str] = None) -> List[Point]:
        """Convert to list of Points."""
        data = self._collect()
        points = []
        for row in data:
            point = Point(
                metric=row.get(metric_column, row.get("metric", "unknown")),
                value=row.get("value", 0.0),
                timestamp=row.get("timestamp", 0),
                tags={k: str(v) for k, v in row.items()
                      if k not in ["metric", "value", "timestamp"] and v is not None},
            )
            points.append(point)
        return points

    def to_pandas(self):
        """Convert to pandas DataFrame."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for to_pandas()")

        return pd.DataFrame(self._collect())

    def to_polars(self):
        """Convert to polars DataFrame."""
        try:
            import polars as pl
        except ImportError:
            raise ImportError("polars is required for to_polars()")

        return pl.DataFrame(self._collect())

    def to_numpy(self, column: Optional[str] = None):
        """Convert column to numpy array."""
        try:
            import numpy as np
        except ImportError:
            raise ImportError("numpy is required for to_numpy()")

        data = self._collect()
        if column:
            return np.array([r.get(column) for r in data])
        return np.array([[v for v in r.values()] for r in data])

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self._collect())

    def to_csv(self) -> str:
        """Convert to CSV string."""
        data = self._collect()
        if not data:
            return ""

        columns = list(data[0].keys())
        lines = [",".join(columns)]
        for row in data:
            lines.append(",".join(str(row.get(c, "")) for c in columns))
        return "\n".join(lines)

    # Properties
    @property
    def columns(self) -> List[str]:
        """Get column names."""
        data = self._collect()
        if not data:
            return []
        return list(data[0].keys())

    @property
    def shape(self) -> Tuple[int, int]:
        """Get (rows, columns) shape."""
        data = self._collect()
        if not data:
            return (0, 0)
        return (len(data), len(data[0]))

    def __len__(self) -> int:
        """Get number of rows."""
        return len(self._collect())

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Iterate over rows."""
        return iter(self._collect())

    def __repr__(self) -> str:
        """String representation."""
        data = self._collect()
        n_rows = len(data)
        n_cols = len(data[0]) if data else 0
        return f"ChronicleDataFrame({n_rows} rows, {n_cols} columns)"


class GroupBy:
    """Group by operations."""

    def __init__(self, df: ChronicleDataFrame, columns: List[str]):
        self._df = df
        self._columns = columns

    def agg(self, exprs: Union[Expr, List[Expr]]) -> ChronicleDataFrame:
        """Apply aggregations."""
        if isinstance(exprs, Expr):
            exprs = [exprs]

        df = self._df._clone()
        df._operations.append(("group_by", (self._columns, exprs)))
        return df

    def count(self) -> ChronicleDataFrame:
        """Count rows per group."""
        return self.agg(col("value").count().alias("count"))

    def sum(self, column: str = "value") -> ChronicleDataFrame:
        """Sum values per group."""
        return self.agg(col(column).sum().alias(f"{column}_sum"))

    def mean(self, column: str = "value") -> ChronicleDataFrame:
        """Mean value per group."""
        return self.agg(col(column).mean().alias(f"{column}_mean"))

    def min(self, column: str = "value") -> ChronicleDataFrame:
        """Min value per group."""
        return self.agg(col(column).min().alias(f"{column}_min"))

    def max(self, column: str = "value") -> ChronicleDataFrame:
        """Max value per group."""
        return self.agg(col(column).max().alias(f"{column}_max"))

    def first(self, column: str = "value") -> ChronicleDataFrame:
        """First value per group."""
        return self.agg(col(column).first().alias(f"{column}_first"))

    def last(self, column: str = "value") -> ChronicleDataFrame:
        """Last value per group."""
        return self.agg(col(column).last().alias(f"{column}_last"))


# Utility functions
def read_chronicle(path: str, metric: str, **kwargs) -> ChronicleDataFrame:
    """
    Read data from Chronicle database.

    Args:
        path: Database path
        metric: Metric to query
        **kwargs: Additional query parameters

    Returns:
        ChronicleDataFrame
    """
    conn = connect(path)
    return conn.query_df(metric, **kwargs)


def concat(dfs: List[ChronicleDataFrame]) -> ChronicleDataFrame:
    """Concatenate DataFrames."""
    all_data = []
    for df in dfs:
        all_data.extend(df._collect())
    return ChronicleDataFrame(data=all_data)


if __name__ == "__main__":
    # Example usage
    print("Chronicle DataFrame API")
    print("=" * 40)

    # Create sample data
    sample_data = [
        {"timestamp": 1000000000, "metric": "cpu", "value": 45.0, "host": "server1"},
        {"timestamp": 1000001000, "metric": "cpu", "value": 55.0, "host": "server1"},
        {"timestamp": 1000002000, "metric": "cpu", "value": 65.0, "host": "server1"},
        {"timestamp": 1000003000, "metric": "cpu", "value": 75.0, "host": "server2"},
        {"timestamp": 1000004000, "metric": "cpu", "value": 35.0, "host": "server2"},
    ]

    df = ChronicleDataFrame(data=sample_data)

    print(f"\nOriginal: {df}")
    print(f"Columns: {df.columns}")
    print(f"Shape: {df.shape}")

    # Filter
    filtered = df.filter(col("value") > 50)
    print(f"\nFiltered (value > 50): {len(filtered)} rows")

    # With columns
    with_cols = df.with_columns([
        (col("value") * 2).alias("doubled"),
        col("value").rolling_mean(window=2).alias("rolling_avg"),
    ])
    print(f"\nWith new columns: {with_cols.columns}")

    # Group by
    grouped = df.group_by("host").agg([
        col("value").mean().alias("avg_cpu"),
        col("value").max().alias("max_cpu"),
        col("value").count().alias("count"),
    ])
    print(f"\nGrouped by host: {len(grouped)} groups")
    for row in grouped:
        print(f"  {row}")

    # Sort
    sorted_df = df.sort("value", descending=True)
    print(f"\nSorted by value (desc):")
    for row in sorted_df.head(3):
        print(f"  {row}")

    print("\nDone!")
