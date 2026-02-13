# Python Client SDK

Chronicle provides a Python client for data engineers and data scientists who need to interact with Chronicle from Python.

## Installation

```bash
pip install chronicle-client
```

## Quick Start

```python
from chronicle import ChronicleClient

# Connect to a Chronicle HTTP server
client = ChronicleClient("http://localhost:8080")

# Write data
client.write("cpu.usage", 42.5, tags={"host": "prod-1", "region": "us-east"})

# Write batch
client.write_batch([
    {"metric": "temperature", "value": 22.1, "tags": {"sensor": "s1"}},
    {"metric": "temperature", "value": 23.5, "tags": {"sensor": "s2"}},
    {"metric": "humidity", "value": 65.0, "tags": {"sensor": "s1"}},
])

# Query
result = client.query("temperature", start="-1h", tags={"sensor": "s1"})
for point in result.points:
    print(f"{point.timestamp}: {point.value}")
```

## pandas Integration

```python
import pandas as pd
from chronicle import ChronicleClient

client = ChronicleClient("http://localhost:8080")

# Read as DataFrame
df = client.to_dataframe("cpu.usage", start="-24h", tags={"host": "prod-1"})
print(df.describe())

# Write from DataFrame
df = pd.DataFrame({
    "timestamp": pd.date_range("2024-01-01", periods=100, freq="1min"),
    "value": range(100),
})
client.from_dataframe("my_metric", df, tags={"source": "pandas"})
```

## Async Support

```python
import asyncio
from chronicle import AsyncChronicleClient

async def main():
    client = AsyncChronicleClient("http://localhost:8080")

    # Async write
    await client.write("async.metric", 42.0)

    # Async query
    result = await client.query("async.metric", start="-1h")
    print(f"Got {len(result.points)} points")

    await client.close()

asyncio.run(main())
```

## API Reference

### ChronicleClient

| Method | Description |
|--------|-------------|
| `write(metric, value, tags=None, timestamp=None)` | Write a single point |
| `write_batch(points)` | Write multiple points |
| `query(metric, start=None, end=None, tags=None, limit=None)` | Query time-series data |
| `query_sql(sql)` | Execute a SQL query |
| `query_promql(promql)` | Execute a PromQL query |
| `metrics()` | List all known metric names |
| `health()` | Check server health |
| `to_dataframe(metric, ...)` | Query and return as pandas DataFrame |
| `from_dataframe(metric, df, ...)` | Write pandas DataFrame as points |

### Configuration

```python
client = ChronicleClient(
    url="http://localhost:8080",
    api_key="your-api-key",          # Optional authentication
    timeout=30,                       # Request timeout in seconds
    max_retries=3,                   # Retry failed requests
    batch_size=10000,                # Max points per batch
    compression=True,                # Enable gzip compression
)
```

## Auto-Generation

The Python client is auto-generated from Chronicle's OpenAPI specification:

```bash
# Generate from OpenAPI spec
chronicle openapi > openapi.json
openapi-python-client generate --path openapi.json --output chronicle-client
```

## Development

```bash
git clone https://github.com/chronicle-db/chronicle-python.git
cd chronicle-python
pip install -e ".[dev]"
pytest
```
