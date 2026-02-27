# Chronicle SDKs

This directory contains official Chronicle client SDKs for multiple programming languages.

## Supported Languages

| Language | Directory | Package Manager |
|----------|-----------|-----------------|
| Python | `python/` | pip |
| Java | `java/` | Maven |
| Node.js | `nodejs/` | npm |
| Rust | `rust/` | Cargo |

Each SDK provides a high-level client for writing and querying time-series data against a Chronicle server.

## Quick Start

### Python

```bash
pip install chronicle-client
```

```python
from chronicle import ChronicleClient

client = ChronicleClient("http://localhost:8086")
client.write("cpu", {"host": "server01"}, 0.64)
result = client.query("SELECT mean(value) FROM cpu GROUP BY time(5m)")
```

### Node.js

```bash
npm install chronicle-client
```

```javascript
const { ChronicleClient } = require('chronicle-client');

const client = new ChronicleClient('http://localhost:8086');
await client.write('cpu', { host: 'server01' }, 0.64);
const result = await client.query('SELECT mean(value) FROM cpu');
```

### Java

Add to `pom.xml`:

```xml
<dependency>
  <groupId>io.chronicle</groupId>
  <artifactId>chronicle-client</artifactId>
</dependency>
```

### Rust

Add to `Cargo.toml`:

```toml
[dependencies]
chronicle-client = "0.1"
```

## SDK Features

All SDKs provide:

- **Write** — Single and batch point ingestion
- **Query** — CQL and PromQL query execution
- **Streaming** — WebSocket-based real-time subscriptions
- **Connection pooling** — Efficient HTTP connection reuse
- **Retry logic** — Configurable retry with exponential backoff

See individual SDK directories for language-specific documentation and API references.
