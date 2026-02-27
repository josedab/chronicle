# Chronicle FFI Bindings

This directory contains Foreign Function Interface (FFI) bindings that allow embedding Chronicle directly into applications written in other languages, without requiring a separate server process.

## Contents

| Path | Description |
|------|-------------|
| `chronicle.h` | C header file defining the FFI interface |
| `python/` | Python bindings (ctypes/cffi) |
| `node/` | Node.js native bindings (N-API) |
| `rust/` | Rust FFI bindings |

## Overview

Unlike the [SDKs](../sdk/), which communicate with a Chronicle server over HTTP, the FFI bindings link directly against the Chronicle shared library. This enables:

- **Embedded databases** — No separate server process required
- **Lower latency** — Direct function calls instead of HTTP round-trips
- **In-process access** — Use Chronicle as an embedded time-series engine

## Build Requirements

1. **Go toolchain** (1.24+) with CGO enabled
2. **C compiler** (gcc or clang)
3. Platform-specific build tools

Build the shared library:

```bash
make cffi
# Produces: libchronicle.so (Linux), libchronicle.dylib (macOS)
```

## Usage

### Python

```python
from chronicle import Chronicle

db = Chronicle("/tmp/mydb")
db.write("cpu", {"host": "server01"}, 0.64)
results = db.query("SELECT * FROM cpu")
db.close()
```

### Node.js

```javascript
const chronicle = require('./chronicle');

const db = chronicle.open('/tmp/mydb');
db.write('cpu', { host: 'server01' }, 0.64);
db.close();
```

### C

```c
#include "chronicle.h"

chronicle_config cfg = {.path = "/tmp/mydb"};
chronicle_db *db = chronicle_open(cfg);
chronicle_write(db, "cpu,host=server01 value=0.64");
chronicle_close(db);
```

## API Reference

See `chronicle.h` for the complete C API documentation. The Python and Node.js bindings mirror the C API with language-idiomatic wrappers.
