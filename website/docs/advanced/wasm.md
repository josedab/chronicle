---
sidebar_position: 5
---

# WASM Compilation

Chronicle can be compiled to WebAssembly (WASM) for running in browsers and edge environments.

## Overview

WASM compilation enables:
- Client-side time-series analysis in browsers
- Edge computing with Chronicle
- Embedded systems support
- Cross-platform deployment

## Building for WASM

### Prerequisites

```bash
# Install TinyGo (recommended for smaller binaries)
brew install tinygo  # macOS
# or
go install github.com/AgileBits/tinygo@latest
```

### Standard Go WASM

```bash
GOOS=js GOARCH=wasm go build -o chronicle.wasm ./wasm
```

### TinyGo (Smaller Binary)

```bash
tinygo build -o chronicle.wasm -target wasm ./wasm
```

### Binary Sizes

| Build | Size |
|-------|------|
| Go WASM | ~15 MB |
| TinyGo WASM | ~2 MB |
| TinyGo + compression | ~500 KB |

## Browser Usage

### Load WASM Module

```html
<!DOCTYPE html>
<html>
<head>
    <script src="wasm_exec.js"></script>
    <script>
        const go = new Go();
        WebAssembly.instantiateStreaming(fetch("chronicle.wasm"), go.importObject)
            .then((result) => {
                go.run(result.instance);
                console.log("Chronicle WASM loaded");
            });
    </script>
</head>
<body>
    <div id="app"></div>
</body>
</html>
```

### JavaScript API

```javascript
// Create in-memory database
const db = Chronicle.open(":memory:");

// Write data
db.write({
    metric: "page_load_time",
    tags: { page: "/home" },
    value: 1.23,
    timestamp: Date.now() * 1000000
});

// Query data
const result = db.query({
    metric: "page_load_time",
    start: Date.now() - 3600000,
    end: Date.now()
});

console.log(result.points);

// Close when done
db.close();
```

## API Reference

### JavaScript Bindings

```typescript
// TypeScript definitions
declare namespace Chronicle {
    function open(path: string): Database;
    
    interface Database {
        write(point: Point): void;
        writeBatch(points: Point[]): void;
        query(query: Query): Result;
        execute(sql: string): Result;
        metrics(): string[];
        close(): void;
    }
    
    interface Point {
        metric: string;
        tags?: Record<string, string>;
        value: number;
        timestamp?: number;
    }
    
    interface Query {
        metric: string;
        tags?: Record<string, string>;
        start?: number;
        end?: number;
        aggregation?: Aggregation;
    }
    
    interface Result {
        points: Point[];
    }
}
```

## Use Cases

### Client-Side Analytics

```javascript
// Track client-side metrics
const db = Chronicle.open(":memory:");

// Capture performance metrics
const observer = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
        db.write({
            metric: entry.entryType,
            tags: { name: entry.name },
            value: entry.duration
        });
    }
});
observer.observe({ entryTypes: ['measure', 'resource'] });

// Query aggregated data
function getAverageLoadTime() {
    const result = db.query({
        metric: "resource",
        aggregation: { function: "mean" }
    });
    return result.points[0]?.value || 0;
}
```

### Edge Computing

```javascript
// Cloudflare Worker / Deno Deploy
import { Chronicle } from './chronicle.wasm';

addEventListener('fetch', async (event) => {
    const db = Chronicle.open(":memory:");
    
    // Process request metrics
    const start = Date.now();
    const response = await handleRequest(event.request);
    const duration = Date.now() - start;
    
    db.write({
        metric: "request_duration",
        tags: { path: new URL(event.request.url).pathname },
        value: duration
    });
    
    return response;
});
```

### Offline-First Applications

```javascript
// IndexedDB persistence
class OfflineChronicle {
    constructor() {
        this.db = Chronicle.open(":memory:");
        this.loadFromIndexedDB();
    }
    
    async loadFromIndexedDB() {
        const stored = await idb.get('chronicle-data');
        if (stored) {
            for (const point of stored) {
                this.db.write(point);
            }
        }
    }
    
    async persist() {
        const result = this.db.query({ metric: '*' });
        await idb.set('chronicle-data', result.points);
    }
    
    write(point) {
        this.db.write(point);
        // Debounced persist
        this.schedulePersist();
    }
}
```

## Limitations

WASM builds have some limitations compared to native:

| Feature | Native | WASM |
|---------|--------|------|
| File I/O | ✅ | ❌ Memory only |
| HTTP Server | ✅ | ❌ |
| Goroutines | ✅ | ⚠️ Limited |
| CGO | ✅ | ❌ |
| Compression | ✅ | ⚠️ Slower |
| Max memory | Unlimited | ~4GB |

## Performance Tips

### Minimize Allocations

```javascript
// ❌ Creates garbage
for (let i = 0; i < 1000; i++) {
    db.write({ metric: "test", value: i });
}

// ✅ Batch writes
const points = [];
for (let i = 0; i < 1000; i++) {
    points.push({ metric: "test", value: i });
}
db.writeBatch(points);
```

### Use Typed Arrays

```javascript
// ❌ Slow: regular arrays
const values = [1.0, 2.0, 3.0, ...];

// ✅ Fast: typed arrays
const values = new Float64Array([1.0, 2.0, 3.0, ...]);
```

### Memory Management

```javascript
// Release resources when done
const db = Chronicle.open(":memory:");
// ... use db ...
db.close();  // Free memory
```

## Building Custom WASM

### Minimal Build

```go
// wasm/main.go
//go:build js && wasm

package main

import (
    "syscall/js"
    "github.com/chronicle-db/chronicle"
)

func main() {
    js.Global().Set("Chronicle", map[string]interface{}{
        "open": js.FuncOf(openDB),
    })
    
    // Keep alive
    select {}
}

func openDB(this js.Value, args []js.Value) interface{} {
    // Implementation
}
```

### Build Script

```bash
#!/bin/bash
# build-wasm.sh

GOOS=js GOARCH=wasm go build -ldflags="-s -w" -o chronicle.wasm ./wasm

# Optimize with wasm-opt (optional)
wasm-opt -O3 chronicle.wasm -o chronicle-opt.wasm

# Compress
gzip -k chronicle-opt.wasm

echo "Built: chronicle-opt.wasm.gz ($(du -h chronicle-opt.wasm.gz | cut -f1))"
```
