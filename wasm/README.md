# Chronicle WASM Build

This directory contains the WebAssembly build for Chronicle.

## Building

```bash
# Build the WASM binary
GOOS=js GOARCH=wasm go build -o chronicle.wasm ./wasm

# Copy the Go WASM support file
cp "$(go env GOROOT)/misc/wasm/wasm_exec.js" ./wasm/
```

## Usage

### In Browser

```html
<script src="wasm_exec.js"></script>
<script>
    const go = new Go();
    WebAssembly.instantiateStreaming(fetch("chronicle.wasm"), go.importObject)
        .then((result) => {
            go.run(result.instance);
            // Chronicle is now available globally
        });
</script>
```

### API

```javascript
// Open a database (in-memory in browser)
await chronicle.open("mydb.db", {
    bufferSize: 1000,
    partitionDuration: "1h"
});

// Write a single point
await chronicle.write("cpu_usage", 45.5, { host: "server-1" });

// Write multiple points
await chronicle.writeBatch([
    { metric: "cpu_usage", value: 45.5, tags: { host: "server-1" } },
    { metric: "cpu_usage", value: 52.1, tags: { host: "server-2" } }
]);

// Query with SQL-like syntax
const results = await chronicle.query("SELECT mean(value) FROM cpu_usage GROUP BY time(5m)");

// Query with object
const results = await chronicle.execute({
    metric: "cpu_usage",
    tags: { host: "server-1" },
    start: Date.now() - 3600000,
    end: Date.now()
});

// Close database
await chronicle.close();
```

## Demo

Serve the files and open `index.html`:

```bash
cd wasm
python3 -m http.server 8080
# Open http://localhost:8080
```

## Limitations

- File system operations use in-memory storage in browsers
- No persistence between page reloads (unless using IndexedDB wrapper)
- Performance may vary based on browser WASM implementation

## Size Optimization

For production use, consider:

```bash
# Build with size optimizations
GOOS=js GOARCH=wasm go build -ldflags="-s -w" -o chronicle.wasm ./wasm

# Compress with Brotli
brotli chronicle.wasm
```
