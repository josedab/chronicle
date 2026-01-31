# ADR-0003: Adaptive Per-Column Compression Selection

## Status

Accepted

## Context

Time-series data consists of different data types with distinct characteristics:

- **Timestamps**: Monotonically increasing integers, often with regular intervals
- **Float values**: Metric measurements that may change slowly or have patterns
- **String tags**: Repeated categorical values (metric names, host names, regions)

A one-size-fits-all compression approach would sacrifice significant compression ratio. We needed a strategy that:

1. **Maximizes compression**: Different algorithms excel for different data types
2. **Minimizes CPU overhead**: Edge devices have limited compute resources
3. **Requires no user configuration**: Users shouldn't need to understand compression internals
4. **Handles edge cases**: Unusual data patterns shouldn't cause failures

We evaluated compression approaches:

- **Generic compression (gzip, snappy)**: Simple but suboptimal for structured time-series data
- **Fixed per-type compression**: Better but doesn't adapt to data characteristics
- **Adaptive selection**: Best compression but more complex implementation

## Decision

Chronicle implements **adaptive per-column compression** with specialized algorithms:

### Compression Algorithms

| Algorithm | Target Data | Technique |
|-----------|-------------|-----------|
| **Gorilla** | float64 values | XOR-based encoding exploiting value similarity |
| **Delta-of-Delta** | int64 timestamps | Variable-bit encoding of timestamp differences |
| **Dictionary** | strings (tags) | Integer indices into shared string table |
| **RLE** | repeated values | Run-length encoding for constant sequences |

### Selection Strategy

For each column, the encoder:

1. Identifies the data type (timestamp, float, string)
2. Applies all applicable compression algorithms
3. Selects the algorithm producing the smallest output
4. Stores the algorithm identifier with the compressed data

```go
type Column struct {
    Name     string
    DataType string  // "timestamp", "float64", "string"
    Encoding string  // "gorilla", "delta", "dictionary", "rle", "raw"
    Data     []byte  // compressed data
}
```

### Algorithm Details

**Gorilla (float64)**:
- First value stored in full (64 bits)
- Subsequent values: XOR with previous, encode only differing bits
- Tracks leading/trailing zero counts for adaptive bit-packing
- Achieves ~1.37 bytes/value for typical metrics (vs 8 bytes raw)

**Delta-of-Delta (timestamps)**:
- First value: full 64-bit
- Second value: varint delta
- Subsequent: delta-of-delta with variable encoding:
  - 1 bit if DoD = 0 (common for regular intervals)
  - 9 bits if DoD in [-63, 64]
  - 12 bits if DoD in [-255, 256]
  - 64 bits fallback

**Dictionary (strings)**:
- Build dictionary of unique strings
- Replace strings with varint indices
- Dictionary shared across partition for better compression

## Consequences

### Positive

- **Excellent compression ratios**: 
  - Timestamps: 10-100x compression for regular intervals
  - Floats: ~6x compression typical (8 bytes → ~1.4 bytes)
  - Strings: Proportional to repetition (often 10x+)
- **No configuration required**: Automatic selection removes user burden
- **Graceful degradation**: Falls back to raw encoding for incompressible data
- **Fast decompression**: All algorithms designed for sequential scan performance

### Negative

- **Compression CPU cost**: Trying multiple algorithms costs CPU during writes
- **Implementation complexity**: Four compression algorithms to maintain and test
- **Random access penalty**: Gorilla and delta encoding require sequential decompression from start
- **Memory during encoding**: Must buffer column data before compression

### Mitigations

- **Compression cost amortized**: Write buffering batches many points before compression
- **Parallel decompression**: Different columns can decompress in parallel
- **Block-level access**: Partition boundaries provide natural random access points

### Enabled

- Storage efficiency suitable for resource-constrained edge devices
- Competitive with specialized time-series compression libraries
- Column-level encoding metadata for future format evolution

### Prevented

- True random access within a partition (must decompress from partition start)
- User-specified compression preferences (could be added if needed)
