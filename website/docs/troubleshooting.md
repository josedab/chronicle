---
sidebar_position: 101
---

# Troubleshooting

Common issues and their solutions.

## Startup Issues

### "file is locked" error

```
error: database file is locked
```

**Cause:** Another process has the database file open.

**Solutions:**
1. Ensure only one process opens the database
2. Check for zombie processes: `lsof | grep data.db`
3. If the process crashed, delete the lock file (if present)

### "path is required" error

```
error: path is required
```

**Cause:** Empty path in configuration.

**Solution:**
```go
// Always specify a path
db, err := chronicle.Open("data.db", chronicle.Config{
    Path: "data.db",  // Required
})
```

### "WAL replay failed" error

```
error: WAL replay failed: corrupt entry
```

**Cause:** WAL file corruption, possibly from crash or disk issue.

**Solutions:**
1. **Option 1:** Delete WAL files (loses uncommitted data)
   ```bash
   rm data.db.wal*
   ```
2. **Option 2:** Restore from backup
3. **Option 3:** Try to recover partial data
   ```go
   db, err := chronicle.Open("data.db", chronicle.Config{
       IgnoreCorruptWAL: true,
   })
   ```

## Write Issues

### "schema validation failed" error

```
error: schema validation failed: value 150 exceeds max 100
```

**Cause:** Data doesn't match registered schema.

**Solutions:**
1. Fix the data to match schema
2. Update the schema
3. Disable strict schema validation

### "cardinality limit exceeded" error

```
error: cardinality limit exceeded: max 100000 series
```

**Cause:** Too many unique series (metric + tag combinations).

**Solutions:**
1. Reduce tag cardinality (avoid high-cardinality tags like user IDs)
2. Increase the limit:
   ```go
   tracker := chronicle.NewCardinalityTracker(db, chronicle.CardinalityConfig{
       MaxTotalSeries: 1_000_000,
   })
   ```
3. Delete old series that are no longer needed

### Writes are slow

**Possible causes and solutions:**

| Cause | Solution |
|-------|----------|
| Too frequent WAL sync | Increase `SyncInterval` |
| Small buffer | Increase `BufferSize` |
| Single writes | Use `WriteBatch` |
| Slow disk | Use SSD/NVMe |

```go
// Optimized for write throughput
config := chronicle.Config{
    SyncInterval: 5 * time.Second,  // Less frequent sync
    BufferSize:   100_000,          // Larger buffer
}
```

## Query Issues

### "query timeout" error

```
error: query timeout
```

**Cause:** Query took longer than `QueryTimeout`.

**Solutions:**
1. Add time bounds to reduce scan range
2. Increase timeout:
   ```go
   config := chronicle.Config{
       QueryTimeout: 60 * time.Second,
   }
   ```
3. Use aggregation to reduce data volume
4. Add indexes via tag filters

### "query memory budget exceeded" error

```
error: query memory budget exceeded
```

**Cause:** Query result too large for memory budget.

**Solutions:**
1. Add `LIMIT` to query
2. Use aggregation to reduce results
3. Narrow time range
4. Increase `MaxMemory`:
   ```go
   config := chronicle.Config{
       MaxMemory: 512 * 1024 * 1024,  // 512MB
   }
   ```

### Queries return no data

**Checklist:**
1. Verify metric name is correct (case-sensitive)
2. Check time range includes data
3. Verify tags match exactly
4. Flush writes before querying:
   ```go
   db.Flush()
   result, _ := db.Execute(query)
   ```

### PromQL query fails

```
error: unknown function 'xyz'
```

**Cause:** PromQL function not supported.

**Solution:** Check [supported PromQL features](/docs/guides/prometheus-integration#supported-promql-features). Use native Chronicle queries for unsupported features.

## Performance Issues

### High memory usage

**Diagnosis:**
```go
stats := db.CardinalityStats()
fmt.Printf("Series: %d\n", stats.TotalSeries)
```

**Solutions:**
1. Reduce `MaxMemory` in config
2. Reduce `BufferSize`
3. Enable retention to delete old data
4. Reduce cardinality (fewer unique series)

### High CPU usage

**Common causes:**
1. **Compaction** - Normal during background compaction
2. **Queries** - Complex aggregations
3. **High write rate** - Consider batching

**Solutions:**
```go
config := chronicle.Config{
    CompactionWorkers:  1,              // Reduce parallel compaction
    CompactionInterval: 60 * time.Minute, // Less frequent
}
```

### High disk usage

**Solutions:**
1. Enable retention:
   ```go
   config := chronicle.Config{
       RetentionDuration: 7 * 24 * time.Hour,
   }
   ```
2. Enable downsampling for old data
3. Run manual compaction
4. Check for cardinality explosion

### Disk I/O spikes

**Causes and solutions:**
- **WAL sync** - Increase `SyncInterval`
- **Compaction** - Reduce `CompactionWorkers`
- **Queries** - Add memory for caching

## HTTP API Issues

### Connection refused

**Checklist:**
1. Is HTTP enabled?
   ```go
   config := chronicle.Config{
       HTTPEnabled: true,
       HTTPPort:    8086,
   }
   ```
2. Check firewall/security groups
3. Verify correct port
4. Chronicle binds to `127.0.0.1` by default

### 404 Not Found

**Cause:** Endpoint doesn't exist or wrong HTTP method.

**Solutions:**
1. Check endpoint URL
2. Verify HTTP method (GET vs POST)
3. See [HTTP Endpoints](/docs/api-reference/http-endpoints) for correct URLs

### 500 Internal Server Error

**Diagnosis:** Check server logs for stack trace.

**Common causes:**
- Database file issues
- Memory exhaustion
- Concurrent access problems

## Data Corruption

### Symptoms

- Queries return unexpected results
- "checksum mismatch" errors
- Database won't open

### Recovery Steps

1. **Stop Chronicle immediately**

2. **Check disk health**
   ```bash
   dmesg | grep -i error
   smartctl -a /dev/sda
   ```

3. **Try to open with recovery mode**
   ```go
   db, err := chronicle.Open("data.db", chronicle.Config{
       RecoveryMode: true,
   })
   ```

4. **Restore from backup** (recommended)
   ```bash
   chronicle restore --source /backup/latest --dest /data/
   ```

5. **Export salvageable data**
   ```go
   exporter := chronicle.NewExporter(db)
   exporter.Export(ctx, chronicle.ExportOptions{
       IgnoreErrors: true,
       Output:       file,
   })
   ```

## Getting Help

If you can't resolve an issue:

1. **Check GitHub Issues** - Someone may have encountered the same problem
2. **Enable debug logging** - Get more details about what's happening
3. **Create a minimal reproduction** - Isolate the problem
4. **Open a GitHub Issue** with:
   - Chronicle version
   - Go version
   - Operating system
   - Configuration
   - Error message and stack trace
   - Steps to reproduce
