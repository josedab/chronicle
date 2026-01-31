# Chronicle Benchmarks

Benchmarks run on Apple M1 Max, 16GB RAM, macOS 14, Go 1.24.

## Quick Results

```
go test -bench=. -benchmem

BenchmarkWriteBatch-10          	     100	  13159807 ns/op	 7513681 B/op	  117038 allocs/op
BenchmarkQueryAggregate-10      	    1112	   1001847 ns/op	  424798 B/op	   21028 allocs/op
BenchmarkSchemaValidation-10    	12846394	        91.05 ns/op	       0 B/op	       0 allocs/op
BenchmarkEncryption-10          	  454695	      2839 ns/op	    8976 B/op	       3 allocs/op
BenchmarkPromQLParse-10         	  600037	      2026 ns/op	    1376 B/op	      25 allocs/op
BenchmarkStreamingPublish-10    	  740533	      1635 ns/op	       0 B/op	       0 allocs/op
BenchmarkMultiTenantWrite-10    	  527542	      2400 ns/op	    2823 B/op	      77 allocs/op
```

## Write Performance

| Operation | ops/sec | ns/op | Allocs/op | Bytes/op |
|-----------|---------|-------|-----------|----------|
| Batch Write (10K points) | 76 | 13.2M | 117,038 | 7.5MB |
| Multi-Tenant Write | 416K | 2,400 | 77 | 2.8KB |

## Query Performance

| Query Type | ops/sec | ns/op | Allocs | Notes |
|------------|---------|-------|--------|-------|
| Aggregate (mean, 50 hosts, 10K points) | 998 | 1M | 21,028 | GROUP BY host |

## Feature Benchmarks

| Feature | ops/sec | ns/op | Allocs | Notes |
|---------|---------|-------|--------|-------|
| Schema Validation | 10.9M | 91 | 0 | Zero allocation validation |
| Encryption (4KB) | 352K | 2,839 | 3 | AES-256-GCM encrypt+decrypt |
| PromQL Parse (4 queries) | 493K | 2,026 | 25 | Mixed complexity queries |
| Streaming Publish (100 subs) | 611K | 1,635 | 0 | Fan-out to subscribers |

## Key Insights

- **Schema validation is essentially free** at 91ns with zero allocations
- **Encryption overhead is minimal** at ~3μs for 4KB blocks
- **PromQL parsing** is fast enough for real-time query translation
- **Streaming fan-out** to 100 subscribers costs <2μs per message

## Running Benchmarks

```bash
# All benchmarks
make bench

# Specific benchmark
go test -bench=BenchmarkWrite -benchmem

# With CPU profiling
go test -bench=. -cpuprofile=cpu.out
go tool pprof cpu.out

# With memory profiling
go test -bench=. -memprofile=mem.out
go tool pprof mem.out
```

---

*Last updated: 2026-01-29*
*Chronicle version: v0.4.0*
