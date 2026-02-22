# Testing Guide

This document describes how to write, run, and debug tests for Chronicle.

## Running Tests

```bash
# Quick validation (vet + internal tests, ~15s)
make check

# All tests, short mode (~30s)
make test-short

# All tests with race detector (~45s)
make test

# Internal packages only (~15s)
make test-fast

# With coverage summary
make cover

# With HTML coverage report
make test-cover
open coverage.html

# Run a specific test
go test -run TestWriteAndQuery -count=1 -v

# Run benchmarks
make bench
```

## Writing Tests

### Use the shared test helper

Most tests need a database. Use `setupTestDB` instead of manually creating
a temp directory, config, and calling Open:

```go
func TestMyFeature(t *testing.T) {
    db := setupTestDB(t)  // auto cleanup via t.TempDir()
    defer db.Close()

    // ... test logic ...
}
```

### Always flush before querying

The default buffer holds 10,000 points. Small test writes stay in the buffer
and are **not visible to queries** until flushed:

```go
// ❌ Wrong — query returns 0 points
db.Write(Point{Metric: "cpu", Value: 42, Timestamp: time.Now().UnixNano()})
result, _ := db.Execute(&Query{Metric: "cpu"})
// result.Points is empty!

// ✅ Correct — flush first
db.Write(Point{Metric: "cpu", Value: 42, Timestamp: time.Now().UnixNano()})
db.Flush()
result, _ := db.Execute(&Query{Metric: "cpu"})
// result.Points has 1 element
```

### Use writeTestPoints for bulk data

```go
func TestAnalytics(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close()

    // Writes 100 points for "cpu.usage", spaced 1s apart, and flushes
    writeTestPoints(t, db, "cpu.usage", 100, time.Now().Add(-time.Hour))
    assertPointCount(t, db, "cpu.usage", 100)
}
```

### Query time ranges

When querying with `Start`/`End` time bounds, use a start time **before the
hour boundary** that contains your data. The internal partition index uses
hour-aligned boundaries, and a query start exactly at `time.Now()` may miss
the current partition:

```go
// ✅ Safe — starts before the partition boundary
result, _ := db.Execute(&Query{
    Metric: "cpu",
    Start:  time.Now().Add(-time.Hour).UnixNano(),
    End:    time.Now().Add(time.Hour).UnixNano(),
})

// ✅ Also safe — no time bounds
result, _ := db.Execute(&Query{Metric: "cpu"})
```

## Test File Template

Use this pattern when creating a new test file. It follows Chronicle conventions:
table-driven tests, `t.Run` subtests, and the shared `setupTestDB` helper.

```go
package chronicle

import (
	"testing"
	"time"
)

func TestMyFeature(t *testing.T) {
	t.Parallel()
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		input   Point
		wantErr bool
	}{
		{
			name: "basic write",
			input: Point{
				Metric:    "cpu.usage",
				Tags:      map[string]string{"host": "server1"},
				Value:     42.0,
				Timestamp: time.Now().UnixNano(),
			},
		},
		{
			name: "missing metric",
			input: Point{
				Value:     1.0,
				Timestamp: time.Now().UnixNano(),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.Write(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
```

**Key conventions:**
- File: `<feature>_test.go` in `package chronicle` (not `_test` suffix)
- Use `setupTestDB(t)` — auto cleanup via `t.TempDir()`
- Use `writeTestPoints(t, db, metric, count, start)` for bulk data
- Use `assertPointCount(t, db, metric, expected)` for verification
- Call `db.Flush()` before querying (see [Common Pitfalls](#common-pitfalls))

## Parallel Tests

Add `t.Parallel()` as the first line of any top-level test function that uses
isolated state (e.g., `setupTestDB`, `t.TempDir()`, or pure-function tests):

```go
func TestMyFeature(t *testing.T) {
    t.Parallel()
    db := setupTestDB(t) // creates isolated temp dir
    defer db.Close()
    // ...
}
```

**When to use `t.Parallel()`:**
- ✅ Tests that use `setupTestDB(t)` or `t.TempDir()` — each gets its own directory
- ✅ Pure function tests (parsers, validators, formatters)
- ✅ Tests using `httptest.NewRecorder()` with isolated DB instances
- ❌ Tests that modify global state (`os.Setenv`, package-level variables)
- ❌ Tests that share a database instance across subtests
- ❌ Tests that rely on specific timing or ordering

## Test File Conventions

- Test files use `package chronicle` (not `_test` suffix) for access to internals
- Helper functions live in `admin_ui_test_helpers_test.go`
- Internal packages can use `internal/testutil` for shared path helpers
- Each feature's tests live in `<feature>_test.go`

## Environment Variables in Tests

Use `t.Setenv` instead of `os.Setenv`/`os.Unsetenv`. It automatically restores
the original value when the test completes:

```go
// ✅ Correct — automatically restored after test
func TestWithEnv(t *testing.T) {
    t.Setenv("CHRONICLE_DEBUG", "true")
    // ... test logic ...
}

// ❌ Wrong — leaks state between tests, not parallel-safe
func TestWithEnv(t *testing.T) {
    os.Setenv("CHRONICLE_DEBUG", "true")
    defer os.Unsetenv("CHRONICLE_DEBUG")
    // ...
}
```

> **Note:** `t.Setenv` calls `t.Setenv` internally marks the test as incompatible
> with `t.Parallel()`. If your test needs both env vars and parallelism, restructure
> the test to avoid environment mutation.

## Common Pitfalls

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Query returns 0 points | Missing `db.Flush()` | Add `db.Flush()` after writes |
| Points not found with Start/End | Start time after partition boundary | Use `time.Now().Add(-time.Hour)` |
| `go vet` lock-copy warning | Struct with `sync.Mutex` copied by value | Use pointer semantics |
| Sub-millisecond latency = 0 | `Duration.Milliseconds()` truncates | Use `Microseconds()/1000.0` |
| Query optimizer cache hits | Same metric name reused across iterations | Use unique metric names |

## Debugging Tips

```bash
# Run a single test with verbose output
go test -run TestMyFeature -count=1 -v

# Run with race detector
go test -run TestMyFeature -race -count=1

# Print all goroutines on failure
GOTRACEBACK=all go test -run TestMyFeature -count=1

# Profile a test
go test -run TestMyFeature -cpuprofile=cpu.out -memprofile=mem.out
go tool pprof cpu.out
```
