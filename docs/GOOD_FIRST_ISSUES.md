# Good First Issues

Looking to contribute? Here are well-scoped tasks that are great for getting started.
Each issue includes the relevant files, expected approach, and acceptance criteria.

## Documentation (No Go Required)

### 1. Add GoDoc examples for 5 more exported types
**Files**: `example_test.go`
**Task**: Add `Example` test functions for `SchemaRegistry`, `AlertManager`, `Forecaster`, `StreamHub`, and `ExemplarStore`.
**Criteria**: `go test -run Example -v` passes; examples show in `go doc`.

### 2. Improve FAQ with common error messages
**Files**: `docs/FAQ.md`
**Task**: Add 5 common error messages users might see (e.g., "WAL file locked", "partition not found") with solutions.

### 3. Document HTTP API response formats
**Files**: `docs/API.md`
**Task**: Add response body examples (JSON) for `/write`, `/query`, `/metrics`, `/health` endpoints.

## Testing

### 4. Add edge-case tests for Config validation
**Files**: `config_test.go`
**Task**: Add tests for: zero-value PartitionDuration, very large MaxMemory, empty encryption password with key provided.

### 5. Add regex tag filter end-to-end test
**Files**: `integration_test.go`
**Task**: Write test that writes data with varied tags and queries with `TagOpRegex` and `TagOpNotRegex`, verifying correct filtering.

### 6. Add PromQL binary operator edge cases
**Files**: `promql_precedence_test.go`
**Task**: Add tests for deeply nested expressions like `(a + b) * (c - d)`, division by zero handling, chained comparisons.

### 7. Test WAL recovery after crash
**Files**: `wal_test.go`
**Task**: Write a test that writes points, kills the WAL mid-flush (simulate by truncating file), then verifies recovery reads valid points.

## Code Quality

### 8. Replace magic numbers with named constants
**Files**: `wal.go`, `buffer.go`, `partition_core.go`
**Task**: Find magic numbers (e.g., buffer sizes, timeouts) and replace with descriptively named `const` declarations.

### 9. Add `String()` methods to enum types
**Files**: `query.go`
**Task**: Add `String()` method to `AggFunc` and `TagOp` types for better debug output.

### 10. Improve error messages in parser.go
**Files**: `parser.go`
**Task**: Replace generic "parse error" messages with specific context (e.g., "expected metric name after SELECT, got '{'").

## Small Features

### 11. Add `DB.PointCount()` method
**Files**: `db_core.go`, `db_test.go`
**Task**: Add method that returns total number of points across all partitions. Used by CLI `inspect` command.

### 12. Add JSON output to CLI `inspect` command
**Files**: `cmd/chronicle/main.go`
**Task**: Add `-json` flag to `chronicle inspect` that outputs database stats as JSON (for scripting).

### 13. Add `--format` flag to CLI `query` command
**Files**: `cmd/chronicle/main.go`
**Task**: Support `--format=json|csv|table` for query output formatting.

### 14. Implement `DB.PartitionCount()` method
**Files**: `db_core.go`
**Task**: Return the number of active partitions. Useful for monitoring and capacity planning.

### 15. Add histogram bucket count validation
**Files**: `histogram.go`
**Task**: Validate that bucket boundaries are sorted and non-negative in `NewHistogramStore`.

## Integration

### 16. Add health check endpoint tests
**Files**: `http_test.go`
**Task**: Test that `/health` returns 200 with `{"status":"ok"}` body, and verify it works when DB is healthy and when it's closed.

### 17. Test PromQL `/api/v1/query` endpoint
**Files**: `http_test.go`
**Task**: Write integration test that writes data, then queries via PromQL HTTP endpoint and verifies response format.

### 18. Add CSV import round-trip test
**Files**: `export_test.go`
**Task**: Export data to CSV, then re-import it and verify all points match.

### 19. Verify OpenAPI spec matches actual endpoints
**Files**: `openapi_spec_test.go`
**Task**: Generate OpenAPI spec and verify every listed path has a corresponding handler in the HTTP server.

### 20. Add Prometheus remote write test
**Files**: `http_test.go`
**Task**: Send a Prometheus remote write request to `/api/v1/prom/write` and verify data is queryable.

---

## How to Pick an Issue

1. Choose one from above that matches your skill level
2. Comment on the [GitHub issue](https://github.com/chronicle-db/chronicle/issues) (or create one referencing this list)
3. Fork the repo and create a branch: `git checkout -b good-first-issue/N`
4. Make your changes and run `make check` (15 seconds)
5. Submit a PR

## Need Help?

Open a [Discussion](https://github.com/chronicle-db/chronicle/discussions) or comment on the issue. We're happy to guide you through your first contribution!
