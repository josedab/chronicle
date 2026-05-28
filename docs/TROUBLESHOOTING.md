# Troubleshooting

Common build, setup, and operational issues for Chronicle contributors.

## Build & Setup Issues

### CGO disabled errors (SQLite dependency)

**Problem:** Build fails with errors like:

```
# modernc.org/sqlite
cgo: C compiler "gcc" not found: exec: "gcc": executable file not found in $PATH
```

or:

```
package modernc.org/sqlite: build constraints exclude all Go files
```

**Cause:** Chronicle uses `modernc.org/sqlite` which requires CGO support. If CGO is disabled or a C compiler is missing, the build fails.

**Fix:**

- **macOS:** Install Xcode command-line tools:
  ```bash
  xcode-select --install
  ```
- **Linux (Debian/Ubuntu):**
  ```bash
  sudo apt-get install gcc build-essential
  ```
- **Linux (RHEL/Fedora):**
  ```bash
  sudo dnf install gcc
  ```
- Ensure CGO is enabled (it is by default):
  ```bash
  export CGO_ENABLED=1
  ```

### `make setup` fails on Apple Silicon (M1/M2/M3)

**Problem:** `make setup` fails with architecture mismatch errors or missing libraries.

**Cause:** Some dependencies need to be compiled for `arm64`. Homebrew on Apple Silicon installs to `/opt/homebrew` instead of `/usr/local`.

**Fix:**

1. Ensure you're running the native ARM64 Go binary (not Rosetta):
   ```bash
   go version
   # Should show: go1.24.x darwin/arm64
   ```

2. If using Homebrew, ensure your `PATH` includes `/opt/homebrew/bin`:
   ```bash
   export PATH="/opt/homebrew/bin:$PATH"
   ```

3. Clean and retry:
   ```bash
   go clean -cache
   make setup
   ```

### golangci-lint version mismatches

**Problem:** Lint passes locally but fails in CI, or vice versa.

**Cause:** Different versions of `golangci-lint` may report different issues. CI uses `golangci-lint-action@v6` which pins a specific version.

**Fix:**

- Run `make lint-ci` locally to match the exact CI configuration:
  ```bash
  make lint-ci
  ```

- If you have a globally installed version, ensure it matches:
  ```bash
  golangci-lint --version
  ```

- The Makefile uses `go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest` to avoid version skew. Prefer `make lint` over running `golangci-lint` directly.

### MinIO integration test setup

**Problem:** Integration tests that require S3-compatible storage fail or are skipped.

**Cause:** S3 backend integration tests need a running MinIO instance.

**Fix:**

1. Start MinIO locally:
   ```bash
   docker run -d --name minio \
     -p 9000:9000 -p 9001:9001 \
     -e MINIO_ROOT_USER=minioadmin \
     -e MINIO_ROOT_PASSWORD=minioadmin \
     minio/minio server /data --console-address ":9001"
   ```

2. Set environment variables:
   ```bash
   export AWS_ENDPOINT=http://localhost:9000
   export AWS_ACCESS_KEY_ID=minioadmin
   export AWS_SECRET_ACCESS_KEY=minioadmin
   export AWS_REGION=us-east-1
   ```

3. Run integration tests:
   ```bash
   make test-integration
   ```

### Common `go mod` issues

**Problem:** `go mod tidy` reports missing or conflicting dependencies.

**Fix:**

- Clean module cache and re-download:
  ```bash
  go clean -modcache
  go mod download
  go mod tidy
  ```

- If you see `go.sum` mismatch errors:
  ```bash
  go mod verify
  ```

- If a dependency upgrade breaks the build, check `go.sum` for conflicts:
  ```bash
  go mod graph | grep <problematic-package>
  ```

## Test & Runtime Issues

### Tests hang indefinitely

**Problem:** `go test ./...` hangs without output.

**Cause:** Leaked goroutines, deadlocks, or tests waiting for resources that were never initialized.

**Fix:**

- Run with a timeout:
  ```bash
  go test -timeout 30s ./...
  ```

- Use the race detector to find data races:
  ```bash
  go test -race -timeout 60s ./...
  ```

- Use `make test-fast` for quicker feedback during development:
  ```bash
  make test-fast    # runs only ./internal/... tests
  ```

### Coverage dropped unexpectedly

**Problem:** CI reports lower coverage than expected after your changes.

**Fix:**

- Generate a per-package coverage report to find uncovered files:
  ```bash
  make cover-report
  ```

- Check coverage for your specific package:
  ```bash
  go test -cover -coverprofile=coverage.out ./...
  go tool cover -func=coverage.out | grep <your-package>
  ```

### Lint timeout

**Problem:** `golangci-lint` times out during local runs.

**Cause:** The default timeout may be too short for the full codebase.

**Fix:**

- Use the CI-matching lint target with extended timeout:
  ```bash
  make lint-ci    # runs with --timeout=5m
  ```

- Or increase the timeout manually:
  ```bash
  golangci-lint run --timeout=10m
  ```

## Chronicle Error Reference

Chronicle exposes sentinel errors and typed wrappers from `errors.go`. Use
`errors.Is` for sentinel checks and `errors.As` when you need wrapper details
such as the query, metric, field, or storage path.

### Sentinel errors

| Error | Typical message | What it means | Troubleshooting steps |
|---|---|---|---|
| `ErrClosed` | `database is closed` | A read, write, or query used a DB handle after `Close()`. | Check lifecycle ownership, defer `Close()` only after all workers stop, and avoid reusing handles across tests. |
| `ErrQueryTimeout` | `query timeout` | The query exceeded its configured deadline. | Narrow the time range, add metric/tag filters, increase the query timeout, or inspect slow storage backends. |
| `ErrMemoryBudgetExceeded` | `query memory budget exceeded` | Query execution exceeded the configured memory budget. | Reduce cardinality, aggregate earlier, query a smaller range, or raise the query memory budget for the workload. |
| `ErrInvalidQuery` | `invalid query` | CQL, PromQL, GraphQL, or SQL parsing/validation failed. | Validate syntax, confirm the endpoint expects that query language, and check examples in `docs/API.md`. |
| `ErrQueryCanceled` | `query canceled` | The request context was canceled before the query completed. | Check client disconnects, HTTP timeouts, parent contexts, and cancellation paths in tests. |
| `ErrSchemaValidation` | `schema validation failed` | A point does not match the configured schema. | Confirm metric name, value type, required tags, and timestamp units before writing. |
| `ErrCardinalityLimit` | `cardinality limit exceeded` | A write would create more series than the configured limit allows. | Remove high-cardinality tags such as request IDs, bucket tags, or increase the limit intentionally. |
| `ErrStorageCorruption` | `storage corruption detected` | Chronicle detected invalid or unreadable persisted data. | Stop writes, preserve the file for inspection, restore from backup if available, and check recent disk or sync failures. |
| `ErrWALSync` | `WAL sync failed` | A WAL flush or fsync failed. | Check disk space, file permissions, mount health, and whether the filesystem supports durable sync. |
| `ErrStorageRead` | `storage read failed` | The active storage backend could not read data. | Verify file/object existence, permissions, credentials, network access, and backend configuration. |
| `ErrStorageWrite` | `storage write failed` | The active storage backend could not persist data. | Check disk space, write permissions, object store credentials, and retry/backoff settings. |
| `ErrUnsupportedOperation` | `operation not supported` | The selected backend or build does not implement the requested operation. | Check backend capability docs and switch to a backend that supports the operation. |
| `ErrFeatureDisabled` | `feature is disabled` | A route, integration, or optional module was used while disabled. | Enable the relevant config flag or build option, then restart the service. |

### Typed error wrappers

| Type | Extra context | How to use it |
|---|---|---|
| `QueryError` | Query error type, message, query, and cause. | Use `errors.Is(err, ErrInvalidQuery)`, `ErrQueryTimeout`, `ErrMemoryBudgetExceeded`, or `ErrQueryCanceled`; use `errors.As` to log the failing query. |
| `StorageError` | Storage error type, path, message, and cause. | Use `errors.Is` for `ErrStorageRead`, `ErrStorageWrite`, `ErrStorageCorruption`, or `ErrWALSync`; include `Path` in logs. |
| `WALSyncError` | Separate flush and sync errors. | Inspect both causes with `errors.As`; when both exist, fix the first durable-write failure before retrying. |
| `WriteError` | Metric name plus the underlying cause. | Log the metric and unwrap the cause to distinguish schema, cardinality, and storage failures. |
| `ConfigError` | Invalid config field and message. | Surface the field name to users and fix config before opening the database. |

### HTTP API errors

HTTP handlers serialize failures as:

```json
{
  "error": "error message description"
}
```

Common responses:

| Status | Usually caused by | First checks |
|---|---|---|
| `400 Bad Request` | Invalid JSON, invalid line protocol, invalid CQL/PromQL, bad timestamps, or schema validation. | Re-run with the examples in `docs/API.md`, verify `Content-Type`, and confirm timestamps are in the documented units. |
| `404 Not Found` | Unknown route, missing resource, or disabled optional endpoint. | Check the exact path and whether the feature is enabled in configuration. |
| `500 Internal Server Error` | Storage, WAL, plugin, or unexpected server-side failures. | Check server logs for wrapped errors, then follow the matching sentinel error guidance above. |

## See Also

- [FAQ](FAQ.md) — Common runtime gotchas and debugging tips
- [Getting Started](GETTING_STARTED.md) — Initial setup guide
- [Contributing](../CONTRIBUTING.md) — Development workflow

## Diagnostics (`make doctor`)

Run `make doctor` to get a full environment health check. The output covers
several sections — here's what each means and how to fix issues:

### System

| Check | What it shows | If wrong |
|-------|--------------|----------|
| OS | Operating system and architecture | Informational only |
| Shell | Current shell path | Informational only |
| Disk free | Available disk space on the volume | Free up space if critically low |

### Go

| Check | What it shows | If wrong |
|-------|--------------|----------|
| Version | Go compiler version | Install Go 1.24+ from https://go.dev/dl/ |
| GOPATH | Go workspace directory | Ensure `$GOPATH/bin` is on your `PATH` |
| GOBIN | Binary install directory | Defaults to `$GOPATH/bin` if unset |

### Tools

Each tool shows `✓` (found) or `✗` (not found):

| Tool | Purpose | How to install |
|------|---------|----------------|
| `golangci-lint` | Linter aggregator | `make setup` |
| `goimports` | Import formatter | `make setup` |
| `govulncheck` | Vulnerability scanner | `make setup` |
| `benchstat` | Benchmark comparisons | `make setup` |

If any tool shows `✗`, run `make setup` to install all development tools.

### Git Hooks

| Check | What it means | How to fix |
|-------|--------------|-----------|
| `✓ pre-commit hook installed` | Runs `go vet` + fast tests before commit | — |
| `✗ pre-commit hook missing` | Commits aren't validated locally | Run `make install-hooks` |
| `✓ commit-msg hook installed` | Enforces Conventional Commits format | — |
| `✗ commit-msg hook missing` | Commit messages aren't validated | Run `make install-hooks` |

### Module Status

| Check | What it means | How to fix |
|-------|--------------|-----------|
| `✓ go.mod is tidy` | Dependencies are clean | — |
| `⚠ go.mod may need tidying` | Extra or missing deps | Run `go mod tidy` |

### Grafana Plugin & Node.js

| Check | What it means | How to fix |
|-------|--------------|-----------|
| `✓ grafana-plugin/node_modules present` | Plugin dependencies installed | — |
| `✗ grafana-plugin/node_modules missing` | Plugin can't build | Run `make setup-grafana` |
| `✓ Node.js: vX.Y.Z` | Node.js available | — |
| `✗ Node.js not found` | Needed for website/ and grafana-plugin/ | Install from https://nodejs.org/ |

### Quick Fix

If `make doctor` shows multiple issues, run this sequence:

```bash
make setup          # Install all Go tools
make install-hooks  # Install git hooks
make setup-grafana  # Install Grafana plugin deps (optional)
```

Or use the one-command setup: `make quickstart`
