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

## See Also

- [FAQ](FAQ.md) — Common runtime gotchas and debugging tips
- [Getting Started](GETTING_STARTED.md) — Initial setup guide
- [Contributing](../CONTRIBUTING.md) — Development workflow
