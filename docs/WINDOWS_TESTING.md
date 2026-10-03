# Windows testing results

Date: 2026-10-03
Host: Windows 10 Home, build 19045, amd64
Go: go1.26.1 windows/amd64 (the repository CI matrix requests Go 1.24)
CGO: disabled (`CGO_ENABLED=0`); `gcc`, `make`, and `golangci-lint` are not installed.

## Results

| Command | Result |
| --- | --- |
| `go mod download` | Passed |
| `go build ./...` on the unmodified `main` | Failed: `health_check.go` used Unix-only `syscall.Statfs_t` and `syscall.Statfs`. |
| `go build ./...` after the Windows disk-space implementation | Passed |
| `go vet ./...` | Could not complete: with CGO disabled, `cffi_test.go` references `GenerateCHeader`, `DefaultFFIConfig`, and related symbols from CGO-only files. |
| `go test -short -count=1 ./...` | Could not complete on this host. The root package has the same CGO-only symbol errors. Other packages also reported Windows test issues described below. |

The Windows `diskUsage` implementation was exercised with a focused temporary smoke test using a real temporary directory; it passed. The permanent `TestDiskUsage` regression test is included in `health_check_test.go` and will run in the repository's Windows CI environment where the CGO toolchain is available.

## Issues observed

- `internal/continuousquery`: `TestContinuousQueryEngine` and `TestContinuousQueryEngine_Stats` observed fewer queries than expected.
- `internal/digitaltwin` and `internal/raft`: test cleanup could not remove database/log files because Windows reported that they were still open in another process.
- `internal/oteldistro`: `TestDistroMetrics` observed zero uptime where it expected a positive value.

These test failures are outside the disk-space change. The full CI matrix, including its native Windows runner, is needed to determine which persist with CGO enabled and the repository's Go 1.24 toolchain. No result is reported as passing unless the command completed successfully.
