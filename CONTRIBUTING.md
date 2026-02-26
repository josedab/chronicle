# Contributing to Chronicle

Thank you for your interest in contributing to Chronicle! This document provides guidelines and information for contributors.

## Getting Started

### Prerequisites

- Go 1.24 or later
- Git

### Development Setup

1. Fork and clone the repository:
   ```bash
   git clone https://github.com/YOUR_USERNAME/chronicle.git
   cd chronicle
   ```

2. Install development dependencies:
   ```bash
   go mod download
   go mod tidy
   make setup
   ```

3. Verify the setup:
   ```bash
   go build ./...
   go test ./...
   ```

4. Install the pre-commit hook (recommended):
   ```bash
   make install-hooks
   ```
   This installs two hooks:
   - **pre-commit**: runs `go vet` and fast tests (~15s) before each commit
   - **commit-msg**: enforces [Conventional Commits](https://www.conventionalcommits.org/) format

   > **Note on `.pre-commit-config.yaml`**: If you see this file in the repo, it's an
   > optional alternative for teams that use the Python `pre-commit` framework
   > (`pip install pre-commit && pre-commit install`). **The canonical setup is
   > `make install-hooks`** — it requires no extra tooling and is what CI expects.

## Development Workflow

### Quick Lint Check

Before pushing, run `make lint` to check if your code passes all linters:

```bash
make lint          # go vet + golangci-lint (dry-run, no changes)
make lint-ci       # same as CI: golangci-lint with --timeout=5m
make lint-fix      # auto-fix fixable lint issues + reformat
```

`make lint` runs the same checks as CI. If it passes locally, lint will pass in the pipeline. Run `make lint-ci` before pushing to match the exact CI timeout configuration.

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with race detector
go test -race ./...

# Run tests with coverage
go test -cover ./...

# Per-package function-level coverage
make test-cover-pkg PKG=./internal/query/...
```

### Performance Tips

`make test` and `make test-ci` run with the race detector (`-race`) explicitly.
Other targets like `make test-short` do not include the race detector, giving you
faster iteration. To opt-in to race detection for any target:

```bash
GOFLAGS=-race make test-short  # Add race detector to test-short
make test-race                 # Short tests WITH race detector (~45s, best pre-push check)
make test-fast                 # Internal packages only (~5s, TDD loop)
make test-verbose TEST=TestMyThing  # Single test with verbose output
```

### Code Style

We use standard Go formatting and linting:

```bash
# Format code
gofmt -w .

# Run linter (install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
golangci-lint run ./...

# Run go vet
go vet ./...
```

### Code Guidelines

- Follow [Effective Go](https://go.dev/doc/effective_go) guidelines
- Follow [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- Write clear, descriptive commit messages
- Add tests for new functionality
- Update documentation for API changes
- Keep pull requests focused on a single change

### Naming Conventions

Chronicle uses consistent naming patterns for public APIs:

| Pattern | Example | Purpose |
|---------|---------|---------|
| `DefaultXConfig()` | `DefaultAlertBuilderConfig()` | Returns config with sensible defaults |
| `NewX(db, config)` | `NewAlertBuilder(db, config)` | Creates an engine/manager instance |
| `XBuilder` | `ConfigBuilder`, `QueryBuilder` | Fluent builder for constructing X |
| `NewXBuilder(...)` | `NewConfigBuilder(path)` | Creates a builder instance |
| `WithY(...)` | `WithMaxMemory(bytes)` | Builder method (returns builder) |
| `Build()` | `builder.Build()` | Finalizes builder, returns value + error |

When adding new features, follow these patterns for consistency.

### File Size Guidelines

Production Go files have a soft limit of **800 lines**. `make check-file-size`
warns about files that exceed this threshold.

**When exceptions are acceptable:**
- Generated code (e.g., `feature_manager_gen.go`)
- Query language parsers (e.g., `promql.go`, `parser.go`) where splitting reduces readability
- Test files with many table-driven cases

**Navigating large files:** Use `// Section:` markers to delineate logical blocks:

```go
// Section: Configuration

type Config struct { ... }

// Section: Initialization

func New(...) { ... }

// Section: Query execution

func (e *Engine) Execute(...) { ... }
```

These markers work well with editor search (`Cmd+Shift+O` in VS Code, `grep -n 'Section:'`)
and make large files navigable without splitting them prematurely.

## Submitting Changes

### Pull Request Process

1. Create a feature branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and commit:
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

3. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

4. Open a Pull Request against `main`

### Commit Message Format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Test additions or modifications
- `refactor:` - Code refactoring
- `perf:` - Performance improvements
- `chore:` - Maintenance tasks

### PR Checklist

Before submitting, run `make validate` for full local CI parity:

```bash
make validate    # vet → lint → test-short → check-file-size → check-generate
```

This matches the PR-blocking CI jobs and catches issues before pushing.

Please also ensure:

- [ ] Code compiles without errors (`go build ./...`)
- [ ] All tests pass (`go test ./...`)
- [ ] `go vet` passes (`go vet ./...`)
- [ ] Race detector passes (`go test -race ./...`)
- [ ] Code is formatted (`gofmt -w .`)
- [ ] Documentation is updated if needed
- [ ] Commit messages follow conventions

**Quick check**: Run `make check` to validate vet + fast tests in ~15 seconds.

## Troubleshooting

Build or setup issues? See the **[Troubleshooting Guide](docs/TROUBLESHOOTING.md)** for solutions to common problems including CGO errors, Apple Silicon setup, golangci-lint version mismatches, and test failures. Run `make doctor` for a full environment diagnostic — see the [Diagnostics section](docs/TROUBLESHOOTING.md#diagnostics-make-doctor) for what each check means.

## Reporting Issues

### Bug Reports

When reporting bugs, please include:

- Chronicle version
- Go version (`go version`)
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs or error messages

### Feature Requests

Feature requests are welcome! Please describe:

- The problem you're trying to solve
- Your proposed solution
- Any alternatives you've considered

## First-Time Contributors

Looking for your first contribution? See **[Good First Issues](docs/GOOD_FIRST_ISSUES.md)** for 20 well-scoped tasks ranging from documentation to small features.

### Project Layout

The root package has 600+ Go files. See **[docs/CODE_MAP.md](docs/CODE_MAP.md)** for a domain-grouped navigation map of every file.

### Architecture Quick Reference

```
chronicle/
├── db_core.go          ← DB struct, Open(), Close(), lifecycle
├── db_write.go         ← Write(), WriteBatch(), WriteContext()
├── query.go            ← Execute(), Query struct, Result struct
├── config.go           ← Config, Validate(), DefaultConfig()
├── wal.go              ← Write-ahead log (crash recovery)
├── btree.go            ← Partition index
├── buffer.go           ← Write buffer (batching)
├── partition_core.go   ← Partition data structure
├── partition_query.go  ← Query execution within partitions
├── feature_manager.go  ← Lazy feature initialization
├── feature_registry.go ← Plugin-style feature management (new)
├── http_server.go      ← HTTP API server
├── promql.go           ← PromQL parser
├── internal/           ← Private implementation packages
├── cmd/chronicle/      ← CLI tool
└── examples/           ← Example applications
```

**Key concepts**:
- Data flows: Point → WriteBuffer → WAL → Partition → BTree index
- Queries: Query → BTree lookup → Partition scan → Aggregation → Result
- Features use lazy init via `sync.Once` (see `feature_manager.go`)

## Code of Conduct

Please be respectful and constructive in all interactions. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for details.

## Code Ownership

Chronicle uses [GitHub CODEOWNERS](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners) to automatically assign reviewers to pull requests.

### Current owners

| Area | Owner | Scope |
|------|-------|-------|
| Overall | `@chronicle-db/maintainers` | All files (default) |
| Documentation | `@chronicle-db/docs-team` | `*.md`, `docs/` |
| CI/CD | `@chronicle-db/platform-team` | `.github/` |

### How reviews work

- When you open a PR, GitHub automatically requests reviews from the relevant
  CODEOWNERS based on which files you changed.
- PRs that touch documentation are routed to the docs team; CI changes go to
  the platform team; everything else goes to maintainers.
- At least one approval from a code owner is required before merging.

### Becoming an owner

Active contributors can become owners of a subsystem:

1. Contribute multiple high-quality PRs to the area you want to own
2. Demonstrate familiarity with the subsystem's design and conventions
3. A current maintainer will propose adding you to the relevant team
4. Once approved, you'll be added to `.github/CODEOWNERS` and the GitHub team

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.

## Questions?

Feel free to open an issue for any questions about contributing.
