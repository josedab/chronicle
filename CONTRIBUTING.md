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
   This runs `go vet` and fast tests (~15s) before each commit.

## Development Workflow

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with race detector
go test -race ./...

# Run tests with coverage
go test -cover ./...
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

Before submitting, please ensure:

- [ ] Code compiles without errors (`go build ./...`)
- [ ] All tests pass (`go test ./...`)
- [ ] `go vet` passes (`go vet ./...`)
- [ ] Race detector passes (`go test -race ./...`)
- [ ] Code is formatted (`gofmt -w .`)
- [ ] Documentation is updated if needed
- [ ] Commit messages follow conventions

**Quick check**: Run `make check` to validate vet + fast tests in ~15 seconds.

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

## Code of Conduct

Please be respectful and constructive in all interactions. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for details.

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.

## Questions?

Feel free to open an issue for any questions about contributing.
