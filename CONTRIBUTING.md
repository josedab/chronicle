# Contributing to Chronicle

Thank you for your interest in contributing to Chronicle! This document provides guidelines and information for contributors.

## Getting Started

### Prerequisites

- Go 1.23 or later
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
   ```

3. Verify the setup:
   ```bash
   go build ./...
   go test ./...
   ```

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
- [ ] Race detector passes (`go test -race ./...`)
- [ ] Code is formatted (`gofmt -w .`)
- [ ] Linter passes (`golangci-lint run ./...`)
- [ ] Documentation is updated if needed
- [ ] Commit messages follow conventions

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
