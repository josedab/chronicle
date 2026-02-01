---
sidebar_position: 106
---

# Contributing

We welcome contributions to Chronicle! This guide will help you get started.

## Quick Links

- [GitHub Repository](https://github.com/chronicle-db/chronicle)
- [Issue Tracker](https://github.com/chronicle-db/chronicle/issues)
- [Discussions](https://github.com/chronicle-db/chronicle/discussions)

## Your First Contribution

New to Chronicle? Here's how to make your first contribution:

### 🏷️ Good First Issues

We label beginner-friendly issues with `good first issue`. These are:

- Well-documented with clear requirements
- Limited in scope (typically < 100 lines of code)
- Don't require deep knowledge of the codebase

**[Browse good first issues →](https://github.com/chronicle-db/chronicle/labels/good%20first%20issue)**

### 📝 Documentation Improvements

The easiest way to contribute:

1. Find a typo, unclear explanation, or missing example
2. Click "Edit this page" at the bottom of any docs page
3. Make your change and submit a PR

No local setup required!

### 🧪 Adding Test Coverage

Help improve test coverage:

```bash
# Find areas needing tests
go test -cover ./...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

Look for files with < 70% coverage and add tests.

## Step-by-Step: Your First Pull Request

### 1. Find an Issue

- Check [good first issues](https://github.com/chronicle-db/chronicle/labels/good%20first%20issue)
- Comment "I'd like to work on this" to claim it
- Wait for maintainer confirmation

### 2. Set Up Your Environment

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/chronicle.git
cd chronicle

# Add upstream remote
git remote add upstream https://github.com/chronicle-db/chronicle.git

# Install dependencies and verify setup
make all
```

### 3. Create a Branch

```bash
# Sync with upstream
git fetch upstream
git checkout main
git merge upstream/main

# Create feature branch
git checkout -b feat/your-feature-name
```

### 4. Make Your Changes

- Write code following our [style guidelines](#code-guidelines)
- Add tests for new functionality
- Update documentation if needed

### 5. Test Locally

```bash
# Run all tests
make test

# Run tests with race detector
go test -race ./...

# Run linters
make lint

# Run everything
make all
```

### 6. Commit and Push

```bash
git add .
git commit -m "feat: add your feature description"
git push origin feat/your-feature-name
```

### 7. Open a Pull Request

1. Go to your fork on GitHub
2. Click "Compare & pull request"
3. Fill out the PR template
4. Request review from maintainers

## Ways to Contribute

### 🐛 Report Bugs

Found a bug? [Open an issue](https://github.com/chronicle-db/chronicle/issues/new) with:

- Chronicle version
- Go version
- Operating system
- Steps to reproduce
- Expected vs actual behavior

### 💡 Suggest Features

Have an idea? Start a [discussion](https://github.com/chronicle-db/chronicle/discussions) to:

- Describe the use case
- Explain the proposed solution
- Discuss alternatives considered

### 📖 Improve Documentation

Documentation improvements are always welcome:

- Fix typos or unclear explanations
- Add examples
- Improve API documentation
- Translate to other languages

### 🔧 Submit Code

Ready to code? Here's how:

1. **Fork** the repository
2. **Clone** your fork locally
3. **Create a branch** for your change
4. **Make your changes** with tests
5. **Run checks** locally
6. **Submit a pull request**

## Development Setup

```bash
# Clone the repository
git clone https://github.com/chronicle-db/chronicle.git
cd chronicle

# Run tests
make test

# Run linters
make lint

# Run all checks
make all
```

### Project Structure

```
chronicle/
├── *.go              # Main package source files
├── *_test.go         # Test files
├── internal/         # Private implementation
│   ├── bits/         # Bit-level I/O
│   ├── encoding/     # Compression codecs
│   └── query/        # Query parsing
├── docs/             # Documentation source
├── examples/         # Example applications
├── website/          # Docusaurus documentation site
└── wasm/             # WebAssembly compilation
```

### Key Files to Understand

| File | Purpose |
|------|---------|
| `db.go` | Main database API |
| `query.go` | Query execution |
| `storage.go` | Storage engine |
| `wal.go` | Write-ahead log |
| `http.go` | HTTP API handlers |

## Code Guidelines

### Go Style

- Follow [Effective Go](https://golang.org/doc/effective_go.html)
- Use `gofmt` for formatting
- Keep functions focused and small
- Document exported types and functions

### Testing

- Write tests for new functionality
- Maintain or improve code coverage
- Include both unit and integration tests
- Use table-driven tests where appropriate

```go
func TestMyFunction(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected string
    }{
        {"empty", "", ""},
        {"simple", "hello", "HELLO"},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := MyFunction(tt.input)
            if result != tt.expected {
                t.Errorf("got %q, want %q", result, tt.expected)
            }
        })
    }
}
```

### Commit Messages

Use clear, descriptive commit messages:

```
feat: add histogram support for native Prometheus histograms

- Implement exponential bucketing
- Add WriteHistogram API
- Update HTTP endpoint for histogram ingestion

Closes #123
```

Prefixes:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation only
- `test:` - Adding tests
- `refactor:` - Code refactoring
- `perf:` - Performance improvement

## Pull Request Process

1. **Update documentation** if needed
2. **Add tests** for new functionality
3. **Ensure CI passes** - all tests and linters
4. **Request review** from maintainers
5. **Address feedback** promptly

### PR Checklist

- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] CHANGELOG.md updated (for user-facing changes)
- [ ] No breaking changes (or documented if unavoidable)

### What to Expect

- **Initial response**: Within 48 hours
- **Review**: 1-5 business days depending on complexity
- **Merge**: After approval and CI passing

## Code of Conduct

Please read our [Code of Conduct](https://github.com/chronicle-db/chronicle/blob/main/CODE_OF_CONDUCT.md). We expect all contributors to follow it.

## Getting Help

- **Questions?** Open a [discussion](https://github.com/chronicle-db/chronicle/discussions)
- **Bug?** Open an [issue](https://github.com/chronicle-db/chronicle/issues)
- **Security?** See [SECURITY.md](https://github.com/chronicle-db/chronicle/blob/main/SECURITY.md)
- **Stuck?** Comment on the issue you're working on

## Recognition

Contributors are recognized in:
- GitHub contributors list
- Release notes for significant contributions
- CHANGELOG.md for user-facing changes

Thank you for contributing to Chronicle! 🎉
