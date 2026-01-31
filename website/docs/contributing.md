---
sidebar_position: 104
---

# Contributing

We welcome contributions to Chronicle! This guide will help you get started.

## Quick Links

- [GitHub Repository](https://github.com/chronicle-db/chronicle)
- [Issue Tracker](https://github.com/chronicle-db/chronicle/issues)
- [Discussions](https://github.com/chronicle-db/chronicle/discussions)

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

## Code of Conduct

Please read our [Code of Conduct](https://github.com/chronicle-db/chronicle/blob/main/CODE_OF_CONDUCT.md). We expect all contributors to follow it.

## Getting Help

- **Questions?** Open a [discussion](https://github.com/chronicle-db/chronicle/discussions)
- **Bug?** Open an [issue](https://github.com/chronicle-db/chronicle/issues)
- **Security?** See [SECURITY.md](https://github.com/chronicle-db/chronicle/blob/main/SECURITY.md)

## Recognition

Contributors are recognized in:
- GitHub contributors list
- Release notes for significant contributions
- CHANGELOG.md for user-facing changes

Thank you for contributing to Chronicle! 🎉
