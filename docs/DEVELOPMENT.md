# Development Guide

> **New here?** Start with **[CONTRIBUTING.md](../CONTRIBUTING.md)** for setup, workflow,
> and contribution guidelines. This page covers **advanced topics** only.

## Debugging

### VS Code Debugger

Use the launch configurations in `.vscode/launch.json`:

- **Run HTTP Server Example**: Launches the example HTTP server with debugger attached
- **Debug Current Test File**: Runs tests in the current file with `-test.v`
- **Run CLI**: Launches the Chronicle CLI tool

### Delve (command line)

```bash
# Debug a specific test
dlv test . -- -test.run TestMyFeature

# Debug the HTTP server example
dlv debug ./examples/http-server
```

## Benchmarks &amp; Profiling

```bash
# Run all benchmarks
make bench

# Save benchmark results for comparison
make benchmark
# Compare: benchstat old.txt benchmark-results.txt
```

See [BENCHMARKS.md](BENCHMARKS.md) for benchmark methodology and historical results.

## Environment Setup

### Optional: mise (recommended)

[mise](https://mise.jdx.dev/) manages tool versions automatically:

```bash
mise install    # Installs exact Go version + all dev tools
mise run check  # Quick validation
```

### Environment Variables

Copy `.env.example` to `.env` and uncomment as needed. See [ENVIRONMENT_VARIABLES.md](ENVIRONMENT_VARIABLES.md) for full documentation.

### VS Code

The repository includes `.vscode/` configuration with Go linting, test flags, and debugger launch configs. Recommended extensions are listed in `.vscode/extensions.json`.

## Code Coverage

```bash
make cover         # Summary with 70% threshold warning
make cover-report  # Detailed per-package report
make test-cover    # HTML coverage report (opens coverage.html)
```

CI enforces a 70% coverage threshold. Run `make cover` locally to check before pushing.

## Useful Make Targets

Run `make help` for the full list. Highlights for advanced workflows:

```bash
make doctor         # Diagnose dev environment issues
make check-versions # Check Go version consistency across configs
make vuln           # Check for known vulnerabilities
make release-check  # Full pre-release validation
make validate       # Full local CI parity — run before pushing
```

## Project Navigation

Chronicle has a flat package structure with ~300 root-level files. See [CODE_MAP.md](CODE_MAP.md) for a domain-grouped guide to navigating the codebase, and [PACKAGES.md](PACKAGES.md) for the restructuring plan.

## Further Reading

- [../CONTRIBUTING.md](../CONTRIBUTING.md) — **Start here**: setup, workflow, and guidelines
- [TESTING.md](TESTING.md) — Test patterns, helpers, and debugging
- [BENCHMARKS.md](BENCHMARKS.md) — Benchmark methodology and results
- [FAQ.md](FAQ.md) — Frequently asked questions
- [CONFIGURATION.md](CONFIGURATION.md) — Configuration reference
- [ENVIRONMENT_VARIABLES.md](ENVIRONMENT_VARIABLES.md) — Environment variable reference
- [PACKAGES.md](PACKAGES.md) — Package restructuring plan
- [CODE_MAP.md](CODE_MAP.md) — Domain-grouped file navigation guide
- [PLUGIN_DEVELOPMENT.md](PLUGIN_DEVELOPMENT.md) — Plugin development guide

## CI Pipeline

The following GitHub Actions workflows run automatically. Use the "Local equivalent" column to reproduce CI checks on your machine.

| Workflow | Trigger | What it does | Local equivalent |
|---|---|---|---|
| **CI** (`ci.yml`) | Push / PR to `main` | Tests (3 OS × Go 1.24), lint, benchmarks, integration (MinIO), vuln check, fuzz | `make test-ci` / `make lint` |
| **Benchmarks** (`benchmarks.yml`) | Push / PR to `main` | Comparative benchmark (count=5), benchstat regression check (>10%) | `make benchmark` |
| **Performance Regression** (`perf-regression.yml`) | PR to `main` | Benchmark diff vs base branch, posts PR comment | `make benchmark` |
| **CodeQL** (`codeql.yml`) | Push / PR to `main`, weekly | Static analysis (Go) via GitHub CodeQL | — |
| **Security Audit** (`security-audit.yml`) | Push / PR to `main`, weekly | `govulncheck`, `staticcheck`, `go vet` | `make vuln` / `make vet` |
| **Dependency Review** (`dependency-review.yml`) | PR to `main` | Flags high-severity dependency changes | — |
| **OpenSSF Scorecard** (`scorecard.yml`) | Push to `main`, weekly | Supply-chain security scoring | — |
| **Deploy Documentation** (`docs.yml`) | Push to `main` (docs/website paths) | Build & deploy Docusaurus site to GitHub Pages | `cd website && npm run build` |
| **Release** (`release.yml`) | Push tag `v*` | GoReleaser build, SBOM (SPDX), Sigstore signing, SLSA provenance | `make release-check` |
| **Docker Multi-Arch** (`docker-multiarch.yml`) | Push tag `v*`, manual | Multi-arch Docker image (amd64/arm64/arm/v7) to GHCR | — |
| **Release Please** (`release-please.yml`) | Push to `main` | Auto-generate release PR with changelog | — |
| **Welcome** (`welcome.yml`) | Issue / PR opened | Greet new contributors with links | — |
