# CI/CD Workflows

This document describes the GitHub Actions workflows in `.github/workflows/`.

## Workflow Summary

| Workflow | File | Trigger | Purpose |
|----------|------|---------|---------|
| [CI](#ci) | `ci.yml` | Push to `main`, PRs | Core test suite, linting, coverage |
| [Benchmarks](#benchmarks) | `benchmarks.yml` | Push to `main`, PRs | Performance benchmarks with regression alerts |
| [Performance Regression](#performance-regression) | `perf-regression.yml` | PRs | Gates PRs on >10% performance regression |
| [CodeQL](#codeql) | `codeql.yml` | Push to `main`, PRs, weekly | Static analysis for security vulnerabilities |
| [Security Audit](#security-audit) | `security-audit.yml` | Push to `main`, PRs, weekly | govulncheck, staticcheck, go vet |
| [Dependency Review](#dependency-review) | `dependency-review.yml` | PRs | Checks new dependencies for vulnerabilities |
| [Deploy Documentation](#deploy-documentation) | `docs.yml` | Push to `main` (docs/website paths) | Builds and deploys Docusaurus site |
| [Release Please](#release-please) | `release-please.yml` | Push to `main` | Auto-generates release PRs from conventional commits |
| [Release](#release) | `release.yml` | Tags (`v*`) | GoReleaser build, SBOM, Sigstore signing |
| [Docker Multi-Arch](#docker-multi-arch) | `docker-multiarch.yml` | Tags (`v*`), manual | Multi-arch Docker images to GHCR |
| [OpenSSF Scorecard](#openssf-scorecard) | `scorecard.yml` | Push to `main`, weekly | Security scorecard analysis |
| [Welcome](#welcome) | `welcome.yml` | New issues/PRs | Welcome message for first-time contributors |

## Workflow Details

### CI

**File:** `ci.yml`
**Triggers:** Push to `main`, pull requests targeting `main`

Runs the core test suite across multiple OS/Go version combinations. Includes:
- `go vet` and `golangci-lint`
- Unit tests with race detector
- Short tests for coverage reporting
- Integration tests

**Troubleshooting:**
- **Lint failures:** Run `make lint-fix` locally to auto-fix, then `make lint` to verify.
- **Test failures:** Run `make test` locally. Use `make test-pkg PKG=<domain>` to isolate.
- **Race detector failures:** These indicate real concurrency bugs. Reproduce with `go test -race ./...`.

### Benchmarks

**File:** `benchmarks.yml`
**Triggers:** Push to `main`, pull requests

Runs `go test -bench` and compares results against the `main` branch. Posts regression alerts as PR comments.

**Troubleshooting:**
- **Benchmark variance:** Re-run the workflow; some variance is expected on shared runners.
- **Local comparison:** `make benchmark` saves results, then use `benchstat` to compare.

### Performance Regression

**File:** `perf-regression.yml`
**Triggers:** Pull requests targeting `main`

Benchmarks both the PR branch and `main`, compares with `benchstat`, and fails the check if any benchmark regresses by more than 10%.

**Troubleshooting:**
- **False positives:** Re-run the check. If consistent, profile with `go test -bench=BenchmarkName -cpuprofile=cpu.prof`.
- **Intentional regression:** Add a comment explaining the tradeoff and request maintainer approval.

### CodeQL

**File:** `codeql.yml`
**Triggers:** Push to `main`, pull requests, weekly schedule

Performs CodeQL static analysis on Go source code to detect security vulnerabilities (injection, crypto issues, etc.).

**Troubleshooting:**
- **Alerts:** Review in the Security tab → Code scanning alerts. Fix the flagged code pattern.
- **False positives:** Add a `// codeql[rule-id]` suppression comment with justification.

### Security Audit

**File:** `security-audit.yml`
**Triggers:** Push to `main`, pull requests, weekly schedule

Runs `govulncheck`, `staticcheck`, and `go vet` for comprehensive security and quality analysis.

**Troubleshooting:**
- **govulncheck findings:** Update the vulnerable dependency with `go get <module>@latest && go mod tidy`.
- **staticcheck issues:** Run `staticcheck ./...` locally and fix the reported issues.

### Dependency Review

**File:** `dependency-review.yml`
**Triggers:** Pull requests targeting `main`

Reviews dependency changes in PRs for high-severity vulnerabilities and license issues.

**Troubleshooting:**
- **Blocked dependency:** Check the vulnerability details in the PR comment. Update to a patched version or find an alternative.

### Deploy Documentation

**File:** `docs.yml`
**Triggers:** Push to `main` (changes to `docs/` or `website/` paths), manual dispatch

Builds the Docusaurus documentation site from `website/` and deploys to GitHub Pages.

**Troubleshooting:**
- **Build failures:** Run `cd website && npm install && npm run build` locally.
- **Missing pages:** Check `website/sidebars.js` and `website/docusaurus.config.js`.

### Release Please

**File:** `release-please.yml`
**Triggers:** Push to `main`

Automatically creates and updates release PRs based on conventional commit messages. Generates changelogs from commit history.

**Troubleshooting:**
- **No release PR created:** Ensure commits follow [conventional commit](https://www.conventionalcommits.org/) format (`feat:`, `fix:`, `chore:`, etc.).
- **Wrong version bump:** `feat:` triggers minor bump, `fix:` triggers patch. Use `feat!:` or `BREAKING CHANGE:` footer for major bumps.

### Release

**File:** `release.yml`
**Triggers:** Tags matching `v*`

Builds release artifacts via GoReleaser, generates SBOM (Software Bill of Materials), and signs with Sigstore provenance.

**Troubleshooting:**
- **GoReleaser failures:** Run `goreleaser check` locally to validate `.goreleaser.yml`.
- **Signing issues:** These are usually transient Sigstore/Fulcio issues; re-run the workflow.

### Docker Multi-Arch

**File:** `docker-multiarch.yml`
**Triggers:** Tags matching `v*`, manual dispatch

Builds multi-architecture Docker images (amd64, arm64, armv7) and pushes to GitHub Container Registry (GHCR) with SLSA provenance.

**Troubleshooting:**
- **Build failures on arm:** Check for architecture-specific code or CGo dependencies.
- **Push failures:** Verify `GITHUB_TOKEN` permissions include `packages: write`.

### OpenSSF Scorecard

**File:** `scorecard.yml`
**Triggers:** Push to `main`, weekly schedule, branch protection rule changes

Runs the [OpenSSF Scorecard](https://securityscorecards.dev/) analysis and uploads results to the Security tab.

**Troubleshooting:**
- **Low scores:** Review the scorecard results in Security → Code scanning. Common fixes: enable branch protection, add SECURITY.md, pin CI action versions.

### Welcome

**File:** `welcome.yml`
**Triggers:** First issue or PR opened by a new contributor

Posts a welcome message with links to contributing guidelines and resources.

**Troubleshooting:**
- **Not triggering:** Uses `pull_request_target` for fork safety. Check that the contributor has no prior issues/PRs.

## Running CI Checks Locally

```bash
# Run the same checks as CI
make test-ci

# Full local CI parity (vet + lint + tests + file-size + generated code)
make validate

# Full pre-release validation
make release-check

# Quick pre-commit check
make check
```

### Local Equivalents for CI Workflows

| CI Workflow | Local Equivalent | Notes |
|-------------|-----------------|-------|
| CI (ci.yml) | `make test-ci` | vet + short tests with race detector |
| Benchmarks | `make benchmark` | Runs benchmarks, saves results for comparison |
| Performance Regression | `make benchmark` | Compare with `benchstat` |
| CodeQL | `make lint` | `go vet` + golangci-lint catches most issues; install `gosec` for deeper security analysis |
| Security Audit | `govulncheck ./...` | Run `make vuln` or install `staticcheck` |
| Dependency Review | `make deps-check` | Checks tidy status and outdated dependencies |
| Deploy Documentation | `cd website && npm run build` | Verifies docs build locally |
| Release Please | — | GitHub-only (conventional commit automation) |
| Release | `goreleaser check` | Validates `.goreleaser.yml` config |
| Docker Multi-Arch | `docker build .` | Single-arch local build only |
| OpenSSF Scorecard | — | GitHub-only (requires repository metadata) |
| Welcome | — | GitHub-only (contributor greeting) |
