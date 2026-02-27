# Scripts

Development and build scripts for the Chronicle project.

## Contents

| Script | Description |
|--------|-------------|
| `build-wasm.sh` | Build Chronicle WebAssembly module for browser/edge deployment |
| `coverage.sh` | Run test suite and generate code coverage reports |
| `gen_feature_accessors.go` | Code generator for feature manager accessor methods |
| `commit-msg` | Git hook for validating commit message format |
| `pre-commit` | Git hook for running pre-commit checks (formatting, linting) |

## Usage

### Build WASM

```bash
./scripts/build-wasm.sh
```

### Generate Coverage Report

```bash
./scripts/coverage.sh
```

### Code Generation

```bash
go run ./scripts/gen_feature_accessors.go
```

## Git Hooks

Install the git hooks to enforce code quality:

```bash
cp scripts/pre-commit .git/hooks/pre-commit
cp scripts/commit-msg .git/hooks/commit-msg
chmod +x .git/hooks/pre-commit .git/hooks/commit-msg
```
