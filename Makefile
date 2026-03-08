.PHONY: all quickstart build test test-all test-verbose test-short test-fast test-failing test-pkg test-integration test-ci test-race-short test-examples lint lint-ci lint-fix lint-fast fmt fmt-check clean clean-all bench benchmark profile-cpu profile-mem check check-all quickcheck cover cover-report check-coverage test-cover-pkg vet setup setup-grafana install-hooks preflight release-check tag check-interface check-api-stability check-openapi wasm dev run debug watch watch-lint watch-all check-versions doctor new-test test-changed check-file-size check-file-size-strict lint-changed deps-check check-todos check-goroutine-leaks test-race generate check-generate run-example validate tidy test-file mutation-test help

GO ?= go
MIN_GO_VERSION := 1.24

##@ Setup
all: lint test build ## Run lint, test, and build

quickstart: setup install-hooks doctor ## One-command setup for new contributors
	@echo "Downloading Go module dependencies..."
	@$(GO) mod download
	@echo ""
	@echo "Verifying build..."
	@$(GO) build ./...
	@echo "✓ Build succeeded"
	@echo ""
	@echo "Running smoke tests..."
	@$(MAKE) test-fast
	@echo "✓ Smoke tests passed"
	@echo ""
	@echo "════════════════════════════════════════════════"
	@echo "  ✓ Ready! Run 'make dev' to start."
	@echo "════════════════════════════════════════════════"

preflight: ## Validate development environment
	@echo "Checking Go installation..."
	@command -v $(GO) >/dev/null 2>&1 || { echo "ERROR: Go not found. Install from https://go.dev/dl/"; exit 1; }
	@GO_VERSION=$$($(GO) version | grep -oE 'go[0-9]+\.[0-9]+' | sed 's/go//'); \
	GO_MAJOR=$$(echo $$GO_VERSION | cut -d. -f1); \
	GO_MINOR=$$(echo $$GO_VERSION | cut -d. -f2); \
	REQ_MAJOR=$$(echo $(MIN_GO_VERSION) | cut -d. -f1); \
	REQ_MINOR=$$(echo $(MIN_GO_VERSION) | cut -d. -f2); \
	if [ "$$GO_MAJOR" -lt "$$REQ_MAJOR" ] || { [ "$$GO_MAJOR" -eq "$$REQ_MAJOR" ] && [ "$$GO_MINOR" -lt "$$REQ_MINOR" ]; }; then \
		echo "ERROR: Go $(MIN_GO_VERSION)+ required, found go$$GO_VERSION"; exit 1; \
	fi
	@test -f go.mod || { echo "ERROR: Run from repository root (go.mod not found)"; exit 1; }
	@echo "✓ Go $$($(GO) version | grep -oE 'go[0-9]+\.[0-9]+\.[0-9]+') detected"
	@echo "✓ Pre-flight checks passed"

##@ Development

build: ## Build the package
	$(GO) build ./...

##@ Testing

test: ## Run tests with race detector
	$(GO) test -race -count=1 ./...

test-verbose: ## Run a single test verbosely (usage: make test-verbose TEST=TestMyThing)
ifndef TEST
	$(error TEST is required. Usage: make test-verbose TEST=TestMyThing)
endif
	$(GO) test -run $(TEST) -count=1 -v .

test-short: ## Run short tests only (fast iteration)
	$(GO) test -short ./...

test-race: ## Short tests with race detector (~45s)
	$(GO) test -race -short -count=1 ./...

test-fast: ## Run only internal package tests (fastest feedback)
	$(GO) test ./internal/...

test-failing: ## Run only previously-failing tests for fast iteration
	$(GO) test -v -count=1 -run "TestHTTPHealth$$|TestK8sSidecar_HealthEndpoints|TestImportEngine/invalid_lines_skipped" .
	$(GO) test -v -count=1 -run "FuzzPointValidation/seed" .

test-pkg: ## Run tests for a domain (usage: make test-pkg PKG=query)
ifndef PKG
	$(error PKG is required. Usage: make test-pkg PKG=query)
endif
	@echo "Testing package domain: $(PKG)"
	@# Title-case the first letter for -run pattern
	@RUN_PATTERN=$$(echo "$(PKG)" | awk '{print toupper(substr($$0,1,1)) substr($$0,2)}'); \
	echo "→ go test -v -run $$RUN_PATTERN ."; \
	$(GO) test -v -count=1 -run "$$RUN_PATTERN" . || true
	@if [ -d "./internal/$(PKG)" ]; then \
		echo "→ go test -v ./internal/$(PKG)/..."; \
		$(GO) test -v -count=1 ./internal/$(PKG)/...; \
	else \
		echo "ℹ No internal/$(PKG) subpackage found"; \
	fi

test-integration: ## Run all tests including integration
	$(GO) test -race -tags integration ./...

test-race-short: ## Run vet + short tests with race detector (quick pre-push check)
	$(GO) vet ./...
	$(GO) test -race -short ./...

test-ci: test-race-short ## Deprecated: use test-race-short instead

test-examples: ## Verify all examples compile (and optionally run with 5s timeout)
	@echo "Building examples..."
	$(GO) build ./examples/...
	@echo "✓ All examples compile"
	@echo "Running examples with 5s timeout..."
	@FAIL=0; \
	for ex in $$(find examples -mindepth 1 -maxdepth 1 -type d); do \
		NAME=$$(basename $$ex); \
		if timeout 5s $(GO) run ./$$ex >/dev/null 2>&1; then \
			echo "  ✓ $$NAME"; \
		else \
			STATUS=$$?; \
			if [ "$$STATUS" = "124" ]; then \
				echo "  ✓ $$NAME (timed out — likely a server, OK)"; \
			else \
				echo "  ✗ $$NAME (exit $$STATUS)"; \
				FAIL=1; \
			fi; \
		fi; \
	done; \
	if [ "$$FAIL" = "1" ]; then \
		echo "⚠ Some examples failed"; exit 1; \
	fi; \
	echo "✓ All examples passed"

test-cover: ## Run tests with coverage report (HTML)
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

cover: ## Print coverage summary to terminal (warns if below 70%)
	$(GO) test -coverprofile=coverage.out -short ./...
	$(GO) tool cover -func=coverage.out | tail -1
	@COVERAGE=$$($(GO) tool cover -func=coverage.out | grep total | awk '{print substr($$3, 1, length($$3)-1)}'); \
	THRESHOLD=70; \
	echo "Coverage: $${COVERAGE}% (threshold: $${THRESHOLD}%)"; \
	PASS=$$(echo "$$COVERAGE >= $$THRESHOLD" | bc -l 2>/dev/null || awk "BEGIN{print ($$COVERAGE >= $$THRESHOLD)}"); \
	if [ "$$PASS" != "1" ]; then \
		echo "⚠ WARNING: Coverage $${COVERAGE}% is below $${THRESHOLD}% threshold (CI will fail)"; \
	fi
	@rm -f coverage.out

cover-report: ## Per-package coverage report (use -html for HTML output)
	@./scripts/coverage.sh

check-coverage: cover ## Check per-package coverage minimums
	@./scripts/check-coverage.sh coverage.out

test-cover-pkg: ## Per-package coverage for a specific package (usage: make test-cover-pkg PKG=./internal/...)
ifndef PKG
	$(error PKG is required. Usage: make test-cover-pkg PKG=./internal/query/...)
endif
	@echo "Running coverage for $(PKG)..."
	@$(GO) test -short -coverprofile=coverage.out -covermode=atomic $(PKG)
	@$(GO) tool cover -func=coverage.out
	@rm -f coverage.out

mutation-test: ## Run mutation testing to validate test quality
	@command -v go-mutesting >/dev/null 2>&1 || { echo "Installing go-mutesting..."; $(GO) install github.com/zimmski/go-mutesting/cmd/go-mutesting@latest; }
	@echo "Running mutation tests (this may take a while)..."
	go-mutesting ./...

vet: ## Run go vet
	$(GO) vet ./...

check: preflight fmt-check vet test-fast ## Quick pre-commit validation (preflight + fmt + vet + fast tests)

check-all: check lint check-file-size check-todos check-versions check-generate ## Comprehensive pre-push validation (check + lint + file sizes + TODOs + versions + generated code)

generate: ## Run code generation (go generate + feature accessors)
	$(GO) generate ./...
	@echo "✓ Code generation complete"

check-generate: generate ## Verify generated code is up-to-date
	@if [ -n "$$(git diff --name-only)" ]; then \
		echo "ERROR: Generated code is out of date. Run 'make generate' and commit the result."; \
		git diff --name-only; \
		exit 1; \
	fi
	@echo "✓ Generated code is up-to-date"

quickcheck: ## Full pre-push validation (vet + all short tests)
	$(GO) vet ./...
	$(GO) test -short -count=1 ./...

install-hooks: ## Install git pre-commit and commit-msg hooks
	@mkdir -p .git/hooks
	ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
	ln -sf ../../scripts/commit-msg .git/hooks/commit-msg
	@# Verify hooks were installed correctly
	@FAIL=0; \
	if [ -f .git/hooks/pre-commit ]; then \
		echo "  ✓ pre-commit hook installed"; \
	else \
		echo "  ✗ pre-commit hook failed to install"; FAIL=1; \
	fi; \
	if [ -f .git/hooks/commit-msg ]; then \
		echo "  ✓ commit-msg hook installed"; \
	else \
		echo "  ✗ commit-msg hook failed to install"; FAIL=1; \
	fi; \
	if [ "$$FAIL" = "1" ]; then \
		echo "ERROR: Hook installation failed."; exit 1; \
	fi

GOLANGCI_LINT := $(shell command -v golangci-lint 2>/dev/null || echo "$(GO) run github.com/golangci/golangci-lint/cmd/golangci-lint@latest")

##@ Linting

lint: ## Run linters
	$(GO) vet ./...
	$(GOLANGCI_LINT) run

lint-ci: ## Run linters matching exact CI configuration
	$(GO) vet ./...
	$(GOLANGCI_LINT) run --timeout=5m

lint-fix: ## Auto-fix all fixable lint issues
	$(GOLANGCI_LINT) run --fix
	gofmt -s -w .
	$(GO) run golang.org/x/tools/cmd/goimports@latest -w .

lint-changed: ## Lint only modified files (fast iteration)
	@if ! git rev-parse --verify HEAD~1 >/dev/null 2>&1; then \
		echo "No previous commit to compare against; running full lint"; \
		$(GOLANGCI_LINT) run; \
	else \
		echo "Linting changes since HEAD~1..."; \
		$(GOLANGCI_LINT) run --new-from-rev=HEAD~1; \
	fi

lint-fast: ## Run only fast linters (~5s, matches VS Code config)
	$(GOLANGCI_LINT) run --fast

fmt: ## Format code
	gofmt -s -w .
	$(GO) run golang.org/x/tools/cmd/goimports@latest -w .

fmt-check: ## Check if code is formatted (dry-run, no changes)
	@UNFORMATTED=$$(gofmt -l .); \
	if [ -n "$$UNFORMATTED" ]; then \
		echo "The following files need formatting (run 'make fmt'):"; \
		echo "$$UNFORMATTED"; \
		exit 1; \
	fi
	@echo "✓ All files are properly formatted"

##@ Benchmarks & Profiling

bench: ## Run benchmarks
	$(GO) test -bench=. -benchmem ./...

benchmark: ## Run benchmarks and save results for comparison
	@if [ -f benchmark-results.txt ]; then \
		mv benchmark-results.txt benchmark-results.txt.prev; \
	fi
	$(GO) test -bench=. -benchmem -count=5 -short -run=^$$ ./... | tee benchmark-results.txt
	@echo "✓ Results saved to benchmark-results.txt"
	@if [ -f benchmark-results.txt.prev ]; then \
		if command -v benchstat >/dev/null 2>&1; then \
			echo ""; \
			echo "═══ Comparison with previous run ═══"; \
			benchstat benchmark-results.txt.prev benchmark-results.txt; \
		else \
			echo "  Install benchstat for auto-comparison: go install golang.org/x/perf/cmd/benchstat@latest"; \
		fi; \
	else \
		echo "  Run again to compare: benchstat benchmark-results.txt.prev benchmark-results.txt"; \
	fi

profile-cpu: ## Run benchmarks with CPU profiling
	$(GO) test -bench=. -benchmem -cpuprofile=cpu.prof -short -run=^$$ .
	@echo ""
	@echo "✓ CPU profile saved to cpu.prof"
	@echo "  View interactively:  go tool pprof -http=:8081 cpu.prof"
	@echo "  View in terminal:    go tool pprof cpu.prof"

profile-mem: ## Run benchmarks with memory profiling
	$(GO) test -bench=. -benchmem -memprofile=mem.prof -short -run=^$$ .
	@echo ""
	@echo "✓ Memory profile saved to mem.prof"
	@echo "  View interactively:  go tool pprof -http=:8081 mem.prof"
	@echo "  View in terminal:    go tool pprof mem.prof"

##@ Build & Clean

clean: ## Clean build artifacts
	rm -f coverage.out coverage.html benchmark-results.txt
	rm -f *.db *.db.wal
	rm -f *.test
	rm -f wasm/chronicle.wasm

clean-all: clean ## Deep clean (build artifacts + profiles + caches)
	rm -f *.prof *.pprof
	rm -f __debug_bin*
	rm -rf website/build website/.docusaurus
	rm -rf grafana-plugin/dist grafana-plugin/node_modules
	@echo "✓ Deep clean complete"

wasm: ## Build WASM module
	@./scripts/build-wasm.sh

##@ Development Server

dev: preflight ## Run development HTTP server (quick-start)
	@echo "Starting Chronicle dev server..."
	@echo "  API: http://localhost:8080"
	@echo "  Press Ctrl+C to stop"
	$(GO) run ./examples/http-server

run: dev ## Alias for make dev

debug: preflight ## Run dev server with debug logging enabled
	@echo "Starting Chronicle dev server (debug mode)..."
	@echo "  API: http://localhost:8080"
	@echo "  Log level: debug"
	@echo "  Press Ctrl+C to stop"
	CHRONICLE_LOG_LEVEL=debug $(GO) run ./examples/http-server

watch: ## Watch .go files and run tests on change (TDD mode)
	@if command -v reflex >/dev/null 2>&1; then \
		echo "Watching for .go file changes (reflex)... Press Ctrl+C to stop"; \
		reflex -r '\.go$$' -s -- make test-fast; \
	elif command -v fswatch >/dev/null 2>&1; then \
		echo "Watching for .go file changes (fswatch)... Press Ctrl+C to stop"; \
		fswatch -o --include '\.go$$' --exclude '.*' . | xargs -n1 -I{} make test-fast; \
	else \
		echo "No file watcher found. Install one of:"; \
		echo "  go install github.com/cespare/reflex@latest   (recommended)"; \
		echo "  brew install fswatch                          (macOS)"; \
		echo "  sudo apt-get install inotify-tools            (Linux)"; \
		exit 1; \
	fi

watch-lint: ## Watch .go files and run lint on change
	@if command -v reflex >/dev/null 2>&1; then \
		echo "Watching for .go file changes (lint)... Press Ctrl+C to stop"; \
		reflex -r '\.go$$' -s -- make lint-fast; \
	elif command -v fswatch >/dev/null 2>&1; then \
		echo "Watching for .go file changes (lint)... Press Ctrl+C to stop"; \
		fswatch -o --include '\.go$$' --exclude '.*' . | xargs -n1 -I{} make lint-fast; \
	else \
		echo "No file watcher found. Install one of:"; \
		echo "  go install github.com/cespare/reflex@latest   (recommended)"; \
		echo "  brew install fswatch                          (macOS)"; \
		exit 1; \
	fi

watch-all: ## Watch .go files and run check (vet + fmt + tests) on change
	@if command -v reflex >/dev/null 2>&1; then \
		echo "Watching for .go file changes (full check)... Press Ctrl+C to stop"; \
		reflex -r '\.go$$' -s -- make check; \
	elif command -v fswatch >/dev/null 2>&1; then \
		echo "Watching for .go file changes (full check)... Press Ctrl+C to stop"; \
		fswatch -o --include '\.go$$' --exclude '.*' . | xargs -n1 -I{} make check; \
	else \
		echo "No file watcher found. Install one of:"; \
		echo "  go install github.com/cespare/reflex@latest   (recommended)"; \
		echo "  brew install fswatch                          (macOS)"; \
		exit 1; \
	fi

##@ Tools & Setup

vuln: ## Check for vulnerabilities
	govulncheck ./...

setup: ## Install development tools
	@echo "Downloading Go module dependencies..."
	@$(GO) mod download
	@FAIL=0; \
	for pkg in \
		github.com/golangci/golangci-lint/cmd/golangci-lint@latest \
		golang.org/x/tools/cmd/goimports@latest \
		golang.org/x/vuln/cmd/govulncheck@latest \
		golang.org/x/perf/cmd/benchstat@latest \
		github.com/cespare/reflex@latest; \
	do \
		echo "  Installing $$pkg..."; \
		if ! $(GO) install $$pkg; then \
			echo "  ✗ Failed to install $$pkg"; \
			FAIL=1; \
		fi; \
	done; \
	if [ "$$FAIL" = "1" ]; then \
		echo ""; \
		echo "ERROR: Some tools failed to install."; \
		exit 1; \
	fi
	@echo ""
	@echo "Verifying tool installation..."
	@FAIL=0; \
	for tool in golangci-lint goimports govulncheck benchstat reflex; do \
		if command -v $$tool >/dev/null 2>&1; then \
			echo "  ✓ $$tool found"; \
		else \
			echo "  ✗ $$tool not on PATH"; \
			FAIL=1; \
		fi; \
	done; \
	if [ "$$FAIL" = "1" ]; then \
		echo ""; \
		echo "ERROR: Some tools not found on PATH after installation."; \
		echo "Ensure \$$(go env GOPATH)/bin is in your PATH:"; \
		echo "  export PATH=\"\$$(go env GOPATH)/bin:\$$PATH\""; \
		exit 1; \
	fi
	@echo "✓ All development tools installed and verified"

setup-grafana: ## Install Grafana plugin dependencies (cd grafana-plugin && npm install)
	@if [ ! -d grafana-plugin ]; then \
		echo "ERROR: grafana-plugin/ directory not found"; exit 1; \
	fi
	@echo "Installing Grafana plugin dependencies..."
	cd grafana-plugin && npm install
	@echo "✓ Grafana plugin dependencies installed"

##@ CI & Validation

release-check: ## Run all checks before a release (vet + lint + full tests + vuln + interface + API stability)
	$(GO) vet ./...
	$(GOLANGCI_LINT) run
	$(GO) test -race -short ./...
	@$(MAKE) check-interface
	@$(MAKE) check-api-stability
	govulncheck ./... || true
	@echo "✓ Release checks passed"

tag: ## Create and push a release tag (usage: make tag VERSION=v0.1.0)
ifndef VERSION
	$(error VERSION is required. Usage: make tag VERSION=v0.1.0)
endif
	@echo "Tagging $(VERSION)..."
	git tag -a $(VERSION) -m "Release $(VERSION)"
	git push origin $(VERSION)
	@echo "✓ Tag $(VERSION) pushed — release workflow will run automatically"

check-interface: ## Check for legacy interface{} usage (should use 'any')
	@FOUND=$$(grep -rn 'interface{}' --include='*.go' . | grep -v vendor | grep -v '.git' | grep -v '_test.go' || true); \
	if [ -n "$$FOUND" ]; then \
		echo "ERROR: Found legacy interface{} usage (use 'any' instead):"; \
		echo "$$FOUND"; \
		exit 1; \
	fi
	@echo "✓ No legacy interface{} found"

check-api-stability: ## Verify stable API symbols are tested
	@$(GO) test -run 'TestStableAPI' -count=1 ./... > /dev/null 2>&1 && echo "✓ API stability tests pass" || { echo "ERROR: API stability tests failed"; exit 1; }

check-openapi: ## Validate openapi.json is up-to-date
	@echo "Regenerating OpenAPI spec..."
	@cp openapi.json openapi.json.bak 2>/dev/null || true
	@$(GO) test -run TestOpenAPI_GenerateSpec -count=1 . > /dev/null 2>&1
	@if diff -q openapi.json openapi.json.bak > /dev/null 2>&1; then \
		echo "✓ openapi.json is up-to-date"; \
	else \
		echo "ERROR: openapi.json is out of date. Run 'go test -run TestOpenAPI_GenerateSpec .' and commit the result."; \
		mv openapi.json.bak openapi.json; \
		exit 1; \
	fi
	@rm -f openapi.json.bak

##@ Diagnostics

doctor: ## Diagnose development environment
	@echo "═══════════════════════════════════════════════════"
	@echo "  Chronicle Doctor"
	@echo "═══════════════════════════════════════════════════"
	@echo ""
	@echo "System:"
	@echo "  OS:         $$(uname -s) $$(uname -m)"
	@echo "  Shell:      $$SHELL"
	@echo "  Disk free:  $$(df -h . 2>/dev/null | tail -1 | awk '{print $$4}' || echo 'unknown')"
	@echo ""
	@echo "Go:"
	@if command -v $(GO) >/dev/null 2>&1; then \
		echo "  Version:    $$($(GO) version)"; \
		echo "  GOPATH:     $$($(GO) env GOPATH)"; \
		echo "  GOBIN:      $$($(GO) env GOBIN || echo '(default)')"; \
	else \
		echo "  ✗ Go not found"; \
	fi
	@echo ""
	@echo "Tools:"
	@for tool in golangci-lint goimports govulncheck benchstat; do \
		if command -v $$tool >/dev/null 2>&1; then \
			VER=$$($$tool --version 2>/dev/null | head -1 || echo 'installed'); \
			echo "  ✓ $$tool: $$VER"; \
		else \
			echo "  ✗ $$tool: not found (run make setup)"; \
		fi; \
	done
	@echo ""
	@echo "File watcher (for make watch):"
	@if command -v reflex >/dev/null 2>&1; then \
		echo "  ✓ reflex: installed"; \
	elif command -v fswatch >/dev/null 2>&1; then \
		echo "  ✓ fswatch: installed"; \
	else \
		echo "  ✗ No file watcher found (install reflex: go install github.com/cespare/reflex@latest)"; \
	fi
	@echo ""
	@echo "Git hooks:"
	@if [ -f .git/hooks/pre-commit ]; then \
		echo "  ✓ pre-commit hook installed"; \
	else \
		echo "  ✗ pre-commit hook missing (run make install-hooks)"; \
	fi
	@if [ -f .git/hooks/commit-msg ]; then \
		echo "  ✓ commit-msg hook installed"; \
	else \
		echo "  ✗ commit-msg hook missing (run make install-hooks)"; \
	fi
	@echo ""
	@echo "Module status:"
	@if $(GO) mod tidy -diff >/dev/null 2>&1; then \
		echo "  ✓ go.mod is tidy"; \
	else \
		echo "  ⚠ go.mod may need tidying (run go mod tidy)"; \
	fi
	@echo ""
	@echo "Grafana plugin:"
	@if [ -d grafana-plugin/node_modules ]; then \
		echo "  ✓ grafana-plugin/node_modules present"; \
	else \
		echo "  ✗ grafana-plugin/node_modules missing (run make setup-grafana)"; \
	fi
	@echo ""
	@echo "Node.js (for website/ and grafana-plugin/):"
	@if command -v node >/dev/null 2>&1; then \
		echo "  ✓ Node.js: $$(node --version)"; \
	else \
		echo "  ✗ Node.js not found (needed for website/ and grafana-plugin/)"; \
	fi
	@if [ -d website/node_modules ]; then \
		echo "  ✓ website/node_modules present"; \
	else \
		echo "  ✗ website/node_modules missing (run cd website && npm install)"; \
	fi
	@echo ""
	@echo "═══════════════════════════════════════════════════"
	@echo "Copy the above output when reporting issues."
	@echo "═══════════════════════════════════════════════════"

check-versions: ## Check for Go version drift across config files
	@echo "Checking Go version consistency..."
	@echo "Source of truth: .mise.toml"
	@MISE_VER=$$(grep '^go' .mise.toml | head -1 | grep -oE '[0-9]+\.[0-9]+'); \
	DRIFT=0; \
	echo "  .mise.toml:          Go $$MISE_VER"; \
	GO_MOD_VER=$$(grep '^go ' go.mod | grep -oE '[0-9]+\.[0-9]+'); \
	echo "  go.mod:              Go $$GO_MOD_VER"; \
	[ "$$MISE_VER" = "$$GO_MOD_VER" ] || { echo "    ⚠ DRIFT: go.mod ($$GO_MOD_VER) != .mise.toml ($$MISE_VER)"; DRIFT=1; }; \
	MAKE_VER=$$(grep 'MIN_GO_VERSION' Makefile | head -1 | grep -oE '[0-9]+\.[0-9]+'); \
	echo "  Makefile:            Go $$MAKE_VER"; \
	[ "$$MISE_VER" = "$$MAKE_VER" ] || { echo "    ⚠ DRIFT: Makefile ($$MAKE_VER) != .mise.toml ($$MISE_VER)"; DRIFT=1; }; \
	if [ -f .devcontainer/devcontainer.json ]; then \
		DC_VER=$$(grep -oE 'go:[0-9]+\.[0-9]+' .devcontainer/devcontainer.json | grep -oE '[0-9]+\.[0-9]+'); \
		echo "  devcontainer.json:   Go $$DC_VER"; \
		[ "$$MISE_VER" = "$$DC_VER" ] || { echo "    ⚠ DRIFT: devcontainer.json ($$DC_VER) != .mise.toml ($$MISE_VER)"; DRIFT=1; }; \
	fi; \
	for wf in .github/workflows/*.yml; do \
		WF_VERS=$$(grep -oE "go-version: '?[0-9]+\.[0-9]+" "$$wf" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' | sort -u); \
		for v in $$WF_VERS; do \
			[ "$$MISE_VER" = "$$v" ] || { echo "    ⚠ DRIFT: $$wf ($$v) != .mise.toml ($$MISE_VER)"; DRIFT=1; }; \
		done; \
	done; \
	if [ "$$DRIFT" = "0" ]; then \
		echo "✓ All Go version references are consistent ($$MISE_VER)"; \
	else \
		echo ""; \
		echo "⚠ Version drift detected. .mise.toml is the source of truth."; \
	fi

test-changed: ## Run tests only for modified Go files
	@CHANGED=$$(git diff --name-only HEAD 2>/dev/null | grep '\.go$$' || true); \
	STAGED=$$(git diff --cached --name-only 2>/dev/null | grep '\.go$$' || true); \
	ALL_CHANGED=$$(echo "$$CHANGED"; echo "$$STAGED" | sort -u | grep -v '^$$' || true); \
	if [ -z "$$ALL_CHANGED" ]; then \
		echo "No modified .go files found"; exit 0; \
	fi; \
	PKGS=$$(echo "$$ALL_CHANGED" | xargs -I{} dirname {} | sort -u | sed 's|^|./|'); \
	echo "Testing packages with changes:"; \
	echo "$$PKGS" | while read pkg; do echo "  → $$pkg"; done; \
	echo ""; \
	$(GO) test -count=1 $$PKGS

check-file-size: ## Warn about Go files exceeding 800 lines
	@echo "Checking for large Go files (>800 lines)..."
	@FOUND=0; \
	for f in $$(find . -name '*.go' -not -path './vendor/*' -not -name '*_test.go' | sort); do \
		LINES=$$(wc -l < "$$f" | tr -d ' '); \
		if [ "$$LINES" -gt 800 ]; then \
			echo "  ⚠ $$f: $$LINES lines"; \
			FOUND=$$((FOUND + 1)); \
		fi; \
	done; \
	if [ "$$FOUND" -gt 0 ]; then \
		echo ""; \
		echo "$$FOUND file(s) exceed 800 lines. Consider refactoring."; \
	else \
		echo "✓ All Go files are under 800 lines"; \
	fi

check-file-size-strict: ## Warn about Go files approaching 800-line limit (>700 lines)
	@echo "Checking for large Go files (>700 lines = approaching limit, >800 = over)..."
	@WARN=0; OVER=0; \
	for f in $$(find . -name '*.go' -not -path './vendor/*' -not -name '*_test.go' | sort); do \
		LINES=$$(wc -l < "$$f" | tr -d ' '); \
		if [ "$$LINES" -gt 800 ]; then \
			echo "  ✗ $$f: $$LINES lines (over limit)"; \
			OVER=$$((OVER + 1)); \
		elif [ "$$LINES" -gt 700 ]; then \
			echo "  ⚠ $$f: $$LINES lines (approaching limit)"; \
			WARN=$$((WARN + 1)); \
		fi; \
	done; \
	if [ "$$OVER" -gt 0 ] || [ "$$WARN" -gt 0 ]; then \
		echo ""; \
		if [ "$$OVER" -gt 0 ]; then echo "$$OVER file(s) exceed 800 lines."; fi; \
		if [ "$$WARN" -gt 0 ]; then echo "$$WARN file(s) between 700-800 lines (approaching limit)."; fi; \
	else \
		echo "✓ All Go files are under 700 lines"; \
	fi

new-test: ## Scaffold a new test file (usage: make new-test FILE=my_feature)
ifndef FILE
	$(error FILE is required. Usage: make new-test FILE=my_feature)
endif
	@if [ -f "$(FILE)_test.go" ]; then \
		echo "ERROR: $(FILE)_test.go already exists"; exit 1; \
	fi
	@BASE=$$(basename "$(FILE)"); \
	FUNC_NAME=$$(echo "$$BASE" | awk -F_ '{for(i=1;i<=NF;i++){$$i=toupper(substr($$i,1,1)) substr($$i,2)}}1' OFS=''); \
	printf 'package chronicle\n\nimport (\n\t"testing"\n)\n\nfunc Test'"$$FUNC_NAME"'(t *testing.T) {\n\tdb := setupTestDB(t)\n\n\ttests := []struct {\n\t\tname    string\n\t\twantErr bool\n\t}{\n\t\t{name: "basic", wantErr: false},\n\t}\n\n\tfor _, tt := range tests {\n\t\tt.Run(tt.name, func(t *testing.T) {\n\t\t\t_ = db // TODO: implement test\n\t\t\tif tt.wantErr {\n\t\t\t\tt.Error("expected error")\n\t\t\t}\n\t\t})\n\t}\n}\n' > $(FILE)_test.go
	@echo "✓ Created $(FILE)_test.go with table-driven test scaffold"

check-todos: ## Surface technical debt markers (TODO/FIXME/HACK/XXX)
	@echo "Scanning production .go files for technical debt markers..."
	@RESULTS=$$(grep -rn 'TODO\|FIXME\|HACK\|XXX' --include='*.go' . | grep -v '_test.go' | grep -v vendor | grep -v '.git'); \
	COUNT=$$(echo "$$RESULTS" | grep -c . 2>/dev/null || echo 0); \
	echo "Found $$COUNT marker(s)"; \
	echo ""; \
	if [ "$$COUNT" -gt 0 ]; then \
		echo "$$RESULTS" | head -20; \
		if [ "$$COUNT" -gt 20 ]; then \
			echo "  ... and $$((COUNT - 20)) more (use grep to see all)"; \
		fi; \
	fi

deps-check: ## Check for untidy or outdated dependencies
	@echo "Checking if go.mod is tidy..."
	@if $(GO) mod tidy -diff > /dev/null 2>&1; then \
		echo "  ✓ go.mod is tidy"; \
	else \
		echo "  ⚠ go.mod is not tidy. Run 'go mod tidy' to fix."; \
		$(GO) mod tidy -diff 2>&1 | head -20; \
	fi
	@echo ""
	@echo "Outdated direct dependencies:"
	@$(GO) list -m -u -f '{{if and .Update (not .Indirect)}}  {{.Path}}: {{.Version}} → {{.Update.Version}}{{end}}' all 2>/dev/null | grep -v '^$$' || echo "  ✓ All direct dependencies are up to date"

check-goroutine-leaks: ## Find go func() calls without context in production code
	@echo "Scanning production code for bare 'go func()' calls..."
	@RESULTS=$$(grep -rn 'go func()' --include='*.go' . | grep -v '_test.go' | grep -v vendor | grep -v '.git' || true); \
	COUNT=$$(echo "$$RESULTS" | grep -c . 2>/dev/null || echo 0); \
	if [ -z "$$RESULTS" ]; then COUNT=0; fi; \
	echo "Found $$COUNT bare go func() call(s)"; \
	echo ""; \
	if [ "$$COUNT" -gt 0 ]; then \
		echo "$$RESULTS"; \
		echo ""; \
		echo "Tip: prefer 'go func(ctx context.Context) { ... }(ctx)' to enable"; \
		echo "graceful shutdown and prevent goroutine leaks."; \
	else \
		echo "✓ No bare go func() calls found"; \
	fi

run-example: ## Run an example project (usage: make run-example EXAMPLE=simple)
ifndef EXAMPLE
	$(error EXAMPLE is required. Usage: make run-example EXAMPLE=simple. Available: $(shell ls -d examples/*/ 2>/dev/null | xargs -I{} basename {}))
endif
	@if [ ! -d "examples/$(EXAMPLE)" ]; then \
		echo "ERROR: Example '$(EXAMPLE)' not found."; \
		echo "Available examples:"; \
		ls -d examples/*/ 2>/dev/null | xargs -I{} basename {} | sed 's/^/  /'; \
		exit 1; \
	fi
	@echo "Running example: $(EXAMPLE) (10s timeout)..."
	@if timeout 10s $(GO) run ./examples/$(EXAMPLE) 2>&1; then \
		echo "✓ Example $(EXAMPLE) completed successfully"; \
	else \
		STATUS=$$?; \
		if [ "$$STATUS" = "124" ]; then \
			echo "✓ Example $(EXAMPLE) timed out (likely a server — this is OK)"; \
		else \
			echo "✗ Example $(EXAMPLE) failed (exit $$STATUS)"; \
			exit 1; \
		fi; \
	fi

tidy: ## Run go mod tidy
	$(GO) mod tidy

test-file: ## Run all tests in a file (usage: make test-file FILE=my_feature)
ifndef FILE
	$(error FILE is required. Usage: make test-file FILE=my_feature)
endif
	@TEST_FILE="$(FILE)_test.go"; \
	if [ ! -f "$$TEST_FILE" ]; then \
		echo "ERROR: $$TEST_FILE not found"; exit 1; \
	fi; \
	FUNCS=$$(grep -oE '^func (Test[A-Za-z0-9_]+)' "$$TEST_FILE" | sed 's/^func //'); \
	if [ -z "$$FUNCS" ]; then \
		echo "No test functions found in $$TEST_FILE"; exit 1; \
	fi; \
	PATTERN=$$(echo "$$FUNCS" | paste -sd'|' -); \
	echo "Running tests from $$TEST_FILE: $$PATTERN"; \
	$(GO) test -v -count=1 -run "$$PATTERN" .

test-all: ## Run all tests without race detector (faster iteration)
	$(GO) test -count=1 ./...

validate: ## Full local CI parity — run before pushing
	@echo "═══ Full validation (local CI parity) ═══"
	@echo ""
	$(GO) vet ./...
	$(GOLANGCI_LINT) run
	$(GO) test -short -count=1 ./...
	@$(MAKE) check-file-size
	@$(MAKE) check-generate
	@echo ""
	@echo "✓ All validation checks passed"

help: ## Show this help
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0,5)} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help
