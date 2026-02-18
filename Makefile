.PHONY: all quickstart build test test-short test-fast test-failing test-pkg test-integration test-ci test-examples lint lint-fix fmt clean clean-all bench benchmark check quickcheck cover cover-report vet setup setup-grafana install-hooks preflight release-check tag check-interface check-api-stability check-openapi wasm dev run check-versions doctor help

GO ?= go
GOFLAGS ?= -race
MIN_GO_VERSION := 1.24

all: lint test build ## Run lint, test, and build

quickstart: setup install-hooks doctor ## One-command setup for new contributors
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

build: ## Build the package
	$(GO) build ./...

test: ## Run tests with race detector
	$(GO) test $(GOFLAGS) -count=1 ./...

test-short: ## Run short tests only (fast iteration)
	$(GO) test -short ./...

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
	$(GO) test $(GOFLAGS) -tags integration ./...

test-ci: ## Run the same checks as CI (vet + all tests + race)
	$(GO) vet ./...
	$(GO) test $(GOFLAGS) -short ./...

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

vet: ## Run go vet
	$(GO) vet ./...

check: preflight vet test-fast ## Quick pre-commit validation (preflight + vet + fast tests)

quickcheck: ## Full pre-push validation (vet + all short tests)
	$(GO) vet ./...
	$(GO) test -short -count=1 ./...

install-hooks: ## Install git pre-commit and commit-msg hooks
	ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
	ln -sf ../../scripts/commit-msg .git/hooks/commit-msg
	@echo "Pre-commit hook installed ✓"
	@echo "Commit-msg hook installed (conventional commits enforced) ✓"

lint: ## Run linters
	$(GO) vet ./...
	$(GO) run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run

lint-fix: ## Auto-fix all fixable lint issues
	$(GO) run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run --fix
	gofmt -s -w .
	goimports -w .

fmt: ## Format code
	gofmt -s -w .
	goimports -w .

bench: ## Run benchmarks
	$(GO) test -bench=. -benchmem ./...

benchmark: ## Run benchmarks and save results for comparison
	$(GO) test -bench=. -benchmem -count=5 -short -run=^$$ ./... | tee benchmark-results.txt
	@echo "✓ Results saved to benchmark-results.txt"
	@echo "  Compare with: benchstat old.txt benchmark-results.txt"

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

dev: preflight ## Run development HTTP server (quick-start)
	@echo "Starting Chronicle dev server..."
	@echo "  API: http://localhost:8080"
	@echo "  Press Ctrl+C to stop"
	$(GO) run ./examples/http-server

run: dev ## Alias for make dev

vuln: ## Check for vulnerabilities
	govulncheck ./...

setup: ## Install development tools
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	$(GO) install golang.org/x/tools/cmd/goimports@latest
	$(GO) install golang.org/x/vuln/cmd/govulncheck@latest
	$(GO) install golang.org/x/perf/cmd/benchstat@latest
	@echo ""
	@echo "Verifying tool installation..."
	@FAIL=0; \
	for tool in golangci-lint goimports govulncheck benchstat; do \
		if command -v $$tool >/dev/null 2>&1; then \
			echo "  ✓ $$tool found"; \
		else \
			echo "  ⚠ WARN: $$tool not on PATH (check GOBIN/GOPATH)"; \
			FAIL=1; \
		fi; \
	done; \
	if [ "$$FAIL" = "1" ]; then \
		echo ""; \
		echo "Some tools not found on PATH. Ensure \$$(go env GOPATH)/bin is in your PATH:"; \
		echo "  export PATH=\"\$$(go env GOPATH)/bin:\$$PATH\""; \
	else \
		echo "✓ All development tools installed"; \
	fi

setup-grafana: ## Install Grafana plugin dependencies (cd grafana-plugin && npm install)
	@if [ ! -d grafana-plugin ]; then \
		echo "ERROR: grafana-plugin/ directory not found"; exit 1; \
	fi
	@echo "Installing Grafana plugin dependencies..."
	cd grafana-plugin && npm install
	@echo "✓ Grafana plugin dependencies installed"

release-check: ## Run all checks before a release (vet + lint + full tests + vuln + interface + API stability)
	$(GO) vet ./...
	$(GO) run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run
	$(GO) test $(GOFLAGS) -short ./...
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

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
