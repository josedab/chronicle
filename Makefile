.PHONY: all build test test-short test-fast test-integration test-ci lint fmt clean bench check quickcheck cover cover-report vet setup install-hooks preflight help

GO ?= go
GOFLAGS ?= -race
MIN_GO_VERSION := 1.24

all: lint test build ## Run lint, test, and build

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
	$(GO) test $(GOFLAGS) ./...

test-short: ## Run short tests only (fast iteration)
	$(GO) test -short ./...

test-fast: ## Run only internal package tests (fastest feedback)
	$(GO) test ./internal/...

test-integration: ## Run all tests including integration
	$(GO) test $(GOFLAGS) -tags integration ./...

test-ci: ## Run the same checks as CI (vet + all tests + race)
	$(GO) vet ./...
	$(GO) test $(GOFLAGS) -short ./...

test-cover: ## Run tests with coverage report (HTML)
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

cover: ## Print coverage summary to terminal
	$(GO) test -coverprofile=coverage.out -short ./...
	$(GO) tool cover -func=coverage.out | tail -1
	@rm -f coverage.out

cover-report: ## Per-package coverage report (use -html for HTML output)
	@./scripts/coverage.sh

vet: ## Run go vet
	$(GO) vet ./...

check: preflight vet test-fast ## Quick pre-commit validation (preflight + vet + fast tests)

quickcheck: ## Full pre-push validation (vet + all short tests)
	$(GO) vet ./...
	$(GO) test -short -count=1 ./...

install-hooks: ## Install git pre-commit hook
	ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
	@echo "Pre-commit hook installed ✓"

lint: ## Run linters
	$(GO) vet ./...
	$(GO) run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run

fmt: ## Format code
	gofmt -s -w .
	goimports -w .

bench: ## Run benchmarks
	$(GO) test -bench=. -benchmem ./...

clean: ## Clean build artifacts
	rm -f coverage.out coverage.html
	rm -f *.db *.db.wal
	rm -f *.test

vuln: ## Check for vulnerabilities
	govulncheck ./...

setup: ## Install development tools
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	$(GO) install golang.org/x/tools/cmd/goimports@latest
	$(GO) install golang.org/x/vuln/cmd/govulncheck@latest

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
