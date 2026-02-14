.PHONY: all build test test-short test-fast test-integration lint fmt clean bench check cover cover-report vet setup help

GO ?= go
GOFLAGS ?= -race

all: lint test build ## Run lint, test, and build

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

check: vet test-fast ## Quick pre-commit validation (vet + fast tests)

lint: ## Run linters
	$(GO) vet ./...
	golangci-lint run

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
