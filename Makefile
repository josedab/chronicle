.PHONY: all build test lint fmt clean bench help

GO ?= go
GOFLAGS ?= -race

all: lint test build ## Run lint, test, and build

build: ## Build the package
	$(GO) build ./...

test: ## Run tests with race detector
	$(GO) test $(GOFLAGS) ./...

test-cover: ## Run tests with coverage
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

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

vuln: ## Check for vulnerabilities
	govulncheck ./...

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
