# Contributor Bounty Program

Chronicle offers bounties for valuable contributions to accelerate community growth.

## How It Works

1. Find an issue labeled `bounty` + `good first issue`
2. Comment "I'd like to work on this" to claim it
3. Submit a PR following our [Contributing Guide](../CONTRIBUTING.md)
4. Once merged, receive the bounty via GitHub Sponsors or PayPal

## Current Bounties

### 🟢 Documentation ($25 each)

| # | Title | Description | Status |
|---|-------|-------------|--------|
| 1 | Add PromQL query examples | Add 10+ PromQL examples to docs/API.md | Open |
| 2 | Add CQL query examples | Document CQL-specific features (WINDOW, GAP_FILL, ASOF JOIN) | Open |
| 3 | Add FAQ entries | Add 10+ frequently asked questions to docs/FAQ.md | Open |
| 4 | Improve error messages doc | Document all error types with troubleshooting steps | Open |
| 5 | Write "Chronicle vs X" comparison | Compare with InfluxDB, VictoriaMetrics, or Prometheus | Open |

### 🟡 Testing ($50 each)

| # | Title | Description | Status |
|---|-------|-------------|--------|
| 6 | Add edge case tests for parser | Test SQL parser with Unicode, escapes, injection attempts | Open |
| 7 | Add concurrent write stress test | 100 goroutines × 10K points each, verify no data loss | Open |
| 8 | Add large dataset test | Write 1M+ points, verify query correctness | Open |
| 9 | Test on Windows | Verify all tests pass on Windows, document any issues | Open |
| 10 | Test on 32-bit ARM | Run on Pi Zero 2, document limitations | Open |

### 🟡 Examples ($50 each)

| # | Title | Description | Status |
|---|-------|-------------|--------|
| 11 | Kubernetes sidecar example | Deploy Chronicle as a sidecar collecting pod metrics | Open |
| 12 | Docker Compose monitoring stack | Chronicle + Grafana + alerting in docker-compose | Open |
| 13 | Home automation example | Collect sensor data from MQTT/Zigbee/Z-Wave | Open |
| 14 | Financial tick data example | Store and query high-frequency trading data | Open |
| 15 | Weather station example | Collect and visualize weather sensor data | Open |

### 🔴 Code ($100-200 each)

| # | Title | Description | Bounty | Status |
|---|-------|-------------|--------|--------|
| 16 | Implement LAST_VALUE aggregation | Add AggLastValue to query engine | $100 | Open |
| 17 | Add PERCENTILE aggregation | Implement configurable percentile (p50, p90, p95, p99) | $100 | Open |
| 18 | Implement NOT IN tag filter | Add TagOpNotIn to complement TagOpIn | $100 | Open |
| 19 | Add Prometheus metrics exposition | Expose Chronicle internals as /metrics in Prometheus format | $150 | Open |
| 20 | Implement query result streaming | Stream large query results via chunked HTTP response | $200 | Open |

## Rules

- One person per bounty (first to claim gets it)
- PRs must include tests and pass CI
- PRs must follow coding conventions (see CONTRIBUTING.md)
- Bounty paid within 7 days of merge
- Maintainer decision on acceptance is final

## Claiming a Bounty

1. Check the issue isn't already claimed
2. Comment: "I'd like to work on this"
3. You have 7 days to submit a PR (extensions available)
4. If no PR after 14 days, the bounty reopens

## Contact

Questions? Open a Discussion or email chronicle-contributors@googlegroups.com
