# ADR 0016: Four Query Languages

## Status
Accepted

## Context
Different user communities expect different query interfaces:
- DevOps teams know PromQL from Prometheus
- Data engineers prefer SQL
- Advanced users need time-series-specific operations (GAP_FILL, ASOF JOIN)
- Frontend developers prefer GraphQL

Supporting only one language limits adoption to one community.

## Decision
Chronicle supports four query languages from a single engine:

1. **SQL-like** (Production) — core parser in parser.go; familiar SELECT/FROM/WHERE/GROUP BY
2. **PromQL** (Beta) — subset implementation in promql.go; compatible with Grafana/Prometheus
3. **CQL** (Beta) — Chronicle Query Language in cql_bridge.go; adds WINDOW, GAP_FILL, ALIGN, ASOF JOIN
4. **GraphQL** (Beta) — graphql.go with interactive playground

All four languages compile down to the same internal Query struct.
The core Execute() method only knows about Query — not about query languages.

## Consequences
- **Positive**: Each community can use their preferred syntax; Grafana works out-of-box with PromQL
- **Negative**: Four parsers to maintain; edge cases in translation
- **Mitigation**: CQL and GraphQL delegate to the same Query struct; PromQL uses Prometheus's own parser
