# Conference Talk Proposals

## GopherCon 2026 CFP

### Title
Building an Embedded Time-Series Database in Go: Architecture, Compression, and Lessons Learned

### Abstract (300 words)
Time-series databases power IoT, observability, and real-time analytics, but deploying them at the edge means running heavyweight servers on constrained devices. What if your TSDB was just a Go library?

Chronicle is an embedded time-series database designed to run inside your Go process — no separate server, no Docker, no network overhead. In this talk, I'll walk through the architecture decisions, tradeoffs, and lessons learned from building a 250K-line TSDB that stores everything in a single file.

We'll cover:
- **Columnar storage design**: Why append-only partitions with Gorilla float compression achieve 8-12x ratios
- **B-tree indexing**: How to make partition lookups O(log N) while keeping the index in-memory
- **WAL crash recovery**: Designing for power-loss on edge devices running on unreliable power
- **The FeatureManager pattern**: How sync.Once enables 126 lazy-initialized features without startup cost
- **Write pipeline architecture**: Composing validation, hooks, schema checking, and audit logging into the core Write() path
- **Supporting 4 query languages**: SQL, PromQL, CQL, and GraphQL from one engine without 4x the code
- **API stability at scale**: Managing 25 stable, 28 beta, and 59 experimental symbols with clear graduation paths

The audience will leave with practical patterns for building high-performance embedded databases in Go, applicable to any domain where you need structured storage without external infrastructure.

### Bio
[Speaker bio here]

### Talk Type
Regular session (25-45 minutes)

### Target Audience
Intermediate to advanced Go developers interested in database internals, performance optimization, and embedded systems.

### Outline
1. **The Problem** (3 min): Why edge devices need embedded TSDBs
2. **Architecture Overview** (5 min): Layers, partitions, WAL, index
3. **Compression Deep Dive** (7 min): Gorilla, delta, dictionary — live demo
4. **FeatureManager Pattern** (5 min): sync.Once at scale
5. **Write Pipeline** (5 min): Composable middleware for data integrity
6. **Multi-Language Queries** (5 min): Parsing 4 languages without 4 parsers
7. **Lessons Learned** (5 min): What worked, what didn't, what I'd change
8. **Live Demo** (5 min): Write, query, and visualize on a Raspberry Pi
9. **Q&A** (5 min)

---

## GoLab / GoCon CFP (shorter version)

### Title
The SQLite of Time-Series: Building an Embeddable TSDB in Go

### Abstract (150 words)
Every time-series database requires a separate server. Chronicle changes that — it's a TSDB that runs inside your Go process, stores data in a single file, and supports SQL, PromQL, CQL, and GraphQL queries. No Docker, no ports, no infrastructure.

In this talk, I'll share the key architectural decisions: why Gorilla compression achieves 8-12x ratios on sensor data, how a B-tree index enables sub-millisecond lookups across millions of partitions, and how the FeatureManager pattern lets 126 features coexist without startup cost using sync.Once.

You'll leave with practical patterns for building high-performance embedded storage in Go.

---

## Meetup Version (15 min)

### Title
Chronicle: An Embedded TSDB for Go

### Format
15-minute lightning talk with live demo on Raspberry Pi
