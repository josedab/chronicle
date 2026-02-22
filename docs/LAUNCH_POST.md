# Launch Post: Show HN

## Title
Show HN: Chronicle – Embedded time-series database for Go (single file, 4 query languages)

## Post Body

Hi HN, I built Chronicle – an embedded time-series database that runs inside your Go process. No separate server, no Docker, no ports – just `go get` and import.

**What it does:**
- Single-file storage with Gorilla compression (8-12x)
- 4 query languages: SQL, PromQL, CQL, GraphQL
- Write pipeline with validation, hooks, and audit logging
- Built for IoT/edge: runs on Raspberry Pi with 64MB RAM budget
- Prometheus remote write compatible
- Health checks, rate limiting, tenant isolation

**Quick example:**
```go
db, _ := chronicle.Open("data.db", chronicle.DefaultConfig("data.db"))
db.Write(chronicle.Point{Metric: "temperature", Value: 22.5, Tags: map[string]string{"room": "lab"}})
result, _ := db.Execute(&chronicle.Query{Metric: "temperature", Limit: 10})
```

**Why I built this:** I needed a TSDB for IoT edge gateways where running InfluxDB/Prometheus was too heavy. Every alternative requires a separate process, separate port, separate lifecycle management. I wanted `import "chronicle"` and done.

**What makes it different:**
- True in-process embedding (not a sidecar)
- Single file = backup is `cp data.db backup.db`
- 250K LOC, 25 stable API symbols, 28 beta, 59 experimental
- 9 CI workflows, 21 docs, 9 examples
- Apache 2.0 license

**What I'm looking for:**
- Feedback on the API design
- Performance testing on different hardware
- Use cases I haven't thought of
- Contributors (good-first-issues labeled)

Repo: https://github.com/chronicle-db/chronicle
Docs: https://chronicle-db.github.io/chronicle
Playground: https://chronicle-db.github.io/chronicle/playground

---

## Reddit Post (r/golang)

**Title:** I built an embedded time-series database for Go – single file, 4 query languages, runs on Raspberry Pi

Same body as HN, plus:

"I've been a Go developer for X years and this is my attempt at solving the 'I just need a lightweight TSDB' problem. Would love feedback from the r/golang community."

---

## Posting Strategy

**Timing:**
- HN: Tuesday or Wednesday, 10:00-11:00 AM ET (peak)
- Reddit r/golang: Same day, 2 hours after HN
- Reddit r/selfhosted: Day after

**Preparation checklist:**
- [ ] README is polished and has quick-start at top
- [ ] Playground is working
- [ ] Benchmarks page is published
- [ ] At least 3 good-first-issues are labeled
- [ ] Demo video is recorded and linked

**Expected Q&A:**

Q: "How does this compare to SQLite + time functions?"
A: "SQLite doesn't have Gorilla compression, PromQL, or automatic retention/downsampling. Chronicle is purpose-built for time-series with 8-12x compression."

Q: "Why not just use Prometheus?"
A: "Prometheus runs as a separate process with its own lifecycle. Chronicle is a library – same process, same binary, zero network overhead."

Q: "Bus factor of 1 – why should I trust this?"
A: "Fair concern. The stable API is only 25 symbols, covered by semver. The code has 252 test files and 9 CI workflows. I'm actively seeking co-maintainers."

Q: "250K LOC in 26 days? Is this AI-generated?"
A: "Parts of the experimental feature surface were AI-assisted. The core engine (storage, WAL, index, query) is hand-written and thoroughly tested."
