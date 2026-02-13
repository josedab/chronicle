# CNCF Landscape Submission

## Target Category
Observability and Analysis → Time Series

## Submission PR Template

Repository: https://github.com/cncf/landscape
File to edit: `landscape.yml`

### Addition to Observability > Time Series

```yaml
- item:
    name: Chronicle
    homepage_url: https://chronicle-db.github.io/chronicle
    repo_url: https://github.com/chronicle-db/chronicle
    logo: chronicle.svg
    description: Embedded time-series database for Go designed for constrained and edge environments
    project: false
    category: Observability and Analysis
    subcategory: Time Series
```

### PR Description Template

**Project Name:** Chronicle
**Project URL:** https://github.com/chronicle-db/chronicle
**Project Logo:** [SVG logo to be created]

**What is Chronicle?**
Chronicle is an embedded time-series database for Go optimized for constrained and edge environments. Unlike InfluxDB, VictoriaMetrics, or Prometheus (which run as separate processes), Chronicle is a Go library that runs inside your application process with single-file storage.

**Why should Chronicle be on the landscape?**
- Unique positioning: only in-process embeddable TSDB for Go
- Apache 2.0 license
- Production-ready core with 25 stable API symbols
- 9 CI workflows including CodeQL and OpenSSF Scorecard
- Prometheus remote write compatible
- OpenTelemetry OTLP ingestion support

**Category justification:**
Chronicle belongs in "Time Series" alongside InfluxDB, VictoriaMetrics, TimescaleDB, and QuestDB. While those are standalone servers, Chronicle serves the same data storage and query use case as an embedded library — similar to how SQLite is listed alongside PostgreSQL in database landscapes.

## Prerequisites Checklist

- [x] Open source with Apache 2.0 license
- [x] Public GitHub repository
- [x] README with clear description
- [x] Active development (291 commits)
- [x] CI/CD pipeline
- [ ] Logo in SVG format (needs creation)
- [ ] 50+ GitHub stars (needs launch)
- [x] Clear project governance (CONTRIBUTING.md, SECURITY.md)

## Logo Requirements
- SVG format
- Square aspect ratio recommended
- Must be original work or properly licensed
- Submitted alongside landscape PR

## Timeline
1. Create SVG logo
2. Achieve initial community traction (50+ stars)
3. Submit landscape PR
4. Respond to reviewer feedback
5. Expected merge: 2-4 weeks after submission
