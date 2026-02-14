# HTTP Server Example

Full HTTP API server with automatic sample data generation.

## Run

```bash
go run main.go
```

Then test with:

```bash
curl http://localhost:8086/health
curl http://localhost:8086/metrics
```

## Features Demonstrated

- HTTP API endpoints (write, query, metrics)
- Prometheus remote write ingestion
- GraphQL playground at `/graphql/playground`
- Admin dashboard at `/admin`
