# OpenAPI Specification

Chronicle provides an OpenAPI 3.0.3 specification for its HTTP API.

## Generating the Spec

```bash
# Generate openapi.json from Go code
go test -run TestOpenAPI_GenerateSpec -v .

# The spec is written to openapi.json in the project root
```

## Using the Spec

### View in Swagger UI
```bash
# Using docker
docker run -p 8081:8080 -e SWAGGER_JSON=/spec/openapi.json \
  -v $(pwd)/openapi.json:/spec/openapi.json \
  swaggerapi/swagger-ui
```

### Generate Python Client
```bash
pip install openapi-python-client
openapi-python-client generate --path openapi.json --output chronicle-python
```

### Generate TypeScript Client
```bash
npx openapi-typescript openapi.json --output chronicle-api.d.ts
```

### Generate Rust Client
```bash
cargo install openapi-generator-cli
openapi-generator-cli generate -i openapi.json -g rust -o chronicle-rust
```

## Endpoints Covered

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/write` | POST | Write points (Influx line protocol) |
| `/query` | POST | Execute SQL-like query |
| `/api/v1/prom/write` | POST | Prometheus remote write |
| `/api/v1/query` | POST | PromQL instant query |
| `/api/v1/query_range` | POST | PromQL range query |
| `/api/v1/export` | GET | Export data |
| `/api/v1/forecast` | POST | Time-series forecasting |
| `/health` | GET | Health check |
| `/health/ready` | GET | Readiness probe |
| `/health/live` | GET | Liveness probe |
| `/metrics` | GET | List metrics |

## Versioning

The API version is `0.1.0` (pre-1.0). Breaking changes may occur
between minor versions for Beta/Experimental endpoints.
Stable endpoints (marked in api_stability.go) follow semver.
