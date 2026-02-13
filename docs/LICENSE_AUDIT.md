# Dependency License Audit

## Summary

All 10 direct dependencies use licenses compatible with Chronicle's Apache-2.0 license.
No copyleft (GPL/AGPL) or restrictive licenses found.

## Direct Dependencies

| Dependency | Version | License | Compatible |
|-----------|---------|---------|------------|
| github.com/aws/aws-sdk-go-v2 | v1.41.1 | Apache-2.0 | ✅ |
| github.com/aws/aws-sdk-go-v2/config | v1.27.11 | Apache-2.0 | ✅ |
| github.com/aws/aws-sdk-go-v2/credentials | v1.17.11 | Apache-2.0 | ✅ |
| github.com/aws/aws-sdk-go-v2/service/s3 | v1.53.1 | Apache-2.0 | ✅ |
| github.com/golang/snappy | v1.0.0 | BSD-3-Clause | ✅ |
| github.com/gorilla/websocket | v1.5.4 | BSD-2-Clause | ✅ |
| github.com/prometheus/prometheus | v0.53.0 | Apache-2.0 | ✅ |
| golang.org/x/crypto | v0.28.0 | BSD-3-Clause | ✅ |
| gopkg.in/yaml.v3 | v3.0.1 | MIT + Apache-2.0 | ✅ |
| modernc.org/sqlite | v1.44.3 | BSD-3-Clause | ✅ |

## License Compatibility Matrix

| License | Compatible with Apache-2.0? | Found in Chronicle deps? |
|---------|---------------------------|-------------------------|
| Apache-2.0 | ✅ Yes | AWS SDK, Prometheus |
| MIT | ✅ Yes | yaml.v3 |
| BSD-2-Clause | ✅ Yes | gorilla/websocket |
| BSD-3-Clause | ✅ Yes | snappy, x/crypto, sqlite |
| GPL-2.0 | ❌ No (copyleft) | Not used |
| AGPL-3.0 | ❌ No (network copyleft) | Not used |
| LGPL | ⚠️ Conditional | Not used |

## Generating SBOM

```bash
# Install go-licenses
go install github.com/google/go-licenses@latest

# Check all licenses
go-licenses check ./...

# Generate CSV report
go-licenses csv ./... > licenses.csv

# Generate NOTICE file
go-licenses save ./... --save_path=./NOTICES
```

## CI Integration

The dependency-review workflow (.github/workflows/dependency-review.yml)
automatically checks new dependencies for license compatibility on every PR.

## Audit Date
2026-02-22

## Auditor
Automated analysis + manual verification of license headers.
