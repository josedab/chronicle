# Compliance Certification Guide

This guide maps Chronicle's security features to common compliance frameworks.

## Feature Matrix

| Chronicle Feature | SOC 2 | HIPAA | GDPR | PCI DSS |
|-------------------|-------|-------|------|---------|
| Encryption at rest (AES-256-GCM) | CC6.1 | §164.312(a)(2)(iv) | Art. 32 | Req 3.4 |
| Audit logging | CC7.2 | §164.312(b) | Art. 30 | Req 10 |
| Data masking | CC6.1 | §164.514 | Art. 25 | Req 3.3 |
| Tenant isolation | CC6.3 | §164.312(a)(1) | Art. 32 | Req 7 |
| Rate limiting | CC6.6 | — | — | Req 6.5.10 |
| Schema validation | CC8.1 | — | Art. 25 | — |
| Hot backups | CC7.5 | §164.308(a)(7) | Art. 32 | Req 9.5 |
| Health checks | CC7.1 | — | — | Req 11.5 |
| Point validation | CC8.1 | — | Art. 25 | — |
| Write hooks (enrichment) | CC8.1 | — | Art. 25 | — |

## SOC 2 Configuration

```go
cfg := chronicle.DefaultConfig(path)

// CC6.1 — Logical and Physical Access Controls
cfg.Encryption = &chronicle.EncryptionConfig{
    Enabled:    true,
    Algorithm:  "aes-256-gcm",
    KeyDerivation: "pbkdf2",
}
cfg.Auth = &chronicle.AuthConfig{
    Enabled: true,
    APIKeys: []string{"your-api-key"},
}

// CC6.6 — System Operations (rate limiting)
cfg.RateLimitPerSecond = 10000

db, _ := chronicle.Open(path, cfg)

// CC7.2 — System Monitoring (audit log)
al := db.AuditLog()  // automatically records all operations

// CC8.1 — Change Management (point validation)
pv := db.PointValidator()  // validates all writes
```

## HIPAA Configuration

```go
// §164.312(a)(2)(iv) — Encryption
cfg.Encryption = &chronicle.EncryptionConfig{Enabled: true}

// §164.514 — De-identification via data masking
dm := db.DataMasking()
dm.AddRule(chronicle.MaskingRule{
    ID:            "phi-redact",
    MetricPattern: "*",
    TagKey:        "patient_id",
    Action:        "hash",
    Role:          "analyst",
})

// §164.308(a)(7) — Contingency Plan (backups)
ib := db.IncrementalBackup()
// Schedule regular backups

// §164.312(b) — Audit Controls
al := db.AuditLog()  // all access is logged
```

## GDPR Configuration

```go
// Art. 25 — Data Protection by Design
pv := db.PointValidator()  // enforces data quality at ingestion

// Art. 32 — Security of Processing
cfg.Encryption = &chronicle.EncryptionConfig{Enabled: true}
ti := db.TenantIsolation()  // per-tenant data isolation

// Art. 17 — Right to Erasure
ml := db.MetricLifecycle()  // metric archival and deletion

// Art. 30 — Records of Processing
al := db.AuditLog()  // comprehensive access log

// Art. 25 — Data Minimization via masking
dm := db.DataMasking()
dm.AddRule(chronicle.MaskingRule{
    ID:            "pii-redact",
    MetricPattern: "user.*",
    TagKey:        "email",
    Action:        "redact",
    Role:          "default",
})
```

## Audit Log Retention

For compliance, configure audit log retention:

```go
alCfg := chronicle.DefaultAuditLogConfig()
alCfg.MaxEntries = 1000000    // Keep 1M entries
alCfg.LogWrites = true        // Log all write operations
alCfg.LogQueries = true       // Log all query operations
alCfg.LogAdmin = true         // Log all admin operations
```

## Disclaimer

This guide provides configuration recommendations for meeting compliance requirements.
It does not constitute legal advice or a compliance certification. Consult with your
compliance team and auditors for specific requirements.
