# ADR-0008: Tag-Based Multi-Tenancy Isolation

## Status

Accepted

## Context

Chronicle deployments often need to serve multiple tenants:

1. **SaaS platforms**: Single Chronicle instance serving multiple customers
2. **Organizational boundaries**: Different teams sharing infrastructure
3. **Environment isolation**: Dev/staging/prod data in single instance

Multi-tenancy approaches vary in isolation strength and operational complexity:

| Approach | Isolation | Overhead | Complexity |
|----------|-----------|----------|------------|
| Separate instances | Complete | High (N instances) | Low |
| Separate databases | Strong | Medium (N databases) | Medium |
| Separate schemas | Medium | Low | Medium |
| Row-level (tags) | Logical | Minimal | Low |

For edge deployments where Chronicle runs, resource efficiency is critical. Running separate instances per tenant is often impractical.

## Decision

Chronicle implements **tag-based multi-tenancy** with automatic tenant tag injection:

### Tenant Tag

All multi-tenant operations use a reserved tag:

```go
const TenantTagKey = "__tenant__"
```

### Write Path

When writing points with tenancy enabled:

1. Tenant identifier extracted from request context (header, API key, etc.)
2. `__tenant__` tag automatically added to each point
3. Points stored with tenant tag like any other tag

```go
func (tm *TenantManager) TagPoint(point Point, tenant string) Point {
    if point.Tags == nil {
        point.Tags = make(map[string]string)
    }
    point.Tags[TenantTagKey] = tenant
    return point
}
```

### Query Path

When querying with tenancy enabled:

1. Tenant identifier extracted from request context
2. `__tenant__` filter automatically injected into query
3. Results returned with tenant tag stripped (transparent to client)

```go
func (tm *TenantManager) ScopeQuery(query Query, tenant string) Query {
    if query.Tags == nil {
        query.Tags = make(map[string]string)
    }
    query.Tags[TenantTagKey] = tenant
    return query
}
```

### Configuration

```go
type TenantConfig struct {
    Enabled       bool
    AllowedList   []string  // Optional: restrict to known tenants
    DefaultTenant string    // Fallback when not specified
}
```

## Consequences

### Positive

- **Resource efficiency**: Single instance serves all tenants
- **Operational simplicity**: One database to backup, monitor, upgrade
- **Query flexibility**: Cross-tenant queries possible for admin users
- **Transparent to clients**: Tenant isolation handled automatically
- **Consistent model**: Tenancy uses same tag mechanism as other metadata

### Negative

- **Logical isolation only**: No physical separation, shared resources
- **Noisy neighbor risk**: One tenant's load affects others
- **Security boundary**: Bugs could leak data across tenants
- **Index overhead**: Tenant tag increases index size
- **No per-tenant limits**: Resource quotas must be implemented separately

### Security Considerations

**Attack surface**:
- Query injection could bypass tenant filter (mitigated by parser-level injection)
- Tag manipulation could spoof tenant (mitigated by server-side tag injection)
- API key compromise exposes tenant data (standard credential security applies)

**Mitigations implemented**:
- Tenant tag is reserved; client-provided `__tenant__` tags are rejected
- Query scoping happens at lowest level, after parsing
- Tenant ID extracted from authenticated context, not request body

### Operational Patterns

**Tenant identification**:
```
Authorization: Bearer <api_key>  → API key maps to tenant
X-Tenant-ID: customer-123       → Explicit tenant header (admin only)
```

**Cross-tenant queries** (admin):
```sql
SELECT * FROM cpu_usage WHERE __tenant__ IN ('tenant-a', 'tenant-b')
```

**Tenant metrics**:
```sql
SELECT count(*) FROM * GROUP BY __tenant__  -- Points per tenant
```

### Comparison with Alternatives

| Feature | Tag-based | Separate DBs | Separate Instances |
|---------|-----------|--------------|-------------------|
| Resource efficiency | High | Medium | Low |
| Isolation strength | Logical | Strong | Complete |
| Cross-tenant query | Easy | Possible | Difficult |
| Per-tenant backup | Complex | Easy | Easy |
| Per-tenant scaling | No | Limited | Full |

### Enabled

- Multi-tenant SaaS deployments on edge hardware
- Organizational data sharing with access control
- Simplified operations for multi-tenant scenarios
- Tenant-aware usage tracking and billing

### Prevented

- Physical data isolation (tenants share storage)
- Per-tenant resource guarantees (no quotas in this ADR)
- Tenant-specific retention policies (same retention applies to all)
- Compliance requiring physical separation (consider separate instances)
