package chronicle

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// TenantTagKey is the tag key used to identify tenants
	TenantTagKey = "__tenant__"
	// TenantSeparator separates tenant from metric name in storage
	TenantSeparator = "/"
)

// TenantConfig configures multi-tenancy settings.
type TenantConfig struct {
	// Enabled turns on multi-tenancy mode
	Enabled bool
	// DefaultTenant is used when no tenant is specified
	DefaultTenant string
	// AllowedTenants restricts which tenants can write data
	// If nil, all tenants are allowed
	AllowedTenants []string
	// IsolateQueries if true requires tenant to be specified in queries
	IsolateQueries bool
}

// Tenant represents a tenant context for database operations.
type Tenant struct {
	db   *DB
	name string
}

// TenantManager manages multi-tenant access to the database.
type TenantManager struct {
	db      *DB
	config  TenantConfig
	mu      sync.RWMutex
	allowed map[string]struct{}
}

// NewTenantManager creates a new tenant manager.
func NewTenantManager(db *DB, cfg TenantConfig) *TenantManager {
	tm := &TenantManager{
		db:     db,
		config: cfg,
	}
	if len(cfg.AllowedTenants) > 0 {
		tm.allowed = make(map[string]struct{}, len(cfg.AllowedTenants))
		for _, t := range cfg.AllowedTenants {
			tm.allowed[t] = struct{}{}
		}
	}
	return tm
}

// GetTenant returns a tenant context for the given tenant name.
func (tm *TenantManager) GetTenant(name string) (*Tenant, error) {
	if name == "" {
		if tm.config.DefaultTenant != "" {
			name = tm.config.DefaultTenant
		} else {
			return nil, errors.New("tenant name is required")
		}
	}

	if !tm.isAllowed(name) {
		return nil, fmt.Errorf("tenant %q is not allowed", name)
	}

	return &Tenant{db: tm.db, name: name}, nil
}

// isAllowed checks if a tenant is in the allowed list.
func (tm *TenantManager) isAllowed(name string) bool {
	if tm.allowed == nil {
		return true
	}
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	_, ok := tm.allowed[name]
	return ok
}

// AddAllowedTenant adds a tenant to the allowed list.
func (tm *TenantManager) AddAllowedTenant(name string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.allowed == nil {
		tm.allowed = make(map[string]struct{})
	}
	tm.allowed[name] = struct{}{}
}

// RemoveAllowedTenant removes a tenant from the allowed list.
func (tm *TenantManager) RemoveAllowedTenant(name string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.allowed, name)
}

// ListTenants returns all allowed tenants.
func (tm *TenantManager) ListTenants() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if tm.allowed == nil {
		return nil
	}
	result := make([]string, 0, len(tm.allowed))
	for t := range tm.allowed {
		result = append(result, t)
	}
	return result
}

// Write writes a point within this tenant's namespace.
func (t *Tenant) Write(p Point) error {
	p = t.tagPoint(p)
	return t.db.Write(p)
}

// WriteBatch writes multiple points within this tenant's namespace.
func (t *Tenant) WriteBatch(points []Point) error {
	tagged := make([]Point, len(points))
	for i, p := range points {
		tagged[i] = t.tagPoint(p)
	}
	return t.db.WriteBatch(tagged)
}

// Execute runs a query within this tenant's namespace.
func (t *Tenant) Execute(q *Query) (*Result, error) {
	q = t.scopeQuery(q)
	return t.db.Execute(q)
}

// Query runs a SQL-like query within this tenant's namespace.
func (t *Tenant) Query(sql string) (*Result, error) {
	parser := &QueryParser{}
	q, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	q = t.scopeQuery(q)
	return t.db.Execute(q)
}

// tagPoint adds tenant tag to a point.
func (t *Tenant) tagPoint(p Point) Point {
	if p.Tags == nil {
		p.Tags = make(map[string]string)
	}
	p.Tags[TenantTagKey] = t.name
	return p
}

// scopeQuery adds tenant filter to a query.
func (t *Tenant) scopeQuery(q *Query) *Query {
	if q == nil {
		return nil
	}
	// Clone the query to avoid modifying the original
	scoped := *q
	if scoped.Tags == nil {
		scoped.Tags = make(map[string]string)
	}
	scoped.Tags[TenantTagKey] = t.name
	return &scoped
}

// Name returns the tenant name.
func (t *Tenant) Name() string {
	return t.name
}

// TenantFromTags extracts the tenant name from point tags.
func TenantFromTags(tags map[string]string) string {
	if tags == nil {
		return ""
	}
	return tags[TenantTagKey]
}

// StripTenantTag removes the tenant tag from a tags map.
func StripTenantTag(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	result := make(map[string]string, len(tags))
	for k, v := range tags {
		if k != TenantTagKey {
			result[k] = v
		}
	}
	return result
}

// TenantMetricName creates a namespaced metric name.
func TenantMetricName(tenant, metric string) string {
	return tenant + TenantSeparator + metric
}

// ParseTenantMetric splits a namespaced metric name.
func ParseTenantMetric(name string) (tenant, metric string) {
	idx := strings.Index(name, TenantSeparator)
	if idx < 0 {
		return "", name
	}
	return name[:idx], name[idx+1:]
}
