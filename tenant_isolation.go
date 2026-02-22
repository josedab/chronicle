package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// TenantIsolationConfig configures multi-tenant isolation.
type TenantIsolationConfig struct {
	Enabled              bool  `json:"enabled"`
	MaxTenants           int   `json:"max_tenants"`
	DefaultMemoryBudgetMB int64 `json:"default_memory_budget_mb"`
	DefaultQueryQuota    int   `json:"default_query_quota"`
	DefaultStorageLimitMB int64 `json:"default_storage_limit_mb"`
}

// DefaultTenantIsolationConfig returns sensible defaults.
func DefaultTenantIsolationConfig() TenantIsolationConfig {
	return TenantIsolationConfig{
		Enabled:              true,
		MaxTenants:           100,
		DefaultMemoryBudgetMB: 256,
		DefaultQueryQuota:    1000,
		DefaultStorageLimitMB: 10240,
	}
}

// IsolationTenant represents a tenant with resource limits.
type IsolationTenant struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	MemoryBudgetMB  int64     `json:"memory_budget_mb"`
	QueryQuota      int       `json:"query_quota"`
	StorageLimitMB  int64     `json:"storage_limit_mb"`
	CurrentMemoryMB int64     `json:"current_memory_mb"`
	QueriesUsed     int       `json:"queries_used"`
	StorageUsedMB   int64     `json:"storage_used_mb"`
	Active          bool      `json:"active"`
	CreatedAt       time.Time `json:"created_at"`
}

// IsolationViolation represents a resource quota violation.
type IsolationViolation struct {
	TenantID   string    `json:"tenant_id"`
	Type       string    `json:"type"`
	Limit      int64     `json:"limit"`
	Current    int64     `json:"current"`
	Message    string    `json:"message"`
	DetectedAt time.Time `json:"detected_at"`
}

// TenantIsolationStats tracks isolation statistics.
type TenantIsolationStats struct {
	TotalTenants    int            `json:"total_tenants"`
	ActiveTenants   int            `json:"active_tenants"`
	TotalViolations int            `json:"total_violations"`
	ViolationsByType map[string]int `json:"violations_by_type"`
}

// TenantIsolationEngine manages multi-tenant resource isolation.
type TenantIsolationEngine struct {
	db     *DB
	config TenantIsolationConfig

	tenants         map[string]*IsolationTenant
	violations      []IsolationViolation
	violationCounts map[string]int
	running         bool
	stopCh          chan struct{}

	mu sync.RWMutex
}

// NewTenantIsolationEngine creates a new tenant isolation engine.
func NewTenantIsolationEngine(db *DB, cfg TenantIsolationConfig) *TenantIsolationEngine {
	return &TenantIsolationEngine{
		db:              db,
		config:          cfg,
		tenants:         make(map[string]*IsolationTenant),
		violationCounts: make(map[string]int),
		stopCh:          make(chan struct{}),
	}
}

// Start begins the isolation engine.
func (e *TenantIsolationEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return nil
	}
	e.running = true
	return nil
}

// Stop halts the isolation engine.
func (e *TenantIsolationEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return nil
	}
	e.running = false
	return nil
}

// CreateTenant registers a new tenant with resource limits.
func (e *TenantIsolationEngine) CreateTenant(tenant IsolationTenant) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.tenants[tenant.ID]; exists {
		return fmt.Errorf("tenant %q already exists", tenant.ID)
	}

	if len(e.tenants) >= e.config.MaxTenants {
		return fmt.Errorf("max tenants (%d) reached", e.config.MaxTenants)
	}

	if tenant.MemoryBudgetMB <= 0 {
		tenant.MemoryBudgetMB = e.config.DefaultMemoryBudgetMB
	}
	if tenant.QueryQuota <= 0 {
		tenant.QueryQuota = e.config.DefaultQueryQuota
	}
	if tenant.StorageLimitMB <= 0 {
		tenant.StorageLimitMB = e.config.DefaultStorageLimitMB
	}
	if tenant.CreatedAt.IsZero() {
		tenant.CreatedAt = time.Now()
	}

	e.tenants[tenant.ID] = &tenant
	return nil
}

// DeleteTenant removes a tenant by ID.
func (e *TenantIsolationEngine) DeleteTenant(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.tenants[id]; !exists {
		return fmt.Errorf("tenant %q not found", id)
	}

	delete(e.tenants, id)
	return nil
}

// GetTenant retrieves a tenant by ID.
func (e *TenantIsolationEngine) GetTenant(id string) (*IsolationTenant, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	t, exists := e.tenants[id]
	if !exists {
		return nil, fmt.Errorf("tenant %q not found", id)
	}

	copy := *t
	return &copy, nil
}

// ListTenants returns all registered tenants.
func (e *TenantIsolationEngine) ListTenants() []IsolationTenant {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]IsolationTenant, 0, len(e.tenants))
	for _, t := range e.tenants {
		result = append(result, *t)
	}
	return result
}

// CheckQuota verifies if a tenant's resource usage is within limits.
func (e *TenantIsolationEngine) CheckQuota(tenantID, resource string) (bool, *IsolationViolation) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	t, exists := e.tenants[tenantID]
	if !exists {
		return false, &IsolationViolation{
			TenantID:   tenantID,
			Type:       resource,
			Message:    fmt.Sprintf("tenant %q not found", tenantID),
			DetectedAt: time.Now(),
		}
	}

	switch resource {
	case "memory":
		if t.CurrentMemoryMB >= t.MemoryBudgetMB {
			return false, &IsolationViolation{
				TenantID:   tenantID,
				Type:       "memory",
				Limit:      t.MemoryBudgetMB,
				Current:    t.CurrentMemoryMB,
				Message:    fmt.Sprintf("memory budget exceeded: %dMB / %dMB", t.CurrentMemoryMB, t.MemoryBudgetMB),
				DetectedAt: time.Now(),
			}
		}
	case "query":
		if int64(t.QueriesUsed) >= int64(t.QueryQuota) {
			return false, &IsolationViolation{
				TenantID:   tenantID,
				Type:       "query",
				Limit:      int64(t.QueryQuota),
				Current:    int64(t.QueriesUsed),
				Message:    fmt.Sprintf("query quota exceeded: %d / %d", t.QueriesUsed, t.QueryQuota),
				DetectedAt: time.Now(),
			}
		}
	case "storage":
		if t.StorageUsedMB >= t.StorageLimitMB {
			return false, &IsolationViolation{
				TenantID:   tenantID,
				Type:       "storage",
				Limit:      t.StorageLimitMB,
				Current:    t.StorageUsedMB,
				Message:    fmt.Sprintf("storage limit exceeded: %dMB / %dMB", t.StorageUsedMB, t.StorageLimitMB),
				DetectedAt: time.Now(),
			}
		}
	}

	return true, nil
}

// RecordUsage tracks resource usage for a tenant.
func (e *TenantIsolationEngine) RecordUsage(tenantID, resource string, amount int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	t, exists := e.tenants[tenantID]
	if !exists {
		return fmt.Errorf("tenant %q not found", tenantID)
	}

	switch resource {
	case "memory":
		t.CurrentMemoryMB += amount
	case "query":
		t.QueriesUsed += int(amount)
	case "storage":
		t.StorageUsedMB += amount
	default:
		return fmt.Errorf("unknown resource %q", resource)
	}

	// Check for violations
	ok, violation := e.checkQuotaLocked(t, resource)
	if !ok && violation != nil {
		e.violations = append(e.violations, *violation)
		e.violationCounts[resource]++
	}

	return nil
}

func (e *TenantIsolationEngine) checkQuotaLocked(t *IsolationTenant, resource string) (bool, *IsolationViolation) {
	switch resource {
	case "memory":
		if t.CurrentMemoryMB > t.MemoryBudgetMB {
			return false, &IsolationViolation{
				TenantID:   t.ID,
				Type:       "memory",
				Limit:      t.MemoryBudgetMB,
				Current:    t.CurrentMemoryMB,
				Message:    "memory budget exceeded",
				DetectedAt: time.Now(),
			}
		}
	case "query":
		if t.QueriesUsed > t.QueryQuota {
			return false, &IsolationViolation{
				TenantID:   t.ID,
				Type:       "query",
				Limit:      int64(t.QueryQuota),
				Current:    int64(t.QueriesUsed),
				Message:    "query quota exceeded",
				DetectedAt: time.Now(),
			}
		}
	case "storage":
		if t.StorageUsedMB > t.StorageLimitMB {
			return false, &IsolationViolation{
				TenantID:   t.ID,
				Type:       "storage",
				Limit:      t.StorageLimitMB,
				Current:    t.StorageUsedMB,
				Message:    "storage limit exceeded",
				DetectedAt: time.Now(),
			}
		}
	}
	return true, nil
}

// ResetQuotas resets periodic quotas (like query counts) for all tenants.
func (e *TenantIsolationEngine) ResetQuotas() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, t := range e.tenants {
		t.QueriesUsed = 0
	}
}

// Stats returns current isolation statistics.
func (e *TenantIsolationEngine) Stats() TenantIsolationStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := TenantIsolationStats{
		TotalTenants:     len(e.tenants),
		TotalViolations:  len(e.violations),
		ViolationsByType: make(map[string]int),
	}

	for _, t := range e.tenants {
		if t.Active {
			stats.ActiveTenants++
		}
	}

	for k, v := range e.violationCounts {
		stats.ViolationsByType[k] = v
	}

	return stats
}

// RegisterHTTPHandlers registers tenant isolation HTTP endpoints.
func (e *TenantIsolationEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/isolation/tenants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListTenants())
	})
	mux.HandleFunc("/api/v1/isolation/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}
