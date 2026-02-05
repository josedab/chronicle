package chronicle

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// TenantStatus represents the state of a SaaS tenant.
type TenantStatus string

const (
	TenantActive    TenantStatus = "active"
	TenantSuspended TenantStatus = "suspended"
	TenantPending   TenantStatus = "pending"
	TenantDeleted   TenantStatus = "deleted"
)

// SaaSControlPlaneConfig configures the multi-tenant SaaS control plane.
type SaaSControlPlaneConfig struct {
	Enabled            bool          `json:"enabled"`
	MaxTenants         int           `json:"max_tenants"`
	DefaultQuota       TenantQuota   `json:"default_quota"`
	MeteringInterval   time.Duration `json:"metering_interval"`
	BillingWebhookURL  string        `json:"billing_webhook_url,omitempty"`
	EnableRBAC         bool          `json:"enable_rbac"`
	EnableRateLimiting bool          `json:"enable_rate_limiting"`
	APIKeyRotationDays int           `json:"api_key_rotation_days"`
}

// DefaultSaaSControlPlaneConfig returns sensible defaults.
func DefaultSaaSControlPlaneConfig() SaaSControlPlaneConfig {
	return SaaSControlPlaneConfig{
		Enabled:    false,
		MaxTenants: 1000,
		DefaultQuota: TenantQuota{
			MaxWritesPerSecond: 10000,
			MaxQueriesPerSecond: 100,
			MaxStorageBytes:    10 * 1024 * 1024 * 1024, // 10GB
			MaxMetrics:         10000,
			MaxRetentionDays:   30,
		},
		MeteringInterval:   time.Minute,
		EnableRBAC:         true,
		EnableRateLimiting: true,
		APIKeyRotationDays: 90,
	}
}

// TenantQuota defines resource limits for a tenant.
type TenantQuota struct {
	MaxWritesPerSecond  int   `json:"max_writes_per_second"`
	MaxQueriesPerSecond int   `json:"max_queries_per_second"`
	MaxStorageBytes     int64 `json:"max_storage_bytes"`
	MaxMetrics          int   `json:"max_metrics"`
	MaxRetentionDays    int   `json:"max_retention_days"`
}

// SaaSTenant represents a tenant in the control plane.
type SaaSTenant struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Status        TenantStatus      `json:"status"`
	Quota         TenantQuota       `json:"quota"`
	APIKey        string            `json:"api_key"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Usage         TenantUsage       `json:"usage"`
}

// TenantUsage tracks resource consumption.
type TenantUsage struct {
	WritesTotal      int64     `json:"writes_total"`
	QueriesTotal     int64     `json:"queries_total"`
	StorageBytesUsed int64     `json:"storage_bytes_used"`
	MetricsCount     int       `json:"metrics_count"`
	LastWriteAt      time.Time `json:"last_write_at,omitempty"`
	LastQueryAt      time.Time `json:"last_query_at,omitempty"`
	PeriodStart      time.Time `json:"period_start"`
}

// APIKeyInfo represents an API key with metadata.
type APIKeyInfo struct {
	Key       string    `json:"key"`
	TenantID  string    `json:"tenant_id"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Scopes    []string  `json:"scopes"`
}

// SaaSControlPlane manages multi-tenant Chronicle deployments.
type SaaSControlPlane struct {
	config  SaaSControlPlaneConfig
	db      *DB
	tenants map[string]*SaaSTenant
	apiKeys map[string]*APIKeyInfo // key -> info

	mu   sync.RWMutex
	done chan struct{}
}

// NewSaaSControlPlane creates a new SaaS control plane.
func NewSaaSControlPlane(db *DB, config SaaSControlPlaneConfig) *SaaSControlPlane {
	return &SaaSControlPlane{
		config:  config,
		db:      db,
		tenants: make(map[string]*SaaSTenant),
		apiKeys: make(map[string]*APIKeyInfo),
		done:    make(chan struct{}),
	}
}

// ProvisionTenant creates a new tenant with default quota.
func (cp *SaaSControlPlane) ProvisionTenant(id, name string) (*SaaSTenant, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if _, exists := cp.tenants[id]; exists {
		return nil, fmt.Errorf("tenant %q already exists", id)
	}
	if len(cp.tenants) >= cp.config.MaxTenants {
		return nil, fmt.Errorf("max tenants reached (%d)", cp.config.MaxTenants)
	}

	apiKey := fmt.Sprintf("ck_%s_%d", id, time.Now().UnixNano())
	tenant := &SaaSTenant{
		ID:        id,
		Name:      name,
		Status:    TenantActive,
		Quota:     cp.config.DefaultQuota,
		APIKey:    apiKey,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Metadata:  make(map[string]string),
		Usage: TenantUsage{
			PeriodStart: time.Now(),
		},
	}

	cp.tenants[id] = tenant
	cp.apiKeys[apiKey] = &APIKeyInfo{
		Key:       apiKey,
		TenantID:  id,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().AddDate(0, 0, cp.config.APIKeyRotationDays),
		Scopes:    []string{"read", "write"},
	}

	return tenant, nil
}

// GetTenant returns a tenant by ID.
func (cp *SaaSControlPlane) GetTenant(id string) (*SaaSTenant, bool) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	t, ok := cp.tenants[id]
	return t, ok
}

// ListTenants returns all tenants.
func (cp *SaaSControlPlane) ListTenants() []*SaaSTenant {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	result := make([]*SaaSTenant, 0, len(cp.tenants))
	for _, t := range cp.tenants {
		result = append(result, t)
	}
	return result
}

// SuspendTenant suspends a tenant.
func (cp *SaaSControlPlane) SuspendTenant(id string) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	t, exists := cp.tenants[id]
	if !exists {
		return fmt.Errorf("tenant %q not found", id)
	}
	t.Status = TenantSuspended
	t.UpdatedAt = time.Now()
	return nil
}

// ActivateTenant reactivates a suspended tenant.
func (cp *SaaSControlPlane) ActivateTenant(id string) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	t, exists := cp.tenants[id]
	if !exists {
		return fmt.Errorf("tenant %q not found", id)
	}
	t.Status = TenantActive
	t.UpdatedAt = time.Now()
	return nil
}

// DeleteTenant removes a tenant and its resources.
func (cp *SaaSControlPlane) DeleteTenant(id string) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	t, exists := cp.tenants[id]
	if !exists {
		return fmt.Errorf("tenant %q not found", id)
	}

	// Remove API key
	delete(cp.apiKeys, t.APIKey)
	delete(cp.tenants, id)
	return nil
}

// UpdateQuota updates a tenant's resource quota.
func (cp *SaaSControlPlane) UpdateQuota(id string, quota TenantQuota) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	t, exists := cp.tenants[id]
	if !exists {
		return fmt.Errorf("tenant %q not found", id)
	}
	t.Quota = quota
	t.UpdatedAt = time.Now()
	return nil
}

// RecordWrite records a write operation for a tenant.
func (cp *SaaSControlPlane) RecordWrite(tenantID string, points int) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	t, exists := cp.tenants[tenantID]
	if !exists {
		return fmt.Errorf("tenant %q not found", tenantID)
	}
	if t.Status != TenantActive {
		return fmt.Errorf("tenant %q is %s", tenantID, t.Status)
	}

	t.Usage.WritesTotal += int64(points)
	t.Usage.LastWriteAt = time.Now()
	return nil
}

// RecordQuery records a query operation for a tenant.
func (cp *SaaSControlPlane) RecordQuery(tenantID string) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	t, exists := cp.tenants[tenantID]
	if !exists {
		return fmt.Errorf("tenant %q not found", tenantID)
	}

	t.Usage.QueriesTotal++
	t.Usage.LastQueryAt = time.Now()
	return nil
}

// ValidateAPIKey validates an API key and returns the associated tenant ID.
func (cp *SaaSControlPlane) ValidateAPIKey(key string) (string, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	info, exists := cp.apiKeys[key]
	if !exists {
		return "", fmt.Errorf("invalid API key")
	}
	if time.Now().After(info.ExpiresAt) {
		return "", fmt.Errorf("API key expired")
	}

	t, exists := cp.tenants[info.TenantID]
	if !exists {
		return "", fmt.Errorf("tenant not found")
	}
	if t.Status != TenantActive {
		return "", fmt.Errorf("tenant is %s", t.Status)
	}

	return info.TenantID, nil
}

// CheckQuota verifies a tenant hasn't exceeded their quota.
func (cp *SaaSControlPlane) CheckQuota(tenantID string) error {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	t, exists := cp.tenants[tenantID]
	if !exists {
		return fmt.Errorf("tenant %q not found", tenantID)
	}

	if t.Usage.StorageBytesUsed > t.Quota.MaxStorageBytes {
		return fmt.Errorf("storage quota exceeded: %d > %d bytes", t.Usage.StorageBytesUsed, t.Quota.MaxStorageBytes)
	}
	if t.Usage.MetricsCount > t.Quota.MaxMetrics {
		return fmt.Errorf("metrics quota exceeded: %d > %d", t.Usage.MetricsCount, t.Quota.MaxMetrics)
	}
	return nil
}

// GetUsageSummary returns aggregated usage across all tenants.
func (cp *SaaSControlPlane) GetUsageSummary() map[string]interface{} {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	var totalWrites, totalQueries int64
	var totalStorage int64
	activeTenants := 0

	for _, t := range cp.tenants {
		if t.Status == TenantActive {
			activeTenants++
		}
		totalWrites += t.Usage.WritesTotal
		totalQueries += t.Usage.QueriesTotal
		totalStorage += t.Usage.StorageBytesUsed
	}

	return map[string]interface{}{
		"total_tenants":     len(cp.tenants),
		"active_tenants":    activeTenants,
		"total_writes":      totalWrites,
		"total_queries":     totalQueries,
		"total_storage_bytes": totalStorage,
	}
}

// Start begins the metering background loop.
func (cp *SaaSControlPlane) Start() {
	if !cp.config.Enabled {
		return
	}
	go cp.meteringLoop()
}

// Stop halts the control plane.
func (cp *SaaSControlPlane) Stop() {
	select {
	case <-cp.done:
	default:
		close(cp.done)
	}
}

// TenantMiddleware returns an HTTP middleware that validates tenant API keys.
func (cp *SaaSControlPlane) TenantMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		if key == "" {
			key = r.URL.Query().Get("api_key")
		}
		if key == "" {
			http.Error(w, "missing API key", http.StatusUnauthorized)
			return
		}

		tenantID, err := cp.ValidateAPIKey(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}

		if err := cp.CheckQuota(tenantID); err != nil {
			http.Error(w, err.Error(), http.StatusTooManyRequests)
			return
		}

		r.Header.Set("X-Tenant-ID", tenantID)
		next(w, r)
	}
}

func (cp *SaaSControlPlane) meteringLoop() {
	ticker := time.NewTicker(cp.config.MeteringInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cp.done:
			return
		case <-ticker.C:
			// Metering: aggregate and optionally push to billing webhook
		}
	}
}
