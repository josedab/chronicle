package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var cloudIDCounter atomic.Int64

// CloudTier represents the subscription tier for a cloud organization.
type CloudTier string

const (
	TierFree       CloudTier = "free"
	TierPro        CloudTier = "pro"
	TierEnterprise CloudTier = "enterprise"
)

// CloudInstanceState represents the lifecycle state of a cloud instance.
type CloudInstanceState string

const (
	InstanceProvisioning CloudInstanceState = "provisioning"
	InstanceRunning      CloudInstanceState = "running"
	InstanceStopped      CloudInstanceState = "stopped"
	InstanceFailed       CloudInstanceState = "failed"
	InstanceTerminating  CloudInstanceState = "terminating"
)

// CloudSaaSConfig configures the managed edge-to-cloud SaaS platform.
type CloudSaaSConfig struct {
	Region            string        `json:"region"`
	Tier              CloudTier     `json:"tier"`
	MaxStorageBytes   int64         `json:"max_storage_bytes"`
	MaxInstances      int           `json:"max_instances"`
	BillingEnabled    bool          `json:"billing_enabled"`
	AutoDeploy        bool          `json:"auto_deploy"`
	ControlPlaneURL   string        `json:"control_plane_url"`
	APIKey            string        `json:"api_key,omitempty"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	MetricRetention   time.Duration `json:"metric_retention"`
}

// DefaultCloudSaaSConfig returns sensible defaults for the cloud SaaS platform.
func DefaultCloudSaaSConfig() CloudSaaSConfig {
	return CloudSaaSConfig{
		Region:            "us-east-1",
		Tier:              TierFree,
		MaxStorageBytes:   10 * 1024 * 1024 * 1024, // 10GB
		MaxInstances:      100,
		BillingEnabled:    true,
		AutoDeploy:        true,
		ControlPlaneURL:   "https://api.chronicle.dev",
		HeartbeatInterval: 30 * time.Second,
		MetricRetention:   30 * 24 * time.Hour, // 30 days
	}
}

// CloudInstance represents a managed Chronicle instance in the cloud.
type CloudInstance struct {
	ID              string             `json:"id"`
	OrgID           string             `json:"org_id"`
	Name            string             `json:"name"`
	Region          string             `json:"region"`
	State           CloudInstanceState `json:"state"`
	Tier            CloudTier          `json:"tier"`
	CreatedAt       time.Time          `json:"created_at"`
	EndpointURL     string             `json:"endpoint_url"`
	StorageUsed     int64              `json:"storage_used"`
	MetricsCount    int64              `json:"metrics_count"`
	LastHeartbeat   time.Time          `json:"last_heartbeat"`
	Version         string             `json:"version"`
	ConfigOverrides map[string]string  `json:"config_overrides,omitempty"`
}

// CloudOrganization represents a customer organization in the SaaS platform.
type CloudOrganization struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	OwnerEmail  string          `json:"owner_email"`
	Tier        CloudTier       `json:"tier"`
	Instances   []CloudInstance `json:"instances"`
	APIKeys     []CloudAPIKey   `json:"api_keys"`
	CreatedAt   time.Time       `json:"created_at"`
	BillingInfo BillingInfo     `json:"billing_info"`
}

// CloudAPIKey represents an API key for programmatic access.
type CloudAPIKey struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	KeyHash     string    `json:"key_hash"`
	Permissions []string  `json:"permissions"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	LastUsedAt  time.Time `json:"last_used_at,omitempty"`
}

// UsageRecord captures a point-in-time usage snapshot.
type UsageRecord struct {
	OrgID        string    `json:"org_id"`
	InstanceID   string    `json:"instance_id"`
	Timestamp    time.Time `json:"timestamp"`
	StorageBytes int64     `json:"storage_bytes"`
	Queries      int64     `json:"queries"`
	Writes       int64     `json:"writes"`
	Reads        int64     `json:"reads"`
}

// BillingInfo contains billing details for an organization.
type BillingInfo struct {
	Plan            string    `json:"plan"`
	MonthlyCost     float64   `json:"monthly_cost"`
	Usage           float64   `json:"usage"`
	Overages        float64   `json:"overages"`
	NextInvoiceDate time.Time `json:"next_invoice_date"`
}

// DeploymentSpec defines parameters for deploying a new instance.
type DeploymentSpec struct {
	Name         string            `json:"name"`
	Region       string            `json:"region"`
	Tier         CloudTier         `json:"tier"`
	Config       map[string]string `json:"config,omitempty"`
	AutoScale    bool              `json:"auto_scale"`
	MinInstances int               `json:"min_instances"`
	MaxInstances int               `json:"max_instances"`
}

// UsageSummary provides aggregated usage for an organization.
type UsageSummary struct {
	OrgID        string `json:"org_id"`
	TotalStorage int64  `json:"total_storage"`
	TotalQueries int64  `json:"total_queries"`
	TotalWrites  int64  `json:"total_writes"`
	TotalReads   int64  `json:"total_reads"`
	RecordCount  int    `json:"record_count"`
}

// CloudSaaSStats contains platform-wide statistics.
type CloudSaaSStats struct {
	TotalOrgs              int     `json:"total_orgs"`
	TotalInstances         int     `json:"total_instances"`
	ActiveInstances        int     `json:"active_instances"`
	TotalStorage           int64   `json:"total_storage"`
	TotalQueries           int64   `json:"total_queries"`
	MonthlyRevenueEstimate float64 `json:"monthly_revenue_estimate"`
}

// CloudSaaSEngine manages the Chronicle Cloud SaaS platform.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type CloudSaaSEngine struct {
	db            *DB
	config        CloudSaaSConfig
	organizations map[string]*CloudOrganization
	instances     map[string]*CloudInstance
	usageRecords  []UsageRecord
	apiKeys       map[string]*CloudAPIKey
	mu            sync.RWMutex
}

// NewCloudSaaSEngine creates a new cloud SaaS engine.
func NewCloudSaaSEngine(db *DB, config CloudSaaSConfig) *CloudSaaSEngine {
	return &CloudSaaSEngine{
		db:            db,
		config:        config,
		organizations: make(map[string]*CloudOrganization),
		instances:     make(map[string]*CloudInstance),
		usageRecords:  make([]UsageRecord, 0),
		apiKeys:       make(map[string]*CloudAPIKey),
	}
}

// CreateOrganization creates a new organization in the SaaS platform.
func (e *CloudSaaSEngine) CreateOrganization(name, email string, tier CloudTier) (*CloudOrganization, error) {
	if name == "" {
		return nil, fmt.Errorf("cloud_saas: organization name required")
	}
	if email == "" {
		return nil, fmt.Errorf("cloud_saas: owner email required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	id := fmt.Sprintf("org_%d", cloudIDCounter.Add(1))

	billing := billingForTier(tier)

	org := &CloudOrganization{
		ID:          id,
		Name:        name,
		OwnerEmail:  email,
		Tier:        tier,
		Instances:   make([]CloudInstance, 0),
		APIKeys:     make([]CloudAPIKey, 0),
		CreatedAt:   time.Now(),
		BillingInfo: billing,
	}

	e.organizations[id] = org
	return org, nil
}

// GetOrganization returns an organization by ID.
func (e *CloudSaaSEngine) GetOrganization(id string) (*CloudOrganization, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	org, ok := e.organizations[id]
	if !ok {
		return nil, fmt.Errorf("cloud_saas: organization %q not found", id)
	}
	return org, nil
}

// ListOrganizations returns all organizations.
func (e *CloudSaaSEngine) ListOrganizations() []*CloudOrganization {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*CloudOrganization, 0, len(e.organizations))
	for _, org := range e.organizations {
		result = append(result, org)
	}
	return result
}

// DeployInstance provisions a new Chronicle instance for an organization.
func (e *CloudSaaSEngine) DeployInstance(orgID string, spec DeploymentSpec) (*CloudInstance, error) {
	if spec.Name == "" {
		return nil, fmt.Errorf("cloud_saas: instance name required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	org, ok := e.organizations[orgID]
	if !ok {
		return nil, fmt.Errorf("cloud_saas: organization %q not found", orgID)
	}

	orgCount := 0
	for _, inst := range e.instances {
		if inst.OrgID == orgID {
			orgCount++
		}
	}
	if orgCount >= e.config.MaxInstances {
		return nil, fmt.Errorf("cloud_saas: max instances (%d) reached for org %q", e.config.MaxInstances, orgID)
	}
	_ = org

	region := spec.Region
	if region == "" {
		region = e.config.Region
	}

	id := fmt.Sprintf("inst_%d", cloudIDCounter.Add(1))

	instance := &CloudInstance{
		ID:              id,
		OrgID:           orgID,
		Name:            spec.Name,
		Region:          region,
		State:           InstanceRunning,
		Tier:            spec.Tier,
		CreatedAt:       time.Now(),
		EndpointURL:     fmt.Sprintf("https://%s.%s.chronicle.dev", id, region),
		LastHeartbeat:   time.Now(),
		Version:         "1.0.0",
		ConfigOverrides: spec.Config,
	}

	e.instances[id] = instance

	return instance, nil
}

// GetInstance returns a cloud instance by ID.
func (e *CloudSaaSEngine) GetInstance(id string) (*CloudInstance, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	inst, ok := e.instances[id]
	if !ok {
		return nil, fmt.Errorf("cloud_saas: instance %q not found", id)
	}
	return inst, nil
}

// ListInstances returns all instances for an organization.
func (e *CloudSaaSEngine) ListInstances(orgID string) []*CloudInstance {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []*CloudInstance
	for _, inst := range e.instances {
		if inst.OrgID == orgID {
			result = append(result, inst)
		}
	}
	return result
}

// StopInstance stops a running cloud instance.
func (e *CloudSaaSEngine) StopInstance(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	inst, ok := e.instances[id]
	if !ok {
		return fmt.Errorf("cloud_saas: instance %q not found", id)
	}
	if inst.State != InstanceRunning {
		return fmt.Errorf("cloud_saas: instance %q is not running (state: %s)", id, inst.State)
	}

	inst.State = InstanceStopped
	return nil
}

// TerminateInstance permanently terminates a cloud instance.
func (e *CloudSaaSEngine) TerminateInstance(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	inst, ok := e.instances[id]
	if !ok {
		return fmt.Errorf("cloud_saas: instance %q not found", id)
	}

	inst.State = InstanceTerminating
	delete(e.instances, id)

	// Remove from organization's instance list
	for _, org := range e.organizations {
		for i, oi := range org.Instances {
			if oi.ID == id {
				org.Instances = append(org.Instances[:i], org.Instances[i+1:]...)
				break
			}
		}
	}

	return nil
}

// GenerateAPIKey creates a new API key for an organization.
func (e *CloudSaaSEngine) GenerateAPIKey(orgID, name string, permissions []string) (*CloudAPIKey, error) {
	if name == "" {
		return nil, fmt.Errorf("cloud_saas: API key name required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	org, ok := e.organizations[orgID]
	if !ok {
		return nil, fmt.Errorf("cloud_saas: organization %q not found", orgID)
	}

	id := fmt.Sprintf("key_%d", cloudIDCounter.Add(1))
	keyHash := fmt.Sprintf("csk_%s_%d", orgID, cloudIDCounter.Add(1))

	key := CloudAPIKey{
		ID:          id,
		Name:        name,
		KeyHash:     keyHash,
		Permissions: permissions,
		CreatedAt:   time.Now(),
		ExpiresAt:   time.Now().AddDate(0, 0, 90),
	}

	org.APIKeys = append(org.APIKeys, key)
	e.apiKeys[id] = &org.APIKeys[len(org.APIKeys)-1]

	return e.apiKeys[id], nil
}

// RevokeAPIKey revokes an API key by ID.
func (e *CloudSaaSEngine) RevokeAPIKey(keyID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.apiKeys[keyID]; !ok {
		return fmt.Errorf("cloud_saas: API key %q not found", keyID)
	}

	delete(e.apiKeys, keyID)

	// Remove from organization's key list
	for _, org := range e.organizations {
		for i, k := range org.APIKeys {
			if k.ID == keyID {
				org.APIKeys = append(org.APIKeys[:i], org.APIKeys[i+1:]...)
				break
			}
		}
	}

	return nil
}

// RecordUsage records a usage data point for billing and analytics.
func (e *CloudSaaSEngine) RecordUsage(record UsageRecord) error {
	if record.OrgID == "" {
		return fmt.Errorf("cloud_saas: org ID required in usage record")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.organizations[record.OrgID]; !ok {
		return fmt.Errorf("cloud_saas: organization %q not found", record.OrgID)
	}

	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now()
	}

	e.usageRecords = append(e.usageRecords, record)
	return nil
}

// GetUsageSummary returns aggregated usage for an organization.
func (e *CloudSaaSEngine) GetUsageSummary(orgID string) *UsageSummary {
	e.mu.RLock()
	defer e.mu.RUnlock()

	summary := &UsageSummary{OrgID: orgID}
	for _, r := range e.usageRecords {
		if r.OrgID != orgID {
			continue
		}
		summary.TotalStorage += r.StorageBytes
		summary.TotalQueries += r.Queries
		summary.TotalWrites += r.Writes
		summary.TotalReads += r.Reads
		summary.RecordCount++
	}
	return summary
}

// GetBilling returns billing information for an organization.
func (e *CloudSaaSEngine) GetBilling(orgID string) *BillingInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	org, ok := e.organizations[orgID]
	if !ok {
		return nil
	}

	billing := org.BillingInfo

	// Calculate usage-based costs from usage records
	var totalQueries int64
	for _, r := range e.usageRecords {
		if r.OrgID == orgID {
			totalQueries += r.Queries
		}
	}
	billing.Usage = float64(totalQueries) * 0.001 // $0.001 per query
	if billing.Usage > billing.MonthlyCost {
		billing.Overages = billing.Usage - billing.MonthlyCost
	}

	return &billing
}

// FederatedQuery executes a query across all instances in an organization.
func (e *CloudSaaSEngine) FederatedQuery(orgID, query string) (any, error) {
	if query == "" {
		return nil, fmt.Errorf("cloud_saas: query required")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	_, ok := e.organizations[orgID]
	if !ok {
		return nil, fmt.Errorf("cloud_saas: organization %q not found", orgID)
	}

	var activeInstances []string
	for _, inst := range e.instances {
		if inst.OrgID == orgID && inst.State == InstanceRunning {
			activeInstances = append(activeInstances, inst.ID)
		}
	}

	if len(activeInstances) == 0 {
		return nil, fmt.Errorf("cloud_saas: no running instances for org %q", orgID)
	}

	result := map[string]any{
		"org_id":    orgID,
		"query":     query,
		"instances": activeInstances,
		"status":    "completed",
	}

	return result, nil
}

// Stats returns platform-wide statistics.
func (e *CloudSaaSEngine) Stats() CloudSaaSStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := CloudSaaSStats{
		TotalOrgs:      len(e.organizations),
		TotalInstances: len(e.instances),
	}

	for _, inst := range e.instances {
		if inst.State == InstanceRunning {
			stats.ActiveInstances++
		}
		stats.TotalStorage += inst.StorageUsed
	}

	for _, r := range e.usageRecords {
		stats.TotalQueries += r.Queries
	}

	for _, org := range e.organizations {
		stats.MonthlyRevenueEstimate += org.BillingInfo.MonthlyCost
	}

	return stats
}

// RegisterHTTPHandlers registers HTTP endpoints for the cloud SaaS API.
func (e *CloudSaaSEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/cloud/orgs", e.handleOrgs)
	mux.HandleFunc("/api/v1/cloud/instances", e.handleInstances)
	mux.HandleFunc("/api/v1/cloud/usage", e.handleUsage)
	mux.HandleFunc("/api/v1/cloud/stats", e.handleStats)
}

func (e *CloudSaaSEngine) handleOrgs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		orgs := e.ListOrganizations()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(orgs)
	case http.MethodPost:
		var req struct {
			Name  string    `json:"name"`
			Email string    `json:"email"`
			Tier  CloudTier `json:"tier"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		org, err := e.CreateOrganization(req.Name, req.Email, req.Tier)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(org)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (e *CloudSaaSEngine) handleInstances(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		orgID := r.URL.Query().Get("org_id")
		if orgID == "" {
			http.Error(w, "org_id required", http.StatusBadRequest)
			return
		}
		instances := e.ListInstances(orgID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(instances)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (e *CloudSaaSEngine) handleUsage(w http.ResponseWriter, r *http.Request) {
	orgID := r.URL.Query().Get("org_id")
	if orgID == "" {
		http.Error(w, "org_id required", http.StatusBadRequest)
		return
	}
	summary := e.GetUsageSummary(orgID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summary)
}

func (e *CloudSaaSEngine) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := e.Stats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func billingForTier(tier CloudTier) BillingInfo {
	now := time.Now()
	nextInvoice := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)

	switch tier {
	case TierPro:
		return BillingInfo{
			Plan:            "pro",
			MonthlyCost:     99.0,
			NextInvoiceDate: nextInvoice,
		}
	case TierEnterprise:
		return BillingInfo{
			Plan:            "enterprise",
			MonthlyCost:     499.0,
			NextInvoiceDate: nextInvoice,
		}
	default:
		return BillingInfo{
			Plan:            "free",
			MonthlyCost:     0.0,
			NextInvoiceDate: nextInvoice,
		}
	}
}
