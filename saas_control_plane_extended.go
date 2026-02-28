package chronicle

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// --- SSO / Identity Federation ---

// SSOProvider identifies the SSO protocol.
type SSOProvider string

const (
	SSOProviderOIDC SSOProvider = "oidc"
	SSOProviderSAML SSOProvider = "saml"
)

// SSOConfig configures SSO integration for a tenant.
type SSOConfig struct {
	Provider     SSOProvider `json:"provider"`
	Issuer       string      `json:"issuer"`       // OIDC issuer URL or SAML IdP entity ID
	ClientID     string      `json:"client_id"`     // OIDC client ID
	ClientSecret string      `json:"client_secret"` // OIDC client secret (stored encrypted)
	MetadataURL  string      `json:"metadata_url"`  // SAML metadata URL
	CallbackURL  string      `json:"callback_url"`  // redirect after auth
	AllowedDomains []string  `json:"allowed_domains,omitempty"`
}

// SSOSession represents an authenticated SSO session.
type SSOSession struct {
	SessionID   string    `json:"session_id"`
	TenantID    string    `json:"tenant_id"`
	UserID      string    `json:"user_id"`
	Email       string    `json:"email"`
	DisplayName string    `json:"display_name"`
	Roles       []string  `json:"roles"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	Provider    SSOProvider `json:"provider"`
}

// SSOManager manages SSO configurations and sessions.
type SSOManager struct {
	mu       sync.RWMutex
	configs  map[string]*SSOConfig  // tenantID -> SSO config
	sessions map[string]*SSOSession // sessionID -> session
}

// NewSSOManager creates a new SSO manager.
func NewSSOManager() *SSOManager {
	return &SSOManager{
		configs:  make(map[string]*SSOConfig),
		sessions: make(map[string]*SSOSession),
	}
}

// ConfigureSSO sets up SSO for a tenant.
func (m *SSOManager) ConfigureSSO(tenantID string, config SSOConfig) error {
	if config.Provider == "" {
		return fmt.Errorf("sso: provider required")
	}
	if config.Issuer == "" {
		return fmt.Errorf("sso: issuer required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.configs[tenantID] = &config
	return nil
}

// GetSSOConfig returns SSO config for a tenant.
func (m *SSOManager) GetSSOConfig(tenantID string) (*SSOConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.configs[tenantID]
	return c, ok
}

// CreateSession creates an authenticated SSO session.
func (m *SSOManager) CreateSession(tenantID, userID, email, displayName string, roles []string, provider SSOProvider) (*SSOSession, error) {
	sessionID := generateSaaSSessionID()

	session := &SSOSession{
		SessionID:   sessionID,
		TenantID:    tenantID,
		UserID:      userID,
		Email:       email,
		DisplayName: displayName,
		Roles:       roles,
		CreatedAt:   time.Now(),
		ExpiresAt:   time.Now().Add(24 * time.Hour),
		Provider:    provider,
	}

	m.mu.Lock()
	m.sessions[sessionID] = session
	m.mu.Unlock()

	return session, nil
}

// ValidateSession checks if a session is valid.
func (m *SSOManager) ValidateSession(sessionID string) (*SSOSession, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session not found")
	}
	if time.Now().After(s.ExpiresAt) {
		return nil, fmt.Errorf("session expired")
	}
	return s, nil
}

// RevokeSession invalidates a session.
func (m *SSOManager) RevokeSession(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sessionID)
}

func generateSaaSSessionID() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		// Fallback to timestamp-based ID if crypto/rand fails
		return fmt.Sprintf("sess-%d", time.Now().UnixNano())
	}
	return "sess_" + hex.EncodeToString(b)
}

// --- RBAC (Role-Based Access Control) ---

// RBACRole defines a role with permissions.
type RBACRole struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Permissions []string `json:"permissions"`
}

// RBACBinding binds a user to a role within a tenant.
type RBACBinding struct {
	UserID   string `json:"user_id"`
	TenantID string `json:"tenant_id"`
	Role     string `json:"role"`
}

// RBACManager manages roles and permissions.
type RBACManager struct {
	mu       sync.RWMutex
	roles    map[string]*RBACRole    // roleName -> role
	bindings map[string][]RBACBinding // tenantID -> bindings
}

// NewRBACManager creates a new RBAC manager with default roles.
func NewRBACManager() *RBACManager {
	rm := &RBACManager{
		roles:    make(map[string]*RBACRole),
		bindings: make(map[string][]RBACBinding),
	}

	// Default roles
	rm.roles["admin"] = &RBACRole{
		Name:        "admin",
		Description: "Full access to all resources",
		Permissions: []string{"read", "write", "delete", "admin", "billing", "api_keys"},
	}
	rm.roles["editor"] = &RBACRole{
		Name:        "editor",
		Description: "Read and write data",
		Permissions: []string{"read", "write"},
	}
	rm.roles["viewer"] = &RBACRole{
		Name:        "viewer",
		Description: "Read-only access",
		Permissions: []string{"read"},
	}
	rm.roles["billing"] = &RBACRole{
		Name:        "billing",
		Description: "Billing and usage management",
		Permissions: []string{"read", "billing"},
	}

	return rm
}

// AddRole adds a custom role.
func (rm *RBACManager) AddRole(role RBACRole) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.roles[role.Name] = &role
}

// BindUser assigns a role to a user within a tenant.
func (rm *RBACManager) BindUser(tenantID, userID, role string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, ok := rm.roles[role]; !ok {
		return fmt.Errorf("rbac: role %q not found", role)
	}

	// Check for existing binding
	bindings := rm.bindings[tenantID]
	for _, b := range bindings {
		if b.UserID == userID && b.Role == role {
			return nil // already bound
		}
	}

	rm.bindings[tenantID] = append(bindings, RBACBinding{
		UserID:   userID,
		TenantID: tenantID,
		Role:     role,
	})
	return nil
}

// HasPermission checks if a user has a specific permission.
func (rm *RBACManager) HasPermission(tenantID, userID, permission string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	bindings := rm.bindings[tenantID]
	for _, b := range bindings {
		if b.UserID != userID {
			continue
		}
		role, ok := rm.roles[b.Role]
		if !ok {
			continue
		}
		for _, p := range role.Permissions {
			if p == permission {
				return true
			}
		}
	}
	return false
}

// GetUserRoles returns all roles for a user in a tenant.
func (rm *RBACManager) GetUserRoles(tenantID, userID string) []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	var roles []string
	for _, b := range rm.bindings[tenantID] {
		if b.UserID == userID {
			roles = append(roles, b.Role)
		}
	}
	return roles
}

// --- Billing Integration ---

// BillingPlan defines a subscription plan.
type BillingPlan struct {
	ID              string  `json:"id"`
	Name            string  `json:"name"`
	PricePerMonth   float64 `json:"price_per_month"`
	WritesIncluded  int64   `json:"writes_included"`
	QueriesIncluded int64   `json:"queries_included"`
	StorageGB       int64   `json:"storage_gb"`
	OverageWriteRate float64 `json:"overage_write_rate"` // per 1000 writes
	OverageQueryRate float64 `json:"overage_query_rate"` // per 1000 queries
}

// BillingAccount represents a tenant's billing information.
type BillingAccount struct {
	TenantID       string       `json:"tenant_id"`
	PlanID         string       `json:"plan_id"`
	StripeCustomer string       `json:"stripe_customer,omitempty"`
	StripeSubscription string   `json:"stripe_subscription,omitempty"`
	Status         string       `json:"status"` // "active", "past_due", "cancelled"
	CurrentPeriod  BillingPeriod `json:"current_period"`
	CreatedAt      time.Time    `json:"created_at"`
}

// BillingPeriod tracks usage within a billing cycle.
type BillingPeriod struct {
	Start         time.Time `json:"start"`
	End           time.Time `json:"end"`
	WritesUsed    int64     `json:"writes_used"`
	QueriesUsed   int64     `json:"queries_used"`
	StorageUsedGB float64   `json:"storage_used_gb"`
}

// BillingManager manages billing plans and accounts.
type BillingManager struct {
	mu       sync.RWMutex
	plans    map[string]*BillingPlan
	accounts map[string]*BillingAccount // tenantID -> account
}

// NewBillingManager creates a billing manager with default plans.
func NewBillingManager() *BillingManager {
	bm := &BillingManager{
		plans:    make(map[string]*BillingPlan),
		accounts: make(map[string]*BillingAccount),
	}

	bm.plans["free"] = &BillingPlan{
		ID: "free", Name: "Free", PricePerMonth: 0,
		WritesIncluded: 100_000, QueriesIncluded: 10_000, StorageGB: 1,
	}
	bm.plans["starter"] = &BillingPlan{
		ID: "starter", Name: "Starter", PricePerMonth: 29,
		WritesIncluded: 1_000_000, QueriesIncluded: 100_000, StorageGB: 10,
		OverageWriteRate: 0.10, OverageQueryRate: 0.05,
	}
	bm.plans["pro"] = &BillingPlan{
		ID: "pro", Name: "Pro", PricePerMonth: 99,
		WritesIncluded: 10_000_000, QueriesIncluded: 1_000_000, StorageGB: 100,
		OverageWriteRate: 0.05, OverageQueryRate: 0.02,
	}
	bm.plans["enterprise"] = &BillingPlan{
		ID: "enterprise", Name: "Enterprise", PricePerMonth: 499,
		WritesIncluded: 100_000_000, QueriesIncluded: 10_000_000, StorageGB: 1000,
		OverageWriteRate: 0.01, OverageQueryRate: 0.005,
	}

	return bm
}

// CreateAccount creates a billing account for a tenant.
func (bm *BillingManager) CreateAccount(tenantID, planID string) (*BillingAccount, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if _, ok := bm.plans[planID]; !ok {
		return nil, fmt.Errorf("billing: plan %q not found", planID)
	}

	now := time.Now()
	account := &BillingAccount{
		TenantID:  tenantID,
		PlanID:    planID,
		Status:    "active",
		CreatedAt: now,
		CurrentPeriod: BillingPeriod{
			Start: now,
			End:   now.AddDate(0, 1, 0),
		},
	}

	bm.accounts[tenantID] = account
	return account, nil
}

// GetAccount returns billing info for a tenant.
func (bm *BillingManager) GetAccount(tenantID string) (*BillingAccount, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	a, ok := bm.accounts[tenantID]
	return a, ok
}

// RecordUsage records usage for billing.
func (bm *BillingManager) RecordUsage(tenantID string, writes, queries int64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	if a, ok := bm.accounts[tenantID]; ok {
		a.CurrentPeriod.WritesUsed += writes
		a.CurrentPeriod.QueriesUsed += queries
	}
}

// CalculateInvoice computes the current billing amount.
func (bm *BillingManager) CalculateInvoice(tenantID string) (float64, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	a, ok := bm.accounts[tenantID]
	if !ok {
		return 0, fmt.Errorf("billing: account not found for tenant %q", tenantID)
	}
	plan, ok := bm.plans[a.PlanID]
	if !ok {
		return 0, fmt.Errorf("billing: plan %q not found", a.PlanID)
	}

	total := plan.PricePerMonth

	// Overage charges
	if a.CurrentPeriod.WritesUsed > plan.WritesIncluded {
		overage := float64(a.CurrentPeriod.WritesUsed-plan.WritesIncluded) / 1000
		total += overage * plan.OverageWriteRate
	}
	if a.CurrentPeriod.QueriesUsed > plan.QueriesIncluded {
		overage := float64(a.CurrentPeriod.QueriesUsed-plan.QueriesIncluded) / 1000
		total += overage * plan.OverageQueryRate
	}

	return total, nil
}

// ListPlans returns all available billing plans.
func (bm *BillingManager) ListPlans() []BillingPlan {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	plans := make([]BillingPlan, 0, len(bm.plans))
	for _, p := range bm.plans {
		plans = append(plans, *p)
	}
	return plans
}

// --- Self-Serve Portal API Key Management ---

// PortalAPIKeyRequest represents a request to create an API key.
type PortalAPIKeyRequest struct {
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
}

// PortalAPIKey represents a portal-managed API key.
type PortalAPIKey struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Key       string    `json:"key"`
	Prefix    string    `json:"prefix"` // first 8 chars for display
	TenantID  string    `json:"tenant_id"`
	Scopes    []string  `json:"scopes"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	LastUsed  time.Time `json:"last_used,omitempty"`
	Revoked   bool      `json:"revoked"`
}

// APIKeyManager manages tenant API keys for the self-serve portal.
type APIKeyManager struct {
	mu   sync.RWMutex
	keys map[string]*PortalAPIKey // key -> PortalAPIKey
}

// NewAPIKeyManager creates a new API key manager.
func NewAPIKeyManager() *APIKeyManager {
	return &APIKeyManager{
		keys: make(map[string]*PortalAPIKey),
	}
}

// CreateKey creates a new API key for a tenant.
func (m *APIKeyManager) CreateKey(tenantID, name string, scopes []string, ttl time.Duration) (*PortalAPIKey, error) {
	if name == "" {
		return nil, fmt.Errorf("api key name required")
	}
	if ttl <= 0 {
		ttl = 90 * 24 * time.Hour // 90 days default
	}

	rawKey, err := generateSecureAPIKey()
	if err != nil {
		return nil, err
	}

	keyID := generateSaaSSessionID()
	key := &PortalAPIKey{
		ID:        keyID,
		Name:      name,
		Key:       rawKey,
		Prefix:    rawKey[:8],
		TenantID:  tenantID,
		Scopes:    scopes,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(ttl),
	}

	m.mu.Lock()
	m.keys[rawKey] = key
	m.mu.Unlock()

	return key, nil
}

// RevokeKey revokes an API key.
func (m *APIKeyManager) RevokeKey(keyPrefix, tenantID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, key := range m.keys {
		if key.Prefix == keyPrefix && key.TenantID == tenantID {
			key.Revoked = true
			m.keys[k] = key
			return nil
		}
	}
	return fmt.Errorf("key with prefix %q not found", keyPrefix)
}

// ListKeys returns all keys for a tenant (with keys masked).
func (m *APIKeyManager) ListKeys(tenantID string) []PortalAPIKey {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []PortalAPIKey
	for _, key := range m.keys {
		if key.TenantID == tenantID {
			masked := *key
			masked.Key = masked.Prefix + "..." // mask the full key
			result = append(result, masked)
		}
	}
	return result
}

// ValidateKey validates a key and updates last-used time.
func (m *APIKeyManager) ValidateKey(rawKey string) (*PortalAPIKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key, ok := m.keys[rawKey]
	if !ok {
		return nil, fmt.Errorf("invalid key")
	}
	if key.Revoked {
		return nil, fmt.Errorf("key revoked")
	}
	if time.Now().After(key.ExpiresAt) {
		return nil, fmt.Errorf("key expired")
	}
	key.LastUsed = time.Now()
	return key, nil
}

// --- Onboarding Wizard ---

// OnboardingStep represents a step in the tenant onboarding wizard.
type OnboardingStep struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Completed   bool   `json:"completed"`
	Order       int    `json:"order"`
}

// OnboardingState tracks a tenant's onboarding progress.
type OnboardingState struct {
	TenantID  string           `json:"tenant_id"`
	Steps     []OnboardingStep `json:"steps"`
	StartedAt time.Time        `json:"started_at"`
	Complete  bool             `json:"complete"`
}

// NewOnboardingState creates a new onboarding state for a tenant.
func NewOnboardingState(tenantID string) *OnboardingState {
	return &OnboardingState{
		TenantID:  tenantID,
		StartedAt: time.Now(),
		Steps: []OnboardingStep{
			{ID: "create-account", Title: "Create Account", Description: "Sign up and verify email", Order: 1},
			{ID: "configure-sso", Title: "Configure SSO", Description: "Set up OIDC or SAML authentication", Order: 2},
			{ID: "create-api-key", Title: "Create API Key", Description: "Generate your first API key", Order: 3},
			{ID: "ingest-data", Title: "Ingest Data", Description: "Send your first data points", Order: 4},
			{ID: "create-dashboard", Title: "Create Dashboard", Description: "Build your first visualization", Order: 5},
			{ID: "invite-team", Title: "Invite Team", Description: "Add team members with RBAC roles", Order: 6},
		},
	}
}

// CompleteStep marks a step as complete.
func (o *OnboardingState) CompleteStep(stepID string) {
	for i := range o.Steps {
		if o.Steps[i].ID == stepID {
			o.Steps[i].Completed = true
			break
		}
	}
	// Check if all steps are complete
	allDone := true
	for _, s := range o.Steps {
		if !s.Completed {
			allDone = false
			break
		}
	}
	o.Complete = allDone
}

// Progress returns the percentage of onboarding completion.
func (o *OnboardingState) Progress() float64 {
	if len(o.Steps) == 0 {
		return 100
	}
	completed := 0
	for _, s := range o.Steps {
		if s.Completed {
			completed++
		}
	}
	return float64(completed) / float64(len(o.Steps)) * 100
}

// --- HTTP Handlers for Self-Serve Portal ---

// RegisterPortalHandlers registers self-serve portal HTTP endpoints.
func RegisterPortalHandlers(mux *http.ServeMux, cp *SaaSControlPlane, rbac *RBACManager, billing *BillingManager) {
	mux.HandleFunc("/api/portal/tenants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			tenants := cp.ListTenants()
			writeJSONStatus(w, http.StatusOK, tenants)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/portal/plans", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		plans := billing.ListPlans()
		writeJSONStatus(w, http.StatusOK, plans)
	})
}
