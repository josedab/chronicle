package chronicle

import (
	"fmt"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Cross-Cloud Provider
// ---------------------------------------------------------------------------

// CrossCloudTierProvider identifies a cloud storage provider for cross-cloud tiering.
type CrossCloudTierProvider int

const (
	CloudLocal CrossCloudTierProvider = iota
	CloudAWS
	CloudGCP
	CloudAzure
	CloudCloudflareR2
	CloudMinIO
)

// String returns the name of the cloud provider.
func (p CrossCloudTierProvider) String() string {
	switch p {
	case CloudLocal:
		return "local"
	case CloudAWS:
		return "aws"
	case CloudGCP:
		return "gcp"
	case CloudAzure:
		return "azure"
	case CloudCloudflareR2:
		return "cloudflare-r2"
	case CloudMinIO:
		return "minio"
	default:
		return "unknown"
	}
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// CrossCloudTieringConfig configures the cross-cloud hybrid tiering engine.
type CrossCloudTieringConfig struct {
	Enabled                 bool          `json:"enabled"`
	EvaluationInterval      time.Duration `json:"evaluation_interval"`
	DefaultPolicy           string        `json:"default_policy"`
	CostOptimizationEnabled bool          `json:"cost_optimization_enabled"`
	EgressAware             bool          `json:"egress_aware"`
	MaxConcurrentMigrations int           `json:"max_concurrent_migrations"`
	DryRunMode              bool          `json:"dry_run_mode"`
}

// DefaultCrossCloudTieringConfig returns sensible defaults.
func DefaultCrossCloudTieringConfig() CrossCloudTieringConfig {
	return CrossCloudTieringConfig{
		Enabled:                 true,
		EvaluationInterval:      5 * time.Minute,
		DefaultPolicy:           "cost-optimized",
		CostOptimizationEnabled: true,
		EgressAware:             true,
		MaxConcurrentMigrations: 4,
		DryRunMode:              false,
	}
}

// ---------------------------------------------------------------------------
// Cloud Endpoint
// ---------------------------------------------------------------------------

// CloudEndpoint represents a cloud storage endpoint for cross-cloud tiering.
type CloudEndpoint struct {
	ID         string                 `json:"id"`
	Provider   CrossCloudTierProvider `json:"provider"`
	Region     string                 `json:"region"`
	Bucket     string                 `json:"bucket"`
	Endpoint   string                 `json:"endpoint"`
	CostPerGB  float64                `json:"cost_per_gb"`
	EgressCost float64                `json:"egress_cost"`
	Latency    time.Duration          `json:"latency"`
	Available  bool                   `json:"available"`
	Metadata   map[string]string      `json:"metadata"`
	LastCheck  time.Time              `json:"last_check"`
}

// ---------------------------------------------------------------------------
// Tiering Policy & Rules
// ---------------------------------------------------------------------------

// TieringPolicy defines rules for data placement across clouds.
type TieringPolicy struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Rules       []TieringRule `json:"rules"`
	Priority    int           `json:"priority"`
	Enabled     bool          `json:"enabled"`
	CreatedAt   time.Time     `json:"created_at"`
}

// TieringRule defines a single condition-action pair within a policy.
type TieringRule struct {
	Condition   TieringCondition `json:"condition"`
	Action      TieringAction    `json:"action"`
	TargetCloud string           `json:"target_cloud"`
	TargetTier  string           `json:"target_tier"`
}

// TieringCondition describes when a rule should fire.
type TieringCondition struct {
	Type     string        `json:"type"`     // "age", "access_frequency", "size", "metric_pattern"
	Operator string        `json:"operator"` // "gt", "lt", "eq", "matches"
	Value    string        `json:"value"`
	Duration time.Duration `json:"duration"`
}

// TieringAction describes what to do when a rule fires.
type TieringAction struct {
	Type        string `json:"type"`        // "migrate", "replicate", "delete", "compress"
	Compression string `json:"compression"` // optional compression override
}

// ---------------------------------------------------------------------------
// Data Placement
// ---------------------------------------------------------------------------

// DataPlacement tracks where data currently lives in the cross-cloud topology.
type DataPlacement struct {
	MetricKey   string    `json:"metric_key"`
	Partition   string    `json:"partition"`
	CurrentTier string    `json:"current_tier"`
	CloudID     string    `json:"cloud_id"`
	SizeBytes   int64     `json:"size_bytes"`
	LastAccess  time.Time `json:"last_access"`
	AccessCount int64     `json:"access_count"`
	Cost        float64   `json:"cost"`
	CreatedAt   time.Time `json:"created_at"`
}

// ---------------------------------------------------------------------------
// Migration Job
// ---------------------------------------------------------------------------

// MigrationJob represents a data migration between clouds.
type MigrationJob struct {
	ID            string    `json:"id"`
	SourceCloud   string    `json:"source_cloud"`
	TargetCloud   string    `json:"target_cloud"`
	Partitions    []string  `json:"partitions"`
	State         string    `json:"state"` // "pending", "running", "completed", "failed"
	Progress      float64   `json:"progress"`
	BytesTotal    int64     `json:"bytes_total"`
	BytesMoved    int64     `json:"bytes_moved"`
	StartedAt     time.Time `json:"started_at"`
	CompletedAt   time.Time `json:"completed_at"`
	Error         string    `json:"error,omitempty"`
	DryRun        bool      `json:"dry_run"`
	EstimatedCost float64   `json:"estimated_cost"`
}

// ---------------------------------------------------------------------------
// Cost Report
// ---------------------------------------------------------------------------

// CrossCloudCostReport tracks storage costs across clouds.
type CrossCloudCostReport struct {
	Period          string                         `json:"period"`
	TotalCost       float64                        `json:"total_cost"`
	CostByCloud     map[string]float64             `json:"cost_by_cloud"`
	CostByTier      map[string]float64             `json:"cost_by_tier"`
	EgressCost      float64                        `json:"egress_cost"`
	Savings         float64                        `json:"savings"`
	Recommendations []CrossCloudCostRecommendation `json:"recommendations"`
	GeneratedAt     time.Time                      `json:"generated_at"`
}

// CrossCloudCostRecommendation suggests a cost optimization action.
type CrossCloudCostRecommendation struct {
	Description     string  `json:"description"`
	EstimatedSaving float64 `json:"estimated_saving"`
	Action          string  `json:"action"`
	MetricPattern   string  `json:"metric_pattern"`
	FromTier        string  `json:"from_tier"`
	ToTier          string  `json:"to_tier"`
}

// ---------------------------------------------------------------------------
// Tiering Simulation
// ---------------------------------------------------------------------------

// TieringSimulation shows what would happen if a policy were applied.
type TieringSimulation struct {
	PolicyID        string             `json:"policy_id"`
	AffectedData    int64              `json:"affected_data"`
	MigrationCount  int                `json:"migration_count"`
	EstimatedCost   float64            `json:"estimated_cost"`
	CurrentCost     float64            `json:"current_cost"`
	ProjectedSaving float64            `json:"projected_saving"`
	Details         []SimulationDetail `json:"details"`
}

// SimulationDetail describes the impact on one metric key.
type SimulationDetail struct {
	MetricKey   string  `json:"metric_key"`
	CurrentTier string  `json:"current_tier"`
	TargetTier  string  `json:"target_tier"`
	SizeBytes   int64   `json:"size_bytes"`
	CostBefore  float64 `json:"cost_before"`
	CostAfter   float64 `json:"cost_after"`
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

// CrossCloudTieringStats holds runtime statistics for cross-cloud tiering.
type CrossCloudTieringStats struct {
	TotalEndpoints      int       `json:"total_endpoints"`
	TotalPolicies       int       `json:"total_policies"`
	TotalPlacements     int       `json:"total_placements"`
	ActiveMigrations    int       `json:"active_migrations"`
	CompletedMigrations int64     `json:"completed_migrations"`
	BytesMigrated       int64     `json:"bytes_migrated"`
	TotalStorageCost    float64   `json:"total_storage_cost"`
	EstimatedSavings    float64   `json:"estimated_savings"`
	LastEvaluation      time.Time `json:"last_evaluation"`
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

// CrossCloudTieringEngine orchestrates cross-cloud hybrid tiering.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type CrossCloudTieringEngine struct {
	db         *DB
	config     CrossCloudTieringConfig
	mu         sync.RWMutex
	endpoints  map[string]*CloudEndpoint
	policies   map[string]*TieringPolicy
	placements map[string]*DataPlacement
	migrations map[string]*MigrationJob
	stopCh     chan struct{}
	running    bool
	stats      CrossCloudTieringStats
}

// NewCrossCloudTieringEngine creates a new cross-cloud tiering engine.
func NewCrossCloudTieringEngine(db *DB, cfg CrossCloudTieringConfig) *CrossCloudTieringEngine {
	return &CrossCloudTieringEngine{
		db:         db,
		config:     cfg,
		endpoints:  make(map[string]*CloudEndpoint),
		policies:   make(map[string]*TieringPolicy),
		placements: make(map[string]*DataPlacement),
		migrations: make(map[string]*MigrationJob),
		stopCh:     make(chan struct{}),
	}
}

// Start begins the background evaluation loop.
func (e *CrossCloudTieringEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()

	go e.loop()
}

// Stop halts the background evaluation loop.
func (e *CrossCloudTieringEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

func (e *CrossCloudTieringEngine) loop() {
	interval := e.config.EvaluationInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.EvaluatePolicies()
		}
	}
}

// ---------------------------------------------------------------------------
// Endpoint management
// ---------------------------------------------------------------------------

// AddEndpoint registers a cloud storage endpoint.
func (e *CrossCloudTieringEngine) AddEndpoint(ep CloudEndpoint) error {
	if ep.ID == "" {
		return fmt.Errorf("endpoint ID is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.endpoints[ep.ID]; exists {
		return fmt.Errorf("endpoint %q already exists", ep.ID)
	}
	e.endpoints[ep.ID] = &ep
	e.stats.TotalEndpoints = len(e.endpoints)
	return nil
}

// RemoveEndpoint unregisters a cloud storage endpoint.
func (e *CrossCloudTieringEngine) RemoveEndpoint(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.endpoints[id]; !exists {
		return fmt.Errorf("endpoint %q not found", id)
	}
	delete(e.endpoints, id)
	e.stats.TotalEndpoints = len(e.endpoints)
	return nil
}

// ListEndpoints returns all registered cloud endpoints.
func (e *CrossCloudTieringEngine) ListEndpoints() []*CloudEndpoint {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*CloudEndpoint, 0, len(e.endpoints))
	for _, ep := range e.endpoints {
		out = append(out, ep)
	}
	return out
}

// ---------------------------------------------------------------------------
// Policy management
// ---------------------------------------------------------------------------

// CreatePolicy registers a new tiering policy.
func (e *CrossCloudTieringEngine) CreatePolicy(policy TieringPolicy) error {
	if policy.ID == "" {
		return fmt.Errorf("policy ID is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.policies[policy.ID]; exists {
		return fmt.Errorf("policy %q already exists", policy.ID)
	}
	if policy.CreatedAt.IsZero() {
		policy.CreatedAt = time.Now()
	}
	p := policy
	e.policies[policy.ID] = &p
	e.stats.TotalPolicies = len(e.policies)
	return nil
}

// UpdatePolicy updates an existing tiering policy.
func (e *CrossCloudTieringEngine) UpdatePolicy(policy TieringPolicy) error {
	if policy.ID == "" {
		return fmt.Errorf("policy ID is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.policies[policy.ID]; !exists {
		return fmt.Errorf("policy %q not found", policy.ID)
	}
	p := policy
	e.policies[policy.ID] = &p
	e.stats.TotalPolicies = len(e.policies)
	return nil
}

// DeletePolicy removes a tiering policy.
func (e *CrossCloudTieringEngine) DeletePolicy(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.policies[id]; !exists {
		return fmt.Errorf("policy %q not found", id)
	}
	delete(e.policies, id)
	e.stats.TotalPolicies = len(e.policies)
	return nil
}

// ListPolicies returns all registered tiering policies.
func (e *CrossCloudTieringEngine) ListPolicies() []*TieringPolicy {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*TieringPolicy, 0, len(e.policies))
	for _, p := range e.policies {
		out = append(out, p)
	}
	return out
}

// ---------------------------------------------------------------------------
// Policy evaluation
// ---------------------------------------------------------------------------

// EvaluatePolicies evaluates all enabled policies against current placements
// and returns migration jobs for data that should be moved.
func (e *CrossCloudTieringEngine) EvaluatePolicies() ([]MigrationJob, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.stats.LastEvaluation = time.Now()

	var jobs []MigrationJob
	for _, policy := range e.policies {
		if !policy.Enabled {
			continue
		}
		for _, placement := range e.placements {
			for _, rule := range policy.Rules {
				if e.evaluateRule(rule, placement) {
					jobID := fmt.Sprintf("job-%s-%s-%d", policy.ID, placement.MetricKey, time.Now().UnixNano())
					job := MigrationJob{
						ID:            jobID,
						SourceCloud:   placement.CloudID,
						TargetCloud:   rule.TargetCloud,
						Partitions:    []string{placement.Partition},
						State:         "pending",
						BytesTotal:    placement.SizeBytes,
						DryRun:        e.config.DryRunMode,
						EstimatedCost: e.estimateEgressCost(placement.CloudID, placement.SizeBytes),
					}
					e.migrations[jobID] = &job
					jobs = append(jobs, job)
				}
			}
		}
	}
	return jobs, nil
}
