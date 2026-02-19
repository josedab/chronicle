package chronicle

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	mrand "math/rand"
	"sort"
	"sync"
	"time"
)

// PrivacyFederationConfig configures privacy-preserving federation.
type PrivacyFederationConfig struct {
	// Enabled enables privacy federation
	Enabled bool `json:"enabled"`

	// Epsilon for differential privacy (smaller = more private)
	Epsilon float64 `json:"epsilon"`

	// Delta for differential privacy
	Delta float64 `json:"delta"`

	// NoiseType determines the noise mechanism
	NoiseType NoiseType `json:"noise_type"`

	// MinAggregationSize minimum records for aggregation
	MinAggregationSize int `json:"min_aggregation_size"`

	// SensitivityBound for query sensitivity
	SensitivityBound float64 `json:"sensitivity_bound"`

	// PrivacyBudgetPerQuery budget consumed per query
	PrivacyBudgetPerQuery float64 `json:"privacy_budget_per_query"`

	// TotalPrivacyBudget maximum budget before refresh
	TotalPrivacyBudget float64 `json:"total_privacy_budget"`

	// BudgetRefreshInterval when to reset budget
	BudgetRefreshInterval time.Duration `json:"budget_refresh_interval"`
}

// DefaultPrivacyFederationConfig returns default configuration.
func DefaultPrivacyFederationConfig() PrivacyFederationConfig {
	return PrivacyFederationConfig{
		Enabled:               true,
		Epsilon:               1.0,
		Delta:                 1e-5,
		NoiseType:             NoiseTypeLaplace,
		MinAggregationSize:    10,
		SensitivityBound:      100.0,
		PrivacyBudgetPerQuery: 0.1,
		TotalPrivacyBudget:    10.0,
		BudgetRefreshInterval: 24 * time.Hour,
	}
}

// NoiseType identifies noise mechanism types.
type NoiseType int

const (
	NoiseTypeLaplace NoiseType = iota
	NoiseTypeGaussian
	NoiseTypeExponential
)

func (n NoiseType) String() string {
	switch n {
	case NoiseTypeLaplace:
		return "laplace"
	case NoiseTypeGaussian:
		return "gaussian"
	case NoiseTypeExponential:
		return "exponential"
	default:
		return "unknown"
	}
}

// PrivacyFederation provides privacy-preserving federated queries.
type PrivacyFederation struct {
	db     *DB
	config PrivacyFederationConfig
	mu     sync.RWMutex

	// Privacy budget tracking per tenant/source
	budgets map[string]*PrivacyBudget

	// Registered federated sources
	sources map[string]*FederatedSource

	// Query audit log
	auditLog []*PrivacyAuditEntry

	// Background context
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// PrivacyBudget tracks privacy budget consumption.
type PrivacyBudget struct {
	SourceID    string    `json:"source_id"`
	TotalBudget float64   `json:"total_budget"`
	UsedBudget  float64   `json:"used_budget"`
	QueryCount  int       `json:"query_count"`
	LastRefresh time.Time `json:"last_refresh"`
	NextRefresh time.Time `json:"next_refresh"`
}

// FederatedSource represents a federated data source.
type FederatedSource struct {
	ID            string                `json:"id"`
	Name          string                `json:"name"`
	Endpoint      string                `json:"endpoint"`
	PrivacyPolicy *PrivacyPolicy        `json:"privacy_policy"`
	Metadata      map[string]string     `json:"metadata"`
	LastSeen      time.Time             `json:"last_seen"`
	Status        FederatedSourceStatus `json:"status"`
}

// FederatedSourceStatus indicates source health.
type FederatedSourceStatus string

const (
	FederatedSourceStatusActive   FederatedSourceStatus = "active"
	FederatedSourceStatusInactive FederatedSourceStatus = "inactive"
	FederatedSourceStatusError    FederatedSourceStatus = "error"
)

// PrivacyPolicy defines privacy constraints for a source.
type PrivacyPolicy struct {
	MaxEpsilon          float64       `json:"max_epsilon"`
	MinAggregationSize  int           `json:"min_aggregation_size"`
	AllowedAggregations []string      `json:"allowed_aggregations"`
	SensitiveFields     []string      `json:"sensitive_fields"`
	RedactedFields      []string      `json:"redacted_fields"`
	DataRetention       time.Duration `json:"data_retention"`
}

// PrivacyAuditEntry logs privacy-related operations.
type PrivacyAuditEntry struct {
	Timestamp   time.Time      `json:"timestamp"`
	SourceID    string         `json:"source_id"`
	QueryType   string         `json:"query_type"`
	Epsilon     float64        `json:"epsilon"`
	BudgetUsed  float64        `json:"budget_used"`
	RecordCount int            `json:"record_count"`
	NoiseAdded  float64        `json:"noise_added"`
	Metadata    map[string]any `json:"metadata"`
}

// FederatedQuery represents a privacy-preserving query.
type FederatedQuery struct {
	ID          string          `json:"id"`
	Sources     []string        `json:"sources"`
	Aggregation AggregationType `json:"aggregation"`
	Metric      string          `json:"metric"`
	Filters     map[string]any  `json:"filters"`
	GroupBy     []string        `json:"group_by"`
	TimeRange   TimeRange       `json:"time_range"`
	Epsilon     float64         `json:"epsilon"`
}

// AggregationType identifies aggregation functions.
type AggregationType string

const (
	AggregationTypeCount AggregationType = "count"
	AggregationTypeSum   AggregationType = "sum"
	AggregationTypeAvg   AggregationType = "avg"
	AggregationTypeMin   AggregationType = "min"
	AggregationTypeMax   AggregationType = "max"
	AggregationTypeHist  AggregationType = "histogram"
)

// TimeRange specifies query time bounds.
type TimeRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// PrivacyFederatedResult contains privacy-preserving query results.
type PrivacyFederatedResult struct {
	QueryID     string             `json:"query_id"`
	Timestamp   time.Time          `json:"timestamp"`
	Sources     []string           `json:"sources"`
	Value       float64            `json:"value"`
	NoisyValue  float64            `json:"noisy_value"`
	Confidence  float64            `json:"confidence"`
	Groups      map[string]float64 `json:"groups,omitempty"`
	NoisyGroups map[string]float64 `json:"noisy_groups,omitempty"`
	PrivacyInfo *PrivacyInfo       `json:"privacy_info"`
}

// PrivacyInfo contains privacy-related metadata for results.
type PrivacyInfo struct {
	Epsilon          float64 `json:"epsilon"`
	Delta            float64 `json:"delta"`
	NoiseType        string  `json:"noise_type"`
	NoiseMagnitude   float64 `json:"noise_magnitude"`
	BudgetConsumed   float64 `json:"budget_consumed"`
	BudgetRemaining  float64 `json:"budget_remaining"`
	RecordCount      int     `json:"record_count"`
	SuppressedGroups int     `json:"suppressed_groups"`
}

// NewPrivacyFederation creates a new privacy federation engine.
func NewPrivacyFederation(db *DB, config PrivacyFederationConfig) *PrivacyFederation {
	ctx, cancel := context.WithCancel(context.Background())

	return &PrivacyFederation{
		db:       db,
		config:   config,
		budgets:  make(map[string]*PrivacyBudget),
		sources:  make(map[string]*FederatedSource),
		auditLog: make([]*PrivacyAuditEntry, 0),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start starts the privacy federation engine.
func (p *PrivacyFederation) Start() {
	p.wg.Add(1)
	go p.budgetRefreshLoop()
}

// Stop stops the privacy federation engine.
func (p *PrivacyFederation) Stop() {
	p.cancel()
	p.wg.Wait()
}

func (p *PrivacyFederation) budgetRefreshLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.refreshExpiredBudgets()
		}
	}
}

func (p *PrivacyFederation) refreshExpiredBudgets() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	for _, budget := range p.budgets {
		if now.After(budget.NextRefresh) {
			budget.UsedBudget = 0
			budget.QueryCount = 0
			budget.LastRefresh = now
			budget.NextRefresh = now.Add(p.config.BudgetRefreshInterval)
		}
	}
}

// RegisterSource registers a federated data source.
func (p *PrivacyFederation) RegisterSource(source *FederatedSource) error {
	if source.ID == "" {
		return errors.New("source ID required")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize privacy policy if not set
	if source.PrivacyPolicy == nil {
		source.PrivacyPolicy = &PrivacyPolicy{
			MaxEpsilon:          p.config.Epsilon,
			MinAggregationSize:  p.config.MinAggregationSize,
			AllowedAggregations: []string{"count", "sum", "avg", "min", "max"},
		}
	}

	source.Status = FederatedSourceStatusActive
	source.LastSeen = time.Now()
	p.sources[source.ID] = source

	// Initialize budget
	now := time.Now()
	p.budgets[source.ID] = &PrivacyBudget{
		SourceID:    source.ID,
		TotalBudget: p.config.TotalPrivacyBudget,
		LastRefresh: now,
		NextRefresh: now.Add(p.config.BudgetRefreshInterval),
	}

	return nil
}

// UnregisterSource removes a federated source.
func (p *PrivacyFederation) UnregisterSource(sourceID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.sources, sourceID)
	delete(p.budgets, sourceID)
}

// Execute executes a privacy-preserving federated query.
func (p *PrivacyFederation) Execute(query *FederatedQuery) (*PrivacyFederatedResult, error) {
	if query.ID == "" {
		query.ID = fmt.Sprintf("fq-%d", time.Now().UnixNano())
	}

	// Validate sources
	p.mu.RLock()
	for _, sourceID := range query.Sources {
		source, ok := p.sources[sourceID]
		if !ok {
			p.mu.RUnlock()
			return nil, fmt.Errorf("source not found: %s", sourceID)
		}

		// Check allowed aggregations
		if !isAggregationAllowed(source.PrivacyPolicy, string(query.Aggregation)) {
			p.mu.RUnlock()
			return nil, fmt.Errorf("aggregation %s not allowed for source %s", query.Aggregation, sourceID)
		}
	}
	p.mu.RUnlock()

	// Check and consume privacy budget
	epsilon := query.Epsilon
	if epsilon == 0 {
		epsilon = p.config.Epsilon
	}

	budgetNeeded := p.config.PrivacyBudgetPerQuery
	if err := p.consumeBudget(query.Sources, budgetNeeded); err != nil {
		return nil, err
	}

	// Execute query and get raw results
	rawValue, recordCount, groups, err := p.executeRawQuery(query)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	// Apply differential privacy
	sensitivity := p.calculateSensitivity(query.Aggregation, recordCount)
	noisyValue := p.addNoise(rawValue, epsilon, sensitivity)

	// Add noise to groups if present
	noisyGroups := make(map[string]float64)
	suppressedGroups := 0
	for key, value := range groups {
		// Suppress small groups
		if p.shouldSuppressGroup(key, query) {
			suppressedGroups++
			continue
		}
		noisyGroups[key] = p.addNoise(value, epsilon, sensitivity)
	}

	// Calculate confidence based on noise magnitude
	noiseMagnitude := sensitivity / epsilon
	confidence := 1.0 - (noiseMagnitude / (math.Abs(rawValue) + 1))
	if confidence < 0 {
		confidence = 0
	}

	// Get remaining budget
	budgetRemaining := p.getRemainingBudget(query.Sources)

	result := &PrivacyFederatedResult{
		QueryID:     query.ID,
		Timestamp:   time.Now(),
		Sources:     query.Sources,
		Value:       rawValue,
		NoisyValue:  noisyValue,
		Confidence:  confidence,
		Groups:      groups,
		NoisyGroups: noisyGroups,
		PrivacyInfo: &PrivacyInfo{
			Epsilon:          epsilon,
			Delta:            p.config.Delta,
			NoiseType:        p.config.NoiseType.String(),
			NoiseMagnitude:   noiseMagnitude,
			BudgetConsumed:   budgetNeeded,
			BudgetRemaining:  budgetRemaining,
			RecordCount:      recordCount,
			SuppressedGroups: suppressedGroups,
		},
	}

	// Audit log
	p.addAuditEntry(query, result, noiseMagnitude)

	return result, nil
}

func (p *PrivacyFederation) executeRawQuery(query *FederatedQuery) (float64, int, map[string]float64, error) {
	// Simulate query execution - in production would query actual sources
	// For now, generate synthetic data based on time range

	recordCount := 100 // Simulated
	var value float64
	groups := make(map[string]float64)

	switch query.Aggregation {
	case AggregationTypeCount:
		value = float64(recordCount)
	case AggregationTypeSum:
		value = float64(recordCount) * 50.0 // Simulated avg value
	case AggregationTypeAvg:
		value = 50.0 // Simulated average
	case AggregationTypeMin:
		value = 10.0
	case AggregationTypeMax:
		value = 100.0
	default:
		value = float64(recordCount)
	}

	// Generate group values if GroupBy specified
	if len(query.GroupBy) > 0 {
		sampleGroups := []string{"group_a", "group_b", "group_c", "group_d"}
		for _, g := range sampleGroups {
			groups[g] = value / float64(len(sampleGroups))
		}
	}

	return value, recordCount, groups, nil
}

func (p *PrivacyFederation) consumeBudget(sources []string, amount float64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if all sources have sufficient budget
	for _, sourceID := range sources {
		budget, ok := p.budgets[sourceID]
		if !ok {
			return fmt.Errorf("no budget for source: %s", sourceID)
		}

		if budget.UsedBudget+amount > budget.TotalBudget {
			return fmt.Errorf("privacy budget exhausted for source %s (used: %.2f, needed: %.2f, total: %.2f)",
				sourceID, budget.UsedBudget, amount, budget.TotalBudget)
		}
	}

	// Consume budget
	for _, sourceID := range sources {
		budget := p.budgets[sourceID]
		budget.UsedBudget += amount
		budget.QueryCount++
	}

	return nil
}

func (p *PrivacyFederation) getRemainingBudget(sources []string) float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	minRemaining := math.MaxFloat64
	for _, sourceID := range sources {
		if budget, ok := p.budgets[sourceID]; ok {
			remaining := budget.TotalBudget - budget.UsedBudget
			if remaining < minRemaining {
				minRemaining = remaining
			}
		}
	}

	if minRemaining == math.MaxFloat64 {
		return 0
	}
	return minRemaining
}

func (p *PrivacyFederation) calculateSensitivity(agg AggregationType, recordCount int) float64 {
	switch agg {
	case AggregationTypeCount:
		return 1.0
	case AggregationTypeSum:
		return p.config.SensitivityBound
	case AggregationTypeAvg:
		if recordCount > 0 {
			return p.config.SensitivityBound / float64(recordCount)
		}
		return p.config.SensitivityBound
	case AggregationTypeMin, AggregationTypeMax:
		return p.config.SensitivityBound
	default:
		return p.config.SensitivityBound
	}
}

func (p *PrivacyFederation) addNoise(value, epsilon, sensitivity float64) float64 {
	scale := sensitivity / epsilon

	var noise float64
	switch p.config.NoiseType {
	case NoiseTypeLaplace:
		noise = laplaceSample(scale)
	case NoiseTypeGaussian:
		sigma := scale * math.Sqrt(2*math.Log(1.25/p.config.Delta))
		noise = gaussianSample(sigma)
	default:
		noise = laplaceSample(scale)
	}

	return value + noise
}

func (p *PrivacyFederation) shouldSuppressGroup(groupKey string, query *FederatedQuery) bool {
	// Check minimum aggregation size from source policies
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, sourceID := range query.Sources {
		if source, ok := p.sources[sourceID]; ok {
			if source.PrivacyPolicy != nil && source.PrivacyPolicy.MinAggregationSize > 0 {
				// In production, would check actual group size
				// For now, suppress based on random threshold for demonstration
				return false
			}
		}
	}
	return false
}

func (p *PrivacyFederation) addAuditEntry(query *FederatedQuery, result *PrivacyFederatedResult, noise float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry := &PrivacyAuditEntry{
		Timestamp:   time.Now(),
		SourceID:    query.Sources[0], // Primary source
		QueryType:   string(query.Aggregation),
		Epsilon:     result.PrivacyInfo.Epsilon,
		BudgetUsed:  result.PrivacyInfo.BudgetConsumed,
		RecordCount: result.PrivacyInfo.RecordCount,
		NoiseAdded:  noise,
		Metadata: map[string]any{
			"query_id": query.ID,
			"sources":  query.Sources,
		},
	}

	p.auditLog = append(p.auditLog, entry)

	// Trim old entries (keep last 1000)
	if len(p.auditLog) > 1000 {
		p.auditLog = p.auditLog[len(p.auditLog)-1000:]
	}
}

// GetBudgetStatus returns privacy budget status for all sources.
func (p *PrivacyFederation) GetBudgetStatus() map[string]*PrivacyBudget {
	p.mu.RLock()
	defer p.mu.RUnlock()

	status := make(map[string]*PrivacyBudget)
	for id, budget := range p.budgets {
		status[id] = &PrivacyBudget{
			SourceID:    budget.SourceID,
			TotalBudget: budget.TotalBudget,
			UsedBudget:  budget.UsedBudget,
			QueryCount:  budget.QueryCount,
			LastRefresh: budget.LastRefresh,
			NextRefresh: budget.NextRefresh,
		}
	}
	return status
}

// GetAuditLog returns recent audit entries.
func (p *PrivacyFederation) GetAuditLog(limit int) []*PrivacyAuditEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if limit <= 0 || limit > len(p.auditLog) {
		limit = len(p.auditLog)
	}

	start := len(p.auditLog) - limit
	entries := make([]*PrivacyAuditEntry, limit)
	copy(entries, p.auditLog[start:])
	return entries
}

// ListSources returns all registered sources.
func (p *PrivacyFederation) ListSources() []*FederatedSource {
	p.mu.RLock()
	defer p.mu.RUnlock()

	sources := make([]*FederatedSource, 0, len(p.sources))
	for _, source := range p.sources {
		sources = append(sources, source)
	}

	sort.Slice(sources, func(i, j int) bool {
		return sources[i].Name < sources[j].Name
	})

	return sources
}

// SecureAggregation performs secure multi-party aggregation.
func (p *PrivacyFederation) SecureAggregation(sources []string, aggregation AggregationType, metric string) (*SecureAggResult, error) {
	// Simulate secure aggregation protocol
	// In production, would use actual MPC protocols

	// Generate shares for each source
	shares := make(map[string]float64)
	totalValue := 0.0

	for _, sourceID := range sources {
		// Simulate source contribution
		share := mrand.Float64() * 100 // Random value
		shares[sourceID] = share
		totalValue += share
	}

	// Apply differential privacy to result
	epsilon := p.config.Epsilon
	sensitivity := p.calculateSensitivity(aggregation, len(sources)*100)
	noisyTotal := p.addNoise(totalValue, epsilon, sensitivity)

	return &SecureAggResult{
		Aggregation:      aggregation,
		Metric:           metric,
		ParticipantCount: len(sources),
		RawValue:         totalValue,
		SecureValue:      noisyTotal,
		Timestamp:        time.Now(),
	}, nil
}

// SecureAggResult contains secure aggregation results.
type SecureAggResult struct {
	Aggregation      AggregationType `json:"aggregation"`
	Metric           string          `json:"metric"`
	ParticipantCount int             `json:"participant_count"`
	RawValue         float64         `json:"raw_value"`
	SecureValue      float64         `json:"secure_value"`
	Timestamp        time.Time       `json:"timestamp"`
}

// ExportPrivacyReport generates a privacy compliance report.
func (p *PrivacyFederation) ExportPrivacyReport() ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	report := map[string]any{
		"generated_at": time.Now(),
		"config": map[string]any{
			"epsilon":          p.config.Epsilon,
			"delta":            p.config.Delta,
			"noise_type":       p.config.NoiseType.String(),
			"min_aggregation":  p.config.MinAggregationSize,
			"budget_per_query": p.config.PrivacyBudgetPerQuery,
			"total_budget":     p.config.TotalPrivacyBudget,
			"budget_refresh":   p.config.BudgetRefreshInterval.String(),
		},
		"sources":        p.ListSources(),
		"budget_status":  p.GetBudgetStatus(),
		"recent_queries": len(p.auditLog),
		"summary": map[string]any{
			"total_sources":    len(p.sources),
			"total_queries":    p.getTotalQueryCount(),
			"avg_epsilon_used": p.getAvgEpsilonUsed(),
		},
	}

	return json.MarshalIndent(report, "", "  ")
}

func (p *PrivacyFederation) getTotalQueryCount() int {
	total := 0
	for _, budget := range p.budgets {
		total += budget.QueryCount
	}
	return total
}

func (p *PrivacyFederation) getAvgEpsilonUsed() float64 {
	if len(p.auditLog) == 0 {
		return 0
	}

	var total float64
	for _, entry := range p.auditLog {
		total += entry.Epsilon
	}
	return total / float64(len(p.auditLog))
}

// --- Noise generation functions ---

func laplaceSample(scale float64) float64 {
	// Generate Laplace noise using inverse CDF
	u := uniformSample() - 0.5
	if u == 0 {
		return 0
	}
	sign := 1.0
	if u < 0 {
		sign = -1.0
		u = -u
	}
	return -sign * scale * math.Log(1-2*u)
}

func gaussianSample(sigma float64) float64 {
	// Box-Muller transform
	u1 := uniformSample()
	u2 := uniformSample()
	return sigma * math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

func uniformSample() float64 {
	var buf [8]byte
	rand.Read(buf[:])
	val := float64(buf[0])
	for i := 1; i < 8; i++ {
		val = val*256 + float64(buf[i])
	}
	return val / (1 << 64)
}

func isAggregationAllowed(policy *PrivacyPolicy, agg string) bool {
	if policy == nil || len(policy.AllowedAggregations) == 0 {
		return true
	}
	for _, allowed := range policy.AllowedAggregations {
		if allowed == agg {
			return true
		}
	}
	return false
}
