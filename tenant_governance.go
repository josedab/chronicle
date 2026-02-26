package chronicle

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// TenantGovernanceConfig configures the tenant governance engine.
type TenantGovernanceConfig struct {
	Enabled                bool          `json:"enabled"`
	DefaultCPUBudget       int64         `json:"default_cpu_budget"` // millicores
	DefaultMemoryBudgetMB  int64         `json:"default_memory_budget_mb"`
	DefaultStorageBudgetGB float64       `json:"default_storage_budget_gb"`
	DefaultQPSLimit        float64       `json:"default_qps_limit"`
	DefaultQueryCostBudget float64       `json:"default_query_cost_budget"`
	MeteringInterval       time.Duration `json:"metering_interval"`
	BillingCycleHours      int           `json:"billing_cycle_hours"`
	EnableChargeback       bool          `json:"enable_chargeback"`
	CostPerCPUHour         float64       `json:"cost_per_cpu_hour"`
	CostPerGBHour          float64       `json:"cost_per_gb_hour"`
	CostPerQueryUnit       float64       `json:"cost_per_query_unit"`
	ThrottleGracePeriod    time.Duration `json:"throttle_grace_period"`
}

// DefaultTenantGovernanceConfig returns a default configuration.
func DefaultTenantGovernanceConfig() TenantGovernanceConfig {
	return TenantGovernanceConfig{
		Enabled:                true,
		DefaultCPUBudget:       4000,
		DefaultMemoryBudgetMB:  2048,
		DefaultStorageBudgetGB: 100.0,
		DefaultQPSLimit:        1000,
		DefaultQueryCostBudget: 100000,
		MeteringInterval:       30 * time.Second,
		BillingCycleHours:      720, // 30 days
		EnableChargeback:       false,
		CostPerCPUHour:         0.05,
		CostPerGBHour:          0.01,
		CostPerQueryUnit:       0.001,
		ThrottleGracePeriod:    10 * time.Second,
	}
}

// ResourceBudget defines per-tenant resource limits.
type ResourceBudget struct {
	TenantID        string  `json:"tenant_id"`
	CPUMillicores   int64   `json:"cpu_millicores"`
	MemoryMB        int64   `json:"memory_mb"`
	StorageGB       float64 `json:"storage_gb"`
	QPSLimit        float64 `json:"qps_limit"`
	QueryCostBudget float64 `json:"query_cost_budget"`
	WritesPerSecond float64 `json:"writes_per_second"`
	Priority        int     `json:"priority"` // 1-10, higher = more priority
}

// GovernanceResourceUsage tracks current resource consumption for a tenant.
type GovernanceResourceUsage struct {
	TenantID            string    `json:"tenant_id"`
	CPUUsedMillicores   int64     `json:"cpu_used_millicores"`
	MemoryUsedMB        int64     `json:"memory_used_mb"`
	StorageUsedGB       float64   `json:"storage_used_gb"`
	CurrentQPS          float64   `json:"current_qps"`
	QueryCostUsed       float64   `json:"query_cost_used"`
	WritesCurrentSecond float64   `json:"writes_current_second"`
	MeasuredAt          time.Time `json:"measured_at"`
}

// MeteringRecord captures resource consumption over an interval.
type MeteringRecord struct {
	TenantID        string    `json:"tenant_id"`
	IntervalStart   time.Time `json:"interval_start"`
	IntervalEnd     time.Time `json:"interval_end"`
	CPUSeconds      float64   `json:"cpu_seconds"`
	MemoryGBSeconds float64   `json:"memory_gb_seconds"`
	StorageGBHours  float64   `json:"storage_gb_hours"`
	QueryCount      int64     `json:"query_count"`
	QueryCostTotal  float64   `json:"query_cost_total"`
	WriteCount      int64     `json:"write_count"`
	ReadBytes       int64     `json:"read_bytes"`
	WriteBytes      int64     `json:"write_bytes"`
}

// ChargebackLineItem represents one line in a billing report.
type ChargebackLineItem struct {
	Description string  `json:"description"`
	Quantity    float64 `json:"quantity"`
	UnitCost    float64 `json:"unit_cost"`
	TotalCost   float64 `json:"total_cost"`
}

// ChargebackReport is a billing or showback report for a tenant.
type ChargebackReport struct {
	TenantID           string               `json:"tenant_id"`
	BillingPeriodStart time.Time            `json:"billing_period_start"`
	BillingPeriodEnd   time.Time            `json:"billing_period_end"`
	LineItems          []ChargebackLineItem `json:"line_items"`
	TotalCost          float64              `json:"total_cost"`
	Currency           string               `json:"currency"`
}

// tokenBucket implements a thread-safe token bucket rate limiter.
type governanceTokenBucket struct {
	rate       float64 // tokens per second
	capacity   float64
	tokens     float64
	lastRefill time.Time
	mu         sync.Mutex
}

func newGovernanceTokenBucket(rate, capacity float64) *governanceTokenBucket {
	return &governanceTokenBucket{
		rate:       rate,
		capacity:   capacity,
		tokens:     capacity,
		lastRefill: time.Now(),
	}
}

func (tb *governanceTokenBucket) tryConsume(amount float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	if tb.tokens >= amount {
		tb.tokens -= amount
		return true
	}
	return false
}

func (tb *governanceTokenBucket) available() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	return tb.tokens
}

func (tb *governanceTokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens = math.Min(tb.capacity, tb.tokens+elapsed*tb.rate)
	tb.lastRefill = now
}

func (tb *governanceTokenBucket) updateRate(rate, capacity float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.rate = rate
	tb.capacity = capacity
	if tb.tokens > capacity {
		tb.tokens = capacity
	}
}

// tenantThrottleState tracks per-tenant throttling state.
type tenantThrottleState struct {
	qpsBucket     *governanceTokenBucket
	writeBucket   *governanceTokenBucket
	costBucket    *governanceTokenBucket
	graceStart    time.Time
	inGracePeriod bool
	priority      int
}

// TenantThrottler provides admission control for tenant resource usage.
type TenantThrottler struct {
	tenants     map[string]*tenantThrottleState
	gracePeriod time.Duration
	mu          sync.RWMutex
}

// NewTenantThrottler creates a new TenantThrottler.
func NewTenantThrottler(gracePeriod time.Duration) *TenantThrottler {
	return &TenantThrottler{
		tenants:     make(map[string]*tenantThrottleState),
		gracePeriod: gracePeriod,
	}
}

func (t *TenantThrottler) ensureTenant(tenantID string, budget ResourceBudget) *tenantThrottleState {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.tenants[tenantID]
	if !ok {
		state = &tenantThrottleState{
			qpsBucket:   newGovernanceTokenBucket(budget.QPSLimit, budget.QPSLimit*2),
			writeBucket: newGovernanceTokenBucket(budget.WritesPerSecond, budget.WritesPerSecond*2),
			costBucket:  newGovernanceTokenBucket(budget.QueryCostBudget/3600, budget.QueryCostBudget/3600*10),
			priority:    budget.Priority,
		}
		t.tenants[tenantID] = state
	}
	return state
}

func (t *TenantThrottler) updateTenant(tenantID string, budget ResourceBudget) {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.tenants[tenantID]
	if !ok {
		return
	}
	state.qpsBucket.updateRate(budget.QPSLimit, budget.QPSLimit*2)
	state.writeBucket.updateRate(budget.WritesPerSecond, budget.WritesPerSecond*2)
	state.costBucket.updateRate(budget.QueryCostBudget/3600, budget.QueryCostBudget/3600*10)
	state.priority = budget.Priority
}

// Admit checks whether a tenant operation should be admitted.
func (t *TenantThrottler) Admit(tenantID, resourceType string, amount float64) (bool, string) {
	t.mu.RLock()
	state, ok := t.tenants[tenantID]
	t.mu.RUnlock()
	if !ok {
		return false, fmt.Sprintf("tenant %s has no throttle state configured", tenantID)
	}

	var bucket *governanceTokenBucket
	switch resourceType {
	case "qps":
		bucket = state.qpsBucket
	case "write":
		bucket = state.writeBucket
	case "query_cost":
		bucket = state.costBucket
	default:
		return false, fmt.Sprintf("unknown resource type: %s", resourceType)
	}

	if bucket.tryConsume(amount) {
		// Admitted; clear grace period if active
		if state.inGracePeriod {
			state.inGracePeriod = false
		}
		return true, ""
	}

	// Check grace period
	now := time.Now()
	if !state.inGracePeriod {
		state.inGracePeriod = true
		state.graceStart = now
		return true, fmt.Sprintf("grace period started for tenant %s on %s", tenantID, resourceType)
	}
	if now.Sub(state.graceStart) < t.gracePeriod {
		return true, fmt.Sprintf("in grace period for tenant %s on %s (%.1fs remaining)",
			tenantID, resourceType, t.gracePeriod.Seconds()-now.Sub(state.graceStart).Seconds())
	}

	return false, fmt.Sprintf("tenant %s throttled on %s: budget exceeded and grace period expired", tenantID, resourceType)
}

// ShouldPreempt returns whether tenantA should be preempted in favor of tenantB.
func (t *TenantThrottler) ShouldPreempt(tenantA, tenantB string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	stateA, okA := t.tenants[tenantA]
	stateB, okB := t.tenants[tenantB]
	if !okA || !okB {
		return false
	}
	return stateB.priority > stateA.priority
}

// QueryCostAccounter tracks query cost accumulation per tenant.
type QueryCostAccounter struct {
	usage         map[string]*tenantCostState
	billingCycleH int
	mu            sync.RWMutex
}

type tenantCostState struct {
	totalQueryCost float64
	queryCount     int64
	cycleStart     time.Time
	writeCount     int64
	readBytes      int64
	writeBytes     int64
}

// NewQueryCostAccounter creates a new QueryCostAccounter.
func NewQueryCostAccounter(billingCycleHours int) *QueryCostAccounter {
	return &QueryCostAccounter{
		usage:         make(map[string]*tenantCostState),
		billingCycleH: billingCycleHours,
	}
}

func (a *QueryCostAccounter) ensureTenant(tenantID string) *tenantCostState {
	a.mu.Lock()
	defer a.mu.Unlock()
	state, ok := a.usage[tenantID]
	if !ok {
		state = &tenantCostState{
			cycleStart: time.Now(),
		}
		a.usage[tenantID] = state
	}
	return state
}

// AccountQuery records a query cost for a tenant.
func (a *QueryCostAccounter) AccountQuery(tenantID string, queryCost float64) {
	state := a.ensureTenant(tenantID)
	a.mu.Lock()
	defer a.mu.Unlock()
	state.totalQueryCost += queryCost
	state.queryCount++
}

// AccountWrite records a write operation for a tenant.
func (a *QueryCostAccounter) AccountWrite(tenantID string, count int64, bytes int64) {
	state := a.ensureTenant(tenantID)
	a.mu.Lock()
	defer a.mu.Unlock()
	state.writeCount += count
	state.writeBytes += bytes
}

// AccountRead records read bytes for a tenant.
func (a *QueryCostAccounter) AccountRead(tenantID string, bytes int64) {
	state := a.ensureTenant(tenantID)
	a.mu.Lock()
	defer a.mu.Unlock()
	state.readBytes += bytes
}

// GetUsage returns the current cost state for a tenant.
func (a *QueryCostAccounter) GetUsage(tenantID string) (float64, int64) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	state, ok := a.usage[tenantID]
	if !ok {
		return 0, 0
	}
	return state.totalQueryCost, state.queryCount
}

// GetFullUsage returns the full cost state for a tenant.
func (a *QueryCostAccounter) GetFullUsage(tenantID string) *tenantCostState {
	a.mu.RLock()
	defer a.mu.RUnlock()
	state, ok := a.usage[tenantID]
	if !ok {
		return &tenantCostState{cycleStart: time.Now()}
	}
	cp := *state
	return &cp
}

// ResetBillingCycle resets the accumulated costs for a tenant.
func (a *QueryCostAccounter) ResetBillingCycle(tenantID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.usage[tenantID] = &tenantCostState{
		cycleStart: time.Now(),
	}
}

// TenantGovernanceEngine manages per-tenant resource budgets, metering,
// chargeback/showback, and throttling.
type TenantGovernanceEngine struct {
	db              *DB
	config          TenantGovernanceConfig
	budgets         map[string]*ResourceBudget
	usageSnapshots  map[string]*GovernanceResourceUsage
	meteringRecords map[string][]MeteringRecord
	throttler       *TenantThrottler
	costAccounter   *QueryCostAccounter
	running         bool
	stopCh          chan struct{}
	mu              sync.RWMutex
}

// NewTenantGovernanceEngine creates a new governance engine.
func NewTenantGovernanceEngine(db *DB, cfg TenantGovernanceConfig) *TenantGovernanceEngine {
	return &TenantGovernanceEngine{
		db:              db,
		config:          cfg,
		budgets:         make(map[string]*ResourceBudget),
		usageSnapshots:  make(map[string]*GovernanceResourceUsage),
		meteringRecords: make(map[string][]MeteringRecord),
		throttler:       NewTenantThrottler(cfg.ThrottleGracePeriod),
		costAccounter:   NewQueryCostAccounter(cfg.BillingCycleHours),
		stopCh:          make(chan struct{}),
	}
}

// Start begins the metering collection background loop.
func (e *TenantGovernanceEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return nil
	}
	if !e.config.Enabled {
		return nil
	}
	e.running = true
	e.stopCh = make(chan struct{})
	go e.meteringLoop()
	return nil
}

// Stop halts the governance engine.
func (e *TenantGovernanceEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return nil
	}
	e.running = false
	close(e.stopCh)
	return nil
}

func (e *TenantGovernanceEngine) meteringLoop() {
	ticker := time.NewTicker(e.config.MeteringInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.collectMetering()
		}
	}
}

func (e *TenantGovernanceEngine) collectMetering() {
	e.mu.RLock()
	tenantIDs := make([]string, 0, len(e.budgets))
	for id := range e.budgets {
		tenantIDs = append(tenantIDs, id)
	}
	e.mu.RUnlock()

	now := time.Now()
	intervalDuration := e.config.MeteringInterval

	for _, tenantID := range tenantIDs {
		e.mu.RLock()
		usage, hasUsage := e.usageSnapshots[tenantID]
		e.mu.RUnlock()

		costState := e.costAccounter.GetFullUsage(tenantID)

		record := MeteringRecord{
			TenantID:       tenantID,
			IntervalStart:  now.Add(-intervalDuration),
			IntervalEnd:    now,
			QueryCount:     costState.queryCount,
			QueryCostTotal: costState.totalQueryCost,
			WriteCount:     costState.writeCount,
			ReadBytes:      costState.readBytes,
			WriteBytes:     costState.writeBytes,
		}

		if hasUsage {
			intervalSec := intervalDuration.Seconds()
			record.CPUSeconds = float64(usage.CPUUsedMillicores) / 1000.0 * intervalSec
			record.MemoryGBSeconds = float64(usage.MemoryUsedMB) / 1024.0 * intervalSec
			record.StorageGBHours = usage.StorageUsedGB * (intervalSec / 3600.0)
		}

		e.mu.Lock()
		e.meteringRecords[tenantID] = append(e.meteringRecords[tenantID], record)
		e.mu.Unlock()
	}
}

// CreateBudget sets a resource budget for a tenant.
func (e *TenantGovernanceEngine) CreateBudget(tenantID string, budget ResourceBudget) error {
	if budget.Priority < 1 {
		budget.Priority = 1
	}
	if budget.Priority > 10 {
		budget.Priority = 10
	}
	budget.TenantID = tenantID

	e.mu.Lock()
	if _, exists := e.budgets[tenantID]; exists {
		e.mu.Unlock()
		return fmt.Errorf("budget already exists for tenant %s; use UpdateBudget instead", tenantID)
	}
	e.budgets[tenantID] = &budget
	e.usageSnapshots[tenantID] = &GovernanceResourceUsage{
		TenantID:   tenantID,
		MeasuredAt: time.Now(),
	}
	e.mu.Unlock()

	e.throttler.ensureTenant(tenantID, budget)
	e.costAccounter.ensureTenant(tenantID)
	return nil
}

// UpdateBudget updates an existing tenant resource budget.
func (e *TenantGovernanceEngine) UpdateBudget(tenantID string, budget ResourceBudget) error {
	if budget.Priority < 1 {
		budget.Priority = 1
	}
	if budget.Priority > 10 {
		budget.Priority = 10
	}
	budget.TenantID = tenantID

	e.mu.Lock()
	if _, exists := e.budgets[tenantID]; !exists {
		e.mu.Unlock()
		return fmt.Errorf("no budget found for tenant %s; use CreateBudget first", tenantID)
	}
	e.budgets[tenantID] = &budget
	e.mu.Unlock()

	e.throttler.updateTenant(tenantID, budget)
	return nil
}
