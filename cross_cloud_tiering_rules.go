// cross_cloud_tiering_rules.go contains extended cross cloud tiering functionality.
package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// evaluateRule checks whether a single rule matches a placement.
func (e *CrossCloudTieringEngine) evaluateRule(rule TieringRule, placement *DataPlacement) bool {
	switch rule.Condition.Type {
	case "age":
		age := time.Since(placement.CreatedAt)
		return e.compareDuration(age, rule.Condition.Operator, rule.Condition.Duration)
	case "access_frequency":
		threshold, err := strconv.ParseInt(rule.Condition.Value, 10, 64)
		if err != nil {
			return false
		}
		return e.compareInt64(placement.AccessCount, rule.Condition.Operator, threshold)
	case "size":
		threshold, err := strconv.ParseInt(rule.Condition.Value, 10, 64)
		if err != nil {
			return false
		}
		return e.compareInt64(placement.SizeBytes, rule.Condition.Operator, threshold)
	case "metric_pattern":
		if rule.Condition.Operator == "matches" {
			return strings.Contains(placement.MetricKey, rule.Condition.Value)
		}
		return false
	default:
		return false
	}
}

func (e *CrossCloudTieringEngine) compareInt64(actual int64, op string, threshold int64) bool {
	switch op {
	case "gt":
		return actual > threshold
	case "lt":
		return actual < threshold
	case "eq":
		return actual == threshold
	default:
		return false
	}
}

func (e *CrossCloudTieringEngine) compareDuration(actual time.Duration, op string, threshold time.Duration) bool {
	switch op {
	case "gt":
		return actual > threshold
	case "lt":
		return actual < threshold
	case "eq":
		return actual == threshold
	default:
		return false
	}
}

func (e *CrossCloudTieringEngine) estimateEgressCost(cloudID string, sizeBytes int64) float64 {
	ep, ok := e.endpoints[cloudID]
	if !ok {
		return 0
	}
	return ep.EgressCost * float64(sizeBytes) / float64(1<<30)
}

// ---------------------------------------------------------------------------
// Migration execution
// ---------------------------------------------------------------------------

// Migrate executes a migration job, updating its state as it progresses.
func (e *CrossCloudTieringEngine) Migrate(job *MigrationJob) error {
	e.mu.Lock()
	job.State = "running"
	job.StartedAt = time.Now()
	e.stats.ActiveMigrations++
	e.mu.Unlock()

	if job.DryRun {
		e.mu.Lock()
		job.State = "completed"
		job.Progress = 100.0
		job.BytesMoved = job.BytesTotal
		job.CompletedAt = time.Now()
		e.stats.ActiveMigrations--
		e.stats.CompletedMigrations++
		e.stats.BytesMigrated += job.BytesMoved
		e.mu.Unlock()
		return nil
	}

	// Simulate migration progress for each partition
	for i, partition := range job.Partitions {
		e.mu.RLock()
		placement, exists := e.placements[partition]
		e.mu.RUnlock()
		if !exists {
			continue
		}

		e.mu.Lock()
		job.BytesMoved += placement.SizeBytes
		job.Progress = float64(i+1) / float64(len(job.Partitions)) * 100.0
		// Update placement to reflect new location
		placement.CloudID = job.TargetCloud
		placement.CurrentTier = job.TargetCloud
		e.mu.Unlock()
	}

	e.mu.Lock()
	job.State = "completed"
	job.Progress = 100.0
	job.CompletedAt = time.Now()
	e.stats.ActiveMigrations--
	e.stats.CompletedMigrations++
	e.stats.BytesMigrated += job.BytesMoved
	e.mu.Unlock()

	return nil
}

// ---------------------------------------------------------------------------
// Simulation
// ---------------------------------------------------------------------------

// SimulatePolicy performs a dry-run evaluation of a policy and returns projected impact.
func (e *CrossCloudTieringEngine) SimulatePolicy(policyID string) (*TieringSimulation, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	policy, ok := e.policies[policyID]
	if !ok {
		return nil, fmt.Errorf("policy %q not found", policyID)
	}

	sim := &TieringSimulation{
		PolicyID: policyID,
	}

	for _, placement := range e.placements {
		for _, rule := range policy.Rules {
			if e.evaluateRule(rule, placement) {
				targetEP, hasTarget := e.endpoints[rule.TargetCloud]
				costBefore := placement.Cost
				costAfter := costBefore
				if hasTarget {
					costAfter = targetEP.CostPerGB * float64(placement.SizeBytes) / float64(1<<30)
				}
				detail := SimulationDetail{
					MetricKey:   placement.MetricKey,
					CurrentTier: placement.CurrentTier,
					TargetTier:  rule.TargetTier,
					SizeBytes:   placement.SizeBytes,
					CostBefore:  costBefore,
					CostAfter:   costAfter,
				}
				sim.Details = append(sim.Details, detail)
				sim.AffectedData += placement.SizeBytes
				sim.MigrationCount++
				sim.CurrentCost += costBefore
				sim.EstimatedCost += costAfter
				break // one match per placement per policy
			}
		}
	}
	sim.ProjectedSaving = sim.CurrentCost - sim.EstimatedCost
	return sim, nil
}

// ---------------------------------------------------------------------------
// Cost report
// ---------------------------------------------------------------------------

// GenerateCostReport computes a cost report for the given period.
func (e *CrossCloudTieringEngine) GenerateCostReport(period string) (*CrossCloudCostReport, error) {
	if period != "daily" && period != "weekly" && period != "monthly" {
		return nil, fmt.Errorf("unsupported period %q; use daily, weekly, or monthly", period)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	report := &CrossCloudCostReport{
		Period:      period,
		CostByCloud: make(map[string]float64),
		CostByTier:  make(map[string]float64),
		GeneratedAt: time.Now(),
	}

	var hotTierCost float64
	for _, placement := range e.placements {
		report.TotalCost += placement.Cost
		report.CostByCloud[placement.CloudID] += placement.Cost
		report.CostByTier[placement.CurrentTier] += placement.Cost
		if ep, ok := e.endpoints[placement.CloudID]; ok {
			hotTierCost += ep.CostPerGB * float64(placement.SizeBytes) / float64(1<<30)
		}
	}

	switch period {
	case "daily":
		report.TotalCost /= 30.0
		hotTierCost /= 30.0
		for k := range report.CostByCloud {
			report.CostByCloud[k] /= 30.0
		}
		for k := range report.CostByTier {
			report.CostByTier[k] /= 30.0
		}
	case "weekly":
		report.TotalCost /= 4.0
		hotTierCost /= 4.0
		for k := range report.CostByCloud {
			report.CostByCloud[k] /= 4.0
		}
		for k := range report.CostByTier {
			report.CostByTier[k] /= 4.0
		}
	}

	report.Savings = hotTierCost - report.TotalCost
	if report.Savings < 0 {
		report.Savings = 0
	}

	for _, placement := range e.placements {
		if placement.CurrentTier == "hot" && placement.AccessCount < 10 {
			report.Recommendations = append(report.Recommendations, CrossCloudCostRecommendation{
				Description:     fmt.Sprintf("Move %s to warm tier (low access)", placement.MetricKey),
				EstimatedSaving: placement.Cost * 0.4,
				Action:          "migrate",
				MetricPattern:   placement.MetricKey,
				FromTier:        "hot",
				ToTier:          "warm",
			})
		}
	}

	e.stats.TotalStorageCost = report.TotalCost
	e.stats.EstimatedSavings = report.Savings

	return report, nil
}

// ---------------------------------------------------------------------------
// Data placement & access tracking
// ---------------------------------------------------------------------------

// TrackPlacement records or updates where a metric partition is stored.
func (e *CrossCloudTieringEngine) TrackPlacement(metricKey string, partition string, tier string, cloudID string, size int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := partition
	if p, exists := e.placements[key]; exists {
		p.CurrentTier = tier
		p.CloudID = cloudID
		p.SizeBytes = size
		return
	}

	ep, hasEP := e.endpoints[cloudID]
	cost := 0.0
	if hasEP {
		cost = ep.CostPerGB * float64(size) / float64(1<<30)
	}

	e.placements[key] = &DataPlacement{
		MetricKey:   metricKey,
		Partition:   partition,
		CurrentTier: tier,
		CloudID:     cloudID,
		SizeBytes:   size,
		LastAccess:  time.Now(),
		Cost:        cost,
		CreatedAt:   time.Now(),
	}
	e.stats.TotalPlacements = len(e.placements)
}

// RecordAccess records a data access for cost and tiering analysis.
func (e *CrossCloudTieringEngine) RecordAccess(metricKey string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, p := range e.placements {
		if p.MetricKey == metricKey {
			p.AccessCount++
			p.LastAccess = time.Now()
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Migration queries
// ---------------------------------------------------------------------------

// ListMigrations returns all migration jobs.
func (e *CrossCloudTieringEngine) ListMigrations() []*MigrationJob {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*MigrationJob, 0, len(e.migrations))
	for _, m := range e.migrations {
		out = append(out, m)
	}
	return out
}

// GetMigration returns a specific migration job by ID.
func (e *CrossCloudTieringEngine) GetMigration(id string) *MigrationJob {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.migrations[id]
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

// Stats returns current statistics for cross-cloud tiering.
func (e *CrossCloudTieringEngine) Stats() CrossCloudTieringStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s := e.stats
	s.TotalEndpoints = len(e.endpoints)
	s.TotalPolicies = len(e.policies)
	s.TotalPlacements = len(e.placements)

	active := 0
	for _, m := range e.migrations {
		if m.State == "running" || m.State == "pending" {
			active++
		}
	}
	s.ActiveMigrations = active
	return s
}

// ---------------------------------------------------------------------------
// HTTP Handlers
// ---------------------------------------------------------------------------

// RegisterHTTPHandlers registers cross-cloud tiering API endpoints.
func (e *CrossCloudTieringEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/tiering/endpoints", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			json.NewEncoder(w).Encode(e.ListEndpoints())
		case http.MethodPost:
			var ep CloudEndpoint
			if err := json.NewDecoder(r.Body).Decode(&ep); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if err := e.AddEndpoint(ep); err != nil {
				http.Error(w, "conflict", http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(ep)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/tiering/policies", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			json.NewEncoder(w).Encode(e.ListPolicies())
		case http.MethodPost:
			var p TieringPolicy
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if err := e.CreatePolicy(p); err != nil {
				http.Error(w, "conflict", http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(p)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/tiering/evaluate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		jobs, err := e.EvaluatePolicies()
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		json.NewEncoder(w).Encode(jobs)
	})

	mux.HandleFunc("/api/v1/tiering/simulate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			PolicyID string `json:"policy_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		sim, err := e.SimulatePolicy(req.PolicyID)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(sim)
	})

	mux.HandleFunc("/api/v1/tiering/migrations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListMigrations())
	})

	mux.HandleFunc("/api/v1/tiering/cost-report", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		period := r.URL.Query().Get("period")
		if period == "" {
			period = "monthly"
		}
		report, err := e.GenerateCostReport(period)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(report)
	})

	mux.HandleFunc("/api/v1/tiering/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}
