// capacity_planning_recommendations.go contains extended capacity planning functionality.
package chronicle

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)

func (e *CapacityPlanningEngine) GenerateRecommendations() ([]*CapacityRecommendation, error) {
	e.historyMu.RLock()
	history := e.usageHistory
	e.historyMu.RUnlock()

	if len(history) == 0 {
		return nil, nil
	}

	current := history[len(history)-1]
	recommendations := make([]*CapacityRecommendation, 0)

	// Get forecasts
	e.forecastMu.RLock()
	forecasts := e.forecasts
	e.forecastMu.RUnlock()

	// Storage recommendations
	if forecast, ok := forecasts["storage"]; ok && forecast != nil {
		if forecast.TimeToExhaustion != nil && *forecast.TimeToExhaustion < 7*24*time.Hour {
			genID1, _ := generateID()
			rec := &CapacityRecommendation{
				ID:             genID1,
				Priority:       PriorityHigh,
				Category:       CategoryStorage,
				Title:          "Storage Exhaustion Warning",
				Description:    fmt.Sprintf("Storage projected to be exhausted in %v. Consider increasing storage limit or adjusting retention.", forecast.TimeToExhaustion),
				Metric:         "storage",
				CurrentValue:   float64(current.StorageBytes),
				SuggestedValue: float64(current.StorageBytes) * (1 + e.config.SafetyMargin),
				Impact:         "Prevents data loss and write failures",
				Effort:         "low",
				CreatedAt:      time.Now(),
			}
			recommendations = append(recommendations, rec)
		}
	}

	// Retention policy recommendations
	if current.StorageLimit > 0 {
		utilization := float64(current.StorageBytes) / float64(current.StorageLimit)
		if utilization > 0.8 {
			genID2, _ := generateID()
			rec := &CapacityRecommendation{
				ID:             genID2,
				Priority:       PriorityMedium,
				Category:       CategoryRetention,
				Title:          "Adjust Retention Policy",
				Description:    fmt.Sprintf("Storage utilization is %.1f%%. Consider reducing retention period or enabling more aggressive downsampling.", utilization*100),
				Metric:         "storage_utilization",
				CurrentValue:   utilization,
				SuggestedValue: 0.7,
				Impact:         "Reduces storage usage while maintaining data availability",
				Effort:         "medium",
				AutoApply:      e.config.AutoTuneEnabled,
				CreatedAt:      time.Now(),
			}
			recommendations = append(recommendations, rec)
		}
	}

	// Series cardinality recommendations
	if current.SeriesLimit > 0 {
		utilization := float64(current.SeriesCount) / float64(current.SeriesLimit)
		if utilization > 0.7 {
			genID3, _ := generateID()
			rec := &CapacityRecommendation{
				ID:             genID3,
				Priority:       PriorityMedium,
				Category:       CategoryQuery,
				Title:          "High Series Cardinality",
				Description:    fmt.Sprintf("Series count is %.1f%% of limit. Consider reviewing tag usage or increasing cardinality limits.", utilization*100),
				Metric:         "series_utilization",
				CurrentValue:   float64(current.SeriesCount),
				SuggestedValue: float64(current.SeriesLimit) * 0.5,
				Impact:         "Improves query performance and reduces memory usage",
				Effort:         "high",
				CreatedAt:      time.Now(),
			}
			recommendations = append(recommendations, rec)
		}
	}

	// Partition recommendations
	avgPointsPerPartition := float64(current.PointsCount) / float64(max(current.PartitionCount, 1))
	if avgPointsPerPartition > 10000000 { // 10M points per partition
		genID4, _ := generateID()
		rec := &CapacityRecommendation{
			ID:             genID4,
			Priority:       PriorityLow,
			Category:       CategoryPartition,
			Title:          "Consider Partition Splitting",
			Description:    fmt.Sprintf("Average %.0f points per partition. Consider reducing partition time window for better query performance.", avgPointsPerPartition),
			Metric:         "points_per_partition",
			CurrentValue:   avgPointsPerPartition,
			SuggestedValue: 5000000,
			Impact:         "Improves query latency for time-bounded queries",
			Effort:         "medium",
			AutoApply:      e.config.AutoTuneEnabled,
			CreatedAt:      time.Now(),
		}
		recommendations = append(recommendations, rec)
	}

	// Memory recommendations
	if current.MemoryLimit > 0 {
		utilization := float64(current.MemoryBytes) / float64(current.MemoryLimit)
		if utilization > 0.85 {
			genID5, _ := generateID()
			rec := &CapacityRecommendation{
				ID:             genID5,
				Priority:       PriorityCritical,
				Category:       CategoryMemory,
				Title:          "Memory Pressure Warning",
				Description:    fmt.Sprintf("Memory utilization is %.1f%%. Risk of OOM. Increase memory limit or reduce cache sizes.", utilization*100),
				Metric:         "memory_utilization",
				CurrentValue:   float64(current.MemoryBytes),
				SuggestedValue: float64(current.MemoryLimit) * 0.7,
				Impact:         "Prevents OOM crashes and performance degradation",
				Effort:         "low",
				CreatedAt:      time.Now(),
			}
			recommendations = append(recommendations, rec)
		}
	}

	// Downsampling recommendations based on query patterns
	if forecast, ok := forecasts["query_rate"]; ok && forecast != nil && forecast.Trend == TrendUp {
		genID6, _ := generateID()
		rec := &CapacityRecommendation{
			ID:             genID6,
			Priority:       PriorityLow,
			Category:       CategoryDownsample,
			Title:          "Enable Additional Downsampling",
			Description:    "Query rate is increasing. Consider enabling additional downsampling tiers for faster dashboard queries.",
			Metric:         "query_rate",
			CurrentValue:   current.QueryRate,
			SuggestedValue: current.QueryRate * 0.8,
			Impact:         "Reduces query latency and database load",
			Effort:         "medium",
			AutoApply:      e.config.AutoTuneEnabled,
			CreatedAt:      time.Now(),
		}
		recommendations = append(recommendations, rec)
	}

	// Sort by priority
	sort.Slice(recommendations, func(i, j int) bool {
		return priorityWeight(recommendations[i].Priority) > priorityWeight(recommendations[j].Priority)
	})

	// Store recommendations
	e.recommendationsMu.Lock()
	e.recommendations = recommendations
	e.recommendationsMu.Unlock()

	atomic.AddInt64(&e.recommendationsGenerated, 1)

	// Trigger callbacks for new recommendations
	if e.onRecommendation != nil {
		for _, rec := range recommendations {
			e.onRecommendation(rec)
		}
	}

	// Auto-apply if enabled
	if e.config.AutoTuneEnabled {
		for _, rec := range recommendations {
			if rec.AutoApply {
				e.applyRecommendation(rec)
			}
		}
	}

	return recommendations, nil
}

func priorityWeight(p RecommendationPriority) int {
	switch p {
	case PriorityCritical:
		return 4
	case PriorityHigh:
		return 3
	case PriorityMedium:
		return 2
	case PriorityLow:
		return 1
	default:
		return 0
	}
}

func (e *CapacityPlanningEngine) applyRecommendation(rec *CapacityRecommendation) error {
	// Implementation would apply the recommendation to the database configuration
	// This is a placeholder that logs the action
	now := time.Now()
	rec.AppliedAt = &now
	atomic.AddInt64(&e.autoTunesApplied, 1)
	return nil
}

func (e *CapacityPlanningEngine) checkAlerts(usage *ResourceUsage) {
	e.alertsMu.Lock()
	defer e.alertsMu.Unlock()

	// Check storage alert
	if usage.StorageLimit > 0 {
		utilization := float64(usage.StorageBytes) / float64(usage.StorageLimit)
		alertID := "storage_threshold"
		if utilization > e.config.AlertThreshold {
			if _, exists := e.alerts[alertID]; !exists {
				alert := &CapacityAlert{
					ID:        alertID,
					Severity:  "warning",
					Metric:    "storage_utilization",
					Message:   fmt.Sprintf("Storage utilization %.1f%% exceeds threshold %.1f%%", utilization*100, e.config.AlertThreshold*100),
					Value:     utilization,
					Threshold: e.config.AlertThreshold,
					Triggered: time.Now(),
				}
				e.alerts[alertID] = alert
				if e.onAlert != nil {
					e.onAlert(alert)
				}
			}
		} else if alert, exists := e.alerts[alertID]; exists && alert.Resolved == nil {
			now := time.Now()
			alert.Resolved = &now
		}
	}

	// Check memory alert
	if usage.MemoryLimit > 0 {
		utilization := float64(usage.MemoryBytes) / float64(usage.MemoryLimit)
		alertID := "memory_threshold"
		if utilization > e.config.AlertThreshold {
			if _, exists := e.alerts[alertID]; !exists {
				alert := &CapacityAlert{
					ID:        alertID,
					Severity:  "critical",
					Metric:    "memory_utilization",
					Message:   fmt.Sprintf("Memory utilization %.1f%% exceeds threshold %.1f%%", utilization*100, e.config.AlertThreshold*100),
					Value:     utilization,
					Threshold: e.config.AlertThreshold,
					Triggered: time.Now(),
				}
				e.alerts[alertID] = alert
				if e.onAlert != nil {
					e.onAlert(alert)
				}
			}
		} else if alert, exists := e.alerts[alertID]; exists && alert.Resolved == nil {
			now := time.Now()
			alert.Resolved = &now
		}
	}

	// Check series limit alert
	if usage.SeriesLimit > 0 {
		utilization := float64(usage.SeriesCount) / float64(usage.SeriesLimit)
		alertID := "series_threshold"
		if utilization > e.config.AlertThreshold {
			if _, exists := e.alerts[alertID]; !exists {
				alert := &CapacityAlert{
					ID:        alertID,
					Severity:  "warning",
					Metric:    "series_utilization",
					Message:   fmt.Sprintf("Series count %.1f%% of limit", utilization*100),
					Value:     utilization,
					Threshold: e.config.AlertThreshold,
					Triggered: time.Now(),
				}
				e.alerts[alertID] = alert
				if e.onAlert != nil {
					e.onAlert(alert)
				}
			}
		} else if alert, exists := e.alerts[alertID]; exists && alert.Resolved == nil {
			now := time.Now()
			alert.Resolved = &now
		}
	}
}

func (e *CapacityPlanningEngine) metricsCollectionLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.MetricsCollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.CollectMetrics()
		}
	}
}

func (e *CapacityPlanningEngine) recommendationLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.RecommendationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.GenerateForecasts()
			e.GenerateRecommendations()
		}
	}
}

// GetCurrentUsage returns current resource usage.
func (e *CapacityPlanningEngine) GetCurrentUsage() (*ResourceUsage, error) {
	e.historyMu.RLock()
	defer e.historyMu.RUnlock()

	if len(e.usageHistory) == 0 {
		return nil, fmt.Errorf("no usage data collected")
	}

	latest := e.usageHistory[len(e.usageHistory)-1]
	return &latest, nil
}

// GetUsageHistory returns usage history.
func (e *CapacityPlanningEngine) GetUsageHistory(since time.Time) []ResourceUsage {
	e.historyMu.RLock()
	defer e.historyMu.RUnlock()

	result := make([]ResourceUsage, 0)
	for _, u := range e.usageHistory {
		if u.Timestamp.After(since) {
			result = append(result, u)
		}
	}
	return result
}

// GetForecasts returns current forecasts.
func (e *CapacityPlanningEngine) GetForecasts() map[string]*CapacityForecast {
	e.forecastMu.RLock()
	defer e.forecastMu.RUnlock()

	result := make(map[string]*CapacityForecast)
	for k, v := range e.forecasts {
		result[k] = v
	}
	return result
}

// GetRecommendations returns current recommendations.
func (e *CapacityPlanningEngine) GetRecommendations() []*CapacityRecommendation {
	e.recommendationsMu.RLock()
	defer e.recommendationsMu.RUnlock()

	result := make([]*CapacityRecommendation, len(e.recommendations))
	copy(result, e.recommendations)
	return result
}

// GetAlerts returns active alerts.
func (e *CapacityPlanningEngine) GetAlerts(includeResolved bool) []*CapacityAlert {
	e.alertsMu.RLock()
	defer e.alertsMu.RUnlock()

	result := make([]*CapacityAlert, 0)
	for _, a := range e.alerts {
		if includeResolved || a.Resolved == nil {
			result = append(result, a)
		}
	}
	return result
}

// AcknowledgeAlert acknowledges an alert.
func (e *CapacityPlanningEngine) AcknowledgeAlert(alertID string) error {
	e.alertsMu.Lock()
	defer e.alertsMu.Unlock()

	alert, ok := e.alerts[alertID]
	if !ok {
		return fmt.Errorf("alert not found: %s", alertID)
	}

	alert.Acknowledged = true
	return nil
}

// ApplyRecommendation applies a recommendation.
func (e *CapacityPlanningEngine) ApplyRecommendation(recID string) error {
	e.recommendationsMu.Lock()
	defer e.recommendationsMu.Unlock()

	for _, rec := range e.recommendations {
		if rec.ID == recID {
			return e.applyRecommendation(rec)
		}
	}

	return fmt.Errorf("recommendation not found: %s", recID)
}

// DismissRecommendation dismisses a recommendation.
func (e *CapacityPlanningEngine) DismissRecommendation(recID string) error {
	e.recommendationsMu.Lock()
	defer e.recommendationsMu.Unlock()

	for i, rec := range e.recommendations {
		if rec.ID == recID {
			e.recommendations = append(e.recommendations[:i], e.recommendations[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("recommendation not found: %s", recID)
}

// OnAlert sets the alert callback.
func (e *CapacityPlanningEngine) OnAlert(callback func(*CapacityAlert)) {
	e.onAlert = callback
}

// OnRecommendation sets the recommendation callback.
func (e *CapacityPlanningEngine) OnRecommendation(callback func(*CapacityRecommendation)) {
	e.onRecommendation = callback
}

// Stats returns engine statistics.
func (e *CapacityPlanningEngine) Stats() CapacityPlanningStats {
	e.alertsMu.RLock()
	activeAlerts := 0
	for _, a := range e.alerts {
		if a.Resolved == nil {
			activeAlerts++
		}
	}
	e.alertsMu.RUnlock()

	e.recommendationsMu.RLock()
	recCount := len(e.recommendations)
	e.recommendationsMu.RUnlock()

	return CapacityPlanningStats{
		CollectionsRun:           atomic.LoadInt64(&e.collectionsRun),
		ForecastsGenerated:       atomic.LoadInt64(&e.forecastsGenerated),
		RecommendationsGenerated: atomic.LoadInt64(&e.recommendationsGenerated),
		AutoTunesApplied:         atomic.LoadInt64(&e.autoTunesApplied),
		ActiveAlerts:             activeAlerts,
		PendingRecommendations:   recCount,
	}
}

// CapacityPlanningStats contains engine statistics.
type CapacityPlanningStats struct {
	CollectionsRun           int64 `json:"collections_run"`
	ForecastsGenerated       int64 `json:"forecasts_generated"`
	RecommendationsGenerated int64 `json:"recommendations_generated"`
	AutoTunesApplied         int64 `json:"auto_tunes_applied"`
	ActiveAlerts             int   `json:"active_alerts"`
	PendingRecommendations   int   `json:"pending_recommendations"`
}

// Close shuts down the capacity planning engine.
func (e *CapacityPlanningEngine) Close() error {
	e.cancel()
	e.wg.Wait()
	return nil
}

// ExportReport exports a capacity planning report.
func (e *CapacityPlanningEngine) ExportReport() ([]byte, error) {
	report := struct {
		GeneratedAt     time.Time                    `json:"generated_at"`
		CurrentUsage    *ResourceUsage               `json:"current_usage,omitempty"`
		Forecasts       map[string]*CapacityForecast `json:"forecasts"`
		Recommendations []*CapacityRecommendation    `json:"recommendations"`
		ActiveAlerts    []*CapacityAlert             `json:"active_alerts"`
		Stats           CapacityPlanningStats        `json:"stats"`
	}{
		GeneratedAt:     time.Now(),
		Forecasts:       e.GetForecasts(),
		Recommendations: e.GetRecommendations(),
		ActiveAlerts:    e.GetAlerts(false),
		Stats:           e.Stats(),
	}

	if usage, err := e.GetCurrentUsage(); err == nil {
		report.CurrentUsage = usage
	}

	return json.MarshalIndent(report, "", "  ")
}
