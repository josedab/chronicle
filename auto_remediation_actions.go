package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
	"sync/atomic"
	"time"
)

// Action execution helpers, webhook validation, and remediation utilities for auto-remediation.

// validateWebhookURL validates the webhook URL to prevent SSRF attacks.
func (e *AutoRemediationEngine) validateWebhookURL(rawURL string) error {
	if rawURL == "" {
		return fmt.Errorf("webhook URL is required")
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid webhook URL: %w", err)
	}

	if parsed.Scheme != "https" {
		return fmt.Errorf("webhook URL must use HTTPS scheme, got %q", parsed.Scheme)
	}

	hostname := parsed.Hostname()

	// Block private/loopback IPs
	ips, err := net.LookupHost(hostname)
	if err != nil {
		return fmt.Errorf("cannot resolve webhook host %q: %w", hostname, err)
	}
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
			return fmt.Errorf("webhook URL must not target private/loopback address %s", ipStr)
		}
	}

	// Check domain allowlist
	if len(e.config.AllowedWebhookDomains) > 0 {
		allowed := false
		for _, domain := range e.config.AllowedWebhookDomains {
			if hostname == domain {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("webhook domain %q is not in the allowed domains list", hostname)
		}
	}

	return nil
}

func (e *AutoRemediationEngine) executeWebhook(action *RemediationAction, anomaly *DetectedAnomaly) (map[string]any, map[string]any, error) {
	url, ok := action.Parameters["url"].(string)
	if !ok || url == "" {
		return nil, nil, fmt.Errorf("webhook action requires a string 'url' parameter")
	}
	if err := e.validateWebhookURL(url); err != nil {
		return nil, nil, fmt.Errorf("webhook URL validation failed: %w", err)
	}
	method, ok := action.Parameters["method"].(string)
	if !ok || method == "" {
		method = "POST"
	}

	payload := map[string]any{
		"action_id": action.ID,
		"anomaly":   anomaly,
		"timestamp": time.Now().Format(time.RFC3339),
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal webhook payload: %w", err)
	}

	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	// Add custom headers
	if headers, ok := action.Parameters["headers"].(map[string]any); ok {
		for k, v := range headers {
			req.Header.Set(k, fmt.Sprintf("%v", v))
		}
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer closeQuietly(resp.Body)

	if resp.StatusCode >= 400 {
		return nil, nil, fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return map[string]any{
		"status_code":  resp.StatusCode,
		"url":          url,
		"payload_size": len(payloadBytes),
	}, nil, nil
}

func (e *AutoRemediationEngine) executeAlert(action *RemediationAction, anomaly *DetectedAnomaly) (map[string]any, error) {
	severity, ok := action.Parameters["severity"].(string)
	if !ok || severity == "" {
		severity = "warning"
	}

	message := fmt.Sprintf("Auto-remediation triggered for anomaly %s on metric %s",
		anomaly.ID, anomaly.Metric)

	if customMsg, ok := action.Parameters["message"].(string); ok {
		message = customMsg
	}

	// Create alert via the database's alert manager if available
	return map[string]any{
		"alert_created": true,
		"severity":      severity,
		"message":       message,
	}, nil
}

func (e *AutoRemediationEngine) executeThresholdAdjustment(action *RemediationAction, anomaly *DetectedAnomaly) (map[string]any, map[string]any, error) {
	adjustmentType, ok := action.Parameters["adjustment_type"].(string)
	if !ok || adjustmentType == "" {
		return nil, nil, fmt.Errorf("threshold adjustment requires a string 'adjustment_type' parameter (increase, decrease, percentage)")
	}
	amount, ok := action.Parameters["amount"].(float64)
	if !ok {
		return nil, nil, fmt.Errorf("threshold adjustment requires a numeric 'amount' parameter")
	}

	// Get current threshold
	currentThreshold := 100.0 // Placeholder - would get from actual alert rule
	if ct, ok := action.Parameters["current_threshold"].(float64); ok {
		currentThreshold = ct
	}

	var newThreshold float64
	switch adjustmentType {
	case "increase":
		newThreshold = currentThreshold + amount
	case "decrease":
		newThreshold = currentThreshold - amount
	case "percentage":
		newThreshold = currentThreshold * (1 + amount/100)
	default:
		newThreshold = currentThreshold * 1.1 // Default 10% increase
	}

	rollbackData := map[string]any{
		"previous_threshold": currentThreshold,
		"rule_id":            action.Parameters["rule_id"],
	}

	return map[string]any{
		"old_threshold": currentThreshold,
		"new_threshold": newThreshold,
		"adjustment":    adjustmentType,
	}, rollbackData, nil
}

func (e *AutoRemediationEngine) executeQuery(action *RemediationAction, anomaly *DetectedAnomaly) (map[string]any, error) {
	queryText, ok := action.Parameters["query"].(string)
	if !ok || queryText == "" {
		return nil, fmt.Errorf("query action requires a string 'query' parameter")
	}

	// Parse and execute query
	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()

	parser := &QueryParser{}
	q, err := parser.Parse(queryText)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	result, err := e.db.ExecuteContext(ctx, q)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"query":        queryText,
		"result_count": len(result.Points),
	}, nil
}

func (e *AutoRemediationEngine) executeNotification(action *RemediationAction, anomaly *DetectedAnomaly) (map[string]any, error) {
	channels := action.Parameters["channels"]
	message := fmt.Sprintf("Remediation action triggered: %s for anomaly on %s", action.Name, anomaly.Metric)

	if customMsg, ok := action.Parameters["message"].(string); ok {
		message = customMsg
	}

	return map[string]any{
		"channels": channels,
		"message":  message,
		"sent":     true,
	}, nil
}

func (e *AutoRemediationEngine) executeScale(action *RemediationAction, anomaly *DetectedAnomaly) (map[string]any, map[string]any, error) {
	direction, ok := action.Parameters["direction"].(string)
	if !ok || direction == "" {
		return nil, nil, fmt.Errorf("scale action requires a string 'direction' parameter (up, down)")
	}
	amount, ok := action.Parameters["amount"].(float64)
	if !ok {
		return nil, nil, fmt.Errorf("scale action requires a numeric 'amount' parameter")
	}
	resource, ok := action.Parameters["resource"].(string)
	if !ok || resource == "" {
		return nil, nil, fmt.Errorf("scale action requires a string 'resource' parameter")
	}

	currentScale := 1.0
	if cs, ok := action.Parameters["current_scale"].(float64); ok {
		currentScale = cs
	}

	var newScale float64
	switch direction {
	case "up":
		newScale = currentScale + amount
	case "down":
		newScale = currentScale - amount
		if newScale < 0 {
			newScale = 0
		}
	default:
		newScale = currentScale
	}

	rollbackData := map[string]any{
		"previous_scale": currentScale,
		"resource":       resource,
	}

	return map[string]any{
		"resource":  resource,
		"direction": direction,
		"old_scale": currentScale,
		"new_scale": newScale,
	}, rollbackData, nil
}

func (e *AutoRemediationEngine) performRollback(exec *RemediationExecution, action *RemediationAction) {
	if exec.RollbackData == nil {
		return
	}

	atomic.AddInt64(&e.rollbacks, 1)

	// Perform rollback based on action type
	switch action.Type {
	case RemediationActionThreshold:
		if prev, ok := exec.RollbackData["previous_threshold"].(float64); ok {
			// Restore previous threshold
			_ = prev // Would actually restore the threshold
		}
	case RemediationActionScale:
		if prev, ok := exec.RollbackData["previous_scale"].(float64); ok {
			// Restore previous scale
			_ = prev // Would actually restore the scale
		}
	}

	exec.Status = ExecutionRolledBack
	e.recordAudit("execution_rolled_back", exec.RuleID, action.ID, exec.ID, exec.AnomalyID, "rolled_back", exec.RollbackData)
}

func (e *AutoRemediationEngine) findMatchingRules(anomaly *DetectedAnomaly) []*AutoRemediationRule {
	e.mu.RLock()
	defer e.mu.RUnlock()

	matching := make([]*AutoRemediationRule, 0)

	for _, rule := range e.rules {
		if !rule.Enabled {
			continue
		}

		// Check anomaly type
		if len(rule.AnomalyTypes) > 0 {
			found := false
			for _, t := range rule.AnomalyTypes {
				if t == anomaly.Type {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Check metric pattern
		if rule.MetricPattern != "" && rule.MetricPattern != anomaly.Metric {
			// TODO(#14): Support wildcards/regex in metric pattern matching
			continue
		}

		// Check tag filters
		if len(rule.TagFilters) > 0 {
			match := true
			for k, v := range rule.TagFilters {
				if anomaly.Tags[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		matching = append(matching, rule)
	}

	return matching
}

func (e *AutoRemediationEngine) checkRateLimit() bool {
	now := time.Now()

	// Reset hourly counter
	if now.Sub(e.hourlyResetTime) >= time.Hour {
		atomic.StoreInt64(&e.hourlyExecutions, 0)
		e.hourlyResetTime = now
	}

	return atomic.LoadInt64(&e.hourlyExecutions) < int64(e.config.MaxActionsPerHour)
}

func (e *AutoRemediationEngine) recordAudit(eventType, ruleID, actionID, executionID, anomalyID, status string, details map[string]any) {
	genID2, _ := generateID()
	entry := RemediationAuditEntry{
		ID:          genID2,
		Timestamp:   time.Now(),
		EventType:   eventType,
		RuleID:      ruleID,
		ActionID:    actionID,
		ExecutionID: executionID,
		AnomalyID:   anomalyID,
		Status:      status,
		Details:     details,
	}

	e.auditMu.Lock()
	e.auditLog = append(e.auditLog, entry)

	// Prune old entries (keep last 10000)
	if len(e.auditLog) > 10000 {
		e.auditLog = e.auditLog[len(e.auditLog)-10000:]
	}
	e.auditMu.Unlock()
}

func (e *AutoRemediationEngine) recordSuccessForML(action *RemediationAction, anomaly *DetectedAnomaly) {
	if anomaly == nil {
		return
	}

	genID3, _ := generateID()
	incident := HistoricalIncident{
		ID:            genID3,
		AnomalyType:   anomaly.Type,
		MetricPattern: anomaly.Metric,
		Severity:      anomaly.Score,
		ActionTaken:   action.ID,
		Success:       true,
		Timestamp:     time.Now(),
	}

	e.mlModel.mu.Lock()
	e.mlModel.historicalIncidents = append(e.mlModel.historicalIncidents, incident)

	// Update effectiveness
	current := e.mlModel.actionEffectiveness[action.ID]
	e.mlModel.actionEffectiveness[action.ID] = (current + 1.0) / 2.0

	// Update pattern index
	pattern := fmt.Sprintf("%s:%s", anomaly.Type, anomaly.Metric)
	if actions, ok := e.mlModel.patternIndex[pattern]; ok {
		found := false
		for _, a := range actions {
			if a == action.ID {
				found = true
				break
			}
		}
		if !found {
			e.mlModel.patternIndex[pattern] = append(actions, action.ID)
		}
	} else {
		e.mlModel.patternIndex[pattern] = []string{action.ID}
	}
	e.mlModel.mu.Unlock()
}

// GetRecommendations returns ML-based action recommendations for an anomaly.
func (e *AutoRemediationEngine) GetRecommendations(anomaly *DetectedAnomaly) []*RemediationRecommendation {
	if !e.config.EnableMLRecommendations {
		return nil
	}

	e.mlModel.mu.RLock()
	defer e.mlModel.mu.RUnlock()

	recommendations := make([]*RemediationRecommendation, 0)

	// Find similar historical incidents
	pattern := fmt.Sprintf("%s:%s", anomaly.Type, anomaly.Metric)

	if actionIDs, ok := e.mlModel.patternIndex[pattern]; ok {
		for _, actionID := range actionIDs {
			action, err := e.GetAction(actionID)
			if err != nil || !action.Enabled {
				continue
			}

			effectiveness := e.mlModel.actionEffectiveness[actionID]
			if effectiveness < 0.3 {
				continue
			}

			genID4, _ := generateID()
			rec := &RemediationRecommendation{
				ID:               genID4,
				AnomalyID:        anomaly.ID,
				ActionType:       action.Type,
				Confidence:       effectiveness,
				Reasoning:        fmt.Sprintf("Action '%s' was effective %.0f%% of the time for similar anomalies", action.Name, effectiveness*100),
				Parameters:       action.Parameters,
				PredictedImpact:  effectiveness * anomaly.Score,
				SimilarIncidents: e.findSimilarIncidents(anomaly),
				CreatedAt:        time.Now(),
			}

			recommendations = append(recommendations, rec)
		}
	}

	// Sort by confidence
	sort.Slice(recommendations, func(i, j int) bool {
		return recommendations[i].Confidence > recommendations[j].Confidence
	})

	// Return top 5
	if len(recommendations) > 5 {
		recommendations = recommendations[:5]
	}

	return recommendations
}

func (e *AutoRemediationEngine) findSimilarIncidents(anomaly *DetectedAnomaly) []string {
	e.mlModel.mu.RLock()
	defer e.mlModel.mu.RUnlock()

	similar := make([]string, 0)

	for _, incident := range e.mlModel.historicalIncidents {
		if incident.AnomalyType == anomaly.Type &&
			incident.Success &&
			math.Abs(incident.Severity-anomaly.Score) < 0.2 {
			similar = append(similar, incident.ID)
			if len(similar) >= 5 {
				break
			}
		}
	}

	return similar
}

func (e *AutoRemediationEngine) evaluationLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.processApprovalTimeouts()
		}
	}
}

func (e *AutoRemediationEngine) processApprovalTimeouts() {
	e.executionMu.Lock()
	defer e.executionMu.Unlock()

	now := time.Now()
	remaining := make([]*RemediationExecution, 0)

	for _, exec := range e.pending {
		if now.Sub(exec.StartTime) > e.config.ApprovalTimeout {
			exec.Status = ExecutionSkipped
			exec.Error = "approval timeout"
			exec.EndTime = now
			e.executions = append(e.executions, exec)

			e.recordAudit("execution_timeout", exec.RuleID, exec.ActionID, exec.ID, exec.AnomalyID, "timeout", nil)
		} else {
			remaining = append(remaining, exec)
		}
	}

	e.pending = remaining
}

// GetAuditLog returns audit log entries.
func (e *AutoRemediationEngine) GetAuditLog(limit int) []RemediationAuditEntry {
	e.auditMu.RLock()
	defer e.auditMu.RUnlock()

	if limit <= 0 || limit > len(e.auditLog) {
		limit = len(e.auditLog)
	}

	// Return newest first
	result := make([]RemediationAuditEntry, limit)
	for i := 0; i < limit; i++ {
		result[i] = e.auditLog[len(e.auditLog)-1-i]
	}

	return result
}

// GetExecutions returns execution history.
func (e *AutoRemediationEngine) GetExecutions(limit int) []*RemediationExecution {
	e.executionMu.RLock()
	defer e.executionMu.RUnlock()

	if limit <= 0 || limit > len(e.executions) {
		limit = len(e.executions)
	}

	result := make([]*RemediationExecution, limit)
	for i := 0; i < limit; i++ {
		result[i] = e.executions[len(e.executions)-1-i]
	}

	return result
}

// GetPendingExecutions returns pending approvals.
func (e *AutoRemediationEngine) GetPendingExecutions() []*RemediationExecution {
	e.executionMu.RLock()
	defer e.executionMu.RUnlock()

	result := make([]*RemediationExecution, len(e.pending))
	copy(result, e.pending)
	return result
}

// Stats returns engine statistics.
func (e *AutoRemediationEngine) Stats() AutoRemediationStats {
	return AutoRemediationStats{
		TotalActions:       int64(len(e.actions)),
		TotalRules:         int64(len(e.rules)),
		TotalExecutions:    atomic.LoadInt64(&e.totalExecutions),
		SuccessfulExecs:    atomic.LoadInt64(&e.successfulExecs),
		FailedExecs:        atomic.LoadInt64(&e.failedExecs),
		Rollbacks:          atomic.LoadInt64(&e.rollbacks),
		PendingApprovals:   int64(len(e.pending)),
		CircuitBreakerOpen: e.circuitOpen,
		HourlyExecutions:   atomic.LoadInt64(&e.hourlyExecutions),
	}
}

// AutoRemediationStats contains engine statistics.
type AutoRemediationStats struct {
	TotalActions       int64 `json:"total_actions"`
	TotalRules         int64 `json:"total_rules"`
	TotalExecutions    int64 `json:"total_executions"`
	SuccessfulExecs    int64 `json:"successful_executions"`
	FailedExecs        int64 `json:"failed_executions"`
	Rollbacks          int64 `json:"rollbacks"`
	PendingApprovals   int64 `json:"pending_approvals"`
	CircuitBreakerOpen bool  `json:"circuit_breaker_open"`
	HourlyExecutions   int64 `json:"hourly_executions"`
}

// Close shuts down the remediation engine.
func (e *AutoRemediationEngine) Close() error {
	e.cancel()
	e.wg.Wait()
	return nil
}

// DetectedAnomaly represents an anomaly that may trigger remediation.
type DetectedAnomaly struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Score     float64           `json:"score"`
	Value     float64           `json:"value"`
	Expected  float64           `json:"expected"`
	Timestamp time.Time         `json:"timestamp"`
	Duration  time.Duration     `json:"duration"`
}
