package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Admission control, usage tracking, and chargeback reporting for tenant governance.

// GetBudget retrieves the resource budget for a tenant.
func (e *TenantGovernanceEngine) GetBudget(tenantID string) (*ResourceBudget, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	budget, ok := e.budgets[tenantID]
	if !ok {
		return nil, fmt.Errorf("no budget found for tenant %s", tenantID)
	}
	cp := *budget
	return &cp, nil
}

// GetUsage returns the current resource usage snapshot for a tenant.
func (e *TenantGovernanceEngine) GetUsage(tenantID string) (*GovernanceResourceUsage, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	usage, ok := e.usageSnapshots[tenantID]
	if !ok {
		return nil, fmt.Errorf("no usage data for tenant %s", tenantID)
	}
	cp := *usage
	return &cp, nil
}

// UpdateUsage records a new resource usage snapshot for a tenant.
func (e *TenantGovernanceEngine) UpdateUsage(tenantID string, usage GovernanceResourceUsage) {
	usage.TenantID = tenantID
	usage.MeasuredAt = time.Now()
	e.mu.Lock()
	e.usageSnapshots[tenantID] = &usage
	e.mu.Unlock()
}

// AdmitQuery performs admission control for a query operation.
func (e *TenantGovernanceEngine) AdmitQuery(tenantID string, estimatedCost float64) (bool, string) {
	e.mu.RLock()
	budget, hasBudget := e.budgets[tenantID]
	e.mu.RUnlock()
	if !hasBudget {
		return false, fmt.Sprintf("no budget configured for tenant %s", tenantID)
	}

	// Check cumulative query cost budget
	totalCost, _ := e.costAccounter.GetUsage(tenantID)
	if totalCost+estimatedCost > budget.QueryCostBudget {
		return false, fmt.Sprintf("tenant %s would exceed query cost budget (used=%.2f, requested=%.2f, limit=%.2f)",
			tenantID, totalCost, estimatedCost, budget.QueryCostBudget)
	}

	// Token bucket rate limiting
	admitted, reason := e.throttler.Admit(tenantID, "qps", 1)
	if !admitted {
		return false, reason
	}

	// Check cost-based throttling
	admitted, reason = e.throttler.Admit(tenantID, "query_cost", estimatedCost)
	if !admitted {
		return false, reason
	}

	// Account the cost
	e.costAccounter.AccountQuery(tenantID, estimatedCost)
	return true, ""
}

// AdmitWrite performs admission control for write operations.
func (e *TenantGovernanceEngine) AdmitWrite(tenantID string, pointCount int) (bool, string) {
	e.mu.RLock()
	_, hasBudget := e.budgets[tenantID]
	e.mu.RUnlock()
	if !hasBudget {
		return false, fmt.Sprintf("no budget configured for tenant %s", tenantID)
	}

	admitted, reason := e.throttler.Admit(tenantID, "write", float64(pointCount))
	if !admitted {
		return false, reason
	}

	e.costAccounter.AccountWrite(tenantID, int64(pointCount), int64(pointCount*64))
	return true, ""
}

// GenerateChargebackReport generates a billing report for a tenant.
func (e *TenantGovernanceEngine) GenerateChargebackReport(tenantID string, start, end time.Time) (*ChargebackReport, error) {
	return e.generateReport(tenantID, start, end, true)
}

// GenerateShowbackReport generates a cost visibility report without actual billing.
func (e *TenantGovernanceEngine) GenerateShowbackReport(tenantID string, start, end time.Time) (*ChargebackReport, error) {
	return e.generateReport(tenantID, start, end, false)
}

func (e *TenantGovernanceEngine) generateReport(tenantID string, start, end time.Time, chargeback bool) (*ChargebackReport, error) {
	records := e.GetMeteringRecords(tenantID, start, end)

	var totalCPUSeconds, totalMemGBSec, totalStorageGBH, totalQueryCost float64
	var totalWriteCount int64
	for _, r := range records {
		totalCPUSeconds += r.CPUSeconds
		totalMemGBSec += r.MemoryGBSeconds
		totalStorageGBH += r.StorageGBHours
		totalQueryCost += r.QueryCostTotal
		totalWriteCount += r.WriteCount
	}

	cpuHours := totalCPUSeconds / 3600.0
	memGBHours := totalMemGBSec / 3600.0

	lineItems := []ChargebackLineItem{
		{
			Description: "CPU Usage",
			Quantity:    cpuHours,
			UnitCost:    e.config.CostPerCPUHour,
			TotalCost:   cpuHours * e.config.CostPerCPUHour,
		},
		{
			Description: "Memory Usage",
			Quantity:    memGBHours,
			UnitCost:    e.config.CostPerGBHour,
			TotalCost:   memGBHours * e.config.CostPerGBHour,
		},
		{
			Description: "Storage Usage",
			Quantity:    totalStorageGBH,
			UnitCost:    e.config.CostPerGBHour,
			TotalCost:   totalStorageGBH * e.config.CostPerGBHour,
		},
		{
			Description: "Query Cost",
			Quantity:    totalQueryCost,
			UnitCost:    e.config.CostPerQueryUnit,
			TotalCost:   totalQueryCost * e.config.CostPerQueryUnit,
		},
		{
			Description: "Write Operations",
			Quantity:    float64(totalWriteCount),
			UnitCost:    e.config.CostPerQueryUnit * 0.1,
			TotalCost:   float64(totalWriteCount) * e.config.CostPerQueryUnit * 0.1,
		},
	}

	var totalBill float64
	for i := range lineItems {
		totalBill += lineItems[i].TotalCost
	}

	currency := "USD"
	if !chargeback {
		currency = "SHOWBACK"
	}

	return &ChargebackReport{
		TenantID:           tenantID,
		BillingPeriodStart: start,
		BillingPeriodEnd:   end,
		LineItems:          lineItems,
		TotalCost:          totalBill,
		Currency:           currency,
	}, nil
}

// GetMeteringRecords returns metering records for a tenant within a time range.
func (e *TenantGovernanceEngine) GetMeteringRecords(tenantID string, start, end time.Time) []MeteringRecord {
	e.mu.RLock()
	defer e.mu.RUnlock()
	allRecords := e.meteringRecords[tenantID]
	var result []MeteringRecord
	for _, r := range allRecords {
		if (r.IntervalStart.Equal(start) || r.IntervalStart.After(start)) &&
			(r.IntervalEnd.Equal(end) || r.IntervalEnd.Before(end)) {
			result = append(result, r)
		}
	}
	return result
}

// GovernanceStatus summarises the engine's current state.
type GovernanceStatus struct {
	Running      bool     `json:"running"`
	TenantCount  int      `json:"tenant_count"`
	TenantIDs    []string `json:"tenant_ids"`
	ChargebackOn bool     `json:"chargeback_enabled"`
}

// Status returns the current engine status.
func (e *TenantGovernanceEngine) Status() GovernanceStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()
	ids := make([]string, 0, len(e.budgets))
	for id := range e.budgets {
		ids = append(ids, id)
	}
	return GovernanceStatus{
		Running:      e.running,
		TenantCount:  len(e.budgets),
		TenantIDs:    ids,
		ChargebackOn: e.config.EnableChargeback,
	}
}

// RegisterHTTPHandlers registers the governance HTTP routes.
func (e *TenantGovernanceEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/governance/budgets", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			e.handleCreateBudget(w, r)
		case http.MethodGet:
			e.handleListBudgets(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/governance/budgets/", func(w http.ResponseWriter, r *http.Request) {
		tenantID := strings.TrimPrefix(r.URL.Path, "/api/v1/governance/budgets/")
		if tenantID == "" {
			http.Error(w, "tenant ID required", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodGet:
			e.handleGetBudget(w, tenantID)
		case http.MethodPut:
			e.handleUpdateBudget(w, r, tenantID)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/governance/usage/", func(w http.ResponseWriter, r *http.Request) {
		tenantID := strings.TrimPrefix(r.URL.Path, "/api/v1/governance/usage/")
		if tenantID == "" {
			http.Error(w, "tenant ID required", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		usage, err := e.GetUsage(tenantID)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(usage)
	})

	mux.HandleFunc("/api/v1/governance/admit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		e.handleAdmit(w, r)
	})

	mux.HandleFunc("/api/v1/governance/chargeback/", func(w http.ResponseWriter, r *http.Request) {
		tenantID := strings.TrimPrefix(r.URL.Path, "/api/v1/governance/chargeback/")
		if tenantID == "" {
			http.Error(w, "tenant ID required", http.StatusBadRequest)
			return
		}
		e.handleChargebackReport(w, r, tenantID)
	})

	mux.HandleFunc("/api/v1/governance/metering/", func(w http.ResponseWriter, r *http.Request) {
		tenantID := strings.TrimPrefix(r.URL.Path, "/api/v1/governance/metering/")
		if tenantID == "" {
			http.Error(w, "tenant ID required", http.StatusBadRequest)
			return
		}
		e.handleMeteringRecords(w, r, tenantID)
	})

	mux.HandleFunc("/api/v1/governance/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Status())
	})
}

func (e *TenantGovernanceEngine) handleCreateBudget(w http.ResponseWriter, r *http.Request) {
	var budget ResourceBudget
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&budget); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if budget.TenantID == "" {
		http.Error(w, "tenant_id is required", http.StatusBadRequest)
		return
	}
	if err := e.CreateBudget(budget.TenantID, budget); err != nil {
		http.Error(w, "conflict", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(budget)
}

func (e *TenantGovernanceEngine) handleListBudgets(w http.ResponseWriter, _ *http.Request) {
	e.mu.RLock()
	budgets := make([]ResourceBudget, 0, len(e.budgets))
	for _, b := range e.budgets {
		budgets = append(budgets, *b)
	}
	e.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(budgets)
}

func (e *TenantGovernanceEngine) handleGetBudget(w http.ResponseWriter, tenantID string) {
	budget, err := e.GetBudget(tenantID)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(budget)
}

func (e *TenantGovernanceEngine) handleUpdateBudget(w http.ResponseWriter, r *http.Request, tenantID string) {
	var budget ResourceBudget
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&budget); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := e.UpdateBudget(tenantID, budget); err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(budget)
}

func (e *TenantGovernanceEngine) handleAdmit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TenantID      string  `json:"tenant_id"`
		OperationType string  `json:"operation_type"` // "query" or "write"
		EstimatedCost float64 `json:"estimated_cost"`
		PointCount    int     `json:"point_count"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	var admitted bool
	var reason string
	switch req.OperationType {
	case "query":
		admitted, reason = e.AdmitQuery(req.TenantID, req.EstimatedCost)
	case "write":
		admitted, reason = e.AdmitWrite(req.TenantID, req.PointCount)
	default:
		http.Error(w, "operation_type must be 'query' or 'write'", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if !admitted {
		w.WriteHeader(http.StatusTooManyRequests)
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"admitted": admitted,
		"reason":   reason,
	})
}

func (e *TenantGovernanceEngine) handleChargebackReport(w http.ResponseWriter, r *http.Request, tenantID string) {
	start, end, err := parseTimeRange(r)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	var report *ChargebackReport
	if e.config.EnableChargeback {
		report, err = e.GenerateChargebackReport(tenantID, start, end)
	} else {
		report, err = e.GenerateShowbackReport(tenantID, start, end)
	}
	if err != nil {
		internalError(w, err, "internal error")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(report)
}

func (e *TenantGovernanceEngine) handleMeteringRecords(w http.ResponseWriter, r *http.Request, tenantID string) {
	start, end, err := parseTimeRange(r)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	records := e.GetMeteringRecords(tenantID, start, end)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(records)
}

func parseTimeRange(r *http.Request) (time.Time, time.Time, error) {
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	var start, end time.Time
	var err error

	if startStr != "" {
		start, err = time.Parse(time.RFC3339, startStr)
		if err != nil {
			return start, end, fmt.Errorf("invalid start time: %w", err)
		}
	} else {
		start = time.Now().Add(-24 * time.Hour)
	}

	if endStr != "" {
		end, err = time.Parse(time.RFC3339, endStr)
		if err != nil {
			return start, end, fmt.Errorf("invalid end time: %w", err)
		}
	} else {
		end = time.Now()
	}

	return start, end, nil
}
