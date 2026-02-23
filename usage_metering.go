package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// UsageMeteringConfig configures the usage metering system.
type UsageMeteringConfig struct {
	Enabled           bool          `json:"enabled"`
	CollectionInterval time.Duration `json:"collection_interval"`
	RetentionDays     int           `json:"retention_days"`
	BillingWebhookURL string        `json:"billing_webhook_url,omitempty"`
}

// DefaultUsageMeteringConfig returns sensible defaults.
func DefaultUsageMeteringConfig() UsageMeteringConfig {
	return UsageMeteringConfig{
		Enabled:            true,
		CollectionInterval: time.Minute,
		RetentionDays:      90,
	}
}

// TenantUsageRecord captures per-tenant usage for a time period.
type TenantUsageRecord struct {
	TenantID     string    `json:"tenant_id"`
	PeriodStart  time.Time `json:"period_start"`
	PeriodEnd    time.Time `json:"period_end"`
	Writes       int64     `json:"writes"`
	Queries      int64     `json:"queries"`
	StorageBytes int64     `json:"storage_bytes"`
	MetricsCount int       `json:"metrics_count"`
	SeriesCount  int64     `json:"series_count"`
}

// TenantUsageSummary provides aggregated usage across periods.
type TenantUsageSummary struct {
	TenantID       string         `json:"tenant_id"`
	TotalWrites    int64          `json:"total_writes"`
	TotalQueries   int64          `json:"total_queries"`
	AvgStorage     int64          `json:"avg_storage_bytes"`
	PeakStorage    int64          `json:"peak_storage_bytes"`
	PeakWrites     int64          `json:"peak_writes"`
	PeakQueries    int64          `json:"peak_queries"`
	HourlyRecords  []TenantUsageRecord  `json:"hourly_records,omitempty"`
}

// TenantBillingEvent is sent to billing webhooks.
type TenantBillingEvent struct {
	EventType   string       `json:"event_type"` // "usage_report", "quota_exceeded", "overage"
	TenantID    string       `json:"tenant_id"`
	TenantName  string       `json:"tenant_name"`
	Timestamp   time.Time    `json:"timestamp"`
	Usage       TenantUsageRecord  `json:"usage"`
	QuotaUsed   float64      `json:"quota_used_pct"` // 0-100
}

// UsageMeteringEngine tracks per-tenant resource consumption
// with hourly aggregation and webhook-based billing integration.
type UsageMeteringEngine struct {
	config      UsageMeteringConfig
	controlPlane *SaaSControlPlane

	records     map[string][]TenantUsageRecord // tenantID -> hourly records
	current     map[string]*TenantUsageRecord  // tenantID -> current period
	httpClient  HTTPDoer

	running bool
	stopCh  chan struct{}
	mu      sync.RWMutex
}

// NewUsageMeteringEngine creates a new usage metering engine.
func NewUsageMeteringEngine(cp *SaaSControlPlane, config UsageMeteringConfig) *UsageMeteringEngine {
	return &UsageMeteringEngine{
		config:       config,
		controlPlane: cp,
		records:      make(map[string][]TenantUsageRecord),
		current:      make(map[string]*TenantUsageRecord),
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		stopCh:       make(chan struct{}),
	}
}

// RecordWrite records a write event for a tenant.
func (e *UsageMeteringEngine) RecordWrite(tenantID string, pointCount int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	record := e.getOrCreateCurrent(tenantID)
	record.Writes += pointCount
}

// RecordQuery records a query event for a tenant.
func (e *UsageMeteringEngine) RecordQuery(tenantID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	record := e.getOrCreateCurrent(tenantID)
	record.Queries++
}

// UpdateStorage updates the storage usage for a tenant.
func (e *UsageMeteringEngine) UpdateStorage(tenantID string, storageBytes int64, metricsCount int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	record := e.getOrCreateCurrent(tenantID)
	record.StorageBytes = storageBytes
	record.MetricsCount = metricsCount
}

func (e *UsageMeteringEngine) getOrCreateCurrent(tenantID string) *TenantUsageRecord {
	record, exists := e.current[tenantID]
	if !exists {
		now := time.Now()
		record = &TenantUsageRecord{
			TenantID:    tenantID,
			PeriodStart: now.Truncate(time.Hour),
			PeriodEnd:   now.Truncate(time.Hour).Add(time.Hour),
		}
		e.current[tenantID] = record
	}

	// Roll over to new period if needed
	now := time.Now()
	if now.After(record.PeriodEnd) {
		// Archive current record
		e.records[tenantID] = append(e.records[tenantID], *record)

		// Start new period
		record = &TenantUsageRecord{
			TenantID:    tenantID,
			PeriodStart: now.Truncate(time.Hour),
			PeriodEnd:   now.Truncate(time.Hour).Add(time.Hour),
		}
		e.current[tenantID] = record
	}

	return record
}

// GetTenantUsageSummary returns aggregated usage for a tenant.
func (e *UsageMeteringEngine) GetTenantUsageSummary(tenantID string, hours int) TenantUsageSummary {
	e.mu.RLock()
	defer e.mu.RUnlock()

	summary := TenantUsageSummary{
		TenantID: tenantID,
	}

	records := e.records[tenantID]
	cutoff := time.Now().Add(-time.Duration(hours) * time.Hour)

	for _, r := range records {
		if r.PeriodStart.Before(cutoff) {
			continue
		}

		summary.TotalWrites += r.Writes
		summary.TotalQueries += r.Queries

		if r.StorageBytes > summary.PeakStorage {
			summary.PeakStorage = r.StorageBytes
		}
		if r.Writes > summary.PeakWrites {
			summary.PeakWrites = r.Writes
		}
		if r.Queries > summary.PeakQueries {
			summary.PeakQueries = r.Queries
		}

		summary.AvgStorage += r.StorageBytes
		summary.HourlyRecords = append(summary.HourlyRecords, r)
	}

	// Include current period
	if current, exists := e.current[tenantID]; exists {
		summary.TotalWrites += current.Writes
		summary.TotalQueries += current.Queries
		if current.StorageBytes > summary.PeakStorage {
			summary.PeakStorage = current.StorageBytes
		}
	}

	if len(summary.HourlyRecords) > 0 {
		summary.AvgStorage /= int64(len(summary.HourlyRecords))
	}

	return summary
}

// GetAllUsage returns usage summaries for all tenants.
func (e *UsageMeteringEngine) GetAllUsage(hours int) []TenantUsageSummary {
	tenants := e.controlPlane.ListTenants()
	var summaries []TenantUsageSummary

	for _, t := range tenants {
		summaries = append(summaries, e.GetTenantUsageSummary(t.ID, hours))
	}
	return summaries
}

// FlushPeriod flushes all current period records and sends billing webhooks.
func (e *UsageMeteringEngine) FlushPeriod() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for tenantID, record := range e.current {
		e.records[tenantID] = append(e.records[tenantID], *record)

		// Send billing webhook if configured
		if e.config.BillingWebhookURL != "" {
			go e.sendBillingWebhook(tenantID, *record)
		}
	}

	// Reset current records
	e.current = make(map[string]*TenantUsageRecord)

	// Prune old records
	e.pruneOldRecords()
}

func (e *UsageMeteringEngine) pruneOldRecords() {
	cutoff := time.Now().AddDate(0, 0, -e.config.RetentionDays)
	for tenantID, records := range e.records {
		var kept []TenantUsageRecord
		for _, r := range records {
			if r.PeriodStart.After(cutoff) {
				kept = append(kept, r)
			}
		}
		e.records[tenantID] = kept
	}
}

func (e *UsageMeteringEngine) sendBillingWebhook(tenantID string, record TenantUsageRecord) {
	tenant, ok := e.controlPlane.GetTenant(tenantID)
	if !ok {
		return
	}

	quotaUsed := 0.0
	if tenant.Quota.MaxStorageBytes > 0 {
		quotaUsed = float64(record.StorageBytes) / float64(tenant.Quota.MaxStorageBytes) * 100
	}

	event := TenantBillingEvent{
		EventType:  "usage_report",
		TenantID:   tenantID,
		TenantName: tenant.Name,
		Timestamp:  time.Now(),
		Usage:      record,
		QuotaUsed:  quotaUsed,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.config.BillingWebhookURL, bytes.NewReader(payload))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return
	}
	defer closeQuietly(resp.Body)
}

// Start begins the metering collection loop.
func (e *UsageMeteringEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()

	go e.collectionLoop()
}

// Stop stops the metering engine.
func (e *UsageMeteringEngine) Stop() {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return
	}
	e.running = false
	e.mu.Unlock()

	close(e.stopCh)
}

func (e *UsageMeteringEngine) collectionLoop() {
	ticker := time.NewTicker(e.config.CollectionInterval)
	defer ticker.Stop()

	// Flush on the hour boundary
	hourly := time.NewTicker(time.Hour)
	defer hourly.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-hourly.C:
			e.FlushPeriod()
		case <-ticker.C:
			// Periodic collection (update storage stats, etc.)
		}
	}
}

// RegisterHTTPHandlers registers usage metering HTTP endpoints.
func (e *UsageMeteringEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		tenantID := r.URL.Query().Get("tenant_id")
		if tenantID != "" {
			summary := e.GetTenantUsageSummary(tenantID, 24)
			json.NewEncoder(w).Encode(summary)
		} else {
			json.NewEncoder(w).Encode(e.GetAllUsage(24))
		}
	})

	mux.HandleFunc("/api/v1/usage/flush", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		e.FlushPeriod()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "flushed"})
	})
}
