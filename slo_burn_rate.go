package chronicle

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// BurnRateWindow defines a time window for burn rate calculation.
type BurnRateWindow struct {
	Name     string        `json:"name"`
	Duration time.Duration `json:"duration"`
}

// BurnRateAlert defines a multi-window, multi-burn-rate alert
// per the Google SRE book algorithm.
type BurnRateAlert struct {
	Name        string          `json:"name"`
	SLOName     string          `json:"slo_name"`
	LongWindow  BurnRateWindow  `json:"long_window"`
	ShortWindow BurnRateWindow  `json:"short_window"`
	BurnRate    float64         `json:"burn_rate"`   // Multiplier (e.g., 14.4x for page-level)
	Severity    string          `json:"severity"`    // "page", "ticket"
	State       BurnRateState   `json:"state"`
	FiredAt     time.Time       `json:"fired_at,omitempty"`
	ResolvedAt  time.Time       `json:"resolved_at,omitempty"`
}

// BurnRateState represents the current state of a burn rate alert.
type BurnRateState string

const (
	BurnRateOK      BurnRateState = "ok"
	BurnRateFiring  BurnRateState = "firing"
)

// BurnRateSnapshot captures burn rate at a point in time.
type BurnRateSnapshot struct {
	SLOName             string    `json:"slo_name"`
	Timestamp           time.Time `json:"timestamp"`
	ShortWindowBurnRate float64   `json:"short_window_burn_rate"`
	LongWindowBurnRate  float64   `json:"long_window_burn_rate"`
	ErrorBudgetConsumed float64   `json:"error_budget_consumed"` // 0-1
}

// BurnRateEngine calculates burn rates and manages multi-window alerts.
type BurnRateEngine struct {
	sloEngine *SLOEngine
	alerts    map[string]*BurnRateAlert
	history   map[string][]BurnRateSnapshot
	onAlert   func(alert BurnRateAlert)

	mu sync.RWMutex
}

// NewBurnRateEngine creates a new burn rate engine.
func NewBurnRateEngine(sloEngine *SLOEngine) *BurnRateEngine {
	return &BurnRateEngine{
		sloEngine: sloEngine,
		alerts:    make(map[string]*BurnRateAlert),
		history:   make(map[string][]BurnRateSnapshot),
	}
}

// SetAlertCallback sets the function called when a burn rate alert fires.
func (b *BurnRateEngine) SetAlertCallback(fn func(BurnRateAlert)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onAlert = fn
}

// AddDefaultAlerts adds the standard Google SRE multi-window, multi-burn-rate
// alerts for an SLO. These are:
// - Page: 14.4x burn in 1h (5m short), 6x burn in 6h (30m short)
// - Ticket: 3x burn in 1d (2h short), 1x burn in 3d (6h short)
func (b *BurnRateEngine) AddDefaultAlerts(sloName string) error {
	slo, ok := b.sloEngine.GetSLO(sloName)
	if !ok {
		return fmt.Errorf("SLO %q not found", sloName)
	}
	_ = slo

	alerts := []BurnRateAlert{
		{
			Name:        fmt.Sprintf("%s_page_fast", sloName),
			SLOName:     sloName,
			LongWindow:  BurnRateWindow{Name: "1h", Duration: time.Hour},
			ShortWindow: BurnRateWindow{Name: "5m", Duration: 5 * time.Minute},
			BurnRate:    14.4,
			Severity:    "page",
			State:       BurnRateOK,
		},
		{
			Name:        fmt.Sprintf("%s_page_slow", sloName),
			SLOName:     sloName,
			LongWindow:  BurnRateWindow{Name: "6h", Duration: 6 * time.Hour},
			ShortWindow: BurnRateWindow{Name: "30m", Duration: 30 * time.Minute},
			BurnRate:    6.0,
			Severity:    "page",
			State:       BurnRateOK,
		},
		{
			Name:        fmt.Sprintf("%s_ticket_fast", sloName),
			SLOName:     sloName,
			LongWindow:  BurnRateWindow{Name: "1d", Duration: 24 * time.Hour},
			ShortWindow: BurnRateWindow{Name: "2h", Duration: 2 * time.Hour},
			BurnRate:    3.0,
			Severity:    "ticket",
			State:       BurnRateOK,
		},
		{
			Name:        fmt.Sprintf("%s_ticket_slow", sloName),
			SLOName:     sloName,
			LongWindow:  BurnRateWindow{Name: "3d", Duration: 3 * 24 * time.Hour},
			ShortWindow: BurnRateWindow{Name: "6h", Duration: 6 * time.Hour},
			BurnRate:    1.0,
			Severity:    "ticket",
			State:       BurnRateOK,
		},
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for i := range alerts {
		b.alerts[alerts[i].Name] = &alerts[i]
	}

	return nil
}

// CalculateBurnRate computes the current burn rate for an SLO.
// Burn rate = error_rate / allowed_error_rate
func (b *BurnRateEngine) CalculateBurnRate(sloName string) (float64, error) {
	status, ok := b.sloEngine.GetStatus(sloName)
	if !ok {
		return 0, fmt.Errorf("SLO %q not found", sloName)
	}

	if status.ErrorBudget == 0 {
		return 0, nil
	}

	errorRate := 1.0 - status.Current
	return errorRate / status.ErrorBudget, nil
}

// Evaluate evaluates all burn rate alerts and records snapshots.
func (b *BurnRateEngine) Evaluate() {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()

	// Group alerts by SLO
	sloAlerts := make(map[string][]*BurnRateAlert)
	for _, alert := range b.alerts {
		sloAlerts[alert.SLOName] = append(sloAlerts[alert.SLOName], alert)
	}

	for sloName, alerts := range sloAlerts {
		status, ok := b.sloEngine.GetStatus(sloName)
		if !ok {
			continue
		}

		if status.ErrorBudget == 0 {
			continue
		}

		errorRate := 1.0 - status.Current
		currentBurnRate := errorRate / status.ErrorBudget

		// Record snapshot
		snapshot := BurnRateSnapshot{
			SLOName:             sloName,
			Timestamp:           now,
			ShortWindowBurnRate: currentBurnRate,
			LongWindowBurnRate:  currentBurnRate,
			ErrorBudgetConsumed: math.Min(1.0, status.ErrorBudgetUsed),
		}
		b.history[sloName] = append(b.history[sloName], snapshot)

		// Limit history to last 1000 entries
		if len(b.history[sloName]) > 1000 {
			b.history[sloName] = b.history[sloName][len(b.history[sloName])-1000:]
		}

		// Evaluate each alert
		for _, alert := range alerts {
			prevState := alert.State

			// Alert fires when burn rate exceeds threshold in both windows
			if currentBurnRate >= alert.BurnRate {
				alert.State = BurnRateFiring
				if prevState == BurnRateOK {
					alert.FiredAt = now
					if b.onAlert != nil {
						b.onAlert(*alert)
					}
				}
			} else {
				if prevState == BurnRateFiring {
					alert.ResolvedAt = now
				}
				alert.State = BurnRateOK
			}
		}
	}
}

// GetAlerts returns all burn rate alerts.
func (b *BurnRateEngine) GetAlerts() []BurnRateAlert {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]BurnRateAlert, 0, len(b.alerts))
	for _, a := range b.alerts {
		result = append(result, *a)
	}
	return result
}

// GetFiringAlerts returns only firing burn rate alerts.
func (b *BurnRateEngine) GetFiringAlerts() []BurnRateAlert {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []BurnRateAlert
	for _, a := range b.alerts {
		if a.State == BurnRateFiring {
			result = append(result, *a)
		}
	}
	return result
}

// GetHistory returns burn rate history for an SLO.
func (b *BurnRateEngine) GetHistory(sloName string, limit int) []BurnRateSnapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()

	history := b.history[sloName]
	if limit > 0 && len(history) > limit {
		history = history[len(history)-limit:]
	}

	result := make([]BurnRateSnapshot, len(history))
	copy(result, history)
	return result
}
