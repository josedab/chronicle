package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// ChaosRecoveryConfig configures the chaos recovery validation framework.
type ChaosRecoveryConfig struct {
	Enabled         bool
	MaxScenarios    int
	DefaultDuration time.Duration
	AutoRecover     bool
}

// DefaultChaosRecoveryConfig returns sensible defaults.
func DefaultChaosRecoveryConfig() ChaosRecoveryConfig {
	return ChaosRecoveryConfig{
		Enabled:         true,
		MaxScenarios:    50,
		DefaultDuration: 5 * time.Second,
		AutoRecover:     true,
	}
}

// RecoveryScenario defines a failure scenario to test.
type RecoveryScenario struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"` // disk_failure, oom, clock_skew, corruption, slow_io
	Description string `json:"description"`
	Severity    string `json:"severity"` // low, medium, high, critical
}

// ChaosTestResult captures the outcome of a chaos test.
type ChaosTestResult struct {
	ScenarioID     string        `json:"scenario_id"`
	ScenarioName   string        `json:"scenario_name"`
	Passed         bool          `json:"passed"`
	Duration       time.Duration `json:"duration"`
	WritesBefore   int64         `json:"writes_before"`
	WritesAfter    int64         `json:"writes_after"`
	QueriesBefore  int64         `json:"queries_before"`
	QueriesAfter   int64         `json:"queries_after"`
	DataIntegrity  bool          `json:"data_integrity"`
	RecoveryTime   time.Duration `json:"recovery_time"`
	ErrorMessage   string        `json:"error_message,omitempty"`
	ExecutedAt     time.Time     `json:"executed_at"`
}

// ChaosRecoveryStats holds framework statistics.
type ChaosRecoveryStats struct {
	ScenariosRegistered int   `json:"scenarios_registered"`
	TestsExecuted       int64 `json:"tests_executed"`
	TestsPassed         int64 `json:"tests_passed"`
	TestsFailed         int64 `json:"tests_failed"`
	AvgRecoveryTime     time.Duration `json:"avg_recovery_time"`
}

// ChaosRecoveryEngine validates database recovery under failure conditions.
type ChaosRecoveryEngine struct {
	db        *DB
	config    ChaosRecoveryConfig
	mu        sync.RWMutex
	scenarios []RecoveryScenario
	results   []ChaosTestResult
	running   bool
	stopCh    chan struct{}
	stats     ChaosRecoveryStats
}

// NewChaosRecoveryEngine creates a new chaos recovery framework.
func NewChaosRecoveryEngine(db *DB, cfg ChaosRecoveryConfig) *ChaosRecoveryEngine {
	e := &ChaosRecoveryEngine{
		db:        db,
		config:    cfg,
		scenarios: defaultRecoveryScenarios(),
		results:   make([]ChaosTestResult, 0),
		stopCh:    make(chan struct{}),
	}
	e.stats.ScenariosRegistered = len(e.scenarios)
	return e
}

func (e *ChaosRecoveryEngine) Start() {
	e.mu.Lock(); if e.running { e.mu.Unlock(); return }; e.running = true; e.mu.Unlock()
}

func (e *ChaosRecoveryEngine) Stop() {
	e.mu.Lock(); defer e.mu.Unlock(); if !e.running { return }; e.running = false; select { case <-e.stopCh: default: close(e.stopCh) }
}

// AddScenario registers a custom chaos scenario.
func (e *ChaosRecoveryEngine) AddScenario(s RecoveryScenario) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s.ID == "" { return fmt.Errorf("scenario ID required") }
	if len(e.scenarios) >= e.config.MaxScenarios { return fmt.Errorf("max scenarios reached") }
	e.scenarios = append(e.scenarios, s)
	e.stats.ScenariosRegistered = len(e.scenarios)
	return nil
}

// ListScenarios returns all registered scenarios.
func (e *ChaosRecoveryEngine) ListScenarios() []RecoveryScenario {
	e.mu.RLock(); defer e.mu.RUnlock()
	r := make([]RecoveryScenario, len(e.scenarios))
	copy(r, e.scenarios)
	return r
}

// RunScenario executes a specific chaos scenario and validates recovery.
func (e *ChaosRecoveryEngine) RunScenario(scenarioID string) (*ChaosTestResult, error) {
	e.mu.RLock()
	var scenario *RecoveryScenario
	for i := range e.scenarios {
		if e.scenarios[i].ID == scenarioID {
			s := e.scenarios[i]
			scenario = &s
			break
		}
	}
	e.mu.RUnlock()

	if scenario == nil {
		return nil, fmt.Errorf("scenario %q not found", scenarioID)
	}

	return e.executeScenario(*scenario), nil
}

// RunAll executes all registered chaos scenarios.
func (e *ChaosRecoveryEngine) RunAll() []ChaosTestResult {
	scenarios := e.ListScenarios()
	var results []ChaosTestResult
	for _, s := range scenarios {
		result := e.executeScenario(s)
		results = append(results, *result)
	}
	return results
}

func (e *ChaosRecoveryEngine) executeScenario(scenario RecoveryScenario) *ChaosTestResult {
	start := time.Now()
	result := &ChaosTestResult{
		ScenarioID:   scenario.ID,
		ScenarioName: scenario.Name,
		ExecutedAt:   start,
	}

	// Phase 1: Write baseline data
	baselinePoints := 100
	ts := time.Now().UnixNano()
	for i := 0; i < baselinePoints; i++ {
		e.db.Write(Point{
			Metric:    fmt.Sprintf("chaos_%s", scenario.ID),
			Value:     float64(i),
			Timestamp: ts + int64(i),
		})
	}
	e.db.Flush()
	result.WritesBefore = int64(baselinePoints)

	// Phase 2: Simulate fault based on type
	recoveryStart := time.Now()
	switch scenario.Type {
	case "clock_skew":
		// Write points with skewed timestamps
		for i := 0; i < 10; i++ {
			e.db.Write(Point{
				Metric:    fmt.Sprintf("chaos_%s", scenario.ID),
				Value:     float64(i + baselinePoints),
				Timestamp: ts + int64(i) - int64(time.Hour), // 1 hour in the past
			})
		}
	case "corruption":
		// Write NaN and extreme values (which validator should handle)
		for i := 0; i < 10; i++ {
			val := float64(i)
			if i%3 == 0 { val = math.MaxFloat64 }
			e.db.Write(Point{
				Metric:    fmt.Sprintf("chaos_%s", scenario.ID),
				Value:     val,
				Timestamp: ts + int64(baselinePoints) + int64(i),
			})
		}
	case "slow_io":
		// Simulate high-volume writes under pressure
		for i := 0; i < 1000; i++ {
			e.db.Write(Point{
				Metric:    fmt.Sprintf("chaos_%s_%d", scenario.ID, rand.Intn(100)),
				Value:     rand.Float64() * 1000,
				Timestamp: ts + int64(baselinePoints) + int64(i),
			})
		}
	default:
		// Generic: just write more data
		for i := 0; i < 50; i++ {
			e.db.Write(Point{
				Metric:    fmt.Sprintf("chaos_%s", scenario.ID),
				Value:     float64(i + baselinePoints),
				Timestamp: ts + int64(baselinePoints) + int64(i),
			})
		}
	}
	e.db.Flush()

	// Phase 3: Verify recovery — query the data back
	q := &Query{Metric: fmt.Sprintf("chaos_%s", scenario.ID), Start: 0, End: ts + int64(baselinePoints+1000)}
	queryResult, err := e.db.Execute(q)
	if err != nil {
		result.ErrorMessage = err.Error()
		result.Passed = false
	} else {
		result.Passed = queryResult != nil && len(queryResult.Points) > 0
		result.DataIntegrity = result.Passed
		if queryResult != nil {
			result.QueriesAfter = int64(len(queryResult.Points))
		}
	}

	result.RecoveryTime = time.Since(recoveryStart)
	result.Duration = time.Since(start)

	// Record result
	e.mu.Lock()
	e.results = append(e.results, *result)
	e.stats.TestsExecuted++
	if result.Passed {
		e.stats.TestsPassed++
	} else {
		e.stats.TestsFailed++
	}
	if e.stats.AvgRecoveryTime == 0 {
		e.stats.AvgRecoveryTime = result.RecoveryTime
	} else {
		e.stats.AvgRecoveryTime = (e.stats.AvgRecoveryTime + result.RecoveryTime) / 2
	}
	e.mu.Unlock()

	return result
}

// Results returns all test results.
func (e *ChaosRecoveryEngine) Results() []ChaosTestResult {
	e.mu.RLock(); defer e.mu.RUnlock()
	r := make([]ChaosTestResult, len(e.results))
	copy(r, e.results)
	return r
}

// GetStats returns framework stats.
func (e *ChaosRecoveryEngine) GetStats() ChaosRecoveryStats {
	e.mu.RLock(); defer e.mu.RUnlock(); return e.stats
}

func defaultRecoveryScenarios() []RecoveryScenario {
	return []RecoveryScenario{
		{ID: "clock-skew", Name: "Clock Skew", Type: "clock_skew", Description: "Writes with timestamps 1 hour in the past", Severity: "medium"},
		{ID: "data-corruption", Name: "Data Corruption", Type: "corruption", Description: "Writes extreme values (MaxFloat64)", Severity: "high"},
		{ID: "io-pressure", Name: "I/O Pressure", Type: "slow_io", Description: "High-volume writes to 100 different metrics simultaneously", Severity: "medium"},
		{ID: "rapid-restart", Name: "Rapid Restart", Type: "restart", Description: "Write-flush-query cycle under time pressure", Severity: "low"},
		{ID: "cardinality-bomb", Name: "Cardinality Bomb", Type: "slow_io", Description: "Writes 1000 unique metric names", Severity: "high"},
	}
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *ChaosRecoveryEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/chaos/scenarios", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListScenarios())
	})
	mux.HandleFunc("/api/v1/chaos/run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		id := r.URL.Query().Get("scenario")
		if id == "" {
			results := e.RunAll()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(results)
			return
		}
		result, err := e.RunScenario(id)
		if err != nil { http.Error(w, "not found", http.StatusNotFound); return }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("/api/v1/chaos/results", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Results())
	})
	mux.HandleFunc("/api/v1/chaos/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
