package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"
)

// ScenarioConfig configures a what-if scenario.
type ScenarioConfig struct {
	Name          string            `json:"name"`
	Description   string            `json:"description,omitempty"`
	BaseBranch    string            `json:"base_branch"`
	Transformations []Transformation `json:"transformations"`
	Duration      time.Duration     `json:"duration"`
}

// Transformation defines a data transformation to apply to a scenario branch.
type Transformation struct {
	Type        string  `json:"type"` // "scale", "shift", "noise", "filter"
	Metric      string  `json:"metric"`
	Factor      float64 `json:"factor,omitempty"`      // For scale/shift
	NoiseStddev float64 `json:"noise_stddev,omitempty"` // For noise injection
	FilterMin   float64 `json:"filter_min,omitempty"`   // For filtering
	FilterMax   float64 `json:"filter_max,omitempty"`
}

// ScenarioResult captures the outcome of a scenario comparison.
type ScenarioResult struct {
	ScenarioName   string            `json:"scenario_name"`
	BaseBranch     string            `json:"base_branch"`
	ScenarioBranch string            `json:"scenario_branch"`
	Comparisons    []MetricComparison `json:"comparisons"`
	CreatedAt      time.Time         `json:"created_at"`
}

// MetricComparison compares a metric between base and scenario.
type MetricComparison struct {
	Metric        string  `json:"metric"`
	BaseMean      float64 `json:"base_mean"`
	ScenarioMean  float64 `json:"scenario_mean"`
	BaseStddev    float64 `json:"base_stddev"`
	ScenarioStddev float64 `json:"scenario_stddev"`
	PercentChange float64 `json:"percent_change"`
	Significant   bool    `json:"significant"`
}

// ScenarioEngine manages what-if scenarios using branch-based isolation.
type ScenarioEngine struct {
	storage   *BranchStorage
	scenarios map[string]*ScenarioConfig
	results   map[string]*ScenarioResult
	mu        sync.RWMutex
}

// NewScenarioEngine creates a new scenario engine.
func NewScenarioEngine(storage *BranchStorage) *ScenarioEngine {
	return &ScenarioEngine{
		storage:   storage,
		scenarios: make(map[string]*ScenarioConfig),
		results:   make(map[string]*ScenarioResult),
	}
}

// CreateScenario creates a what-if scenario branch from a base branch
// and applies the specified transformations.
func (e *ScenarioEngine) CreateScenario(config ScenarioConfig) error {
	if config.Name == "" {
		return fmt.Errorf("scenario name is required")
	}
	if config.BaseBranch == "" {
		config.BaseBranch = "main"
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.scenarios[config.Name]; exists {
		return fmt.Errorf("scenario %q already exists", config.Name)
	}

	// Create scenario branch from base
	branchID := "scenario_" + config.Name
	if err := e.storage.CreateBranch(branchID, config.BaseBranch); err != nil {
		return fmt.Errorf("failed to create scenario branch: %w", err)
	}

	// Apply transformations
	points := e.storage.ReadPoints(branchID, "")
	transformed := e.applyTransformations(points, config.Transformations)

	// Replace branch data with transformed data
	e.storage.DeleteBranch(branchID)
	e.storage.CreateBranch(branchID, "")
	if len(transformed) > 0 {
		e.storage.WritePoints(branchID, transformed)
	}

	e.scenarios[config.Name] = &config
	return nil
}

// CompareScenario compares a scenario branch against its base.
func (e *ScenarioEngine) CompareScenario(name string) (*ScenarioResult, error) {
	e.mu.RLock()
	config, exists := e.scenarios[name]
	e.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("scenario %q not found", name)
	}

	branchID := "scenario_" + name
	basePoints := e.storage.ReadPoints(config.BaseBranch, "")
	scenarioPoints := e.storage.ReadPoints(branchID, "")

	// Group by metric
	baseByMetric := groupPointsByMetric(basePoints)
	scenarioByMetric := groupPointsByMetric(scenarioPoints)

	result := &ScenarioResult{
		ScenarioName:   name,
		BaseBranch:     config.BaseBranch,
		ScenarioBranch: branchID,
		CreatedAt:      time.Now(),
	}

	// Compare each metric
	allMetrics := make(map[string]bool)
	for m := range baseByMetric {
		allMetrics[m] = true
	}
	for m := range scenarioByMetric {
		allMetrics[m] = true
	}

	for metric := range allMetrics {
		baseVals := scenarioExtractValues(baseByMetric[metric])
		scenarioVals := scenarioExtractValues(scenarioByMetric[metric])

		baseMean, baseStd := meanStddev(baseVals)
		scenarioMean, scenarioStd := meanStddev(scenarioVals)

		pctChange := 0.0
		if baseMean != 0 {
			pctChange = ((scenarioMean - baseMean) / math.Abs(baseMean)) * 100
		}

		// Simple significance check: >5% change
		significant := math.Abs(pctChange) > 5.0

		result.Comparisons = append(result.Comparisons, MetricComparison{
			Metric:         metric,
			BaseMean:       baseMean,
			ScenarioMean:   scenarioMean,
			BaseStddev:     baseStd,
			ScenarioStddev: scenarioStd,
			PercentChange:  pctChange,
			Significant:    significant,
		})
	}

	e.mu.Lock()
	e.results[name] = result
	e.mu.Unlock()

	return result, nil
}

// DeleteScenario removes a scenario and its branch.
func (e *ScenarioEngine) DeleteScenario(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.scenarios[name]; !exists {
		return fmt.Errorf("scenario %q not found", name)
	}

	branchID := "scenario_" + name
	e.storage.DeleteBranch(branchID)
	delete(e.scenarios, name)
	delete(e.results, name)
	return nil
}

// ListScenarios returns all scenario configs.
func (e *ScenarioEngine) ListScenarios() []ScenarioConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]ScenarioConfig, 0, len(e.scenarios))
	for _, s := range e.scenarios {
		result = append(result, *s)
	}
	return result
}

func (e *ScenarioEngine) applyTransformations(points []Point, transforms []Transformation) []Point {
	result := make([]Point, len(points))
	copy(result, points)

	for _, t := range transforms {
		for i := range result {
			if t.Metric != "" && result[i].Metric != t.Metric {
				continue
			}
			switch t.Type {
			case "scale":
				result[i].Value *= t.Factor
			case "shift":
				result[i].Value += t.Factor
			case "filter":
				if result[i].Value < t.FilterMin || result[i].Value > t.FilterMax {
					result[i].Value = 0
				}
			}
		}
	}

	return result
}

func groupPointsByMetric(points []Point) map[string][]Point {
	result := make(map[string][]Point)
	for _, p := range points {
		result[p.Metric] = append(result[p.Metric], p)
	}
	return result
}

func scenarioExtractValues(points []Point) []float64 {
	vals := make([]float64, len(points))
	for i, p := range points {
		vals[i] = p.Value
	}
	return vals
}

func meanStddev(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}

	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))

	sumSq := 0.0
	for _, v := range values {
		diff := v - mean
		sumSq += diff * diff
	}
	stddev := math.Sqrt(sumSq / float64(len(values)))

	return mean, stddev
}

// RegisterHTTPHandlers registers scenario engine HTTP endpoints.
func (e *ScenarioEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/scenarios", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			json.NewEncoder(w).Encode(e.ListScenarios())
		case http.MethodPost:
			var config ScenarioConfig
			if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if err := e.CreateScenario(config); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]string{"status": "created"})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/scenarios/compare", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		name := r.URL.Query().Get("name")
		if name == "" {
			http.Error(w, "name parameter required", http.StatusBadRequest)
			return
		}
		result, err := e.CompareScenario(name)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(result)
	})
}
