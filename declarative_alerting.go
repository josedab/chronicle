package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// DeclarativeAlertingConfig configures the declarative alerting engine.
type DeclarativeAlertingConfig struct {
	Enabled          bool
	WatchPaths       []string
	ReloadInterval   time.Duration
	ValidationStrict bool
	DryRunMode       bool
	MaxAlertRules    int
}

// DefaultDeclarativeAlertingConfig returns sensible defaults.
func DefaultDeclarativeAlertingConfig() DeclarativeAlertingConfig {
	return DeclarativeAlertingConfig{
		Enabled:          true,
		WatchPaths:       []string{},
		ReloadInterval:   30 * time.Second,
		ValidationStrict: false,
		DryRunMode:       false,
		MaxAlertRules:    1000,
	}
}

// AlertDefinition is a YAML-friendly alert definition.
type AlertDefinition struct {
	APIVersion string        `json:"apiVersion" yaml:"apiVersion"`
	Kind       string        `json:"kind" yaml:"kind"`
	Metadata   AlertMetadata `json:"metadata" yaml:"metadata"`
	Spec       AlertSpec     `json:"spec" yaml:"spec"`
}

// AlertMetadata holds alert identification and labeling.
type AlertMetadata struct {
	Name        string            `json:"name" yaml:"name"`
	Namespace   string            `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Labels      map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty" yaml:"annotations,omitempty"`
}

// AlertSpec defines the alert behavior and routing.
type AlertSpec struct {
	Metric       string               `json:"metric" yaml:"metric"`
	Conditions   []AlertConditionSpec `json:"conditions" yaml:"conditions"`
	Logic        string               `json:"logic,omitempty" yaml:"logic,omitempty"`
	For          string               `json:"for,omitempty" yaml:"for,omitempty"`
	EvalInterval string               `json:"evalInterval,omitempty" yaml:"evalInterval,omitempty"`
	Severity     string               `json:"severity" yaml:"severity"`
	Routing      []AlertRouting       `json:"routing,omitempty" yaml:"routing,omitempty"`
	Enrichment   []AlertEnrichment    `json:"enrichment,omitempty" yaml:"enrichment,omitempty"`
	Silences     []AlertSilence       `json:"silences,omitempty" yaml:"silences,omitempty"`
	Tags         map[string]string    `json:"tags,omitempty" yaml:"tags,omitempty"`
}

// AlertConditionSpec defines a single composable condition.
type AlertConditionSpec struct {
	Type     string  `json:"type" yaml:"type"`
	Operator string  `json:"operator" yaml:"operator"`
	Value    float64 `json:"value" yaml:"value"`
	Window   string  `json:"window,omitempty" yaml:"window,omitempty"`
}

// AlertRouting defines where alert notifications are sent.
type AlertRouting struct {
	Type   string            `json:"type" yaml:"type"`
	Target string            `json:"target" yaml:"target"`
	Config map[string]string `json:"config,omitempty" yaml:"config,omitempty"`
}

// AlertEnrichment adds metadata to fired alerts.
type AlertEnrichment struct {
	Type  string `json:"type" yaml:"type"`
	Key   string `json:"key" yaml:"key"`
	Value string `json:"value" yaml:"value"`
}

// AlertSilence defines silence windows.
type AlertSilence struct {
	StartTime string   `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	EndTime   string   `json:"endTime,omitempty" yaml:"endTime,omitempty"`
	Weekdays  []string `json:"weekdays,omitempty" yaml:"weekdays,omitempty"`
}

// AlertTestCase for CI/CD testing of alert definitions.
type AlertTestCase struct {
	Name         string            `json:"name" yaml:"name"`
	AlertName    string            `json:"alertName" yaml:"alertName"`
	InputPoints  []TestPoint       `json:"inputPoints" yaml:"inputPoints"`
	ExpectFiring bool              `json:"expectFiring" yaml:"expectFiring"`
	ExpectLabels map[string]string `json:"expectLabels,omitempty" yaml:"expectLabels,omitempty"`
}

// TestPoint is a synthetic data point for alert testing.
type TestPoint struct {
	Value     float64 `json:"value" yaml:"value"`
	Timestamp string  `json:"timestamp,omitempty" yaml:"timestamp,omitempty"`
	OffsetSec int64   `json:"offsetSec,omitempty" yaml:"offsetSec,omitempty"`
}

// AlertTestResult holds the outcome of a single test case.
type AlertTestResult struct {
	TestName string        `json:"testName"`
	Passed   bool          `json:"passed"`
	Expected bool          `json:"expected"`
	Actual   bool          `json:"actual"`
	Error    string        `json:"error,omitempty"`
	Duration time.Duration `json:"duration"`
}

// AlertValidationResult from validating a definition.
type AlertValidationResult struct {
	Valid    bool     `json:"valid"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// LoadedAlert is a loaded and active alert.
type LoadedAlert struct {
	Definition AlertDefinition
	State      string // "inactive", "pending", "firing", "resolved"
	LastEval   time.Time
	LastFired  time.Time
	FireCount  int64
	LoadedAt   time.Time
	Error      string
}

// DeclarativeAlertingEngine manages declarative alert definitions.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type DeclarativeAlertingEngine struct {
	db          *DB
	config      DeclarativeAlertingConfig
	mu          sync.RWMutex
	alerts      map[string]*LoadedAlert
	testResults map[string][]AlertTestResult
	stopCh      chan struct{}
	running     bool
	stats       DeclarativeAlertingStats
}

// DeclarativeAlertingStats tracks engine statistics.
type DeclarativeAlertingStats struct {
	TotalDefinitions int       `json:"totalDefinitions"`
	ActiveAlerts     int       `json:"activeAlerts"`
	FiringAlerts     int       `json:"firingAlerts"`
	TotalEvaluations int64     `json:"totalEvaluations"`
	TotalFirings     int64     `json:"totalFirings"`
	TestsRun         int64     `json:"testsRun"`
	TestsPassed      int64     `json:"testsPassed"`
	TestsFailed      int64     `json:"testsFailed"`
	LastReload       time.Time `json:"lastReload"`
	ValidationErrors int64     `json:"validationErrors"`
}

// NewDeclarativeAlertingEngine creates a new declarative alerting engine.
func NewDeclarativeAlertingEngine(db *DB, cfg DeclarativeAlertingConfig) *DeclarativeAlertingEngine {
	return &DeclarativeAlertingEngine{
		db:          db,
		config:      cfg,
		alerts:      make(map[string]*LoadedAlert),
		testResults: make(map[string][]AlertTestResult),
		stopCh:      make(chan struct{}),
	}
}

// Start begins the background evaluation loop.
func (e *DeclarativeAlertingEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()

	go e.evaluationLoop()
}

// Stop halts the background evaluation loop.
func (e *DeclarativeAlertingEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

func (e *DeclarativeAlertingEngine) evaluationLoop() {
	interval := e.config.ReloadInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.EvaluateAll()
		}
	}
}

// LoadDefinition loads a single alert definition into the engine.
func (e *DeclarativeAlertingEngine) LoadDefinition(def AlertDefinition) error {
	vr := e.ValidateDefinition(def)
	if !vr.Valid {
		e.mu.Lock()
		e.stats.ValidationErrors++
		e.mu.Unlock()
		return fmt.Errorf("validation failed: %s", strings.Join(vr.Errors, "; "))
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.config.MaxAlertRules > 0 && len(e.alerts) >= e.config.MaxAlertRules {
		if _, exists := e.alerts[def.Metadata.Name]; !exists {
			return fmt.Errorf("maximum alert rules (%d) reached", e.config.MaxAlertRules)
		}
	}

	e.alerts[def.Metadata.Name] = &LoadedAlert{
		Definition: def,
		State:      "inactive",
		LoadedAt:   time.Now(),
	}
	e.stats.TotalDefinitions = len(e.alerts)
	return nil
}

// LoadFromYAML parses YAML and loads the alert definition.
func (e *DeclarativeAlertingEngine) LoadFromYAML(data []byte) error {
	var def AlertDefinition
	if err := yaml.Unmarshal(data, &def); err != nil {
		return fmt.Errorf("invalid YAML: %w", err)
	}
	return e.LoadDefinition(def)
}

// UnloadAlert removes an alert by name.
func (e *DeclarativeAlertingEngine) UnloadAlert(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.alerts[name]; !exists {
		return fmt.Errorf("alert %q not found", name)
	}
	delete(e.alerts, name)
	e.stats.TotalDefinitions = len(e.alerts)
	return nil
}

// ValidateDefinition validates a definition without loading it.
func (e *DeclarativeAlertingEngine) ValidateDefinition(def AlertDefinition) *AlertValidationResult {
	result := &AlertValidationResult{Valid: true}

	if def.Metadata.Name == "" {
		result.Errors = append(result.Errors, "metadata.name is required")
	}
	if def.Spec.Metric == "" {
		result.Errors = append(result.Errors, "spec.metric is required")
	}
	if len(def.Spec.Conditions) == 0 {
		result.Errors = append(result.Errors, "at least one condition is required")
	}

	validSeverities := map[string]bool{"critical": true, "warning": true, "info": true}
	if def.Spec.Severity != "" && !validSeverities[def.Spec.Severity] {
		result.Errors = append(result.Errors, fmt.Sprintf("invalid severity %q: must be critical, warning, or info", def.Spec.Severity))
	}

	logic := strings.ToLower(def.Spec.Logic)
	if logic != "" && logic != "and" && logic != "or" {
		result.Errors = append(result.Errors, fmt.Sprintf("invalid logic %q: must be and or or", def.Spec.Logic))
	}

	validCondTypes := map[string]bool{"threshold": true, "rate": true, "absent": true, "anomaly": true}
	validOps := map[string]bool{"gt": true, "lt": true, "eq": true, "ne": true, "gte": true, "lte": true}

	for i, c := range def.Spec.Conditions {
		if !validCondTypes[c.Type] {
			result.Errors = append(result.Errors, fmt.Sprintf("condition[%d]: invalid type %q", i, c.Type))
		}
		if c.Type != "absent" && !validOps[c.Operator] {
			result.Errors = append(result.Errors, fmt.Sprintf("condition[%d]: invalid operator %q", i, c.Operator))
		}
		if c.Window != "" {
			if _, err := time.ParseDuration(c.Window); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("condition[%d]: invalid window %q", i, c.Window))
			}
		}
	}

	if def.Spec.For != "" {
		if _, err := time.ParseDuration(def.Spec.For); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("invalid for duration %q", def.Spec.For))
		}
	}

	if def.APIVersion == "" {
		result.Warnings = append(result.Warnings, "apiVersion not set")
	}
	if def.Kind == "" {
		result.Warnings = append(result.Warnings, "kind not set")
	}

	if e.config.ValidationStrict && len(result.Warnings) > 0 {
		result.Errors = append(result.Errors, result.Warnings...)
		result.Warnings = nil
	}

	if len(result.Errors) > 0 {
		result.Valid = false
	}
	return result
}

// EvaluateAlert evaluates a single alert by name.
func (e *DeclarativeAlertingEngine) EvaluateAlert(name string) (bool, error) {
	e.mu.RLock()
	la, exists := e.alerts[name]
	if !exists {
		e.mu.RUnlock()
		return false, fmt.Errorf("alert %q not found", name)
	}
	def := la.Definition
	e.mu.RUnlock()

	firing := e.evaluateConditions(def)

	e.mu.Lock()
	defer e.mu.Unlock()
	e.stats.TotalEvaluations++
	la.LastEval = time.Now()

	prevState := la.State
	if firing {
		if la.Definition.Spec.For != "" && prevState == "inactive" {
			la.State = "pending"
		} else {
			la.State = "firing"
			la.LastFired = time.Now()
			la.FireCount++
			e.stats.TotalFirings++
			if !e.config.DryRunMode {
				go e.route(la)
			}
		}
	} else {
		if prevState == "firing" {
			la.State = "resolved"
		} else {
			la.State = "inactive"
		}
	}

	e.updateActiveStats()
	return firing, nil
}

func (e *DeclarativeAlertingEngine) evaluateConditions(def AlertDefinition) bool {
	logic := strings.ToLower(def.Spec.Logic)
	if logic == "" {
		logic = "and"
	}

	results := make([]bool, 0, len(def.Spec.Conditions))
	for _, cond := range def.Spec.Conditions {
		met, _ := e.evaluateCondition(cond, def.Spec.Metric, def.Spec.Tags)
		results = append(results, met)
	}

	if logic == "or" {
		for _, r := range results {
			if r {
				return true
			}
		}
		return false
	}

	// Default: AND
	for _, r := range results {
		if !r {
			return false
		}
	}
	return len(results) > 0
}

func (e *DeclarativeAlertingEngine) evaluateCondition(spec AlertConditionSpec, metric string, tags map[string]string) (bool, error) {
	if e.db == nil {
		return false, fmt.Errorf("no database connection")
	}

	now := time.Now()
	window := 5 * time.Minute
	if spec.Window != "" {
		if d, err := time.ParseDuration(spec.Window); err == nil {
			window = d
		}
	}
	_ = window

	// Use Start: 0 to ensure BTree range scan finds partitions containing recent writes
	end := now.Add(time.Second).UnixNano()

	switch spec.Type {
	case "absent":
		result, err := e.db.Execute(&Query{
			Metric: metric,
			Tags:   tags,
			Start:  0,
			End:    end,
		})
		if err != nil {
			return false, err
		}
		return len(result.Points) == 0, nil

	case "rate":
		result, err := e.db.Execute(&Query{
			Metric: metric,
			Tags:   tags,
			Start:  0,
			End:    end,
		})
		if err != nil || len(result.Points) < 2 {
			return false, err
		}
		first := result.Points[0].Value
		last := result.Points[len(result.Points)-1].Value
		rate := last - first
		return compareValue(rate, spec.Operator, spec.Value), nil

	default: // "threshold", "anomaly"
		result, err := e.db.Execute(&Query{
			Metric: metric,
			Tags:   tags,
			Start:  0,
			End:    end,
		})
		if err != nil || len(result.Points) == 0 {
			return false, err
		}
		value := result.Points[len(result.Points)-1].Value
		return compareValue(value, spec.Operator, spec.Value), nil
	}
}

func compareValue(value float64, operator string, threshold float64) bool {
	switch operator {
	case "gt":
		return value > threshold
	case "lt":
		return value < threshold
	case "eq":
		return value == threshold
	case "ne":
		return value != threshold
	case "gte":
		return value >= threshold
	case "lte":
		return value <= threshold
	default:
		return false
	}
}

// EvaluateAll evaluates all loaded alerts and returns firing status.
func (e *DeclarativeAlertingEngine) EvaluateAll() map[string]bool {
	e.mu.RLock()
	names := make([]string, 0, len(e.alerts))
	for name := range e.alerts {
		names = append(names, name)
	}
	e.mu.RUnlock()

	results := make(map[string]bool, len(names))
	for _, name := range names {
		firing, err := e.EvaluateAlert(name)
		if err == nil {
			results[name] = firing
		}
	}
	return results
}

// RunTest runs a single alert test case.
func (e *DeclarativeAlertingEngine) RunTest(tc AlertTestCase) *AlertTestResult {
	start := time.Now()
	result := &AlertTestResult{
		TestName: tc.Name,
		Expected: tc.ExpectFiring,
	}

	e.mu.RLock()
	la, exists := e.alerts[tc.AlertName]
	if !exists {
		e.mu.RUnlock()
		result.Error = fmt.Sprintf("alert %q not found", tc.AlertName)
		result.Duration = time.Since(start)
		e.recordTestResult(tc.AlertName, result)
		return result
	}
	def := la.Definition
	e.mu.RUnlock()

	// Write test points
	if e.db != nil {
		for _, tp := range tc.InputPoints {
			ts := time.Now().Add(time.Duration(tp.OffsetSec) * time.Second).UnixNano()
			if tp.Timestamp != "" {
				if t, err := time.Parse(time.RFC3339, tp.Timestamp); err == nil {
					ts = t.UnixNano()
				}
			}
			_ = e.db.Write(Point{
				Metric:    def.Spec.Metric,
				Tags:      def.Spec.Tags,
				Value:     tp.Value,
				Timestamp: ts,
			})
		}
		_ = e.db.Flush()
	}

	firing := e.evaluateConditions(def)
	result.Actual = firing
	result.Passed = (firing == tc.ExpectFiring)

	if tc.ExpectLabels != nil && la.Definition.Metadata.Labels != nil {
		for k, v := range tc.ExpectLabels {
			if la.Definition.Metadata.Labels[k] != v {
				result.Passed = false
				result.Error = fmt.Sprintf("label %q: expected %q, got %q", k, v, la.Definition.Metadata.Labels[k])
				break
			}
		}
	}

	result.Duration = time.Since(start)
	e.recordTestResult(tc.AlertName, result)

	e.mu.Lock()
	e.stats.TestsRun++
	if result.Passed {
		e.stats.TestsPassed++
	} else {
		e.stats.TestsFailed++
	}
	e.mu.Unlock()

	return result
}

func (e *DeclarativeAlertingEngine) recordTestResult(alertName string, result *AlertTestResult) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.testResults[alertName] = append(e.testResults[alertName], *result)
}

// RunTests runs a suite of test cases.
func (e *DeclarativeAlertingEngine) RunTests(tests []AlertTestCase) []AlertTestResult {
	results := make([]AlertTestResult, 0, len(tests))
	for _, tc := range tests {
		r := e.RunTest(tc)
		results = append(results, *r)
	}
	return results
}

// ListAlerts returns all loaded alerts.
func (e *DeclarativeAlertingEngine) ListAlerts() []*LoadedAlert {
	e.mu.RLock()
	defer e.mu.RUnlock()
	alerts := make([]*LoadedAlert, 0, len(e.alerts))
	for _, la := range e.alerts {
		alerts = append(alerts, la)
	}
	return alerts
}

// GetAlert returns a single loaded alert by name.
func (e *DeclarativeAlertingEngine) GetAlert(name string) *LoadedAlert {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.alerts[name]
}

// ListTestResults returns all test results grouped by alert name.
func (e *DeclarativeAlertingEngine) ListTestResults() map[string][]AlertTestResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make(map[string][]AlertTestResult, len(e.testResults))
	for k, v := range e.testResults {
		cp := make([]AlertTestResult, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

func (e *DeclarativeAlertingEngine) route(alert *LoadedAlert) error {
	for _, r := range alert.Definition.Spec.Routing {
		switch r.Type {
		case "log":
			// No-op for log routing in library mode
		case "webhook":
			if r.Target != "" {
				client := &http.Client{Timeout: 10 * time.Second}
				payload, _ := json.Marshal(map[string]interface{}{
					"alert":    alert.Definition.Metadata.Name,
					"state":    alert.State,
					"severity": alert.Definition.Spec.Severity,
				})
				resp, err := client.Post(r.Target, "application/json", strings.NewReader(string(payload)))
				if err != nil {
					return err
				}
				resp.Body.Close()
			}
		case "slack", "pagerduty", "email":
			// Placeholder for external integrations
		}
	}
	return nil
}

// Reload reloads alert definitions from watched paths.
func (e *DeclarativeAlertingEngine) Reload() error {
	e.mu.Lock()
	e.stats.LastReload = time.Now()
	e.mu.Unlock()
	// In a full implementation this would read files from WatchPaths.
	// For now this is a manual trigger point.
	return nil
}

// ExportAll exports all loaded definitions.
func (e *DeclarativeAlertingEngine) ExportAll() []AlertDefinition {
	e.mu.RLock()
	defer e.mu.RUnlock()
	defs := make([]AlertDefinition, 0, len(e.alerts))
	for _, la := range e.alerts {
		defs = append(defs, la.Definition)
	}
	return defs
}

// Stats returns current engine statistics.
func (e *DeclarativeAlertingEngine) Stats() DeclarativeAlertingStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

func (e *DeclarativeAlertingEngine) updateActiveStats() {
	active := 0
	firing := 0
	for _, la := range e.alerts {
		if la.State != "inactive" {
			active++
		}
		if la.State == "firing" {
			firing++
		}
	}
	e.stats.ActiveAlerts = active
	e.stats.FiringAlerts = firing
}

// RegisterHTTPHandlers registers declarative alerting HTTP endpoints.
func (e *DeclarativeAlertingEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/alerting/definitions", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			e.handleListDefinitions(w, r)
		case http.MethodPost:
			e.handleLoadDefinition(w, r)
		case http.MethodDelete:
			name := r.URL.Query().Get("name")
			if name == "" {
				http.Error(w, "name parameter required", http.StatusBadRequest)
				return
			}
			if err := e.UnloadAlert(name); err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/alerting/validate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var def AlertDefinition
		if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		result := e.ValidateDefinition(def)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/api/v1/alerting/evaluate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		results := e.EvaluateAll()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	mux.HandleFunc("/api/v1/alerting/test", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var tests []AlertTestCase
		if err := json.NewDecoder(r.Body).Decode(&tests); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		results := e.RunTests(tests)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	mux.HandleFunc("/api/v1/alerting/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := e.Reload(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "reloaded"})
	})

	mux.HandleFunc("/api/v1/alerting/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}

func (e *DeclarativeAlertingEngine) handleListDefinitions(w http.ResponseWriter, _ *http.Request) {
	alerts := e.ListAlerts()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(alerts)
}

func (e *DeclarativeAlertingEngine) handleLoadDefinition(w http.ResponseWriter, r *http.Request) {
	var def AlertDefinition
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := e.LoadDefinition(def); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "loaded", "name": def.Metadata.Name})
}
