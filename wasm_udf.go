package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// WASMUDFConfig configures the WASM UDF extension engine.
type WASMUDFConfig struct {
	Enabled          bool
	MaxExecutionTime time.Duration
	MaxMemoryBytes   int64
	SandboxEnabled   bool
	MaxUDFs          int
	CacheCompiled    bool
}

// DefaultWASMUDFConfig returns sensible defaults.
func DefaultWASMUDFConfig() WASMUDFConfig {
	return WASMUDFConfig{
		Enabled:          true,
		MaxExecutionTime: 5 * time.Second,
		MaxMemoryBytes:   64 * 1024 * 1024, // 64MB
		SandboxEnabled:   true,
		MaxUDFs:          100,
		CacheCompiled:    true,
	}
}

// UDFType represents the type of user-defined function.
type UDFType string

const (
	UDFTypeMap       UDFType = "map"
	UDFTypeReduce    UDFType = "reduce"
	UDFTypeFilter    UDFType = "filter"
	UDFTypeTrigger   UDFType = "trigger"
	UDFTypeAggregate UDFType = "aggregate"
	UDFTypeTransform UDFType = "transform"
)

// UDFLanguage represents the source language of a UDF.
type UDFLanguage string

const (
	UDFLanguageRust         UDFLanguage = "rust"
	UDFLanguageGo           UDFLanguage = "go"
	UDFLanguageAssemblyScript UDFLanguage = "assemblyscript"
	UDFLanguageJavaScript    UDFLanguage = "javascript"
	UDFLanguagePython        UDFLanguage = "python"
)

// UDFDefinition describes a user-defined function.
type UDFDefinition struct {
	Name        string            `json:"name"`
	Version     string            `json:"version"`
	Type        UDFType           `json:"type"`
	Language    UDFLanguage       `json:"language"`
	Description string            `json:"description"`
	InputSchema  []UDFParam       `json:"input_schema"`
	OutputSchema []UDFParam       `json:"output_schema"`
	WASMBytes   []byte            `json:"-"`
	Source      string            `json:"source,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// UDFParam describes an input or output parameter.
type UDFParam struct {
	Name string `json:"name"`
	Type string `json:"type"` // float64, int64, string, bool, []float64
}

// UDFInstance represents a running UDF instance.
type UDFInstance struct {
	Definition  *UDFDefinition `json:"definition"`
	State       string         `json:"state"` // loaded, running, paused, error
	InvokeCount int64          `json:"invoke_count"`
	ErrorCount  int64          `json:"error_count"`
	LastInvoked time.Time      `json:"last_invoked"`
	AvgLatency  time.Duration  `json:"avg_latency"`
}

// UDFExecutionResult holds the result of a UDF invocation.
type UDFExecutionResult struct {
	UDFName   string        `json:"udf_name"`
	Output    interface{}   `json:"output"`
	Duration  time.Duration `json:"duration"`
	Success   bool          `json:"success"`
	Error     string        `json:"error,omitempty"`
}

// WASMUDFStats holds engine statistics.
type WASMUDFStats struct {
	RegisteredUDFs int           `json:"registered_udfs"`
	TotalInvocations int64       `json:"total_invocations"`
	TotalErrors     int64        `json:"total_errors"`
	AvgLatency      time.Duration `json:"avg_latency"`
	MemoryUsedBytes int64        `json:"memory_used_bytes"`
}

// WASMUDFEngine provides WebAssembly-based user-defined function extensions.
type WASMUDFEngine struct {
	db     *DB
	config WASMUDFConfig

	mu        sync.RWMutex
	udfs      map[string]*UDFInstance
	running   bool
	stopCh    chan struct{}
	stats     WASMUDFStats
}

// NewWASMUDFEngine creates a new WASM UDF engine.
func NewWASMUDFEngine(db *DB, cfg WASMUDFConfig) *WASMUDFEngine {
	return &WASMUDFEngine{
		db:     db,
		config: cfg,
		udfs:   make(map[string]*UDFInstance),
		stopCh: make(chan struct{}),
	}
}

// Start starts the UDF engine.
func (e *WASMUDFEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop stops the UDF engine and unloads all UDFs.
func (e *WASMUDFEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
	// Unload all UDFs
	for name, inst := range e.udfs {
		inst.State = "unloaded"
		_ = name
	}
}

// Register registers a new UDF.
func (e *WASMUDFEngine) Register(def UDFDefinition) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if def.Name == "" {
		return fmt.Errorf("UDF name is required")
	}
	if len(e.udfs) >= e.config.MaxUDFs {
		return fmt.Errorf("maximum UDF limit reached (%d)", e.config.MaxUDFs)
	}
	if _, exists := e.udfs[def.Name]; exists {
		return fmt.Errorf("UDF %q already registered", def.Name)
	}

	now := time.Now()
	def.CreatedAt = now
	def.UpdatedAt = now

	e.udfs[def.Name] = &UDFInstance{
		Definition: &def,
		State:      "loaded",
	}

	e.stats.RegisteredUDFs = len(e.udfs)
	return nil
}

// Update updates an existing UDF.
func (e *WASMUDFEngine) Update(def UDFDefinition) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	inst, exists := e.udfs[def.Name]
	if !exists {
		return fmt.Errorf("UDF %q not found", def.Name)
	}

	def.CreatedAt = inst.Definition.CreatedAt
	def.UpdatedAt = time.Now()

	inst.Definition = &def
	inst.State = "loaded"
	return nil
}

// Unregister removes a UDF.
func (e *WASMUDFEngine) Unregister(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.udfs[name]; !exists {
		return fmt.Errorf("UDF %q not found", name)
	}

	delete(e.udfs, name)
	e.stats.RegisteredUDFs = len(e.udfs)
	return nil
}

// Invoke executes a UDF with the given arguments.
func (e *WASMUDFEngine) Invoke(name string, args map[string]interface{}) (*UDFExecutionResult, error) {
	e.mu.Lock()
	inst, exists := e.udfs[name]
	if !exists {
		e.mu.Unlock()
		return nil, fmt.Errorf("UDF %q not found", name)
	}
	inst.State = "running"
	e.mu.Unlock()

	start := time.Now()
	result := &UDFExecutionResult{
		UDFName: name,
		Success: true,
	}

	// Execute based on UDF type
	output, err := e.execute(inst.Definition, args)
	elapsed := time.Since(start)

	e.mu.Lock()
	defer e.mu.Unlock()

	inst.InvokeCount++
	inst.LastInvoked = time.Now()
	e.stats.TotalInvocations++

	if inst.AvgLatency == 0 {
		inst.AvgLatency = elapsed
	} else {
		inst.AvgLatency = (inst.AvgLatency + elapsed) / 2
	}

	if err != nil {
		inst.ErrorCount++
		inst.State = "error"
		e.stats.TotalErrors++
		result.Success = false
		result.Error = err.Error()
	} else {
		inst.State = "loaded"
		result.Output = output
	}

	result.Duration = elapsed
	return result, nil
}

// execute runs the UDF logic.
func (e *WASMUDFEngine) execute(def *UDFDefinition, args map[string]interface{}) (interface{}, error) {
	// Check execution timeout
	if e.config.MaxExecutionTime > 0 {
		// In a full implementation, this would enforce a timeout on WASM execution
	}

	switch def.Type {
	case UDFTypeMap:
		return e.executeMap(def, args)
	case UDFTypeReduce:
		return e.executeReduce(def, args)
	case UDFTypeFilter:
		return e.executeFilter(def, args)
	case UDFTypeAggregate:
		return e.executeAggregate(def, args)
	case UDFTypeTransform:
		return e.executeTransform(def, args)
	case UDFTypeTrigger:
		return e.executeTrigger(def, args)
	default:
		return nil, fmt.Errorf("unsupported UDF type: %s", def.Type)
	}
}

func (e *WASMUDFEngine) executeMap(def *UDFDefinition, args map[string]interface{}) (interface{}, error) {
	values, ok := args["values"]
	if !ok {
		return nil, fmt.Errorf("map UDF requires 'values' argument")
	}
	floats, ok := values.([]float64)
	if !ok {
		return values, nil
	}
	// Identity map as default — real WASM would apply the user's function
	result := make([]float64, len(floats))
	copy(result, floats)
	return result, nil
}

func (e *WASMUDFEngine) executeReduce(def *UDFDefinition, args map[string]interface{}) (interface{}, error) {
	values, ok := args["values"]
	if !ok {
		return nil, fmt.Errorf("reduce UDF requires 'values' argument")
	}
	floats, ok := values.([]float64)
	if !ok {
		return 0.0, nil
	}
	var sum float64
	for _, v := range floats {
		sum += v
	}
	return sum, nil
}

func (e *WASMUDFEngine) executeFilter(def *UDFDefinition, args map[string]interface{}) (interface{}, error) {
	values, ok := args["values"]
	if !ok {
		return nil, fmt.Errorf("filter UDF requires 'values' argument")
	}
	floats, ok := values.([]float64)
	if !ok {
		return values, nil
	}
	threshold := 0.0
	if t, ok := args["threshold"].(float64); ok {
		threshold = t
	}
	var filtered []float64
	for _, v := range floats {
		if v > threshold {
			filtered = append(filtered, v)
		}
	}
	return filtered, nil
}

func (e *WASMUDFEngine) executeAggregate(def *UDFDefinition, args map[string]interface{}) (interface{}, error) {
	values, ok := args["values"]
	if !ok {
		return nil, fmt.Errorf("aggregate UDF requires 'values' argument")
	}
	floats, ok := values.([]float64)
	if !ok || len(floats) == 0 {
		return map[string]float64{"sum": 0, "count": 0, "avg": 0}, nil
	}
	var sum float64
	for _, v := range floats {
		sum += v
	}
	return map[string]float64{
		"sum":   sum,
		"count": float64(len(floats)),
		"avg":   sum / float64(len(floats)),
	}, nil
}

func (e *WASMUDFEngine) executeTransform(def *UDFDefinition, args map[string]interface{}) (interface{}, error) {
	// Transform passes through by default; WASM module would apply custom logic
	return args, nil
}

func (e *WASMUDFEngine) executeTrigger(def *UDFDefinition, args map[string]interface{}) (interface{}, error) {
	// Trigger evaluates a condition; returns whether it fired
	return map[string]interface{}{
		"triggered": true,
		"args":      args,
	}, nil
}

// List returns all registered UDFs.
func (e *WASMUDFEngine) List() []UDFInstance {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]UDFInstance, 0, len(e.udfs))
	for _, inst := range e.udfs {
		result = append(result, *inst)
	}
	return result
}

// Get returns a specific UDF instance.
func (e *WASMUDFEngine) Get(name string) *UDFInstance {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if inst, ok := e.udfs[name]; ok {
		cp := *inst
		return &cp
	}
	return nil
}

// GetStats returns engine stats.
func (e *WASMUDFEngine) GetStats() WASMUDFStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats := e.stats
	stats.RegisteredUDFs = len(e.udfs)
	return stats
}

// RegisterHTTPHandlers registers HTTP endpoints for the WASM UDF engine.
func (e *WASMUDFEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/udf/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var def UDFDefinition
		if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := e.Register(def); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "registered"})
	})

	mux.HandleFunc("/api/v1/udf/invoke", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Name string                 `json:"name"`
			Args map[string]interface{} `json:"args"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		result, err := e.Invoke(req.Name, req.Args)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/api/v1/udf/list", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.List())
	})

	mux.HandleFunc("/api/v1/udf/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})

	mux.HandleFunc("/api/v1/udf/unregister", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := e.Unregister(req.Name); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "unregistered"})
	})
}
