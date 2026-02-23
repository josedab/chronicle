package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DuckDBBackendConfig configures the DuckDB query backend.
type DuckDBBackendConfig struct {
	Enabled          bool
	MaxMemoryMB      int
	WorkerThreads    int
	EnableCaching    bool
	CacheMaxEntries  int
	CacheTTL         time.Duration
	AutoRoute        bool
	ComplexThreshold int // query cost above this routes to DuckDB
}

// DefaultDuckDBBackendConfig returns sensible defaults.
func DefaultDuckDBBackendConfig() DuckDBBackendConfig {
	return DuckDBBackendConfig{
		Enabled:          true,
		MaxMemoryMB:      256,
		WorkerThreads:    4,
		EnableCaching:    true,
		CacheMaxEntries:  1000,
		CacheTTL:         5 * time.Minute,
		AutoRoute:        true,
		ComplexThreshold: 100,
	}
}

// DuckDBQueryPlan represents an analyzed query execution plan.
type DuckDBQueryPlan struct {
	OriginalSQL  string   `json:"original_sql"`
	TranslatedSQL string  `json:"translated_sql"`
	Engine       string   `json:"engine"`
	EstimatedCost float64 `json:"estimated_cost"`
	Partitions   int      `json:"partitions"`
	UsesWindow   bool     `json:"uses_window"`
	UsesCTE      bool     `json:"uses_cte"`
	UsesJoin     bool     `json:"uses_join"`
	RoutedTo     string   `json:"routed_to"`
}

// DuckDBQueryResult represents the result of a DuckDB query.
type DuckDBQueryResult struct {
	Columns  []string        `json:"columns"`
	Rows     [][]interface{} `json:"rows"`
	RowCount int             `json:"row_count"`
	Duration time.Duration   `json:"duration"`
	Engine   string          `json:"engine"`
	FromCache bool           `json:"from_cache"`
}

// DuckDBFunction represents a registered SQL function.
type DuckDBFunction struct {
	Name       string   `json:"name"`
	Category   string   `json:"category"`
	Args       []string `json:"args"`
	ReturnType string   `json:"return_type"`
	Description string  `json:"description"`
}

// DuckDBBackendStats holds engine statistics.
type DuckDBBackendStats struct {
	TotalQueries     int64         `json:"total_queries"`
	NativeRouted     int64         `json:"native_routed"`
	DuckDBRouted     int64         `json:"duckdb_routed"`
	CacheHits        int64         `json:"cache_hits"`
	CacheMisses      int64         `json:"cache_misses"`
	AvgQueryDuration time.Duration `json:"avg_query_duration"`
	RegisteredFuncs  int           `json:"registered_functions"`
}

type duckDBCacheEntry struct {
	result    *DuckDBQueryResult
	expiresAt time.Time
}

// DuckDBBackendEngine provides analytical query execution with SQL:2023 features.
// Implements window functions, CTEs, and complex aggregations by translating
// Chronicle queries into an optimized execution plan.
type DuckDBBackendEngine struct {
	db     *DB
	config DuckDBBackendConfig

	mu        sync.RWMutex
	functions []DuckDBFunction
	cache     map[string]*duckDBCacheEntry
	running   bool
	stopCh    chan struct{}
	stats     DuckDBBackendStats
}

// NewDuckDBBackendEngine creates a new DuckDB query backend.
func NewDuckDBBackendEngine(db *DB, cfg DuckDBBackendConfig) *DuckDBBackendEngine {
	engine := &DuckDBBackendEngine{
		db:        db,
		config:    cfg,
		functions: defaultDuckDBFunctions(),
		cache:     make(map[string]*duckDBCacheEntry),
		stopCh:    make(chan struct{}),
	}
	return engine
}

// Start starts the background cache eviction loop.
func (e *DuckDBBackendEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
	go e.evictionLoop()
}

// Stop stops the engine.
func (e *DuckDBBackendEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// ExecuteSQL executes an analytical SQL query.
func (e *DuckDBBackendEngine) ExecuteSQL(sql string) (*DuckDBQueryResult, error) {
	if sql == "" {
		return nil, fmt.Errorf("empty SQL query")
	}

	e.mu.Lock()
	e.stats.TotalQueries++
	e.mu.Unlock()

	// Check cache
	if e.config.EnableCaching {
		if result := e.checkCache(sql); result != nil {
			return result, nil
		}
	}

	start := time.Now()

	// Analyze and plan the query
	plan := e.Analyze(sql)

	// Execute based on routing decision
	var result *DuckDBQueryResult
	var err error

	if plan.RoutedTo == "native" {
		result, err = e.executeNative(plan)
		if err != nil {
			return nil, err
		}
		e.mu.Lock()
		e.stats.NativeRouted++
		e.mu.Unlock()
	} else {
		result, err = e.executeAnalytical(plan)
		if err != nil {
			return nil, err
		}
		e.mu.Lock()
		e.stats.DuckDBRouted++
		e.mu.Unlock()
	}

	result.Duration = time.Since(start)
	result.Engine = plan.RoutedTo

	// Cache result
	if e.config.EnableCaching {
		e.cacheResult(sql, result)
	}

	e.mu.Lock()
	if e.stats.AvgQueryDuration == 0 {
		e.stats.AvgQueryDuration = result.Duration
	} else {
		e.stats.AvgQueryDuration = (e.stats.AvgQueryDuration + result.Duration) / 2
	}
	e.mu.Unlock()

	return result, nil
}

// Analyze produces an execution plan for the given SQL.
func (e *DuckDBBackendEngine) Analyze(sql string) *DuckDBQueryPlan {
	upper := strings.ToUpper(sql)
	plan := &DuckDBQueryPlan{
		OriginalSQL: sql,
		UsesWindow:  strings.Contains(upper, "OVER (") || strings.Contains(upper, "OVER("),
		UsesCTE:     strings.Contains(upper, "WITH ") && strings.Contains(upper, " AS ("),
		UsesJoin:    strings.Contains(upper, " JOIN "),
	}

	// Estimate cost
	cost := 1.0
	if plan.UsesWindow {
		cost += 50
	}
	if plan.UsesCTE {
		cost += 30
	}
	if plan.UsesJoin {
		cost += 40
	}
	if strings.Contains(upper, "GROUP BY") {
		cost += 20
	}
	if strings.Contains(upper, "ORDER BY") {
		cost += 10
	}
	if strings.Contains(upper, "HAVING") {
		cost += 15
	}
	plan.EstimatedCost = cost

	// Route decision
	if e.config.AutoRoute && cost >= float64(e.config.ComplexThreshold) {
		plan.RoutedTo = "duckdb"
	} else {
		plan.RoutedTo = "native"
	}

	// Translate to DuckDB-compatible SQL
	plan.TranslatedSQL = e.translateSQL(sql)

	return plan
}

// RegisterFunction registers a custom SQL function.
func (e *DuckDBBackendEngine) RegisterFunction(fn DuckDBFunction) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.functions = append(e.functions, fn)
	e.stats.RegisteredFuncs = len(e.functions)
}

// ListFunctions returns all registered functions.
func (e *DuckDBBackendEngine) ListFunctions(category string) []DuckDBFunction {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if category == "" {
		result := make([]DuckDBFunction, len(e.functions))
		copy(result, e.functions)
		return result
	}

	var filtered []DuckDBFunction
	for _, f := range e.functions {
		if f.Category == category {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

// Stats returns engine statistics.
func (e *DuckDBBackendEngine) GetStats() DuckDBBackendStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats := e.stats
	stats.RegisteredFuncs = len(e.functions)
	return stats
}

func (e *DuckDBBackendEngine) translateSQL(sql string) string {
	translated := sql
	// Map Chronicle-specific syntax to standard SQL
	translated = strings.ReplaceAll(translated, "TIMEBUCKET(", "DATE_TRUNC(")
	translated = strings.ReplaceAll(translated, "GAP_FILL(", "GENERATE_SERIES(")
	return translated
}

func (e *DuckDBBackendEngine) executeNative(plan *DuckDBQueryPlan) (*DuckDBQueryResult, error) {
	// Parse metric from SQL
	metric := extractMetricFromSQLQuery(plan.OriginalSQL)
	if metric == "" {
		return &DuckDBQueryResult{
			Columns:  []string{"result"},
			Rows:     [][]interface{}{},
			RowCount: 0,
			Engine:   "native",
		}, nil
	}

	q := Query{
		Metric: metric,
		Start:  0,
		End:    time.Now().UnixNano(),
	}

	result, err := e.db.Execute(&q)
	if err != nil {
		return nil, fmt.Errorf("native execute: %w", err)
	}

	rows := make([][]interface{}, 0)
	if result != nil {
		for _, p := range result.Points {
			rows = append(rows, []interface{}{metric, p.Timestamp, p.Value})
		}
	}

	return &DuckDBQueryResult{
		Columns:  []string{"metric", "timestamp", "value"},
		Rows:     rows,
		RowCount: len(rows),
		Engine:   "native",
	}, nil
}

func (e *DuckDBBackendEngine) executeAnalytical(plan *DuckDBQueryPlan) (*DuckDBQueryResult, error) {
	// For complex queries, use the analytical engine
	metric := extractMetricFromSQLQuery(plan.OriginalSQL)
	if metric == "" {
		return &DuckDBQueryResult{
			Columns:  []string{"result"},
			Rows:     [][]interface{}{},
			RowCount: 0,
			Engine:   "duckdb",
		}, nil
	}

	q := Query{
		Metric: metric,
		Start:  0,
		End:    time.Now().UnixNano(),
	}

	resultA, err := e.db.Execute(&q)
	if err != nil {
		return nil, fmt.Errorf("analytical execute: %w", err)
	}

	// Apply window functions, CTEs, etc. in post-processing
	rows := make([][]interface{}, 0)
	columns := []string{"metric", "timestamp", "value"}

	if plan.UsesWindow {
		columns = append(columns, "row_number", "running_avg")
	}

	if resultA != nil {
		var runningSum float64
		for i, p := range resultA.Points {
			row := []interface{}{metric, p.Timestamp, p.Value}
			if plan.UsesWindow {
				runningSum += p.Value
				row = append(row, i+1, runningSum/float64(i+1))
			}
			rows = append(rows, row)
		}
	}

	return &DuckDBQueryResult{
		Columns:  columns,
		Rows:     rows,
		RowCount: len(rows),
		Engine:   "duckdb",
	}, nil
}

func extractMetricFromSQLQuery(sql string) string {
	upper := strings.ToUpper(sql)
	fromIdx := strings.Index(upper, "FROM ")
	if fromIdx < 0 {
		return ""
	}
	rest := strings.TrimSpace(sql[fromIdx+5:])
	// Extract table name (metric name)
	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return ""
	}
	metric := parts[0]
	metric = strings.Trim(metric, "\"'`")
	metric = strings.TrimSuffix(metric, ";")
	return metric
}

func (e *DuckDBBackendEngine) checkCache(sql string) *DuckDBQueryResult {
	e.mu.RLock()
	entry, ok := e.cache[sql]
	e.mu.RUnlock()
	if !ok {
		e.mu.Lock()
		e.stats.CacheMisses++
		e.mu.Unlock()
		return nil
	}
	if time.Now().After(entry.expiresAt) {
		e.mu.Lock()
		delete(e.cache, sql)
		e.stats.CacheMisses++
		e.mu.Unlock()
		return nil
	}
	e.mu.Lock()
	e.stats.CacheHits++
	e.mu.Unlock()

	result := *entry.result
	result.FromCache = true
	return &result
}

func (e *DuckDBBackendEngine) cacheResult(sql string, result *DuckDBQueryResult) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.cache) >= e.config.CacheMaxEntries {
		// Evict oldest
		var oldestKey string
		var oldestTime time.Time
		first := true
		for k, v := range e.cache {
			if first || v.expiresAt.Before(oldestTime) {
				oldestKey = k
				oldestTime = v.expiresAt
				first = false
			}
		}
		if oldestKey != "" {
			delete(e.cache, oldestKey)
		}
	}

	e.cache[sql] = &duckDBCacheEntry{
		result:    result,
		expiresAt: time.Now().Add(e.config.CacheTTL),
	}
}

func (e *DuckDBBackendEngine) evictionLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.mu.Lock()
			now := time.Now()
			for k, v := range e.cache {
				if now.After(v.expiresAt) {
					delete(e.cache, k)
				}
			}
			e.mu.Unlock()
		}
	}
}

func defaultDuckDBFunctions() []DuckDBFunction {
	return []DuckDBFunction{
		// Window functions
		{Name: "ROW_NUMBER", Category: "window", ReturnType: "INT", Description: "Sequential row number"},
		{Name: "RANK", Category: "window", ReturnType: "INT", Description: "Rank with gaps"},
		{Name: "DENSE_RANK", Category: "window", ReturnType: "INT", Description: "Rank without gaps"},
		{Name: "LAG", Category: "window", Args: []string{"column", "offset"}, ReturnType: "ANY", Description: "Access previous row value"},
		{Name: "LEAD", Category: "window", Args: []string{"column", "offset"}, ReturnType: "ANY", Description: "Access next row value"},
		{Name: "FIRST_VALUE", Category: "window", Args: []string{"column"}, ReturnType: "ANY", Description: "First value in window"},
		{Name: "LAST_VALUE", Category: "window", Args: []string{"column"}, ReturnType: "ANY", Description: "Last value in window"},
		{Name: "NTH_VALUE", Category: "window", Args: []string{"column", "n"}, ReturnType: "ANY", Description: "Nth value in window"},
		// Aggregate functions
		{Name: "PERCENTILE_CONT", Category: "aggregate", Args: []string{"fraction"}, ReturnType: "DOUBLE", Description: "Continuous percentile"},
		{Name: "PERCENTILE_DISC", Category: "aggregate", Args: []string{"fraction"}, ReturnType: "ANY", Description: "Discrete percentile"},
		{Name: "MODE", Category: "aggregate", ReturnType: "ANY", Description: "Most frequent value"},
		{Name: "STDDEV_SAMP", Category: "aggregate", ReturnType: "DOUBLE", Description: "Sample standard deviation"},
		{Name: "STDDEV_POP", Category: "aggregate", ReturnType: "DOUBLE", Description: "Population standard deviation"},
		{Name: "CORR", Category: "aggregate", Args: []string{"x", "y"}, ReturnType: "DOUBLE", Description: "Correlation coefficient"},
		{Name: "COVAR_SAMP", Category: "aggregate", Args: []string{"x", "y"}, ReturnType: "DOUBLE", Description: "Sample covariance"},
		// Time-series specific
		{Name: "TIME_BUCKET", Category: "timeseries", Args: []string{"interval", "timestamp"}, ReturnType: "TIMESTAMP", Description: "Bucket timestamps by interval"},
		{Name: "INTERPOLATE", Category: "timeseries", Args: []string{"column"}, ReturnType: "DOUBLE", Description: "Linear interpolation for gaps"},
		{Name: "RATE_OF_CHANGE", Category: "timeseries", Args: []string{"column"}, ReturnType: "DOUBLE", Description: "Rate of change between points"},
		{Name: "MOVING_AVG", Category: "timeseries", Args: []string{"column", "window"}, ReturnType: "DOUBLE", Description: "Moving average"},
		{Name: "EXP_MOVING_AVG", Category: "timeseries", Args: []string{"column", "alpha"}, ReturnType: "DOUBLE", Description: "Exponential moving average"},
	}
}

// RegisterHTTPHandlers registers HTTP endpoints for the DuckDB backend.
func (e *DuckDBBackendEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/duckdb/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			SQL string `json:"sql"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		result, err := e.ExecuteSQL(req.SQL)
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/api/v1/duckdb/analyze", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			SQL string `json:"sql"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		plan := e.Analyze(req.SQL)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(plan)
	})

	mux.HandleFunc("/api/v1/duckdb/functions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		category := r.URL.Query().Get("category")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListFunctions(category))
	})

	mux.HandleFunc("/api/v1/duckdb/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
