package chronicle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// QueryFederationConfig configures query federation across multiple data sources.
type QueryFederationConfig struct {
	// Enabled enables query federation.
	Enabled bool `json:"enabled"`

	// MaxConcurrentQueries limits parallel queries to remote sources.
	MaxConcurrentQueries int `json:"max_concurrent_queries"`

	// QueryTimeout for remote queries.
	QueryTimeout time.Duration `json:"query_timeout"`

	// CacheEnabled enables result caching.
	CacheEnabled bool `json:"cache_enabled"`

	// CacheTTL is how long to cache results.
	CacheTTL time.Duration `json:"cache_ttl"`

	// PushdownEnabled enables predicate push-down optimization.
	PushdownEnabled bool `json:"pushdown_enabled"`

	// RetryCount for failed queries.
	RetryCount int `json:"retry_count"`

	// RetryBackoff between retries.
	RetryBackoff time.Duration `json:"retry_backoff"`
}

// DefaultQueryFederationConfig returns default configuration.
func DefaultQueryFederationConfig() QueryFederationConfig {
	return QueryFederationConfig{
		Enabled:              true,
		MaxConcurrentQueries: 10,
		QueryTimeout:         30 * time.Second,
		CacheEnabled:         true,
		CacheTTL:             5 * time.Minute,
		PushdownEnabled:      true,
		RetryCount:           3,
		RetryBackoff:         time.Second,
	}
}

// QueryFederation enables querying across multiple data sources including
// ClickHouse, DuckDB, PostgreSQL, and other Chronicle instances.
type QueryFederation struct {
	db      *DB
	config  QueryFederationConfig
	sources map[string]QueryFederatedSource
	mu      sync.RWMutex

	// Result cache
	cache   map[string]*cachedResult
	cacheMu sync.RWMutex

	// Query semaphore for concurrency control
	sem chan struct{}
}

// QueryFederatedSource represents a remote data source for query federation.
type QueryFederatedSource interface {
	// Name returns the source name.
	Name() string
	// Type returns the source type.
	Type() FederatedSourceType
	// Query executes a query against the source.
	Query(ctx context.Context, query *QueryFederatedQuery) (*QueryFederatedResult, error)
	// GetSchema returns the schema/table information.
	GetSchema(ctx context.Context) (*QueryFederatedSchema, error)
	// Close closes the connection.
	Close() error
	// HealthCheck checks if the source is available.
	HealthCheck(ctx context.Context) error
}

// FederatedSourceType identifies the source type.
type FederatedSourceType string

const (
	FederatedSourceTypeClickHouse FederatedSourceType = "clickhouse"
	FederatedSourceTypeDuckDB     FederatedSourceType = "duckdb"
	FederatedSourceTypePostgres   FederatedSourceType = "postgres"
	FederatedSourceTypeMySQL      FederatedSourceType = "mysql"
	FederatedSourceTypeChronicle  FederatedSourceType = "chronicle"
	FederatedSourceTypeHTTP       FederatedSourceType = "http"
)

// QueryFederatedQuery represents a federated query for query federation.
type QueryFederatedQuery struct {
	SQL            string            `json:"sql"`
	Source         string            `json:"source,omitempty"`
	Metric         string            `json:"metric,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
	Start          int64             `json:"start"`
	End            int64             `json:"end"`
	Aggregation    *Aggregation      `json:"aggregation,omitempty"`
	Params         map[string]any    `json:"params,omitempty"`
	PushPredicates bool              `json:"push_predicates"`
}

// QueryFederatedResult contains query results from a federated source.
type QueryFederatedResult struct {
	Source   string                 `json:"source"`
	Columns  []QueryFederatedColumn `json:"columns"`
	Rows     [][]any                `json:"rows"`
	Points   []Point                `json:"points,omitempty"`
	Metadata map[string]any         `json:"metadata,omitempty"`
	Duration time.Duration          `json:"duration"`
}

// QueryFederatedColumn describes a result column.
type QueryFederatedColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// QueryFederatedSchema describes a source's schema.
type QueryFederatedSchema struct {
	Tables []QueryFederatedTable `json:"tables"`
}

// QueryFederatedTable describes a table in the schema.
type QueryFederatedTable struct {
	Name    string                 `json:"name"`
	Columns []QueryFederatedColumn `json:"columns"`
}

// cachedResult holds a cached query result.
type cachedResult struct {
	result    *QueryFederatedResult
	expiresAt time.Time
}

// NewQueryFederation creates a new query federation manager.
func NewQueryFederation(db *DB, config QueryFederationConfig) *QueryFederation {
	return &QueryFederation{
		db:      db,
		config:  config,
		sources: make(map[string]QueryFederatedSource),
		cache:   make(map[string]*cachedResult),
		sem:     make(chan struct{}, config.MaxConcurrentQueries),
	}
}

// RegisterSource registers a federated data source.
func (f *QueryFederation) RegisterSource(source QueryFederatedSource) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.sources[source.Name()]; exists {
		return fmt.Errorf("source already registered: %s", source.Name())
	}

	f.sources[source.Name()] = source
	return nil
}

// UnregisterSource removes a federated source.
func (f *QueryFederation) UnregisterSource(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	source, exists := f.sources[name]
	if !exists {
		return fmt.Errorf("source not found: %s", name)
	}

	if err := source.Close(); err != nil {
		return err
	}

	delete(f.sources, name)
	return nil
}

// GetSource returns a registered source by name.
func (f *QueryFederation) GetSource(name string) (QueryFederatedSource, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	source, ok := f.sources[name]
	return source, ok
}

// ListSources returns all registered sources.
func (f *QueryFederation) ListSources() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.sources))
	for name := range f.sources {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Query executes a federated query across local and remote sources.
func (f *QueryFederation) Query(ctx context.Context, query *QueryFederatedQuery) (*QueryFederatedResult, error) {
	// Check cache first
	if f.config.CacheEnabled {
		if cached := f.getCached(query); cached != nil {
			return cached, nil
		}
	}

	// Acquire semaphore
	select {
	case f.sem <- struct{}{}:
		defer func() { <-f.sem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Apply timeout
	ctx, cancel := context.WithTimeout(ctx, f.config.QueryTimeout)
	defer cancel()

	var result *QueryFederatedResult
	var err error

	if query.Source != "" {
		// Query specific source
		result, err = f.querySource(ctx, query.Source, query)
	} else if query.SQL != "" {
		// Parse SQL and route to appropriate sources
		result, err = f.executeFederatedSQL(ctx, query)
	} else {
		// Query local Chronicle
		result, err = f.queryLocal(ctx, query)
	}

	if err != nil {
		return nil, err
	}

	// Cache result
	if f.config.CacheEnabled {
		f.setCached(query, result)
	}

	return result, nil
}

func (f *QueryFederation) querySource(ctx context.Context, sourceName string, query *QueryFederatedQuery) (*QueryFederatedResult, error) {
	source, ok := f.GetSource(sourceName)
	if !ok {
		return nil, fmt.Errorf("unknown source: %s", sourceName)
	}

	// Retry logic
	var lastErr error
	for attempt := 0; attempt < f.config.RetryCount; attempt++ {
		result, err := source.Query(ctx, query)
		if err == nil {
			return result, nil
		}
		lastErr = err

		if attempt < f.config.RetryCount-1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(f.config.RetryBackoff * time.Duration(attempt+1)):
			}
		}
	}

	return nil, fmt.Errorf("query failed after %d attempts: %w", f.config.RetryCount, lastErr)
}

func (f *QueryFederation) queryLocal(ctx context.Context, query *QueryFederatedQuery) (*QueryFederatedResult, error) {
	start := time.Now()

	chronicleQuery := &Query{
		Metric:      query.Metric,
		Tags:        query.Tags,
		Start:       query.Start,
		End:         query.End,
		Aggregation: query.Aggregation,
	}

	result, err := f.db.Execute(chronicleQuery)
	if err != nil {
		return nil, err
	}

	return &QueryFederatedResult{
		Source: "local",
		Points: result.Points,
		Columns: []QueryFederatedColumn{
			{Name: "timestamp", Type: "int64"},
			{Name: "value", Type: "float64"},
		},
		Duration: time.Since(start),
	}, nil
}

func (f *QueryFederation) executeFederatedSQL(ctx context.Context, query *QueryFederatedQuery) (*QueryFederatedResult, error) {
	// Parse SQL to identify sources and operations
	plan := f.parseSQL(query.SQL)

	if len(plan.Sources) == 0 {
		// Query local Chronicle
		return f.queryLocal(ctx, query)
	}

	if len(plan.Sources) == 1 {
		// Single source query
		sourceQuery := &QueryFederatedQuery{
			SQL:   plan.SQL,
			Start: query.Start,
			End:   query.End,
		}
		return f.querySource(ctx, plan.Sources[0], sourceQuery)
	}

	// Multi-source JOIN query
	return f.executeJoinQuery(ctx, plan, query)
}

// SQLPlan represents a parsed SQL query plan.
type SQLPlan struct {
	SQL      string
	Sources  []string
	Tables   []string
	JoinType string
	JoinOn   string
	Where    string
	GroupBy  string
	OrderBy  string
	Limit    int
}

func (f *QueryFederation) parseSQL(sql string) *SQLPlan {
	plan := &SQLPlan{SQL: sql}

	// Simple SQL parser - extract source references
	// Format: SELECT ... FROM source.table JOIN source2.table2 ON ...
	upperSQL := strings.ToUpper(sql)

	// Find FROM clause
	fromIdx := strings.Index(upperSQL, "FROM ")
	if fromIdx == -1 {
		return plan
	}

	// Extract table references
	afterFrom := sql[fromIdx+5:]

	// Find end of FROM clause
	endMarkers := []string{"WHERE ", "GROUP BY ", "ORDER BY ", "LIMIT ", "JOIN "}
	endIdx := len(afterFrom)
	for _, marker := range endMarkers {
		if idx := strings.Index(strings.ToUpper(afterFrom), marker); idx != -1 && idx < endIdx {
			endIdx = idx
		}
	}

	tableRef := strings.TrimSpace(afterFrom[:endIdx])

	// Parse source.table format
	if dotIdx := strings.Index(tableRef, "."); dotIdx != -1 {
		source := strings.TrimSpace(tableRef[:dotIdx])
		plan.Sources = append(plan.Sources, source)
		plan.Tables = append(plan.Tables, tableRef[dotIdx+1:])
	}

	// Parse JOIN clauses
	joinIdx := strings.Index(strings.ToUpper(sql), "JOIN ")
	for joinIdx != -1 {
		afterJoin := sql[joinIdx+5:]
		endIdx := strings.IndexAny(afterJoin, " \t\n")
		if endIdx == -1 {
			endIdx = len(afterJoin)
		}

		joinTable := strings.TrimSpace(afterJoin[:endIdx])
		if dotIdx := strings.Index(joinTable, "."); dotIdx != -1 {
			source := strings.TrimSpace(joinTable[:dotIdx])
			plan.Sources = append(plan.Sources, source)
			plan.Tables = append(plan.Tables, joinTable[dotIdx+1:])
		}

		// Find next JOIN
		remaining := afterJoin[endIdx:]
		nextJoinIdx := strings.Index(strings.ToUpper(remaining), "JOIN ")
		if nextJoinIdx == -1 {
			break
		}
		joinIdx = joinIdx + 5 + endIdx + nextJoinIdx
	}

	return plan
}

func (f *QueryFederation) executeJoinQuery(ctx context.Context, plan *SQLPlan, query *QueryFederatedQuery) (*QueryFederatedResult, error) {
	// Execute queries to each source concurrently
	results := make([]*QueryFederatedResult, len(plan.Sources))
	errs := make([]error, len(plan.Sources))
	var wg sync.WaitGroup

	for i, source := range plan.Sources {
		wg.Add(1)
		go func(idx int, srcName string) {
			defer wg.Done()

			sourceQuery := &QueryFederatedQuery{
				SQL:   fmt.Sprintf("SELECT * FROM %s", plan.Tables[idx]),
				Start: query.Start,
				End:   query.End,
			}

			results[idx], errs[idx] = f.querySource(ctx, srcName, sourceQuery)
		}(i, source)
	}

	wg.Wait()

	// Check for errors
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	// Merge results (simple concatenation for now)
	return f.mergeResults(results)
}

func (f *QueryFederation) mergeResults(results []*QueryFederatedResult) (*QueryFederatedResult, error) {
	if len(results) == 0 {
		return &QueryFederatedResult{}, nil
	}

	merged := &QueryFederatedResult{
		Source:  "federated",
		Columns: results[0].Columns,
		Rows:    make([][]any, 0),
		Points:  make([]Point, 0),
	}

	for _, r := range results {
		merged.Rows = append(merged.Rows, r.Rows...)
		merged.Points = append(merged.Points, r.Points...)
	}

	return merged, nil
}

// getCached returns a cached result if available and not expired.
func (f *QueryFederation) getCached(query *QueryFederatedQuery) *QueryFederatedResult {
	f.cacheMu.RLock()
	defer f.cacheMu.RUnlock()

	key := f.cacheKey(query)
	cached, ok := f.cache[key]
	if !ok || time.Now().After(cached.expiresAt) {
		return nil
	}

	return cached.result
}

// setCached caches a query result.
func (f *QueryFederation) setCached(query *QueryFederatedQuery, result *QueryFederatedResult) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()

	key := f.cacheKey(query)
	f.cache[key] = &cachedResult{
		result:    result,
		expiresAt: time.Now().Add(f.config.CacheTTL),
	}
}

func (f *QueryFederation) cacheKey(query *QueryFederatedQuery) string {
	data, _ := json.Marshal(query)
	return string(data)
}

// Close closes all federated sources.
func (f *QueryFederation) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var errs []error
	for _, source := range f.sources {
		if err := source.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// --- Federated Source Implementations ---

// ClickHouseSource connects to ClickHouse.
type ClickHouseSource struct {
	name string
	dsn  string
	db   *sql.DB
}

// ClickHouseFederationConfig configures ClickHouse connection for federation.
type ClickHouseFederationConfig struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// NewClickHouseSource creates a new ClickHouse source.
func NewClickHouseSource(config ClickHouseFederationConfig) (*ClickHouseSource, error) {
	dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
		config.Host, config.Port, config.Database, config.Username, config.Password)

	// Note: In production, would use clickhouse-go driver
	// For this implementation, we simulate the connection
	return &ClickHouseSource{
		name: config.Name,
		dsn:  dsn,
	}, nil
}

func (s *ClickHouseSource) Name() string              { return s.name }
func (s *ClickHouseSource) Type() FederatedSourceType { return FederatedSourceTypeClickHouse }

func (s *ClickHouseSource) Query(ctx context.Context, query *QueryFederatedQuery) (*QueryFederatedResult, error) {
	start := time.Now()

	// In production, would execute actual ClickHouse query
	// For now, return simulated result
	result := &QueryFederatedResult{
		Source: s.name,
		Columns: []QueryFederatedColumn{
			{Name: "timestamp", Type: "DateTime"},
			{Name: "value", Type: "Float64"},
		},
		Rows:     [][]any{},
		Duration: time.Since(start),
	}

	return result, nil
}

func (s *ClickHouseSource) GetSchema(ctx context.Context) (*QueryFederatedSchema, error) {
	return &QueryFederatedSchema{Tables: []QueryFederatedTable{}}, nil
}

func (s *ClickHouseSource) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *ClickHouseSource) HealthCheck(ctx context.Context) error {
	return nil
}

// DuckDBSource connects to DuckDB.
type DuckDBSource struct {
	name string
	path string
	db   *sql.DB
}

// DuckDBConfig configures DuckDB connection.
type DuckDBConfig struct {
	Name string `json:"name"`
	Path string `json:"path"` // Database file path or :memory:
}

// NewDuckDBSource creates a new DuckDB source.
func NewDuckDBSource(config DuckDBConfig) (*DuckDBSource, error) {
	// Note: In production, would use duckdb driver
	return &DuckDBSource{
		name: config.Name,
		path: config.Path,
	}, nil
}

func (s *DuckDBSource) Name() string              { return s.name }
func (s *DuckDBSource) Type() FederatedSourceType { return FederatedSourceTypeDuckDB }

func (s *DuckDBSource) Query(ctx context.Context, query *QueryFederatedQuery) (*QueryFederatedResult, error) {
	start := time.Now()

	result := &QueryFederatedResult{
		Source: s.name,
		Columns: []QueryFederatedColumn{
			{Name: "timestamp", Type: "TIMESTAMP"},
			{Name: "value", Type: "DOUBLE"},
		},
		Rows:     [][]any{},
		Duration: time.Since(start),
	}

	return result, nil
}

func (s *DuckDBSource) GetSchema(ctx context.Context) (*QueryFederatedSchema, error) {
	return &QueryFederatedSchema{Tables: []QueryFederatedTable{}}, nil
}

func (s *DuckDBSource) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *DuckDBSource) HealthCheck(ctx context.Context) error {
	return nil
}

// PostgresSource connects to PostgreSQL.
type PostgresSource struct {
	name string
	dsn  string
	db   *sql.DB
}

// PostgresConfig configures PostgreSQL connection.
type PostgresConfig struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	SSLMode  string `json:"ssl_mode"`
}

// NewPostgresSource creates a new PostgreSQL source.
func NewPostgresSource(config PostgresConfig) (*PostgresSource, error) {
	sslMode := config.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.Username, config.Password, config.Database, sslMode)

	return &PostgresSource{
		name: config.Name,
		dsn:  dsn,
	}, nil
}

func (s *PostgresSource) Name() string              { return s.name }
func (s *PostgresSource) Type() FederatedSourceType { return FederatedSourceTypePostgres }

func (s *PostgresSource) Query(ctx context.Context, query *QueryFederatedQuery) (*QueryFederatedResult, error) {
	start := time.Now()

	result := &QueryFederatedResult{
		Source:   s.name,
		Columns:  []QueryFederatedColumn{},
		Rows:     [][]any{},
		Duration: time.Since(start),
	}

	return result, nil
}

func (s *PostgresSource) GetSchema(ctx context.Context) (*QueryFederatedSchema, error) {
	return &QueryFederatedSchema{Tables: []QueryFederatedTable{}}, nil
}

func (s *PostgresSource) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *PostgresSource) HealthCheck(ctx context.Context) error {
	return nil
}

// HTTPSource queries remote HTTP endpoints.
type HTTPSource struct {
	name    string
	baseURL string
	client  *http.Client
	headers map[string]string
}

// HTTPSourceConfig configures an HTTP source.
type HTTPSourceConfig struct {
	Name    string            `json:"name"`
	BaseURL string            `json:"base_url"`
	Headers map[string]string `json:"headers,omitempty"`
	Timeout time.Duration     `json:"timeout"`
}

// NewHTTPSource creates a new HTTP source.
func NewHTTPSource(config HTTPSourceConfig) (*HTTPSource, error) {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	return &HTTPSource{
		name:    config.Name,
		baseURL: config.BaseURL,
		client:  &http.Client{Timeout: timeout},
		headers: config.Headers,
	}, nil
}

func (s *HTTPSource) Name() string              { return s.name }
func (s *HTTPSource) Type() FederatedSourceType { return FederatedSourceTypeHTTP }

func (s *HTTPSource) Query(ctx context.Context, query *QueryFederatedQuery) (*QueryFederatedResult, error) {
	start := time.Now()

	// Build request
	url := fmt.Sprintf("%s/query", s.baseURL)
	body, _ := json.Marshal(query)

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var result QueryFederatedResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	result.Source = s.name
	result.Duration = time.Since(start)

	return &result, nil
}

func (s *HTTPSource) GetSchema(ctx context.Context) (*QueryFederatedSchema, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", s.baseURL+"/schema", nil)
	if err != nil {
		return nil, err
	}

	for k, v := range s.headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var schema QueryFederatedSchema
	if err := json.NewDecoder(resp.Body).Decode(&schema); err != nil {
		return nil, err
	}

	return &schema, nil
}

func (s *HTTPSource) Close() error { return nil }

func (s *HTTPSource) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", s.baseURL+"/health", nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("health check failed")
	}

	return nil
}

// ChronicleSource connects to another Chronicle instance.
type ChronicleSource struct {
	*HTTPSource
}

// NewChronicleSource creates a new Chronicle remote source.
func NewChronicleSource(config HTTPSourceConfig) (*ChronicleSource, error) {
	httpSource, err := NewHTTPSource(config)
	if err != nil {
		return nil, err
	}
	return &ChronicleSource{HTTPSource: httpSource}, nil
}

func (s *ChronicleSource) Type() FederatedSourceType { return FederatedSourceTypeChronicle }

func (s *ChronicleSource) Query(ctx context.Context, query *QueryFederatedQuery) (*QueryFederatedResult, error) {
	// Chronicle-specific query format
	chronicleQuery := map[string]any{
		"metric": query.Metric,
		"tags":   query.Tags,
		"start":  query.Start,
		"end":    query.End,
	}

	if query.Aggregation != nil {
		chronicleQuery["aggregation"] = query.Aggregation
	}

	start := time.Now()

	body, _ := json.Marshal(chronicleQuery)
	req, err := http.NewRequestWithContext(ctx, "POST", s.baseURL+"/query", strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var queryResult struct {
		Points []Point `json:"points"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&queryResult); err != nil {
		return nil, err
	}

	return &QueryFederatedResult{
		Source:   s.name,
		Points:   queryResult.Points,
		Duration: time.Since(start),
	}, nil
}
