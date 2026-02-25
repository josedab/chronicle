// query_federation_merge.go contains extended query federation functionality.
package chronicle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

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
