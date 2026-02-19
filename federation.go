package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"
)

// Federation enables querying across multiple Chronicle instances.
type Federation struct {
	local   *DB
	remotes map[string]*RemoteInstance
	mu      sync.RWMutex
	client  *http.Client
	config  FederationConfig
}

// RemoteInstance represents a remote Chronicle instance.
type RemoteInstance struct {
	Name     string
	URL      string
	Priority int // Lower is higher priority
	Healthy  bool
	LastPing time.Time
}

// FederationConfig configures federation behavior.
type FederationConfig struct {
	// Timeout for remote queries.
	Timeout time.Duration

	// RetryCount for failed remote queries.
	RetryCount int

	// HealthCheckInterval is how often to check remote health.
	HealthCheckInterval time.Duration

	// MaxConcurrentQueries limits parallel remote queries.
	MaxConcurrentQueries int

	// MergeStrategy determines how results are combined.
	MergeStrategy MergeStrategy
}

// MergeStrategy defines how federated results are merged.
type MergeStrategy int

const (
	// MergeStrategyUnion combines all results.
	MergeStrategyUnion MergeStrategy = iota
	// MergeStrategyFirst uses the first successful result.
	MergeStrategyFirst
	// MergeStrategyPriority uses results by instance priority.
	MergeStrategyPriority
)

// DefaultFederationConfig returns default federation configuration.
func DefaultFederationConfig() FederationConfig {
	return FederationConfig{
		Timeout:              30 * time.Second,
		RetryCount:           3,
		HealthCheckInterval:  time.Minute,
		MaxConcurrentQueries: 10,
		MergeStrategy:        MergeStrategyUnion,
	}
}

// NewFederation creates a federation coordinator.
func NewFederation(local *DB, config FederationConfig) *Federation {
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	if config.MaxConcurrentQueries <= 0 {
		config.MaxConcurrentQueries = 10
	}

	return &Federation{
		local:   local,
		remotes: make(map[string]*RemoteInstance),
		client: &http.Client{
			Timeout: config.Timeout,
		},
		config: config,
	}
}

// AddRemote adds a remote Chronicle instance.
func (f *Federation) AddRemote(name, remoteURL string, priority int) error {
	if name == "" {
		return errors.New("name is required")
	}
	if remoteURL == "" {
		return errors.New("url is required")
	}

	// Validate URL
	if _, err := url.Parse(remoteURL); err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.remotes[name] = &RemoteInstance{
		Name:     name,
		URL:      remoteURL,
		Priority: priority,
		Healthy:  true, // Assume healthy until proven otherwise
	}

	return nil
}

// RemoveRemote removes a remote instance.
func (f *Federation) RemoveRemote(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.remotes, name)
}

// ListRemotes returns all registered remotes.
func (f *Federation) ListRemotes() []*RemoteInstance {
	f.mu.RLock()
	defer f.mu.RUnlock()

	remotes := make([]*RemoteInstance, 0, len(f.remotes))
	for _, r := range f.remotes {
		remotes = append(remotes, &RemoteInstance{
			Name:     r.Name,
			URL:      r.URL,
			Priority: r.Priority,
			Healthy:  r.Healthy,
			LastPing: r.LastPing,
		})
	}

	// Sort by priority
	sort.Slice(remotes, func(i, j int) bool {
		return remotes[i].Priority < remotes[j].Priority
	})

	return remotes
}

// Query executes a federated query across all instances.
func (f *Federation) Query(ctx context.Context, query *Query) (*FederatedResult, error) {
	// Get remotes snapshot
	f.mu.RLock()
	remotes := make([]*RemoteInstance, 0, len(f.remotes))
	for _, r := range f.remotes {
		if r.Healthy {
			remotes = append(remotes, r)
		}
	}
	f.mu.RUnlock()

	// Execute local query
	localResult, localErr := f.local.Execute(query)

	// Execute remote queries in parallel
	results := make(chan *federatedQueryResult, len(remotes)+1)

	var wg sync.WaitGroup

	// Add local result
	wg.Add(1)
	go func() {
		defer wg.Done()
		results <- &federatedQueryResult{
			source: "local",
			result: localResult,
			err:    localErr,
		}
	}()

	// Limit concurrent queries
	sem := make(chan struct{}, f.config.MaxConcurrentQueries)

	for _, remote := range remotes {
		wg.Add(1)
		go func(r *RemoteInstance) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			result, err := f.queryRemote(ctx, r, query)
			results <- &federatedQueryResult{
				source: r.Name,
				result: result,
				err:    err,
			}
		}(remote)
	}

	// Wait for all queries to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	return f.mergeResults(results, len(remotes)+1)
}

type federatedQueryResult struct {
	source string
	result *Result
	err    error
}

// FederatedResult contains results from multiple sources.
type FederatedResult struct {
	// Points contains merged data points.
	Points []Point

	// Sources lists which instances contributed results.
	Sources []string

	// Errors contains any errors from individual sources.
	Errors map[string]error

	// TotalCount is the total number of points before merging.
	TotalCount int
}

func (f *Federation) queryRemote(ctx context.Context, remote *RemoteInstance, query *Query) (*Result, error) {
	// Build query URL
	u, err := url.Parse(remote.URL)
	if err != nil {
		return nil, err
	}
	u.Path = "/api/query"

	// Add query parameters
	q := u.Query()
	q.Set("metric", query.Metric)
	q.Set("start", fmt.Sprintf("%d", query.Start))
	q.Set("end", fmt.Sprintf("%d", query.End))
	u.RawQuery = q.Encode()

	// Make request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := f.client.Do(req)
	if err != nil {
		f.markUnhealthy(remote.Name)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("remote error: %s", string(body))
	}

	// Parse response
	var result Result
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	f.markHealthy(remote.Name)
	return &result, nil
}

func (f *Federation) mergeResults(results <-chan *federatedQueryResult, count int) (*FederatedResult, error) {
	merged := &FederatedResult{
		Errors: make(map[string]error),
	}

	var allPoints []Point
	collected := 0

	for result := range results {
		collected++

		if result.err != nil {
			merged.Errors[result.source] = result.err
			continue
		}

		if result.result != nil {
			merged.Sources = append(merged.Sources, result.source)
			allPoints = append(allPoints, result.result.Points...)
			merged.TotalCount += len(result.result.Points)
		}
	}

	// Merge based on strategy
	switch f.config.MergeStrategy {
	case MergeStrategyFirst:
		// Use first non-empty result
		if len(allPoints) > 0 {
			merged.Points = allPoints
		}
	case MergeStrategyPriority:
		// Already sorted by priority
		merged.Points = allPoints
	default: // MergeStrategyUnion
		// Sort by timestamp and deduplicate
		merged.Points = f.deduplicatePoints(allPoints)
	}

	if len(merged.Sources) == 0 && len(merged.Errors) > 0 {
		// All sources failed
		return merged, errors.New("all federated sources failed")
	}

	return merged, nil
}

func (f *Federation) deduplicatePoints(points []Point) []Point {
	if len(points) == 0 {
		return points
	}

	// Sort by timestamp
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// Deduplicate (same timestamp + metric + tags = duplicate)
	seen := make(map[string]bool)
	result := make([]Point, 0, len(points))

	for _, p := range points {
		key := fmt.Sprintf("%d:%s:%v", p.Timestamp, p.Metric, p.Tags)
		if !seen[key] {
			seen[key] = true
			result = append(result, p)
		}
	}

	return result
}

func (f *Federation) markHealthy(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if r, ok := f.remotes[name]; ok {
		r.Healthy = true
		r.LastPing = time.Now()
	}
}

func (f *Federation) markUnhealthy(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if r, ok := f.remotes[name]; ok {
		r.Healthy = false
	}
}

// CheckHealth checks the health of all remotes.
func (f *Federation) CheckHealth(ctx context.Context) map[string]bool {
	f.mu.RLock()
	remotes := make([]*RemoteInstance, 0, len(f.remotes))
	for _, r := range f.remotes {
		remotes = append(remotes, r)
	}
	f.mu.RUnlock()

	results := make(map[string]bool)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, remote := range remotes {
		wg.Add(1)
		go func(r *RemoteInstance) {
			defer wg.Done()

			healthy := f.pingRemote(ctx, r)

			mu.Lock()
			results[r.Name] = healthy
			mu.Unlock()

			if healthy {
				f.markHealthy(r.Name)
			} else {
				f.markUnhealthy(r.Name)
			}
		}(remote)
	}

	wg.Wait()
	return results
}

func (f *Federation) pingRemote(ctx context.Context, remote *RemoteInstance) bool {
	u, err := url.Parse(remote.URL)
	if err != nil {
		return false
	}
	u.Path = "/health"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// StartHealthChecker starts periodic health checking.
func (f *Federation) StartHealthChecker(ctx context.Context) {
	if f.config.HealthCheckInterval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(f.config.HealthCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				f.CheckHealth(ctx)
			}
		}
	}()
}

// Metrics returns aggregated metrics from all sources.
func (f *Federation) Metrics(ctx context.Context) ([]string, error) {
	// Get local metrics
	localMetrics := f.local.Metrics()

	// Get remote metrics
	f.mu.RLock()
	remotes := make([]*RemoteInstance, 0, len(f.remotes))
	for _, r := range f.remotes {
		if r.Healthy {
			remotes = append(remotes, r)
		}
	}
	f.mu.RUnlock()

	// Collect from remotes
	metricsSet := make(map[string]bool)
	for _, m := range localMetrics {
		metricsSet[m] = true
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, remote := range remotes {
		wg.Add(1)
		go func(r *RemoteInstance) {
			defer wg.Done()

			metrics, err := f.getRemoteMetrics(ctx, r)
			if err != nil {
				return
			}

			mu.Lock()
			for _, m := range metrics {
				metricsSet[m] = true
			}
			mu.Unlock()
		}(remote)
	}

	wg.Wait()

	// Convert to slice
	result := make([]string, 0, len(metricsSet))
	for m := range metricsSet {
		result = append(result, m)
	}
	sort.Strings(result)

	return result, nil
}

func (f *Federation) getRemoteMetrics(ctx context.Context, remote *RemoteInstance) ([]string, error) {
	u, err := url.Parse(remote.URL)
	if err != nil {
		return nil, err
	}
	u.Path = "/metrics"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var metrics []string
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		return nil, err
	}

	return metrics, nil
}
