package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

// DistributedQueryConfig configures the distributed query coordinator.
type DistributedQueryConfig struct {
	Enabled        bool
	MaxFanOut      int
	PartialTimeout time.Duration
	MergeStrategy  string // "sort_preserve", "round_robin", "priority"
	MaxResultSize  int
	RetryAttempts  int
}

// DefaultDistributedQueryConfig returns sensible defaults.
func DefaultDistributedQueryConfig() DistributedQueryConfig {
	return DistributedQueryConfig{
		Enabled:        true,
		MaxFanOut:      10,
		PartialTimeout: 5 * time.Second,
		MergeStrategy:  "sort_preserve",
		MaxResultSize:  100000,
		RetryAttempts:  2,
	}
}

// PartialResult represents a query result from a single node.
type PartialResult struct {
	NodeID     string        `json:"node_id"`
	Points     []Point       `json:"points"`
	PointCount int           `json:"point_count"`
	Duration   time.Duration `json:"duration"`
	Error      string        `json:"error,omitempty"`
}

// DistributedQueryPlan describes how a query will be distributed.
type DistributedQueryPlan struct {
	QueryID     string   `json:"query_id"`
	TargetNodes []string `json:"target_nodes"`
	Metric      string   `json:"metric"`
	FanOut      int      `json:"fan_out"`
	Strategy    string   `json:"merge_strategy"`
}

// DistributedQueryResult holds the merged result of a distributed query.
type DistributedQueryResult struct {
	Points        []Point         `json:"points"`
	TotalPoints   int             `json:"total_points"`
	NodesQueried  int             `json:"nodes_queried"`
	NodesResponded int            `json:"nodes_responded"`
	NodesFailed   int             `json:"nodes_failed"`
	MergeTime     time.Duration   `json:"merge_time"`
	TotalTime     time.Duration   `json:"total_time"`
	PartialResults []PartialResult `json:"partial_results,omitempty"`
}

// DistributedQueryStats holds coordinator statistics.
type DistributedQueryStats struct {
	TotalQueries    int64         `json:"total_queries"`
	SuccessfulMerges int64       `json:"successful_merges"`
	PartialMerges   int64         `json:"partial_merges"`
	FailedQueries   int64         `json:"failed_queries"`
	AvgFanOut       float64       `json:"avg_fan_out"`
	AvgMergeTime    time.Duration `json:"avg_merge_time"`
	AvgTotalTime    time.Duration `json:"avg_total_time"`
}

// DistributedQueryExecutor is a function that executes a query on a specific node.
type DistributedQueryExecutor func(ctx context.Context, nodeID string, q *Query) (*Result, error)

// DistributedQueryCoordinator coordinates scatter-gather queries across cluster nodes.
type DistributedQueryCoordinator struct {
	db     *DB
	config DistributedQueryConfig

	mu       sync.RWMutex
	executor DistributedQueryExecutor
	running  bool
	stopCh   chan struct{}
	stats    DistributedQueryStats
	querySeq int64
}

// NewDistributedQueryCoordinator creates a new coordinator.
func NewDistributedQueryCoordinator(db *DB, cfg DistributedQueryConfig) *DistributedQueryCoordinator {
	c := &DistributedQueryCoordinator{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
	// Default executor: run locally
	c.executor = func(ctx context.Context, nodeID string, q *Query) (*Result, error) {
		return db.ExecuteContext(ctx, q)
	}
	return c
}

// Start starts the coordinator.
func (c *DistributedQueryCoordinator) Start() {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return
	}
	c.running = true
	c.mu.Unlock()
}

// Stop stops the coordinator.
func (c *DistributedQueryCoordinator) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return
	}
	c.running = false
	close(c.stopCh)
}

// SetExecutor sets a custom query executor for remote node execution.
func (c *DistributedQueryCoordinator) SetExecutor(exec DistributedQueryExecutor) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.executor = exec
}

// Plan creates a distributed query plan.
func (c *DistributedQueryCoordinator) Plan(q *Query, nodes []string) *DistributedQueryPlan {
	c.mu.Lock()
	c.querySeq++
	qid := fmt.Sprintf("dq-%d", c.querySeq)
	c.mu.Unlock()

	fanOut := len(nodes)
	if fanOut > c.config.MaxFanOut {
		fanOut = c.config.MaxFanOut
		nodes = nodes[:fanOut]
	}

	return &DistributedQueryPlan{
		QueryID:     qid,
		TargetNodes: nodes,
		Metric:      q.Metric,
		FanOut:      fanOut,
		Strategy:    c.config.MergeStrategy,
	}
}

// Execute runs a scatter-gather query across the given nodes.
func (c *DistributedQueryCoordinator) Execute(ctx context.Context, q *Query, nodes []string) (*DistributedQueryResult, error) {
	if q == nil {
		return nil, fmt.Errorf("nil query")
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no target nodes")
	}

	start := time.Now()
	plan := c.Plan(q, nodes)

	c.mu.Lock()
	c.stats.TotalQueries++
	exec := c.executor
	c.mu.Unlock()

	// Scatter: send query to all nodes concurrently
	resultCh := make(chan PartialResult, len(plan.TargetNodes))
	var wg sync.WaitGroup

	queryCtx, cancel := context.WithTimeout(ctx, c.config.PartialTimeout)
	defer cancel()

	for _, nodeID := range plan.TargetNodes {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()
			nodeStart := time.Now()
			pr := PartialResult{NodeID: nid}

			select {
			case <-queryCtx.Done():
				pr.Error = queryCtx.Err().Error()
				pr.Duration = time.Since(nodeStart)
				resultCh <- pr
				return
			default:
			}

			result, err := exec(queryCtx, nid, q)
			pr.Duration = time.Since(nodeStart)

			if err != nil {
				pr.Error = err.Error()
			} else if result != nil {
				pr.Points = result.Points
				pr.PointCount = len(result.Points)
			}
			resultCh <- pr
		}(nodeID)
	}

	// Wait for all goroutines, then close channel
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Gather: collect partial results
	var partials []PartialResult
	for pr := range resultCh {
		partials = append(partials, pr)
	}

	// Merge
	mergeStart := time.Now()
	merged := c.merge(partials, c.config.MergeStrategy)
	mergeTime := time.Since(mergeStart)

	nodesResponded := 0
	nodesFailed := 0
	for _, pr := range partials {
		if pr.Error != "" {
			nodesFailed++
		} else {
			nodesResponded++
		}
	}

	totalTime := time.Since(start)

	// Cap result size
	if len(merged) > c.config.MaxResultSize {
		merged = merged[:c.config.MaxResultSize]
	}

	result := &DistributedQueryResult{
		Points:         merged,
		TotalPoints:    len(merged),
		NodesQueried:   len(plan.TargetNodes),
		NodesResponded: nodesResponded,
		NodesFailed:    nodesFailed,
		MergeTime:      mergeTime,
		TotalTime:      totalTime,
		PartialResults: partials,
	}

	// Update stats
	c.mu.Lock()
	if nodesFailed == 0 {
		c.stats.SuccessfulMerges++
	} else if nodesResponded > 0 {
		c.stats.PartialMerges++
	} else {
		c.stats.FailedQueries++
	}
	totalQ := c.stats.TotalQueries
	if totalQ > 0 {
		c.stats.AvgFanOut = (c.stats.AvgFanOut*float64(totalQ-1) + float64(len(plan.TargetNodes))) / float64(totalQ)
	}
	if c.stats.AvgMergeTime == 0 {
		c.stats.AvgMergeTime = mergeTime
	} else {
		c.stats.AvgMergeTime = (c.stats.AvgMergeTime + mergeTime) / 2
	}
	if c.stats.AvgTotalTime == 0 {
		c.stats.AvgTotalTime = totalTime
	} else {
		c.stats.AvgTotalTime = (c.stats.AvgTotalTime + totalTime) / 2
	}
	c.mu.Unlock()

	return result, nil
}

// merge combines partial results using the specified strategy.
func (c *DistributedQueryCoordinator) merge(partials []PartialResult, strategy string) []Point {
	switch strategy {
	case "sort_preserve":
		return c.sortPreservingMerge(partials)
	default:
		return c.simpleMerge(partials)
	}
}

// sortPreservingMerge does a k-way merge of sorted partial results.
func (c *DistributedQueryCoordinator) sortPreservingMerge(partials []PartialResult) []Point {
	// Collect all points with their sort position
	type indexedPoint struct {
		point     Point
		partialIdx int
		pointIdx  int
	}

	var total int
	for _, pr := range partials {
		if pr.Error == "" {
			total += len(pr.Points)
		}
	}

	all := make([]Point, 0, total)
	for _, pr := range partials {
		if pr.Error == "" {
			all = append(all, pr.Points...)
		}
	}

	// Sort by timestamp ascending
	sort.Slice(all, func(i, j int) bool {
		return all[i].Timestamp < all[j].Timestamp
	})

	return all
}

// simpleMerge concatenates all partial results.
func (c *DistributedQueryCoordinator) simpleMerge(partials []PartialResult) []Point {
	var all []Point
	for _, pr := range partials {
		if pr.Error == "" {
			all = append(all, pr.Points...)
		}
	}
	return all
}

// GetStats returns coordinator statistics.
func (c *DistributedQueryCoordinator) GetStats() DistributedQueryStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (c *DistributedQueryCoordinator) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/distributed/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Query Query    `json:"query"`
			Nodes []string `json:"nodes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		result, err := c.Execute(r.Context(), &req.Query, req.Nodes)
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/api/v1/distributed/plan", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Query Query    `json:"query"`
			Nodes []string `json:"nodes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		plan := c.Plan(&req.Query, req.Nodes)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(plan)
	})

	mux.HandleFunc("/api/v1/distributed/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(c.GetStats())
	})
}

// Ensure math import is used.
var _ = math.MaxFloat64
